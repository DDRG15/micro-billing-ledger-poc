"""
worker.py — Transactional Outbox drain worker
==============================================
Standalone process that polls the outbox table for undispatched rows,
forwards each event to a downstream system (or logs it when no
DOWNSTREAM_URL is configured), then marks the rows dispatched=1.

Run alongside the billing API:
    python worker.py

The worker is also wired as a Docker service in docker-compose.yml so
it starts automatically next to the billing container.

ENV VARS
--------
APP_DATABASE_URL    (required in a deployment) — the restricted role (billing_app);
                    falls back to DATABASE_URL, which leaves RLS inert
DATABASE_URL        the owner connection; migrations only
DOWNSTREAM_URL      (optional) — if set, HTTP POST each event payload here
WORKER_BATCH_SIZE   (default: 100) — rows read per poll cycle
WORKER_POLL_INTERVAL (default: 5)  — seconds to sleep when queue is empty

DESIGN NOTES
------------
- Claim, then dispatch, then finish (migration 0008). A short transaction claims a batch
  (lease: claimed_until); the HTTP calls run with no transaction open; each row is then
  committed on its own — dispatched, or backed off (attempts, next_attempt_at,
  last_error). No row lock is held across a network call.
- Delivery is at-least-once: a worker that dies after dispatching but before committing
  leaves its claim to expire, and the row is delivered again. Receivers deduplicate on
  transaction_id.
- psycopg2.OperationalError triggers exponential backoff + reconnect.
- SIGTERM sets a stop flag; the worker finishes the current batch then exits 0.
"""

import logging
import os
import signal
import sys
import http.client
import socket
import threading
import time
import urllib.parse
from contextlib import contextmanager
from typing import Generator, List, Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras

import ledger as _ledger  # retry_dlq_batch lives here

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/billing",
)
# The worker connects as the restricted role for the same reason the API does — see the
#     APP_DATABASE_URL block in ledger.py. Resolved through ledger so both processes
#     cannot disagree about which DSN "the application" means; a worker still connected
#     as the owner would bypass RLS on exactly the paths that touch every tenant.
APP_DATABASE_URL: str = _ledger.APP_DATABASE_URL
DOWNSTREAM_URL: str = os.environ.get("DOWNSTREAM_URL", "")
WORKER_BATCH_SIZE: int = int(os.environ.get("WORKER_BATCH_SIZE", "100"))
WORKER_POLL_INTERVAL: float = float(os.environ.get("WORKER_POLL_INTERVAL", "5"))
# WORKER_LOG_ONLY=true enables log-only mode (no HTTP forwarding).
#     Must be set explicitly — the worker refuses to start without a DOWNSTREAM_URL
#     unless this flag is "true", preventing silent data loss in production.
WORKER_LOG_ONLY: bool = os.environ.get("WORKER_LOG_ONLY", "false").lower() == "true"
# A dispatch's socket timeout bounds each connect/read; the deadline bounds the whole
#     request, however slowly the downstream trickles bytes. See _post.
DISPATCH_SOCKET_TIMEOUT: float = float(os.environ.get("DISPATCH_SOCKET_TIMEOUT", "10"))
DISPATCH_DEADLINE_SECONDS: float = float(os.environ.get("DISPATCH_DEADLINE_SECONDS", "15"))
# How long a claim keeps other workers off a row. It must outlast a whole batch of
#     dispatches (batch x the dispatch deadline) or a slow but live worker's rows get
#     claimed twice; it is also how long a crashed worker's rows wait to be reclaimed.
WORKER_LEASE_SECONDS: int = int(os.environ.get(
    "WORKER_LEASE_SECONDS", str(int(WORKER_BATCH_SIZE * DISPATCH_DEADLINE_SECONDS) + 60)))
# Backoff after a failed dispatch, by attempts so far; the last value repeats. Never
#     gives up: the event is in the ledger, and silently abandoning its delivery is the
#     one outcome ruled out. A row stuck here shows in outbox_pending and last_error.
_OUTBOX_BACKOFF_SECONDS = (30, 120, 600, 3600)

# ---------------------------------------------------------------------------
# Logging
# Reuses the formatter from ledger.py so the API and the worker emit the same
#     shape. Two services logging the same event in two different formats means a
#     log backend needs two sets of parsing rules and cannot correlate them on
#     transaction_id, which is the one field you want when tracing an event from
#     ingest to dispatch. LOG_FORMAT=json switches both at once.
# ---------------------------------------------------------------------------
_worker_handler = logging.StreamHandler(stream=sys.stdout)
if _ledger.LOG_FORMAT == "json":
    _worker_handler.setFormatter(_ledger.JsonLogFormatter())
else:
    _worker_handler.setFormatter(
        logging.Formatter("%(asctime)s [worker] %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")
    )
logging.basicConfig(level=logging.INFO, handlers=[_worker_handler])
log = logging.getLogger("worker")

# ---------------------------------------------------------------------------
# Graceful shutdown
# ---------------------------------------------------------------------------
_stop: bool = False


def _handle_sigterm(signum: int, frame: object) -> None:
    global _stop
    log.info("SIGTERM received — finishing current batch then exiting")
    _stop = True


signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _connect() -> psycopg2.extensions.connection:
    """Open a single persistent connection (autocommit=True, like ledger.py)."""
    conn = psycopg2.connect(APP_DATABASE_URL)
    conn.autocommit = True
    return conn


@contextmanager
def _tx(
    conn: psycopg2.extensions.connection,
    org_scope: str,
) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Explicit BEGIN/COMMIT transaction (mirrors ledger.py's _tx helper).

    org_scope is required here for the same reason it is there: the worker holds ONE
    long-lived connection, so a scope established per session rather than per transaction
    would make cross-tenant access the ambient default for the life of the process — on
    the paths that replay other tenants' payloads into the ledger. SET LOCAL confines it
    to the statement group that actually needs it.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("BEGIN")
    cur.execute("SET LOCAL app.org_id = %s", (org_scope,))
    try:
        yield cur
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        cur.close()

# ---------------------------------------------------------------------------
# Dispatch
# ---------------------------------------------------------------------------

def _post(url: str, data: bytes, headers: dict) -> int:
    """POST once and return the status. Never follows a redirect; never runs past
    DISPATCH_DEADLINE_SECONDS in total.

    Redirects: urllib followed a 301/302/303 on a POST by re-issuing it as a GET with no
    body, and the redirect target's 200 read as success — reproduced against the compose
    stack, a paid event marked dispatched with no error while the receiver got an empty
    GET. http.client follows nothing: a 3xx is a non-2xx status, the dispatch fails and
    the row backs off with the status recorded. It also keeps a downstream from steering
    the worker to another host.

    Deadline: the socket timeout bounds each connect and each read, not the request. A
    downstream that trickles one byte every few seconds never trips it, and dispatch is
    serial, so one tenant's endpoint held the whole worker — every other tenant's
    deliveries, the DLQ retry and the prune — for as long as it kept trickling
    (reproduced: 90 s and counting with five rows of another tenant waiting). A timer
    shuts the socket down at the deadline, which unblocks the read in this thread.
    """
    parts = urllib.parse.urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise RuntimeError(f"unsupported downstream URL scheme {parts.scheme!r}")
    conn_cls = http.client.HTTPSConnection if parts.scheme == "https" else http.client.HTTPConnection
    conn = conn_cls(parts.hostname, parts.port, timeout=DISPATCH_SOCKET_TIMEOUT)
    expired = threading.Event()

    def _expire() -> None:
        expired.set()
        sock = conn.sock
        if sock is not None:
            try:
                sock.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass

    timer = threading.Timer(DISPATCH_DEADLINE_SECONDS, _expire)
    timer.daemon = True
    timer.start()
    try:
        path = (parts.path or "/") + (f"?{parts.query}" if parts.query else "")
        conn.request("POST", path, body=data, headers=headers)
        resp = conn.getresponse()
        resp.read(64 * 1024)  # drain a little so the status is final; the body is not used
        if expired.is_set():
            # Cut mid-response: http.client reads EOF after a complete status line as the
            #     end of the headers, so a trickle cut at the deadline would read as "200".
            raise RuntimeError(f"downstream exceeded the {DISPATCH_DEADLINE_SECONDS:g} s dispatch deadline")
        return resp.status
    except (OSError, http.client.HTTPException) as exc:
        if expired.is_set():
            raise RuntimeError(f"downstream exceeded the {DISPATCH_DEADLINE_SECONDS:g} s dispatch deadline")
        raise RuntimeError(f"downstream unreachable: {type(exc).__name__}: {exc}")
    finally:
        timer.cancel()
        conn.close()


def _dispatch_row(row: psycopg2.extras.RealDictRow, destination: Optional[str] = None) -> None:
    """
    Forward a single outbox row to its tenant's downstream system.
    destination is that tenant's org.downstream_url, falling back to the process-wide
    DOWNSTREAM_URL when the tenant has not set one.
    If neither is set: structured log line (POC mode).
    Raises on failure so the caller records it and the row backs off.

    The destination is resolved per tenant because delivering one tenant's billing
    events — customer ids, amounts, raw payloads — to another tenant's endpoint would
    be the largest leak in the system. Isolating the database and then forwarding
    everything to one shared URL is not isolation.
    """
    target = destination or DOWNSTREAM_URL
    if target:
        try:
            status = _post(target, row["payload"].encode(), {
                "Content-Type": "application/json",
                "X-Event-Type": row["event_type"],
                "X-Transaction-Id": row["transaction_id"],
                "X-Org-Id": row.get("org_id", "default"),
            })
        except RuntimeError as exc:
            raise RuntimeError(f"{exc} transaction_id={row['transaction_id']}") from None
        if not (200 <= status < 300):
            raise RuntimeError(
                f"downstream HTTP {status} for transaction_id={row['transaction_id']}"
            )
    else:
        log.info(
            "dispatch id=%s transaction_id=%s event_type=%s",
            row["id"],
            row["transaction_id"],
            row["event_type"],
        )

# ---------------------------------------------------------------------------
# Core drain loop
# ---------------------------------------------------------------------------

_CLAIM_SQL = """
    WITH candidates AS (
        -- Per tenant FIRST, then the batch limit. The previous query capped a global
        -- window of the oldest batch x 10 rows and ranked tenants inside it: a tenant
        -- with more than one window of rows its downstream kept refusing filled the
        -- window on every poll, and no other tenant's rows were ever selected. Each
        -- tenant now contributes its own oldest eligible rows (the org registry is small;
        -- each lookup is one index range scan on idx_outbox_pending_by_org), and rows
        -- that are backing off or leased are not eligible at all.
        SELECT c.id, c.org_id
        FROM   org g
        CROSS  JOIN LATERAL (
            SELECT o.id, o.org_id
            FROM   outbox o
            WHERE  o.org_id = g.org_id
              AND  o.dispatched = 0
              AND  (o.next_attempt_at IS NULL OR o.next_attempt_at <= NOW())
              AND  (o.claimed_until  IS NULL OR o.claimed_until  <  NOW())
            ORDER  BY o.id
            LIMIT  %(batch_size)s
            FOR UPDATE SKIP LOCKED
        ) c
    ), picked AS (
        -- Round-robin across tenants: rank within each tenant, take rank 1 of every
        -- tenant before rank 2 of any. Within a rank, tenants go in org_id order
        -- starting AFTER the last tenant this worker served at rank 1 (%(after)s),
        -- wrapping around. Ordering a rank by row id instead gave the same oldest
        -- backlogs every rank-1 slot on every claim: with more backlogged tenants than
        -- batch slots, a tenant waited until every older backlog had drained.
        SELECT id, org_id, rn
        FROM  (SELECT id, org_id,
                      ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY id) AS rn
               FROM   candidates) r
        -- COLLATE "C": byte order, the same order Python compares the cursor in.
        ORDER  BY rn, (org_id COLLATE "C" <= %(after)s), org_id COLLATE "C", id
        LIMIT  %(batch_size)s
    )
    UPDATE outbox o
    SET    claimed_until = NOW() + make_interval(secs => %(lease)s)
    FROM   picked p
    JOIN   org g ON g.org_id = p.org_id
    WHERE  o.id = p.id
    RETURNING o.id, o.transaction_id, o.event_type, o.payload, o.org_id, o.attempts,
              o.claimed_until, g.downstream_url, p.rn
"""

# The last tenant this process served at rank 1 — where the next claim's rotation
#     starts. Per process: each replica rotates on its own, which keeps every one fair.
_rr_after: str = ""


def _claim_batch(conn: psycopg2.extensions.connection) -> List[psycopg2.extras.RealDictRow]:
    """Claim up to WORKER_BATCH_SIZE rows, fairly across tenants, in one short transaction.

    The row locks taken by FOR UPDATE SKIP LOCKED last only until this transaction
    commits, milliseconds later; what keeps other workers off the rows during dispatch is
    claimed_until. ALL_ORGS: claiming is cross-tenant by definition.
    """
    global _rr_after
    with _tx(conn, _ledger.ALL_ORGS) as cur:
        cur.execute(_CLAIM_SQL, {"batch_size": WORKER_BATCH_SIZE,
                                 "lease": WORKER_LEASE_SECONDS, "after": _rr_after})
        rows = cur.fetchall()
    served = [r["org_id"] for r in rows if r["rn"] == 1]
    if served:
        # The last rank-1 tenant in this claim's rotation order (same key as the SQL).
        _rr_after = max(served, key=lambda o, a=_rr_after: (o <= a, o))
    return sorted(rows, key=lambda r: r["id"])


def _finish_row(conn: psycopg2.extensions.connection, row: psycopg2.extras.RealDictRow,
                error: Optional[str]) -> bool:
    """Commit one row's outcome, in its own transaction scoped to that row's tenant.

    A worker can outlive its lease (a slow downstream, a long GC pause); another worker
    then claims the row and both are dispatching it. The claimed_until value this worker
    wrote is its fencing token:

      success  recorded whenever the row is still undelivered, lease or not. The event
               reached the downstream; saying otherwise only causes another duplicate.
      failure  recorded only while this worker still holds its own lease. A stale
               failure used to clear the live worker's lease (letting a third worker in)
               and stamp attempts / last_error on a row another worker had delivered.

    Returns False when the row was left as it was.
    """
    with _tx(conn, row["org_id"]) as cur:
        if error is None:
            cur.execute(
                "UPDATE outbox SET dispatched = 1, dispatched_at = NOW(), "
                "claimed_until = NULL, last_error = NULL WHERE id = %s AND dispatched = 0",
                (row["id"],),
            )
        else:
            delay = _OUTBOX_BACKOFF_SECONDS[min(row["attempts"], len(_OUTBOX_BACKOFF_SECONDS) - 1)]
            cur.execute(
                "UPDATE outbox SET attempts = attempts + 1, "
                "next_attempt_at = NOW() + make_interval(secs => %s), "
                "claimed_until = NULL, last_error = %s "
                "WHERE id = %s AND dispatched = 0 AND claimed_until = %s",
                (delay, error[:500], row["id"], row["claimed_until"]),
            )
        applied = cur.rowcount == 1
    if not applied:
        log.warning("lease_lost id=%s transaction_id=%s outcome=%s — left to the current holder",
                    row["id"], row["transaction_id"], "delivered" if error is None else "failed")
    return applied


def _release_row(conn: psycopg2.extensions.connection, row: psycopg2.extras.RealDictRow) -> None:
    """Give a claimed row back untried: clear this worker's lease, count no attempt."""
    with _tx(conn, row["org_id"]) as cur:
        cur.execute(
            "UPDATE outbox SET claimed_until = NULL "
            "WHERE id = %s AND dispatched = 0 AND claimed_until = %s",
            (row["id"], row["claimed_until"]),
        )


def drain_batch(conn: psycopg2.extensions.connection) -> int:
    """
    Claim a batch, dispatch it with no transaction open, commit each row's outcome.
    Returns the number of rows dispatched.

    This used to be one transaction: SELECT ... FOR UPDATE, an HTTP call per row (10 s
    timeout each), then one UPDATE for the successes. With 100 rows that held locks for
    up to ~17 minutes, and a failure left the row exactly as it was, to be retried on the
    very next poll. Now:
      1. _claim_batch — short transaction; sets claimed_until (the lease).
      2. dispatch — no transaction, no locks, while the network is slow.
      3. _finish_row — one small transaction per row: dispatched, or backed off by
         _OUTBOX_BACKOFF_SECONDS with the error recorded. A row backing off is not
         eligible, so a tenant whose downstream is down stops occupying the batch.

    Delivery is at-least-once: if this process dies between 2 and 3, the lease expires
    and the row is dispatched again. Receivers deduplicate on transaction_id.

    A tenant whose dispatch fails gives up the rest of its rows in this batch: they are
    released untried (lease cleared, no attempt counted) and the batch moves on to the
    other tenants. Dispatch is serial, so without this one tenant with a dead or slow
    downstream cost every other tenant up to batch x DISPATCH_DEADLINE_SECONDS per batch;
    now it costs at most one deadline.
    """
    rows = _claim_batch(conn)
    if not rows:
        return 0

    dispatched = 0
    failed_orgs: set = set()
    t0 = time.monotonic()
    for row in rows:
        if row["org_id"] in failed_orgs:
            _release_row(conn, row)
            continue
        try:
            _dispatch_row(row, row.get("downstream_url"))
        except Exception as exc:
            log.warning(
                "dispatch_failed id=%s transaction_id=%s attempts=%d error=%s",
                row["id"], row["transaction_id"], row["attempts"] + 1, exc,
            )
            _finish_row(conn, row, str(exc) or type(exc).__name__)
            failed_orgs.add(row["org_id"])
            continue
        _finish_row(conn, row, None)
        dispatched += 1

    if dispatched:
        log.info(
            "dispatched=%d lag_ms=%d oldest_id=%d newest_id=%d",
            dispatched, int((time.monotonic() - t0) * 1000), rows[0]["id"], rows[-1]["id"],
        )
    return dispatched


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

def run_loop(conn: psycopg2.extensions.connection) -> None:
    """
    Infinite polling loop. Drains outbox until empty, then sleeps. Prunes old
    DUPLICATE DLQ rows at most once per hour. Reconnects with exponential backoff on DB errors.
    """
    backoff: float = 5.0
    last_prune: float = 0.0          # monotonic ts of last DLQ prune (0 → prune on first cycle)
    prune_interval: float = 3600.0   # prune DUPLICATE DLQ rows at most once per hour

    while not _stop:
        try:
            count = drain_batch(conn)
            retried = _ledger.retry_dlq_batch(conn)
            now = time.monotonic()
            if now - last_prune >= prune_interval:
                deleted = _ledger.prune_dlq(conn)
                last_prune = now
                if deleted:
                    log.info("dlq_pruned deleted=%d reason=DUPLICATE older_than_days=90", deleted)
            backoff = 5.0  # reset on success
            if count == 0 and retried == 0:
                log.debug("queue_empty — sleeping %.0fs", WORKER_POLL_INTERVAL)
                # Sleep in small increments so SIGTERM is handled quickly.
                deadline = time.monotonic() + WORKER_POLL_INTERVAL
                while time.monotonic() < deadline and not _stop:
                    time.sleep(0.5)
        except psycopg2.OperationalError as exc:
            log.error("db_error retrying in %.0fs: %s", backoff, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
            # B1: keep trying until we actually get a new connection.
            # The old `except: pass` left conn pointing at the broken connection,
            # causing an infinite tight loop of immediate failures.
            new_conn = None
            while new_conn is None and not _stop:
                try:
                    new_conn = _connect()
                    log.info("reconnected to database")
                except psycopg2.OperationalError as reconnect_exc:
                    log.error("reconnect_failed retrying in %.0fs: %s", backoff, reconnect_exc)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 60.0)
            if new_conn is not None:
                conn = new_conn
        except Exception as exc:
            log.error("unexpected_error retrying in %.0fs: %s", backoff, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    # Guard against silent production misconfiguration.
    #     Without a real DOWNSTREAM_URL the worker would mark rows dispatched=1
    #     while forwarding nothing — revenue data silently never reaches downstream.
    #     The operator must either set DOWNSTREAM_URL or explicitly opt in to
    #     log-only mode with WORKER_LOG_ONLY=true.
    if not DOWNSTREAM_URL and not WORKER_LOG_ONLY:
        log.error(
            "startup_error: DOWNSTREAM_URL is not set and WORKER_LOG_ONLY is not 'true'. "
            "Set DOWNSTREAM_URL to a real endpoint, or set WORKER_LOG_ONLY=true to "
            "run in log-only mode (events logged but not forwarded — dev/POC only)."
        )
        sys.exit(1)

    mode = DOWNSTREAM_URL if DOWNSTREAM_URL else "LOG_ONLY (no HTTP forwarding — WORKER_LOG_ONLY=true)"
    if WORKER_LOG_ONLY and not DOWNSTREAM_URL:
        log.warning(
            "running in LOG_ONLY mode — events will be logged and marked dispatched=1 "
            "but NOT forwarded to any downstream system. Set DOWNSTREAM_URL in production."
        )
    log.info(
        "starting batch_size=%d poll_interval=%.0fs downstream=%s",
        WORKER_BATCH_SIZE,
        WORKER_POLL_INTERVAL,
        mode,
    )
    conn: Optional[psycopg2.extensions.connection] = None
    backoff: float = 5.0
    while conn is None:
        try:
            conn = _connect()
            log.info("connected to database")
        except psycopg2.OperationalError as exc:
            log.error("db_connect_failed retrying in %.0fs: %s", backoff, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)

    assert conn is not None  # B6: narrow Optional → connection for mypy
    run_loop(conn)
    log.info("stopped cleanly")


if __name__ == "__main__":
    main()
