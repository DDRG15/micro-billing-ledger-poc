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
WORKER_DISPATCH_CONCURRENCY (default: 8) — max tenants dispatching at once (tasks in flight)

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
- SIGTERM sets a stop flag; the worker claims nothing more, lets each in-flight tenant task
  finish its current row (rows not yet sent are released), records them and exits 0.
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
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from contextlib import contextmanager
from typing import Dict, Generator, List, Optional, Sequence, Tuple

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
# Tenants dispatched in parallel inside one batch (rows of one tenant stay serial). Dispatch
#     used to be fully serial, so a tenant whose downstream trickled bytes cost every other
#     tenant up to one DISPATCH_DEADLINE_SECONDS per batch and capped global throughput at
#     one batch per deadline for as long as that tenant had fresh rows. Threads only do
#     HTTP; every database call stays on the drain_batch thread (see drain_batch).
WORKER_DISPATCH_CONCURRENCY: int = max(1, int(os.environ.get("WORKER_DISPATCH_CONCURRENCY", "8")))
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


# Printable ASCII except "%", which is escaped once a value is encoded at all.
_HEADER_SAFE = "".join(chr(c) for c in range(0x20, 0x7F) if chr(c) != "%")


def _header_value(value: str) -> str:
    """Make a header value safe for http.client; printable ASCII passes through unchanged.

    A transaction_id (or org / event type) holding a control character or a character
    outside latin-1 made http.client raise ValueError while building the request. That is
    not a failure the downstream caused, so the row backed off, was claimed again and
    failed the same way forever. Anything outside printable ASCII now makes the whole
    value percent-encoded as UTF-8 ('%' included, so the encoding is reversible): the row
    is delivered and the receiver recovers the original with urllib.parse.unquote.
    Ordinary ids are sent byte for byte as before.
    """
    if all(0x20 <= ord(ch) <= 0x7E for ch in value):
        return value
    return urllib.parse.quote(value, safe=_HEADER_SAFE)


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
                "X-Event-Type": _header_value(row["event_type"]),
                "X-Transaction-Id": _header_value(row["transaction_id"]),
                "X-Org-Id": _header_value(row.get("org_id", "default")),
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
        -- Tenants that still have a dispatch task in flight in THIS process (%(skip)s)
        --     are not claimed again: a tenant has one task at a time, so its rows stay
        --     serial and in id order across claims.
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
        WHERE  g.org_id <> ALL(%(skip)s::text[])
    ), chosen AS (
        -- At most %(max_orgs)s tenants, taken in the rotation order below. The
        --     continuous dispatcher claims only as many tenants as it has free slots, so
        --     a claimed row never waits in a queue behind a slow task while its lease
        --     runs down.
        SELECT org_id
        FROM   (SELECT DISTINCT org_id FROM candidates) d
        ORDER  BY (org_id COLLATE "C" <= %(after)s), org_id COLLATE "C"
        LIMIT  %(max_orgs)s
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
               FROM   candidates
               WHERE  org_id IN (SELECT org_id FROM chosen)) r
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


def _claim_batch(conn: psycopg2.extensions.connection, skip: Sequence[str] = (),
                 max_orgs: Optional[int] = None) -> List[psycopg2.extras.RealDictRow]:
    """Claim up to WORKER_BATCH_SIZE rows, fairly across tenants, in one short transaction.

    skip: tenants not to claim from (they have a dispatch task in flight).
    max_orgs: claim from at most this many tenants (default: no limit beyond the batch).

    The row locks taken by FOR UPDATE SKIP LOCKED last only until this transaction
    commits, milliseconds later; what keeps other workers off the rows during dispatch is
    claimed_until. ALL_ORGS: claiming is cross-tenant by definition.
    """
    global _rr_after
    with _tx(conn, _ledger.ALL_ORGS) as cur:
        cur.execute(_CLAIM_SQL, {"batch_size": WORKER_BATCH_SIZE,
                                 "lease": WORKER_LEASE_SECONDS, "after": _rr_after,
                                 "skip": list(skip),
                                 "max_orgs": WORKER_BATCH_SIZE if max_orgs is None else max_orgs})
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


# One row's result as a dispatch thread reports it: "sent", "failed" (with the error text)
#     or "released" (not tried: stopping, or its tenant already failed in this batch).
_Outcome = Tuple[psycopg2.extras.RealDictRow, str, Optional[str]]


def _dispatch_tenant(rows: List[psycopg2.extras.RealDictRow]) -> List[_Outcome]:
    """Dispatch one tenant's claimed rows, in id order, and report what happened to each.

    Runs on a pool thread, so it does HTTP and nothing else: a psycopg2 connection must
    not be used from several threads at once, and every outcome is recorded by
    drain_batch on its own thread. _dispatch_row is looked up in module globals at call
    time (not bound at submit time) so a replacement installed on this module applies.

    The first failure ends the tenant's turn: its remaining rows are reported "released"
    untried. A tenant with a dead or slow downstream would otherwise burn one deadline
    per row, holding its pool slot for the rest of the batch.
    """
    outcomes: List[_Outcome] = []
    failed = False
    for row in rows:
        # Stopping (SIGTERM) or this tenant already failed: hand the row back untried.
        #     Leaving it claimed stranded it for the whole lease — a worker restarted
        #     mid-batch kept up to a batch of paid events undelivered for ~17 minutes
        #     (reproduced against the compose stack).
        if _stop or failed:
            outcomes.append((row, "released", None))
            continue
        try:
            _dispatch_row(row, row.get("downstream_url"))
        except Exception as exc:
            outcomes.append((row, "failed", str(exc) or type(exc).__name__))
            failed = True
            continue
        outcomes.append((row, "sent", None))
    return outcomes


def _record_outcomes(conn: psycopg2.extensions.connection, outcomes: List[_Outcome]) -> int:
    """Commit outcomes on the connection-owning thread; returns how many were delivered.

    Consumes the list as it goes, so a caller that retries after a database error resumes
    at the first outcome that was not committed instead of recording any twice.
    """
    sent = 0
    while outcomes:
        row, kind, error = outcomes[0]
        if kind == "released":
            _release_row(conn, row)
        elif kind == "failed":
            log.warning(
                "dispatch_failed id=%s transaction_id=%s attempts=%d error=%s",
                row["id"], row["transaction_id"], row["attempts"] + 1, error,
            )
            _finish_row(conn, row, error)
        else:
            _finish_row(conn, row, None)
            sent += 1
        outcomes.pop(0)
    return sent


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

    Tenants are dispatched concurrently (WORKER_DISPATCH_CONCURRENCY threads), one task
    per tenant, its rows serial in id order. Fully serial dispatch let one tenant whose
    downstream trickled bytes cost every other tenant up to a deadline per batch: with a
    slow tenant that always had fresh rows, global throughput was one batch per deadline.
    A tenant whose dispatch fails still gives up the rest of its rows in this batch
    (released untried, no attempt counted), so it costs its own slot at most one deadline.

    The threads only do HTTP. This thread owns the connection and records each tenant's
    outcomes as that tenant's task completes (as_completed): a tenant that finishes
    early is committed while the slow ones are still in flight. The lease is unchanged —
    a batch can still take batch x DISPATCH_DEADLINE_SECONDS in the worst case (all rows
    one tenant, every one trickling), and the lease default is sized for exactly that.
    """
    rows = _claim_batch(conn)
    if not rows:
        return 0

    by_org: Dict[str, List[psycopg2.extras.RealDictRow]] = {}
    for row in rows:                       # rows are sorted by id: each list keeps id order
        by_org.setdefault(row["org_id"], []).append(row)

    def record(outcomes: List[_Outcome]) -> int:
        return _record_outcomes(conn, outcomes)

    dispatched = 0
    t0 = time.monotonic()
    workers = min(WORKER_DISPATCH_CONCURRENCY, len(by_org))
    if workers <= 1:
        # One tenant, or concurrency set to 1: nothing to overlap, so dispatch on this
        #     thread, exactly as the serial worker did. No pool, no thread hand-off.
        for org_rows in by_org.values():
            dispatched += record(_dispatch_tenant(org_rows))
    else:
        pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dispatch")
        try:
            futures = [pool.submit(_dispatch_tenant, org_rows) for org_rows in by_org.values()]
            for future in as_completed(futures):
                dispatched += record(future.result())
        finally:
            # Normally every future is done. On a database error mid-recording, do not
            #     wait for tenants still in flight (up to a deadline per row): the caller
            #     reconnects, and unrecorded rows are redelivered when their lease expires.
            pool.shutdown(wait=False, cancel_futures=True)

    if dispatched:
        log.info(
            "dispatched=%d lag_ms=%d oldest_id=%d newest_id=%d",
            dispatched, int((time.monotonic() - t0) * 1000), rows[0]["id"], rows[-1]["id"],
        )
    return dispatched


class _TenantTask:
    """One tenant's claimed rows, dispatching on a pool thread."""

    def __init__(self, org_id: str, rows: List[psycopg2.extras.RealDictRow], future: "Future") -> None:
        self.org_id = org_id
        self.rows = rows
        self.future = future
        self.started = time.monotonic()
        self.outcomes: Optional[List[_Outcome]] = None   # taken from the future once done


class _Dispatcher:
    """Continuous dispatch for run_loop: tasks outlive a poll.

    drain_batch is one synchronous round and waits for every tenant task it started. In
    the polling loop that meant a tenant trickling bytes held the NEXT claim back for its
    whole 15 s deadline: rows another tenant received meanwhile waited, and since the
    rotation gives the slow tenant a slot in every claim, each batch lasted one deadline
    (reproduced against the compose stack: 10 fast-tenant rows delivered after 15.2 s).
    Here each poll, on the thread that owns the connection:
      1. records the tasks that have finished (never blocks on one that has not),
      2. claims rows for tenants with NO task in flight, at most as many tenants as free
         slots (WORKER_DISPATCH_CONCURRENCY minus tasks in flight),
      3. submits one task per newly claimed tenant.
    A tenant therefore never has two tasks at once (its rows stay serial and in id
    order), and a slow tenant occupies one slot instead of the whole worker. Pool threads
    do HTTP only; every database call happens in poll() / finish().

    Lease: claims are limited to free slots, so a claimed task starts at once and runs at
    most (its rows) x DISPATCH_DEADLINE_SECONDS, within the lease default.
    """

    def __init__(self) -> None:
        self._slots = WORKER_DISPATCH_CONCURRENCY
        self._pool = ThreadPoolExecutor(max_workers=self._slots, thread_name_prefix="dispatch")
        self._tasks: Dict[str, _TenantTask] = {}

    @property
    def in_flight(self) -> int:
        return len(self._tasks)

    def _collect(self, conn: psycopg2.extensions.connection, block: bool = False) -> int:
        """Record every finished task. Returns the rows delivered plus rows released/failed."""
        handled = 0
        for org_id, task in list(self._tasks.items()):
            if task.outcomes is None:
                if not task.future.done():
                    continue
                try:
                    task.outcomes = task.future.result()
                except Exception as exc:
                    # _dispatch_tenant catches per-row errors, so this is a bug in the
                    #     task itself. Dropping the task frees the tenant; its rows are
                    #     redelivered once their lease expires (at-least-once).
                    log.error("dispatch_task_crashed org_id=%s error=%s", org_id, exc)
                    del self._tasks[org_id]
                    continue
            n = len(task.outcomes)
            sent = _record_outcomes(conn, task.outcomes)   # raises with the rest kept in place
            del self._tasks[org_id]
            handled += n
            if sent:
                log.info(
                    "dispatched=%d lag_ms=%d oldest_id=%d newest_id=%d",
                    sent, int((time.monotonic() - task.started) * 1000),
                    task.rows[0]["id"], task.rows[-1]["id"],
                )
        return handled

    def _claim(self, conn: psycopg2.extensions.connection) -> int:
        free = self._slots - len(self._tasks)
        if _stop or free <= 0:
            return 0
        rows = _claim_batch(conn, skip=list(self._tasks), max_orgs=free)
        by_org: Dict[str, List[psycopg2.extras.RealDictRow]] = {}
        for row in rows:                   # rows are sorted by id: each list keeps id order
            by_org.setdefault(row["org_id"], []).append(row)
        for org_id, org_rows in by_org.items():
            self._tasks[org_id] = _TenantTask(
                org_id, org_rows, self._pool.submit(_dispatch_tenant, org_rows))
        return len(rows)

    def poll(self, conn: psycopg2.extensions.connection) -> bool:
        """One round on the connection-owning thread. True when it recorded or claimed."""
        handled = self._collect(conn)
        claimed = self._claim(conn)
        return bool(handled or claimed)

    def wait(self, seconds: float) -> None:
        """Idle until a task finishes, `seconds` pass or the worker is told to stop."""
        deadline = time.monotonic() + seconds
        while not _stop:
            left = deadline - time.monotonic()
            if left <= 0:
                return
            futures = [t.future for t in self._tasks.values() if t.outcomes is None]
            if futures:
                done, _ = wait(futures, timeout=min(0.5, left), return_when=FIRST_COMPLETED)
                if done:
                    return
            else:
                time.sleep(min(0.5, left))

    def finish(self, conn: psycopg2.extensions.connection, timeout: float) -> None:
        """Shutdown: claim nothing more, let in-flight tasks end, record what they report.

        Each task stops at its current row's deadline and reports the rest "released"
        (it reads _stop between rows), so this waits about one dispatch deadline, inside
        the container's stop_grace_period. A task still running after `timeout` is left:
        its rows are redelivered once the lease expires.
        """
        try:
            wait([t.future for t in self._tasks.values() if t.outcomes is None], timeout=timeout)
            self._collect(conn)
        finally:
            self._pool.shutdown(wait=False, cancel_futures=True)


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

def run_loop(conn: psycopg2.extensions.connection) -> None:
    """
    Infinite polling loop. Each poll records finished dispatch tasks and claims rows for
    tenants not already in flight (see _Dispatcher), so a slow tenant never holds back the
    others; sleeps only when a poll found nothing to do. Prunes old DUPLICATE DLQ rows at
    most once per hour. Reconnects with exponential backoff on DB errors; tasks in flight
    keep running and are recorded on the new connection.
    """
    backoff: float = 5.0
    last_prune: float = 0.0          # monotonic ts of last DLQ prune (0 → prune on first cycle)
    prune_interval: float = 3600.0   # prune DUPLICATE DLQ rows at most once per hour

    dispatcher = _Dispatcher()
    try:
        while not _stop:
            try:
                progressed = dispatcher.poll(conn)
                retried = _ledger.retry_dlq_batch(conn)
                now = time.monotonic()
                if now - last_prune >= prune_interval:
                    deleted = _ledger.prune_dlq(conn)
                    last_prune = now
                    if deleted:
                        log.info("dlq_pruned deleted=%d reason=DUPLICATE older_than_days=90", deleted)
                backoff = 5.0  # reset on success
                if not progressed and retried == 0:
                    # Nothing recorded, claimed or retried: wait for the poll interval, or
                    #     for a task to finish (so its outcome is recorded promptly), in
                    #     small slices so SIGTERM is handled quickly. With work just claimed
                    #     or recorded the loop polls again at once.
                    log.debug("queue_empty — waiting up to %.0fs (%d tenants in flight)",
                              WORKER_POLL_INTERVAL, dispatcher.in_flight)
                    dispatcher.wait(WORKER_POLL_INTERVAL)
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
    finally:
        # SIGTERM (or any exit): in-flight tenant tasks stop after their current row,
        #     report the rest as released, and are recorded here on the connection. The
        #     wait is bounded by one dispatch deadline, inside stop_grace_period.
        try:
            dispatcher.finish(conn, DISPATCH_DEADLINE_SECONDS + 5)
        except Exception as exc:
            log.error("shutdown_record_failed: %s", exc)


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
