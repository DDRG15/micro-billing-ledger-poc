"""
worker.py — Transactional Outbox drain worker
==============================================
EN: Standalone process that polls the outbox table for undispatched rows,
    forwards each event to a downstream system (or logs it when no
    DOWNSTREAM_URL is configured), then marks the rows dispatched=1.

    Run alongside the billing API:
        python worker.py

    The worker is also wired as a Docker service in docker-compose.yml so
    it starts automatically next to the billing container.

ES: Proceso independiente que sondea la tabla outbox por filas no despachadas,
    reenvía cada evento a un sistema downstream (o lo registra si no hay
    DOWNSTREAM_URL configurado), luego marca las filas como dispatched=1.

    Ejecutar junto a la API de facturación:
        python worker.py

    El worker también está configurado como servicio Docker en docker-compose.yml
    para que se inicie automáticamente junto al contenedor billing.

ENV VARS
--------
DATABASE_URL        (required) — same connection string as the billing service
DOWNSTREAM_URL      (optional) — if set, HTTP POST each event payload here
WORKER_BATCH_SIZE   (default: 100) — rows read per poll cycle
WORKER_POLL_INTERVAL (default: 5)  — seconds to sleep when queue is empty

DESIGN NOTES
------------
- SELECT ... FOR UPDATE SKIP LOCKED: two worker replicas never contest the same row.
- The SELECT and its matching UPDATE run inside the same BEGIN/COMMIT (_tx).
  If dispatch fails, the UPDATE is never issued → row is retried next poll.
- psycopg2.OperationalError triggers exponential backoff + reconnect.
- SIGTERM sets a stop flag; the worker finishes the current batch then exits 0.
"""

import logging
import os
import signal
import sys
import time
from contextlib import contextmanager
from typing import Generator, List, Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/billing",
)
DOWNSTREAM_URL: str = os.environ.get("DOWNSTREAM_URL", "")
WORKER_BATCH_SIZE: int = int(os.environ.get("WORKER_BATCH_SIZE", "100"))
WORKER_POLL_INTERVAL: float = float(os.environ.get("WORKER_POLL_INTERVAL", "5"))

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [worker] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
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
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


@contextmanager
def _tx(
    conn: psycopg2.extensions.connection,
) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Explicit BEGIN/COMMIT transaction (mirrors ledger.py's _tx helper)."""
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("BEGIN")
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

def _dispatch_row(row: psycopg2.extras.RealDictRow) -> None:
    """
    EN: Forward a single outbox row to the downstream system.
        If DOWNSTREAM_URL is set: HTTP POST the payload JSON.
        If not set:               structured log line (POC mode).
        Raises on failure so the caller skips the UPDATE (row retried next poll).
    ES: Reenvía una fila del outbox al sistema downstream.
        Si DOWNSTREAM_URL está configurado: HTTP POST del payload JSON.
        Si no:                              línea de log estructurado (modo POC).
        Lanza excepción en fallo para que la llamadora omita el UPDATE (reintento).
    """
    if DOWNSTREAM_URL:
        import urllib.request
        import urllib.error

        data = row["payload"].encode()
        req = urllib.request.Request(
            DOWNSTREAM_URL,
            data=data,
            headers={
                "Content-Type": "application/json",
                "X-Event-Type": row["event_type"],
                "X-Transaction-Id": row["transaction_id"],
            },
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
        except urllib.error.HTTPError as exc:
            status = exc.code
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

def drain_batch(conn: psycopg2.extensions.connection) -> int:
    """
    EN: Read up to WORKER_BATCH_SIZE undispatched rows, dispatch each one,
        then mark them dispatched=1 in the same transaction.
        Returns the number of rows successfully dispatched.
        Returns 0 when the queue is empty.
    ES: Lee hasta WORKER_BATCH_SIZE filas no despachadas, despacha cada una,
        luego las marca como dispatched=1 en la misma transacción.
        Retorna el número de filas despachadas exitosamente.
        Retorna 0 cuando la cola está vacía.
    """
    with _tx(conn) as cur:
        cur.execute(
            """
            SELECT id, transaction_id, event_type, payload
            FROM   outbox
            WHERE  dispatched = 0
            ORDER  BY id
            LIMIT  %(batch_size)s
            FOR UPDATE SKIP LOCKED
            """,
            {"batch_size": WORKER_BATCH_SIZE},
        )
        rows = cur.fetchall()

        if not rows:
            return 0

        dispatched_ids: List[int] = []
        oldest_id = rows[0]["id"]
        newest_id = rows[-1]["id"]
        t0 = time.monotonic()

        for row in rows:
            try:
                _dispatch_row(row)
                dispatched_ids.append(row["id"])
            except Exception as exc:
                log.warning(
                    "dispatch_failed id=%s transaction_id=%s error=%s",
                    row["id"],
                    row["transaction_id"],
                    exc,
                )

        if dispatched_ids:
            cur.execute(
                """
                UPDATE outbox
                SET    dispatched    = 1,
                       dispatched_at = NOW()
                WHERE  id = ANY(%(ids)s)
                """,
                {"ids": dispatched_ids},
            )
            lag_ms = int((time.monotonic() - t0) * 1000)
            log.info(
                "dispatched=%d lag_ms=%d oldest_id=%d newest_id=%d",
                len(dispatched_ids),
                lag_ms,
                oldest_id,
                newest_id,
            )

        return len(dispatched_ids)


# ---------------------------------------------------------------------------
# Main run loop
# ---------------------------------------------------------------------------

def run_loop(conn: psycopg2.extensions.connection) -> None:
    """
    EN: Infinite polling loop. Drains outbox until empty, then sleeps.
        Reconnects with exponential backoff on DB errors.
    ES: Bucle de sondeo infinito. Vacía el outbox hasta que esté vacío, luego duerme.
        Reconecta con backoff exponencial en errores de DB.
    """
    backoff: float = 5.0

    while not _stop:
        try:
            count = drain_batch(conn)
            backoff = 5.0  # reset on success
            if count == 0:
                log.debug("queue_empty — sleeping %.0fs", WORKER_POLL_INTERVAL)
                # Sleep in small increments so SIGTERM is handled quickly.
                deadline = time.monotonic() + WORKER_POLL_INTERVAL
                while time.monotonic() < deadline and not _stop:
                    time.sleep(0.5)
        except psycopg2.OperationalError as exc:
            log.error("db_error retrying in %.0fs: %s", backoff, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
            try:
                conn = _connect()
            except psycopg2.OperationalError:
                pass
        except Exception as exc:
            log.error("unexpected_error retrying in %.0fs: %s", backoff, exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    log.info(
        "starting batch_size=%d poll_interval=%.0fs downstream=%s",
        WORKER_BATCH_SIZE,
        WORKER_POLL_INTERVAL,
        DOWNSTREAM_URL or "(log-only mode)",
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

    run_loop(conn)
    log.info("stopped cleanly")


if __name__ == "__main__":
    main()
