"""
ledger.py — Micro-Billing-Ledger PoC
=====================================
Demonstrates idempotent Stripe webhook ingestion using the Transactional
Outbox pattern from the Aequitas high-throughput skeleton.

Key design decisions
--------------------
Idempotency guard
  Every Stripe event carries a unique `id` (e.g. "evt_3PxK...").  We use
  this as the ledger primary key.  INSERT OR IGNORE means a replayed webhook
  is a no-op at the DB level — no application-level lock needed.  Duplicate
  events are recorded to the Dead-Letter Queue (DLQ) for audit visibility
  rather than silently dropped.

Transactional Outbox
  The ledger row and the outbox row are written in the same BEGIN…COMMIT
  transaction.  A downstream worker (not shown) tails the outbox and forwards
  confirmed events onward.  If the process dies between the HTTP 200 and the
  downstream call, the outbox row survives and the event is retried — no
  dual-write race.

WAL mode
  SQLite in WAL mode allows concurrent readers while the writer commits.
  At PoC scale this is sufficient; the pattern ports directly to Postgres
  (advisory locks + RETURNING) for production.

Usage
-----
  uvicorn ledger:app --port 8000
  python ledger.py --silent          # headless benchmark mode
"""

import argparse
import json
import sqlite3
import time
import pathlib
from contextlib import contextmanager
from typing import Generator

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_PATH = pathlib.Path("billing_ledger.db")

# ---------------------------------------------------------------------------
# Database bootstrap
# ---------------------------------------------------------------------------

def _bootstrap(path: pathlib.Path = DB_PATH) -> sqlite3.Connection:
    """Open (or create) the ledger DB and return a WAL-mode connection."""
    conn = sqlite3.connect(str(path), check_same_thread=False, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS ledger (
            transaction_id  TEXT    PRIMARY KEY,   -- Stripe event id
            event_type      TEXT    NOT NULL,       -- e.g. "invoice.paid"
            customer_id     TEXT    NOT NULL,
            amount_cents    INTEGER NOT NULL,
            currency        TEXT    NOT NULL DEFAULT 'usd',
            status          TEXT    NOT NULL,       -- POSTED | PENDING | VOID
            idempotency_key TEXT    NOT NULL,
            payload         TEXT    NOT NULL,       -- full JSON for outbox replay
            created_at      REAL    NOT NULL        -- unix timestamp
        );

        CREATE TABLE IF NOT EXISTS outbox (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id  TEXT    NOT NULL,
            event_type      TEXT    NOT NULL,
            payload         TEXT    NOT NULL,
            dispatched      INTEGER NOT NULL DEFAULT 0,  -- 0=pending, 1=sent
            created_at      REAL    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dlq (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            transaction_id  TEXT    NOT NULL,
            reason          TEXT    NOT NULL,       -- DUPLICATE | INVALID | UNKNOWN_TYPE
            raw_payload     TEXT    NOT NULL,
            received_at     REAL    NOT NULL
        );
    """)
    conn.commit()
    return conn


@contextmanager
def _tx(conn: sqlite3.Connection) -> Generator[sqlite3.Cursor, None, None]:
    """Minimal context manager for an explicit BEGIN…COMMIT/ROLLBACK."""
    cur = conn.cursor()
    conn.execute("BEGIN")
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise


# ---------------------------------------------------------------------------
# Core ledger logic  (framework-agnostic — easy to unit-test)
# ---------------------------------------------------------------------------

SUPPORTED_EVENT_TYPES = {
    "invoice.paid",
    "invoice.payment_failed",
    "customer.subscription.created",
    "customer.subscription.deleted",
    "customer.subscription.updated",
}

# Maps event_type -> ledger status
_STATUS_MAP = {
    "invoice.paid":                      "POSTED",
    "invoice.payment_failed":            "VOID",
    "customer.subscription.created":     "POSTED",
    "customer.subscription.deleted":     "VOID",
    "customer.subscription.updated":     "PENDING",
}


def process_stripe_event(conn: sqlite3.Connection, event: dict) -> dict:
    """
    Idempotently insert a Stripe event into the ledger + outbox.

    Returns a result dict with keys: outcome, transaction_id, reason.
    Outcomes: POSTED | DLQ_DUPLICATE | DLQ_INVALID | DLQ_UNKNOWN_TYPE
    """
    transaction_id  = event.get("id", "")
    event_type      = event.get("type", "")
    data_object     = event.get("data", {}).get("object", {})
    idempotency_key = event.get("request", {}).get("idempotency_key") or transaction_id
    now             = time.time()

    # ── Validation ────────────────────────────────────────────────────────────
    if not transaction_id or not event_type:
        _write_dlq(conn, transaction_id, "INVALID", event, now)
        return {"outcome": "DLQ_INVALID", "transaction_id": transaction_id,
                "reason": "Missing id or type"}

    if event_type not in SUPPORTED_EVENT_TYPES:
        _write_dlq(conn, transaction_id, "UNKNOWN_TYPE", event, now)
        return {"outcome": "DLQ_UNKNOWN_TYPE", "transaction_id": transaction_id,
                "reason": f"Unsupported event type: {event_type}"}

    # ── Extract billing fields (safe defaults for subscription events) ────────
    amount_cents = data_object.get("amount_paid") or data_object.get("amount", 0)
    customer_id  = data_object.get("customer", "unknown")
    currency     = data_object.get("currency", "usd")
    ledger_status = _STATUS_MAP[event_type]
    payload_json  = json.dumps(event)

    # ── Transactional Outbox: ledger + outbox in one atomic commit ────────────
    try:
        with _tx(conn) as cur:
            cur.execute(
                """
                INSERT OR IGNORE INTO ledger
                  (transaction_id, event_type, customer_id, amount_cents,
                   currency, status, idempotency_key, payload, created_at)
                VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (transaction_id, event_type, customer_id, amount_cents,
                 currency, ledger_status, idempotency_key, payload_json, now),
            )
            inserted = cur.rowcount   # 1 = new row, 0 = duplicate ignored

            if inserted == 1:
                # Write outbox row in the same transaction — zero dual-write gap.
                cur.execute(
                    """
                    INSERT INTO outbox (transaction_id, event_type, payload, created_at)
                    VALUES (?,?,?,?)
                    """,
                    (transaction_id, event_type, payload_json, now),
                )
    except sqlite3.OperationalError as exc:
        # Surface DB errors as 503 — caller (Stripe) will retry.
        raise RuntimeError(f"DB write failed: {exc}") from exc

    # ── Route duplicates to DLQ (visible audit trail, not silent drop) ────────
    if inserted == 0:
        _write_dlq(conn, transaction_id, "DUPLICATE", event, now)
        return {"outcome": "DLQ_DUPLICATE", "transaction_id": transaction_id,
                "reason": "Already processed — idempotency guard fired"}

    return {"outcome": ledger_status, "transaction_id": transaction_id,
            "reason": None}


def _write_dlq(conn: sqlite3.Connection, transaction_id: str,
               reason: str, event: dict, now: float) -> None:
    """Best-effort DLQ append — never raises so the main path is unaffected."""
    try:
        with _tx(conn) as cur:
            cur.execute(
                "INSERT INTO dlq (transaction_id, reason, raw_payload, received_at)"
                " VALUES (?,?,?,?)",
                (transaction_id, reason, json.dumps(event), now),
            )
    except Exception:
        pass   # DLQ write failure must never kill the hot path


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

app   = FastAPI(title="Micro-Billing-Ledger PoC", version="1.0.0")
_conn = _bootstrap()


class StripeWebhookPayload(BaseModel):
    """Minimal Stripe event envelope — mirrors real Stripe webhook shape."""
    id:      str = Field(..., description="Stripe event id, e.g. evt_3Px...")
    type:    str = Field(..., description="Event type, e.g. invoice.paid")
    data:    dict = Field(default_factory=dict)
    request: dict = Field(default_factory=dict)


@app.post("/webhook/stripe", status_code=status.HTTP_200_OK)
async def stripe_webhook(payload: StripeWebhookPayload) -> JSONResponse:
    """
    Receive a Stripe webhook and commit it to the billing ledger.

    Stripe expects HTTP 200 within 30 s; any non-2xx triggers a retry.
    Idempotent: replayed events return 200 with outcome=DLQ_DUPLICATE so
    Stripe's retry logic does not escalate to a failure state.
    """
    try:
        result = process_stripe_event(_conn, payload.model_dump())
    except RuntimeError as exc:
        # 503 tells Stripe to retry — appropriate for transient DB errors.
        raise HTTPException(status_code=503, detail=str(exc))

    return JSONResponse(content=result)


@app.get("/ledger/summary")
async def ledger_summary() -> JSONResponse:
    """Quick sanity-check endpoint: counts per status + DLQ depth."""
    cur = _conn.execute(
        "SELECT status, COUNT(*) FROM ledger GROUP BY status"
    )
    counts   = {row[0]: row[1] for row in cur.fetchall()}
    dlq_depth = _conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
    outbox_pending = _conn.execute(
        "SELECT COUNT(*) FROM outbox WHERE dispatched=0"
    ).fetchone()[0]
    return JSONResponse(content={
        "ledger":         counts,
        "dlq_depth":      dlq_depth,
        "outbox_pending": outbox_pending,
    })


@app.get("/health")
async def health() -> JSONResponse:
    return JSONResponse(content={"status": "ok"})


# ---------------------------------------------------------------------------
# Headless benchmark mode (--silent)
# ---------------------------------------------------------------------------

def _run_headless_benchmark(n: int = 10_000) -> None:
    """
    Drive process_stripe_event() directly — no HTTP stack, no console I/O.
    Measures pure ledger throughput: idempotent inserts + outbox writes.
    """
    import random, string
    conn = _bootstrap(pathlib.Path(":memory:"))   # in-memory for benchmark

    def _fake_event(i: int) -> dict:
        return {
            "id":   f"evt_{''.join(random.choices(string.ascii_lowercase, k=16))}",
            "type": random.choice(list(SUPPORTED_EVENT_TYPES)),
            "data": {"object": {
                "customer":    f"cus_{i:06d}",
                "amount_paid": random.randint(100, 100_000),
                "currency":    "usd",
            }},
            "request": {},
        }

    events = [_fake_event(i) for i in range(n)]
    t0 = time.perf_counter()
    for ev in events:
        process_stripe_event(conn, ev)
    elapsed = time.perf_counter() - t0
    tps = n / elapsed
    print(f"Headless benchmark: {n:,} events in {elapsed:.3f}s  →  {tps:,.0f} TPS")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silent", action="store_true",
                        help="Run headless benchmark instead of HTTP server")
    parser.add_argument("--events", type=int, default=10_000)
    args = parser.parse_args()

    if args.silent:
        _run_headless_benchmark(args.events)
    else:
        import uvicorn
        uvicorn.run("ledger:app", host="0.0.0.0", port=8000, reload=False)
