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
from typing import Generator, Optional
from enum import Enum

from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, model_validator

# ---------------------------------------------------------------------------
# Enums and Pydantic Models (Phase 1)
# ---------------------------------------------------------------------------

class EventType(str, Enum):
    """Supported Stripe event types — the bouncer's rulebook."""
    INVOICE_PAID = "invoice.paid"
    INVOICE_FAILED = "invoice.payment_failed"
    SUB_CREATED = "customer.subscription.created"
    SUB_DELETED = "customer.subscription.deleted"
    SUB_UPDATED = "customer.subscription.updated"


class StripeObject(BaseModel):
    """The 'object' field inside Stripe data — contains billing details."""
    customer: str = Field(
        min_length=4,
        description="Stripe customer ID (e.g., 'cus_ABC123')"
    )
    amount_paid: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount paid in cents (non-negative)"
    )
    amount: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount in cents (fallback to amount_paid)"
    )
    currency: str = Field(
        default="usd",
        pattern=r"^[a-z]{3}$",
        description="ISO 4217 currency code (e.g., 'usd', 'eur')"
    )

    @model_validator(mode='after')
    def check_amount_present(self):
        """Validator: amount_paid OR amount must be provided."""
        actual_amount = self.amount_paid if self.amount_paid is not None else self.amount
        if actual_amount is None:
            raise ValueError(
                "Either amount_paid or amount must be provided and non-null"
            )
        if actual_amount < 0:
            raise ValueError(f"Amount cannot be negative: {actual_amount}")
        return self

    def get_amount(self) -> int:
        """Return the effective amount: prefer amount_paid, fallback to amount."""
        return self.amount_paid if self.amount_paid is not None else self.amount


class StripeEventData(BaseModel):
    """The 'data' field in Stripe webhook — wraps the object."""
    object: StripeObject = Field(description="Billing object with customer, amount, currency")


class StripeEvent(BaseModel):
    """Complete Stripe webhook event — validated at entry boundary."""
    id: str = Field(
        min_length=1,
        description="Unique Stripe event ID (e.g., 'evt_3Px...')"
    )
    type: EventType = Field(
        description="Event type (validated against supported types)"
    )
    data: StripeEventData = Field(
        description="Event data with customer, amount, currency"
    )
    request: Optional[dict] = Field(
        default=None,
        description="Request metadata, may contain idempotency_key"
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Idempotency key (resolved from request or defaults to id)"
    )

    @model_validator(mode='after')
    def resolve_idempotency_key(self):
        """Validator: resolve idempotency_key from request or fallback to id."""
        if self.idempotency_key is None:
            if self.request is None:
                self.idempotency_key = self.id
            else:
                self.idempotency_key = self.request.get("idempotency_key", self.id)
        return self


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

# SUPPORTED_EVENT_TYPES now defined via EventType enum (above)
SUPPORTED_EVENT_TYPES = {e.value for e in EventType}

# Maps EventType -> ledger status (type-safe via Enum)
_STATUS_MAP: dict[EventType, str] = {
    EventType.INVOICE_PAID:     "POSTED",
    EventType.INVOICE_FAILED:   "VOID",
    EventType.SUB_CREATED:      "POSTED",
    EventType.SUB_DELETED:      "VOID",
    EventType.SUB_UPDATED:      "PENDING",
}


def process_stripe_event(conn: sqlite3.Connection, event: dict) -> dict:
    """
    Idempotently insert a Stripe event into the ledger + outbox.
    
    THREE-LAYER VALIDATION PIPELINE (from PDF: "Pipeline Completo"):
    1. Pydantic validation: Type checking (BaseModel) + Field constraints
    2. Business logic: Duplicate detection + outbox write
    3. DLQ routing: Invalid/duplicate events logged with reason codes
    
    Returns a result dict with keys: outcome, transaction_id, reason.
    Outcomes: POSTED | PENDING | VOID | DLQ_INVALID | DLQ_DUPLICATE
    """
    now = time.time()
    
    # ── LAYER 1: Pydantic Validation (bouncer checks ID + dress code) ────────
    try:
        validated_event = StripeEvent(**event)
    except ValidationError as e:
        # Invalid structure/types → Dead Letter Queue
        transaction_id = event.get("id", "unknown")
        _write_dlq(conn, transaction_id, "INVALID", event, now, e)
        return {
            "outcome": "DLQ_INVALID",
            "transaction_id": transaction_id,
            "reason": f"Pydantic validation failed: {e.error_count()} errors"
        }
    
    # ── Extract validated fields (now guaranteed to be valid types/values) ────
    transaction_id  = validated_event.id
    event_type      = validated_event.type
    data_object     = validated_event.data.object
    idempotency_key = validated_event.idempotency_key
    
    # ── LAYER 2: Business Logic ────────────────────────────────────────────────
    # Extract billing data from validated object
    amount_cents = data_object.get_amount()  # Uses smart fallback logic
    customer_id  = data_object.customer
    currency     = data_object.currency
    ledger_status = _STATUS_MAP[event_type]  # Now type-safe: EventType key
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
                (transaction_id, event_type.value, customer_id, amount_cents,
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
                    (transaction_id, event_type.value, payload_json, now),
                )
    except sqlite3.OperationalError as exc:
        # Surface DB errors as 503 — caller (Stripe) will retry.
        raise RuntimeError(f"DB write failed: {exc}") from exc

    # ── LAYER 3: DLQ Routing ───────────────────────────────────────────────────
    if inserted == 0:
        # Duplicate: idempotency guard fired (good!)
        _write_dlq(conn, transaction_id, "DUPLICATE", event, now, None)
        return {
            "outcome": "DLQ_DUPLICATE",
            "transaction_id": transaction_id,
            "reason": "Already processed — idempotency guard fired"
        }

    # Success!
    return {
        "outcome": ledger_status,
        "transaction_id": transaction_id,
        "reason": None
    }


def _write_dlq(conn: sqlite3.Connection, transaction_id: str,
               reason: str, event: dict, now: float, validation_error: Optional[ValidationError] = None) -> None:
    """Best-effort DLQ append — never raises so the main path is unaffected.
    
    Args:
        transaction_id: Stripe event ID
        reason: DLQ reason code (DUPLICATE, INVALID, UNKNOWN_TYPE)
        event: Raw event payload
        now: Timestamp
        validation_error: Optional Pydantic ValidationError with error details
    """
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
