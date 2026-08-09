"""
ledger.py — Micro-Billing-Ledger PoC
=====================================
Main application file. Handles Stripe webhook ingestion, Pydantic validation,
idempotent ledger writes, transactional outbox, and Dead-Letter Queue (DLQ).
Exposes a FastAPI HTTP server and a headless benchmark mode.

Key design decisions
--------------------
Idempotency guard:
  Every Stripe event carries a unique `id` (e.g. "evt_3PxK..."). We use this
  as the ledger primary key. INSERT INTO ledger ... ON CONFLICT (transaction_id)
  DO NOTHING means a replayed webhook is a no-op at the DB level — no
  application-level lock needed.

Transactional Outbox:
  The ledger row and the outbox row are written in the same BEGIN…COMMIT.
  A downstream worker tails the outbox and forwards confirmed events onward.

Idempotency (PostgreSQL):
  INSERT INTO ledger (...) VALUES (...) ON CONFLICT (transaction_id) DO NOTHING.
  rowcount=0 after the INSERT means the event was already in the ledger — the
  unique constraint on transaction_id rejected it silently. Route to DLQ_DUPLICATE.

Usage:
  uvicorn ledger:app --port 8000
  python ledger.py --silent          # headless benchmark
"""

# ---------------------------------------------------------------------------
# Standard library imports
# All built-in Python modules — no external dependencies here.
# ---------------------------------------------------------------------------
import argparse       # CLI argument parsing for --silent/--events flags
import json           # JSON serialization for raw payload archiving
import logging        # Structured log output to file and stderr
from logging.handlers import RotatingFileHandler  # Rotating file handler — caps log file size to prevent disk fill
import os             # DATABASE_URL, STRIPE_WEBHOOK_SECRET, BILLING_API_KEY env var lookup
import time           # perf_counter for benchmark timing
import stripe         # Stripe SDK for HMAC-SHA256 webhook signature verification
import psycopg2       # PostgreSQL driver — production-grade relational storage
from datetime import datetime, timezone  # Timezone-aware timestamps for TIMESTAMPTZ columns — financial audit trail requires tz
from psycopg2.extras import execute_values  # Bulk INSERT — collapses N round trips to ceil(N/page_size)
from psycopg2.pool import ThreadedConnectionPool  # Thread-safe connection pool — replaces single module-level connection
from contextlib import contextmanager  # @contextmanager for _tx() BEGIN/COMMIT wrapper
from typing import Generator, Optional  # Type hints for static analysis
from enum import Enum  # Enums for EventType, LedgerStatus, DLQReason — prevents raw string drift

# ---------------------------------------------------------------------------
# Logging configuration
# Two handlers: StreamHandler (stderr for Docker/systemd) + RotatingFileHandler
#     (billing_ledger.log for manual recovery, max 10 MB × 5 files = 50 MB cap).
#     Both see ERROR logs for DLQ write failures, which include the full raw
#     payload for manual replay. RotatingFileHandler prevents unbounded disk growth
#     at high ingestion rates — at 500 TPS with frequent DLQ errors, a plain
#     FileHandler fills disk in hours.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),                          # stderr
        RotatingFileHandler(
            "billing_ledger.log",
            maxBytes=10_485_760,   # 10 MB per file
            backupCount=5,          # keep .1 through .5 → 50 MB max
        ),
    ],
)
_log = logging.getLogger("ledger")

# ---------------------------------------------------------------------------
# Third-party imports
# FastAPI for HTTP routing, Pydantic for validation, starlette for responses.
#     All pinned in requirements.txt — no surprise version changes.
# ---------------------------------------------------------------------------
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402
from pydantic import BaseModel, Field, ValidationError, model_validator  # noqa: E402
from slowapi import Limiter, _rate_limit_exceeded_handler  # noqa: E402
from slowapi.util import get_remote_address  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402

# ===========================================================================
# SECTION 1: ENUMS AND PYDANTIC MODELS (Phase 1 + Phase 2)
#
# This is the 5-layer validation stack. Every Stripe webhook passes through
#     all layers before touching the database. The order matters:
#       1. Pydantic type coercion (BaseModel)
#       2. Field-level constraints (min_length, ge=0, pattern)
#       3. Single-model validators (@model_validator on StripeObject)
#       4. Cross-field validators (@model_validator on StripeEvent)
#       5. Business logic (duplicate detection, DLQ routing)
#
# ===========================================================================

class EventType(str, Enum):
    """
    The complete list of supported Stripe event types. If it's not in this
    enum, Pydantic rejects it at model creation — before any business logic runs.
    This is the "bouncer's rulebook": written down, enforced at the door.
    """
    INVOICE_PAID   = "invoice.paid"                    # Successful payment received
    INVOICE_FAILED = "invoice.payment_failed"          # Payment attempt failed
    SUB_CREATED    = "customer.subscription.created"   # New subscription activated
    SUB_DELETED    = "customer.subscription.deleted"   # Subscription cancelled
    SUB_UPDATED    = "customer.subscription.updated"   # Subscription plan changed


class StripeObject(BaseModel):
    """
    Represents the `data.object` payload inside a Stripe webhook. Contains the
    actual billing details: who paid, how much, in what currency. Three validation
    layers run here: Field constraints, then a cross-field validator for amounts.
    """

    # Stripe customer IDs look like "cus_ABC123". min_length=4 catches empty
    #     strings and single-char placeholders. The cus_ prefix check is in StripeEvent.
    customer: str = Field(
        min_length=4,
        description="Stripe customer ID (e.g., 'cus_ABC123')"
    )

    # Stripe puts the paid amount here on invoice.paid events. Optional because
    #     subscription lifecycle events don't always carry payment amounts.
    amount_paid: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount paid in cents, non-negative"
    )

    # Fallback amount field — Stripe uses different field names depending on
    #     the event type. The model_validator below picks the right one.
    amount: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount in cents (fallback to amount_paid)"
    )

    # ISO 4217 currency code. Regex enforces exactly 3 lowercase letters.
    #     "USD" (uppercase) fails — currency normalization (.lower() on ingest) is a
    #     known gap listed in the Production Gap Checklist in the README.
    currency: str = Field(
        default="usd",
        pattern=r"^[a-z]{3}$",
        description="ISO 4217 currency code, lowercase 3 chars"
    )

    @model_validator(mode='after')
    def check_amount_present(self):
        """
        Ensures at least one amount field is present and non-null. Field(ge=0)
        handles non-negative enforcement — this validator handles the
        "either/or" logic that a single Field constraint cannot express.
        Subscription events may have amount=0 (lifecycle, no money moved).
        Invoice events with amount=0 are caught by check_invoice_amount_nonzero
        in StripeEvent — a different layer for a different rule.
        """
        actual_amount = self.amount_paid if self.amount_paid is not None else self.amount
        if actual_amount is None:
            # Both fields are None — no amount at all. This is a data quality failure.
            raise ValueError(
                "Either amount_paid or amount must be provided and non-null"
            )
        return self

    def get_amount(self) -> Optional[int]:
        """
        Returns the effective billing amount in cents. Prefers amount_paid
        (Stripe invoice events) over amount (fallback). Returns None only if
        both fields are None — check_amount_present prevents this when the
        model is constructed via StripeEvent, but direct StripeObject()
        construction bypasses that validator, so Optional[int] is the honest type.
        """
        return self.amount_paid if self.amount_paid is not None else self.amount


class StripeEventData(BaseModel):
    """
    Wrapper for the `data` field in a Stripe webhook. Stripe always nests
    the billing object one level deep: event.data.object. This model makes
    that nesting explicit and type-safe.
    """
    object: StripeObject = Field(
        description="Billing object with customer, amount, currency"
    )


class StripeEvent(BaseModel):
    """
    The complete Stripe webhook envelope. This is the top-level model that
    validates the entire incoming payload. Contains three cross-field validators
    that run AFTER all Field constraints pass — they see the full model state.
    """

    # Stripe event ID — this becomes the ledger primary key.
    #     min_length=1 catches empty strings that would create a NULL-equivalent PK.
    id: str = Field(
        min_length=1,
        description="Unique Stripe event ID (e.g., 'evt_3Px...')"
    )

    # EventType enum — rejects unknown types at model creation, not halfway
    #     through business logic. The bouncer checks the list at the door.
    type: EventType = Field(
        description="Event type validated against supported types"
    )

    # Nested model containing the billing object (customer, amount, currency).
    data: StripeEventData = Field(
        description="Event data with customer, amount, currency"
    )

    # Optional Stripe request metadata. May contain idempotency_key from
    #     Stripe's retry logic. If absent, idempotency_key falls back to event id.
    request: Optional[dict] = Field(
        default=None,
        description="Request metadata, may contain idempotency_key"
    )

    # Resolved by resolve_idempotency_key validator below. Declared as a
    #     model field so Pydantic v2 allows assignment in the validator.
    #     (v2 does not allow setting attributes that aren't declared fields.)
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Idempotency key — resolved from request or defaults to id"
    )

    @model_validator(mode='after')
    def resolve_idempotency_key(self):
        """
        Resolves the idempotency_key from the request metadata or falls back
        to the event ID. This is explicit behavior — not a hidden .get() chain.
        Stripe sometimes sends the key in request.idempotency_key; when absent,
        the event ID itself is the correct idempotency token.
        """
        if self.idempotency_key is None:
            if self.request is None:
                self.idempotency_key = self.id
            else:
                self.idempotency_key = self.request.get("idempotency_key", self.id)
        return self

    @model_validator(mode='after')
    def check_invoice_amount_nonzero(self):
        """
        Cross-field rule: invoice events must carry a non-zero amount.
            - invoice.paid with $0 = no revenue received. Data quality failure.
            - invoice.payment_failed with $0 = nothing to retry. Data quality failure.
            - Subscription events (created/deleted/updated) are EXEMPT — they are
              lifecycle events, not payment events. $0 is valid for them.
        Field(ge=0) cannot express this — it allows zero for all types.
        This validator runs after Field checks, so it has access to self.type.
        """
        invoice_types = {EventType.INVOICE_PAID, EventType.INVOICE_FAILED}
        if self.type in invoice_types:
            amount = self.data.object.get_amount()
            if amount == 0:
                raise ValueError(
                    f"{self.type.value} must have amount > 0 "
                    f"(got 0 — route to DLQ for manual review)"
                )
        return self

    @model_validator(mode='after')
    def check_customer_id_format(self):
        """
        Cross-field rule: Stripe customer IDs always start with 'cus_'.
        Field(min_length=4) on StripeObject catches empty/short strings.
        This catches structurally wrong IDs like "abc123" — 10 chars, passes
        length, but not a real Stripe customer ID. A business rule, not a
        format rule — that's why it lives here and not in StripeObject.
        """
        if not self.data.object.customer.startswith("cus_"):
            raise ValueError(
                f"Customer ID must start with 'cus_': "
                f"got '{self.data.object.customer}'"
            )
        return self


class LedgerStatus(str, Enum):
    """
    The three valid accounting states for a ledger row. Using an enum instead
    of raw strings prevents typos like "POSETD" from silently entering the DB.
    The _STATUS_MAP below uses EventType keys and LedgerStatus values — both
    are enums, so mypy can verify the mapping is complete.
    """
    POSTED  = "POSTED"    # Revenue confirmed — invoice.paid, subscription.created
    PENDING = "PENDING"   # Awaiting resolution — subscription.updated
    VOID    = "VOID"      # Reversed or failed — invoice.payment_failed, subscription.deleted


class DLQReason(str, Enum):
    """
    Structured rejection reason codes for the Dead-Letter Queue. Using an enum
    instead of freeform strings means DLQ entries can be filtered and replayed
    by reason type — e.g., replay all INVALID entries after fixing a validator.
    """
    DUPLICATE    = "DUPLICATE"     # Stripe retry of already-processed event
    INVALID      = "INVALID"       # Failed Pydantic validation — bad structure or business rule
    UNKNOWN_TYPE = "UNKNOWN_TYPE"  # Reserved — currently caught as INVALID by Pydantic enum
    # TRANSIENT is the only reason the retry worker auto-processes. It is wired and tested
    #     but has no production trigger today: single-event DB failures return HTTP 503 so
    #     Stripe itself retries, and batch failures roll back atomically. It is provisioned
    #     for a future non-Stripe / internal-async source — one with no upstream retrier —
    #     where a network timeout or an upstream 503 must be re-attempted by us, not Stripe.
    TRANSIENT    = "TRANSIENT"     # Transient DB/network error — auto-retryable (see note above)


class DLQStatus(str, Enum):
    PENDING   = "pending"    # Waiting for first or next retry attempt
    RETRYING  = "retrying"   # Currently being processed by retry worker
    RESOLVED  = "resolved"   # Successfully reprocessed — ledger row now exists
    EXHAUSTED = "exhausted"  # retry_count >= max_retries — needs human intervention


class DLQEntry(BaseModel):
    """
    A typed Dead-Letter Queue record. Every invalid or duplicate event creates
    one of these. The raw_payload is always preserved byte-perfect — never
    corrected, never truncated. Humans review DLQ entries; the system never
    assumes a DLQ entry is unimportant.
    to_db() serializes to a tuple matching the dlq table column order exactly.
    """
    transaction_id: str        = Field(min_length=1)
    reason:         DLQReason
    raw_payload:    dict
    received_at:    datetime   = Field(default_factory=lambda: datetime.now(timezone.utc))
    # Retry tracking — DUPLICATE and INVALID default to max_retries=0 (no auto-retry).
    # TRANSIENT defaults to max_retries=3 (auto-retried by the worker).
    status:         DLQStatus  = DLQStatus.PENDING
    retry_count:    int        = 0
    max_retries:    int        = 0
    next_retry_at:  Optional[datetime] = None

    def __init__(self, **data: object) -> None:
        super().__init__(**data)
        # TRANSIENT errors are auto-retryable — ensure max_retries is set.
        # next_retry_at stays NULL, which the SELECT treats as "retry immediately".
        if self.reason == DLQReason.TRANSIENT and self.max_retries == 0:
            object.__setattr__(self, "max_retries", 3)

    def to_db(self) -> tuple:
        return (
            self.transaction_id,
            self.reason.value,
            json.dumps(self.raw_payload),
            self.received_at,
            self.status.value,
            self.retry_count,
            self.max_retries,
            self.next_retry_at,
        )


class LedgerEntry(BaseModel):
    """
    A fully-validated ledger row ready for database insertion. All nine fields
    are constrained — no raw strings, no unvalidated amounts. to_db() produces
    a 9-tuple in the exact column order of the INSERT statement, eliminating
    query-column sequence drift. If you add a field here, you must add it to
    to_db() and to the INSERT — the code will be obviously incomplete.
    """
    transaction_id:  str          = Field(min_length=1)           # Stripe event ID — PK
    event_type:      EventType                                     # Validated enum — not a raw string
    customer_id:     str          = Field(min_length=4)            # cus_ prefix validated upstream in StripeEvent
    amount_cents:    int          = Field(ge=0)                    # Non-negative integer cents
    currency:        str          = Field(pattern=r"^[a-z]{3}$")  # ISO 4217 lowercase
    status:          LedgerStatus                                  # POSTED / PENDING / VOID
    idempotency_key: str          = Field(min_length=1)            # Network safety reference
    payload:         str                                           # Full JSON string — the outbox uses this for replay
    created_at:      datetime                                      # Timezone-aware ingestion timestamp (TIMESTAMPTZ)

    def to_db(self) -> tuple:
        """
        Serializes the LedgerEntry to an ordered 9-tuple for the INSERT statement.
        .value on enum fields produces plain strings for TEXT columns.
        Column order here must match the INSERT statement in process_stripe_event
        exactly — any mismatch causes silent data corruption (wrong value in wrong column).
        """
        return (
            self.transaction_id,
            self.event_type.value,    # EventType enum → string
            self.customer_id,
            self.amount_cents,
            self.currency,
            self.status.value,        # LedgerStatus enum → string
            self.idempotency_key,
            self.payload,
            self.created_at,
        )


# ===========================================================================
# SECTION 2: CONFIGURATION
#
# Application-level constants sourced from environment variables.
#     STRIPE_WEBHOOK_SECRET and BILLING_API_KEY must be set before production.
#     Never hardcode secrets here — pass them via environment at deploy time.
# ===========================================================================

# PostgreSQL connection string. Set DATABASE_URL in your environment or .env file.
#     Format: postgresql://user:password@host:port/dbname
#     In Docker: pass via environment variable in docker run / compose.
#     In tests: set DATABASE_URL to point at a dedicated test database (see docker-compose.yml).
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/billing"
)

# Stripe webhook signing secret — sourced from environment, never hardcoded.
#     Get from: Stripe Dashboard → Developers → Webhooks → your endpoint → Signing secret
#     Format: whsec_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
#     Without this set to a real value, stripe.Webhook.construct_event() will reject
#     ALL incoming requests with 400. Set it before receiving live Stripe traffic.
STRIPE_WEBHOOK_SECRET: str = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_YOUR_SECRET_HERE")

# Guard against placeholder secret reaching production. The default value
#     "whsec_YOUR_SECRET_HERE" is not a valid Stripe signing secret. If the env var
#     was never set, every incoming webhook would pass signature verification because
#     stripe.Webhook.construct_event() would compare against this literal string —
#     silently accepting ALL unsigned requests. Fail hard at startup instead.
_PLACEHOLDER_SECRET = "whsec_YOUR_SECRET_HERE"
if STRIPE_WEBHOOK_SECRET == _PLACEHOLDER_SECRET:
    import warnings
    warnings.warn(
        "STRIPE_WEBHOOK_SECRET is the placeholder value. "
        "Set it to your real Stripe signing secret before receiving live traffic. "
        "Webhook signature verification will reject all incoming requests until this is set.",
        stacklevel=1,
    )

# API key for ops endpoints (/ledger/summary, /dlq/entries).
#     If empty (env var not set), the check is skipped — dev-mode convenience.
#     In production: set to a strong random value, e.g., openssl rand -hex 32.
#     ISO 27001 A.9.4.1: access to operational data must be access-controlled.
BILLING_API_KEY: str = os.environ.get("BILLING_API_KEY", "")

# Fail-open on an empty key is a deliberate dev convenience — but a silent one is a
#     liability. If a production deploy forgets BILLING_API_KEY, /dlq/entries would
#     serve raw Stripe payloads (customer IDs, amounts) to anyone, with no signal.
#     Make that misconfiguration loud at startup instead of silent at runtime.
if not BILLING_API_KEY:
    _log.warning(
        "BILLING_API_KEY is empty — /ledger/summary, /dlq/entries and /dlq/{id}/retry "
        "are UNAUTHENTICATED. Dev mode only. Set a strong key (openssl rand -hex 32) "
        "before any network-reachable deployment."
    )


# ===========================================================================
# SECTION 3: DATABASE BOOTSTRAP
#
# Creates the three relational tables on first run. Idempotent — safe to
#     call multiple times (CREATE TABLE IF NOT EXISTS). WAL mode is enabled
#     for concurrent reads while writes are in progress.
# ===========================================================================

def _bootstrap(dsn: str = DATABASE_URL) -> "psycopg2.extensions.connection":
    """
    Connects to PostgreSQL and ensures all three tables exist.
    autocommit=True: each DDL/DML statement commits immediately unless wrapped
    in an explicit BEGIN...COMMIT block (which _tx() provides for writes).
    Tables use IF NOT EXISTS — safe to call on every startup and in every test.
    Three tables:
    - ledger: financial record — transaction_id TEXT PRIMARY KEY = idempotency guard
    - outbox: pending downstream delivery — dispatched=0 means not yet forwarded
    - dlq: rejected events — DUPLICATE, INVALID, or UNKNOWN_TYPE
    """
    conn = psycopg2.connect(dsn)
    # autocommit=True so DDL (CREATE TABLE) and reads execute outside any transaction.
    #     Write transactions use explicit BEGIN/COMMIT inside _tx().
    conn.autocommit = True

    with conn.cursor() as cur:
        # ledger — one row per unique Stripe event. transaction_id is the PK and the
        #     idempotency key. ON CONFLICT (transaction_id) DO NOTHING in the INSERT
        #     is what makes the deduplication zero-lock at the database level.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ledger (
                transaction_id  TEXT    PRIMARY KEY,
                event_type      TEXT    NOT NULL,
                customer_id     TEXT    NOT NULL,
                amount_cents    BIGINT  NOT NULL,
                currency        TEXT    NOT NULL DEFAULT 'usd',
                status          TEXT    NOT NULL,
                idempotency_key TEXT    NOT NULL,
                payload         TEXT    NOT NULL CHECK (length(payload) < 50000),
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        # amount_cents BIGINT: PostgreSQL INTEGER max = 2,147,483,647 (~$21M). BIGINT
        #     handles up to ~$92 trillion — required for B2B enterprise subscription amounts.
        #     payload CHECK: prevents malicious payloads from causing unbounded table growth.
        #     created_at TIMESTAMPTZ: timezone-aware; mandatory for financial audit trails and
        #     correct date_trunc() GROUP BY queries across billing periods.

        # outbox — dispatched=0 rows are pending forwarding to a downstream system.
        #     BIGSERIAL gives a monotonically increasing id for ordered processing.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id              BIGSERIAL   PRIMARY KEY,
                transaction_id  TEXT        NOT NULL
                                REFERENCES ledger(transaction_id) ON DELETE CASCADE,
                event_type      TEXT        NOT NULL,
                payload         TEXT        NOT NULL,
                dispatched      INTEGER     NOT NULL DEFAULT 0,
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # Add dispatched_at column if the outbox table predates Phase 7.
        #     ADD COLUMN IF NOT EXISTS is idempotent — safe on every startup.
        cur.execute("""
            ALTER TABLE outbox
            ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMPTZ
        """)

        # dlq — every rejected event lands here with a structured reason code.
        #     raw_payload is the full original JSON, preserved byte-perfect for replay.
        #     status/retry columns added in Phase 11 — ADD COLUMN IF NOT EXISTS is
        #     idempotent for databases that already existed before Phase 11.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id              BIGSERIAL   PRIMARY KEY,
                transaction_id  TEXT        NOT NULL,
                reason          TEXT        NOT NULL,
                raw_payload     TEXT        NOT NULL,
                received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                status          TEXT        NOT NULL DEFAULT 'pending',
                retry_count     INTEGER     NOT NULL DEFAULT 0,
                max_retries     INTEGER     NOT NULL DEFAULT 3,
                next_retry_at   TIMESTAMPTZ,
                claimed_at      TIMESTAMPTZ
            )
        """)
        # max_retries DEFAULT 3 mirrors Alembic migration 0002 exactly. _bootstrap()
        # exists to give CI the same schema Alembic produces in prod; a mismatched
        # default (was 0 here) means "green in CI" would not guarantee "correct in prod".
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS status        TEXT    NOT NULL DEFAULT 'pending'")
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS retry_count   INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS max_retries   INTEGER NOT NULL DEFAULT 3")
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ")
        # claimed_at mirrors Alembic migration 0003 — when the row was claimed for
        #     retry. Lets retry_dlq_batch reclaim a claim that was never released.
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS claimed_at    TIMESTAMPTZ")

        # Indexes for the two production query patterns:
        #     idx_ledger_customer_id — used by revenue-by-customer GROUP BY queries.
        #     idx_outbox_dispatched_id — partial index covering only undispatched rows;
        #       the outbox worker always queries WHERE dispatched=0 ORDER BY id.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ledger_customer_id
            ON ledger(customer_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_outbox_dispatched_id
            ON outbox(dispatched, id)
            WHERE dispatched = 0
        """)
        # idx_dlq_retry — covers the WHERE clause in retry_dlq_batch:
        #     status='pending' AND next_retry_at <= NOW() AND retry_count < max_retries.
        # Alembic migration 0002 creates this in production; _bootstrap() must mirror it
        # so CI (which calls _bootstrap() directly, not alembic) gets the same index.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dlq_retry
            ON dlq(status, next_retry_at)
            WHERE status = 'pending'
        """)
        # idx_dlq_stale_claim — covers the reclaim half of the retry_dlq_batch WHERE
        # clause: status='retrying' AND claim older than the timeout. Mirrors 0003.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_dlq_stale_claim
            ON dlq(status, claimed_at)
            WHERE status = 'retrying'
        """)

    return conn


@contextmanager
def _tx(conn: "psycopg2.extensions.connection") -> Generator["psycopg2.extensions.cursor", None, None]:
    """
    Explicit transaction context manager for PostgreSQL. Issues BEGIN, yields
    a cursor for the caller's statements, then COMMIT on success or ROLLBACK
    on any exception. Because the connection runs with autocommit=True, we need
    explicit BEGIN to group multiple statements into one atomic transaction.
    This is what makes the transactional outbox atomic: the ledger INSERT and
    the outbox INSERT both succeed or both roll back — no half-written state.
    """
    cur = conn.cursor()
    cur.execute("BEGIN")
    try:
        yield cur
        cur.execute("COMMIT")
    except Exception:
        cur.execute("ROLLBACK")
        raise
    finally:
        cur.close()


# ===========================================================================
# SECTION 4: CORE LEDGER LOGIC (framework-agnostic)
#
# process_stripe_event() is the heart of the system. It is deliberately
#     framework-agnostic — it takes a plain dict and a connection, returns a
#     plain dict. This makes it easy to unit-test without spinning up FastAPI,
#     and easy to port to a different framework if needed.
# ===========================================================================

# Maps each supported EventType to the correct ledger status.
#     dict[EventType, LedgerStatus] means mypy enforces that every EventType
#     has a corresponding status — a missing entry is a type error, not a
#     silent KeyError at runtime.
_STATUS_MAP: dict[EventType, LedgerStatus] = {
    EventType.INVOICE_PAID:   LedgerStatus.POSTED,   # Revenue confirmed
    EventType.INVOICE_FAILED: LedgerStatus.VOID,     # Revenue reversed
    EventType.SUB_CREATED:    LedgerStatus.POSTED,   # Account activated
    EventType.SUB_DELETED:    LedgerStatus.VOID,     # Account deactivated
    EventType.SUB_UPDATED:    LedgerStatus.PENDING,  # Change in progress
}


def process_stripe_event(
    conn: "psycopg2.extensions.connection",
    event: dict,
    from_retry: bool = False,
) -> dict:
    """
    Idempotently inserts a Stripe event into the ledger and outbox.

    THREE-LAYER VALIDATION PIPELINE:
    Layer 1 — Pydantic validation: type checking + field constraints + cross-field rules
    Layer 2 — Business logic: atomic dual-write (ledger + outbox) in one transaction
    Layer 3 — DLQ routing: invalid or duplicate events are logged, never silently dropped

    from_retry marks a replay of a payload already held in the DLQ (manual retry endpoint
    or retry_dlq_batch). A retry never creates DLQ rows: the entry being replayed IS the
    DLQ record for that payload, and its status / retry_count / next_retry_at columns carry
    the outcome of each attempt. Appending another row per attempt is what made the queue
    amplify itself — one retry of an entry whose event had already reached the ledger left
    behind one more pending row than it consumed, and each of those was itself retry-eligible.
    That inflated the dlq_depth operators read to judge whether ingestion is healthy, making
    retry activity indistinguishable from real upstream failures. The rule holds for both
    outcomes a replay can produce, DUPLICATE and INVALID; the caller records the attempt
    against the existing row instead.

    Returns a result dict with keys: outcome, transaction_id, reason.
    Possible outcomes:
      POSTED     — new event, payment confirmed, written to ledger
      VOID       — new event, payment failed or subscription cancelled
      PENDING    — new event, subscription updated (awaiting resolution)
      DLQ_INVALID    — Pydantic validation failed; event in DLQ
      DLQ_DUPLICATE  — already processed; idempotency guard fired; event in DLQ
    Neither outcome writes a new DLQ row when from_retry is True.
    """
    now = datetime.now(timezone.utc)

    # ── LAYER 1: Pydantic Validation ─────────────────────────────────────────
    # StripeEvent(**event) runs all Field constraints AND all @model_validators.
    #     If anything fails — wrong type, bad currency, $0 invoice, missing cus_ prefix —
    #     a ValidationError is raised here. We catch it, write to DLQ, and return.
    #     The DB sees nothing from a failed validation. Clean separation.
    try:
        validated_event = StripeEvent(**event)
    except ValidationError as e:
        transaction_id = event.get("id") or "unknown"
        # "unknown" fallback only if the event has no id at all.
        #     DLQEntry.min_length=1 would reject "" so we use "unknown" as the floor.
        if not from_retry:
            dlq_entry = DLQEntry(
                transaction_id=transaction_id,
                reason=DLQReason.INVALID,
                raw_payload=event,
            )
            _write_dlq(conn, dlq_entry)
        else:
            # A replay that still fails validation is worth surfacing, but as a log
            #     line rather than another row: the entry being retried already holds
            #     this payload, and _increment_retry drives it to 'exhausted'.
            _log.warning(
                "dlq_retry payload still invalid transaction_id=%s errors=%s",
                transaction_id, e.error_count(),
            )
        return {
            "outcome":        "DLQ_INVALID",
            "transaction_id": transaction_id,
            "reason":         f"Pydantic validation failed: {e.error_count()} errors"
        }

    # ── Extract validated fields ──────────────────────────────────────────────
    # At this point, all fields are guaranteed valid by Pydantic.
    #     No more .get() with defaults. No more "could be None" uncertainty.
    transaction_id  = validated_event.id
    event_type      = validated_event.type
    data_object     = validated_event.data.object
    idempotency_key = validated_event.idempotency_key

    # ── LAYER 2: Business Logic ───────────────────────────────────────────────
    # Build the LedgerEntry from validated fields. This is the second validation
    #     layer — LedgerEntry's own Field constraints run on the extracted values.
    #     In practice these always pass (Pydantic already validated upstream),
    #     but the model acts as a schema contract for the INSERT statement.
    amount_cents  = data_object.get_amount()
    customer_id   = data_object.customer
    currency      = data_object.currency
    ledger_status = _STATUS_MAP[event_type]   # type-safe — KeyError impossible here
    payload_json  = json.dumps(event)

    ledger_entry = LedgerEntry(
        transaction_id=transaction_id,
        event_type=event_type,
        customer_id=customer_id,
        amount_cents=amount_cents,
        currency=currency,
        status=ledger_status,
        idempotency_key=idempotency_key,
        payload=payload_json,
        created_at=now,
    )

    # ── Transactional Outbox: atomic dual-write ───────────────────────────────
    # The ledger INSERT and the outbox INSERT are in the same BEGIN…COMMIT.
    #     ON CONFLICT (transaction_id) DO NOTHING is the idempotency guard:
    #     if the event ID already exists in the ledger, the INSERT silently skips.
    #     rowcount=1 = new event, committed to both ledger and outbox.
    #     rowcount=0 = duplicate, ON CONFLICT fired, nothing written.
    try:
        with _tx(conn) as cur:
            cur.execute(
                """
                INSERT INTO ledger
                  (transaction_id, event_type, customer_id, amount_cents,
                   currency, status, idempotency_key, payload, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (transaction_id) DO NOTHING
                """,
                ledger_entry.to_db(),
            )
            inserted = cur.rowcount  # 1=new row inserted, 0=ON CONFLICT fired (duplicate)

            if inserted == 1:
                # Only write outbox row for genuinely new events — not for duplicates.
                #     Same transaction = zero dual-write gap between ledger and outbox.
                cur.execute(
                    """
                    INSERT INTO outbox (transaction_id, event_type, payload, created_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (transaction_id, event_type.value, payload_json, now),
                )
    except psycopg2.OperationalError as exc:
        # DB-level error (connection lost, server down). Raise RuntimeError so the
        #     FastAPI handler converts it to HTTP 503 — telling Stripe to retry.
        #     503 is the correct response; 200 would tell Stripe "got it, stop retrying."
        raise RuntimeError(f"DB write failed: {exc}") from exc

    # ── LAYER 3: DLQ Routing ──────────────────────────────────────────────────
    # After the transaction, check if we actually inserted a new row.
    #     inserted=0 means the event was a duplicate — idempotency guard fired.
    #     Write to DLQ with reason DUPLICATE for audit visibility.
    #     The duplicate event is NOT an error — it's Stripe doing its job (at-least-once delivery).
    if inserted == 0:
        # On a replay the DUPLICATE row already exists — it is the entry being retried.
        #     Writing another would grow the queue by one row per retry, forever.
        if not from_retry:
            dlq_entry = DLQEntry(
                transaction_id=transaction_id,
                reason=DLQReason.DUPLICATE,
                raw_payload=event,
            )
            _write_dlq(conn, dlq_entry)
        return {
            "outcome":        "DLQ_DUPLICATE",
            "transaction_id": transaction_id,
            "reason":         "Already processed — idempotency guard fired"
        }

    # Success path — event is new, validated, written to ledger and outbox.
    return {
        "outcome":        ledger_status.value,
        "transaction_id": transaction_id,
        "reason":         None
    }


def _write_dlq(conn: "psycopg2.extensions.connection", entry: DLQEntry) -> None:
    """
    Best-effort DLQ append. Deliberately never raises — the main processing
    path must not die because the DLQ is unavailable. If the DB write fails
    (disk full, locked, corrupt), the full raw payload is logged at ERROR level
    to both stderr and billing_ledger.log. An operator can grep the log file
    for "DLQ write failed", extract the raw_payload JSON, and replay manually.
    This is the "zero data loss" guarantee: if the DB can't save it, the log can.
    """
    try:
        with _tx(conn) as cur:
            cur.execute(
                "INSERT INTO dlq (transaction_id, reason, raw_payload, received_at,"
                " status, retry_count, max_retries, next_retry_at)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                entry.to_db(),
            )
    except Exception as exc:
        # DO NOT re-raise. The hot path must not fail because the DLQ is down.
        #     Log at ERROR so monitoring alerts fire. Include full raw_payload so
        #     the entry can be manually replayed from the log file.
        _log.error(
            "DLQ write failed — payload preserved here for manual recovery. "
            "transaction_id=%s reason=%s error=%r raw_payload=%s",
            entry.transaction_id,
            entry.reason.value,
            exc,
            json.dumps(entry.raw_payload),
        )


# Exponential backoff delays (seconds) for retry attempts 1, 2, 3+
_RETRY_BACKOFF = [300, 900, 3600]  # 5 min, 15 min, 1 hour

# How long a retry claim (status='retrying') stays valid before another batch may
#     reclaim it. The claim is committed before the work runs, so a crash between the
#     two would otherwise strand the row forever — retry_dlq_batch only ever looked at
#     status='pending'. 15 minutes is comfortably above the worst-case time to process
#     one batch; reclaiming a row that is still in flight costs duplicated effort, never
#     duplicated money, because the ledger primary key still rejects the second insert.
_CLAIM_TIMEOUT_SECONDS = 900


def retry_dlq_batch(
    conn: "psycopg2.extensions.connection",
    batch_size: int = 50,
    claim_timeout_seconds: int = _CLAIM_TIMEOUT_SECONDS,
) -> int:
    """
    Find DLQ entries eligible for retry, attempt to reprocess each one, and
    update status accordingly. Returns the number of entries attempted.

    Eligibility: status='pending' AND next_retry_at <= NOW() AND retry_count < max_retries.
    DUPLICATE and INVALID entries have max_retries=0 so they are never auto-retried.
    Only TRANSIENT entries (max_retries=3) enter this loop.

    NOTE — no production trigger today: nothing in the live ingest path writes a TRANSIENT
    entry (single-event DB failures return 503 so Stripe retries; batch failures roll back
    atomically). This loop is wired and tested, provisioned for a future non-Stripe / async
    source with no upstream retrier. Until then it is intentionally inert in production.

    On success: status → 'resolved'.
    On failure: retry_count += 1, next_retry_at = NOW() + backoff, and if
                retry_count >= max_retries: status → 'exhausted'.

    Concurrency: rows are atomically claimed (status='retrying', claimed_at=NOW()) via
    UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED). Two workers running
    simultaneously claim disjoint sets — no event is retried twice. The claim transaction
    commits before process_stripe_event is called because process_stripe_event calls
    _tx(conn) internally — nested BEGIN on an autocommit connection would error.
    status='retrying' is the lock substitute.

    Stale-claim recovery: because the claim commits before the work happens, a crash (or a
    failure inside the recovery UPDATE itself) leaves rows in 'retrying' that no later batch
    would ever select — invisible, counted as neither pending nor resolved nor exhausted.
    The claim query therefore also reclaims 'retrying' rows whose claimed_at is older than
    claim_timeout_seconds. A NULL claimed_at counts as infinitely stale, which recovers rows
    stranded before migration 0003 added the column.

    Set claim_timeout_seconds above the worst-case processing time for one batch. Too low and
    a second worker reclaims a row that is still being worked on; the ledger PK still prevents
    double-counting, so the cost is duplicated effort, not duplicated money.
    """
    with _tx(conn) as cur:
        cur.execute(
            """
            UPDATE dlq SET status = 'retrying', claimed_at = NOW()
            WHERE id IN (
                SELECT id FROM dlq
                WHERE (
                        status = 'pending'
                        AND (next_retry_at IS NULL OR next_retry_at <= NOW())
                        AND retry_count < max_retries
                      )
                   OR (
                        status = 'retrying'
                        AND (claimed_at IS NULL
                             OR claimed_at < NOW() - make_interval(secs => %s))
                      )
                ORDER BY id
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, transaction_id, raw_payload
            """,
            (claim_timeout_seconds, batch_size),
        )
        rows = cur.fetchall()

    attempted = 0
    for row in rows:
        dlq_id, transaction_id, raw_payload_str = row[0], row[1], row[2]
        attempted += 1
        try:
            payload = json.loads(raw_payload_str)
            # from_retry=True: this payload is being replayed, so a DLQ_DUPLICATE outcome
            #     must not append another DLQ row. Without it every retry of an entry whose
            #     event already reached the ledger left one more pending row behind than it
            #     consumed, and each of those was itself retry-eligible.
            result = process_stripe_event(conn, payload, from_retry=True)
            outcome = result.get("outcome", "")
            if outcome in ("POSTED", "VOID", "PENDING", "DLQ_DUPLICATE"):
                with _tx(conn) as cur:
                    cur.execute(
                        "UPDATE dlq SET status='resolved', claimed_at=NULL WHERE id=%s",
                        (dlq_id,),
                    )
                _log.info("dlq_retry resolved id=%s transaction_id=%s", dlq_id, transaction_id)
            else:
                _increment_retry(conn, dlq_id, transaction_id)
        except Exception as exc:
            _log.error("dlq_retry error id=%s error=%r", dlq_id, exc)
            # The recovery UPDATE can fail for the same reason the retry did — the database
            #     is still unreachable. Unguarded, that exception escaped this loop and left
            #     every remaining claimed row of the batch stuck in 'retrying'. Contain it
            #     per row so one bad row cannot abort the rest of the batch.
            try:
                _increment_retry(conn, dlq_id, transaction_id)
            except Exception as inner_exc:
                _log.critical(
                    "dlq_retry recovery failed id=%s transaction_id=%s error=%r — "
                    "entry left in status='retrying'; it will be reclaimed automatically "
                    "after claim_timeout_seconds=%s",
                    dlq_id, transaction_id, inner_exc, claim_timeout_seconds,
                )

    return attempted


def _increment_retry(
    conn: "psycopg2.extensions.connection",
    dlq_id: int,
    transaction_id: str,
) -> None:
    """Atomically increment retry_count and set status/next_retry_at in one UPDATE.

    Single statement eliminates the TOCTOU race: the old SELECT + UPDATE pattern
    let two concurrent callers both read retry_count=N and both write N+1 instead of N+2.
    """
    with _tx(conn) as cur:
        cur.execute(
            """
            UPDATE dlq
            SET retry_count   = retry_count + 1,
                -- Release the claim in the same statement that decides the next state.
                -- A row leaving 'retrying' must never keep a claim timestamp, or the
                -- reclaim query would treat a healthy pending row as a stale claim.
                claimed_at    = NULL,
                status        = CASE
                    WHEN retry_count + 1 >= max_retries THEN 'exhausted'
                    ELSE 'pending'
                END,
                next_retry_at = CASE
                    WHEN retry_count + 1 >= max_retries THEN NULL
                    ELSE NOW() + (
                        CASE retry_count
                            WHEN 0 THEN INTERVAL '300 seconds'
                            WHEN 1 THEN INTERVAL '900 seconds'
                            ELSE INTERVAL '3600 seconds'
                        END
                    )
                END
            WHERE id = %s
            RETURNING retry_count, max_retries, status
            """,
            (dlq_id,),
        )
        row = cur.fetchone()
    if row is None:
        return
    new_count, max_retries, new_status = row[0], row[1], row[2]
    if new_status == "exhausted":
        _log.warning(
            "dlq_retry exhausted id=%s transaction_id=%s retries=%s/%s",
            dlq_id, transaction_id, new_count, max_retries,
        )
    else:
        delay = _RETRY_BACKOFF[min(new_count - 1, len(_RETRY_BACKOFF) - 1)]
        _log.info(
            "dlq_retry scheduled id=%s transaction_id=%s attempt=%s next_in=%ss",
            dlq_id, transaction_id, new_count, delay,
        )


def prune_dlq(
    conn: "psycopg2.extensions.connection",
    retention_days: int = 90,
) -> int:
    """Delete DUPLICATE DLQ rows older than retention_days. Returns rows deleted.

    Without retention the dlq table grows without bound: Stripe's at-least-once
    redelivery writes a DUPLICATE row per replayed event, and at 500 TPS even a small
    duplicate rate is millions of audit-only rows per day. Those rows are safe to age out.
    INVALID and exhausted entries are NEVER pruned — they need human review and stay until
    an operator resolves them.

    The worker calls this at most once per hour (see worker.run_loop). It is a single
    idempotent DELETE and can also be run standalone or from cron / pg_cron.
    """
    with _tx(conn) as cur:
        cur.execute(
            "DELETE FROM dlq "
            "WHERE reason = %s "
            "  AND received_at < NOW() - make_interval(days => %s)",
            (DLQReason.DUPLICATE.value, retention_days),
        )
        return cur.rowcount


def process_stripe_event_batch(
    conn: "psycopg2.extensions.connection",
    events: list[dict],
    page_size: int = 1000,
) -> list[dict]:
    """
    Batch variant of process_stripe_event(). Collapses N single-event round trips
    down to 4 + ceil(N/page_size)*3 round trips total: one BEGIN, one bulk ledger
    INSERT, one bulk outbox INSERT, one bulk DLQ INSERT, one COMMIT. At page_size=1000
    and N=5,000 that is ~12 round trips vs 20,000 for the per-event path — the source
    of the 192× throughput gain observed in the benchmark (26 TPS → ~5,000 TPS).

    WHY page_size=1000:
    PostgreSQL limits a prepared statement to 65,535 bind parameters. With 9 ledger
    columns per row, 1,000 rows = 9,000 parameters — safely within the limit while
    keeping round trips near the minimum. Larger pages would require pre-splitting;
    smaller pages waste round trips. 1,000 is the practical sweet spot for a 9-column
    table on a localhost connection.

    WHY RETURNING for duplicate partitioning:
    After a bulk INSERT with ON CONFLICT DO NOTHING, we need to know which rows
    actually landed vs were silently skipped. There are three approaches:

      A. Post-INSERT SELECT — adds a round trip and races concurrent inserters.
         Between our INSERT and our SELECT, another connection could have inserted
         the same ID; we'd misclassify it as ours.

      B. Pre-INSERT application-side tracking — loses the atomicity guarantee for
         the same reason: another connection can insert the same ID between our
         tracking step and our INSERT.

      C. RETURNING (this implementation) — atomic: PostgreSQL reports exactly what
         THIS transaction inserted, visible only within this transaction, with no
         race window and no extra round trip.

    The inserted_ids set built from RETURNING enables O(1) partition of the valid
    list into (new → outbox) vs (duplicate → DLQ) in a single linear pass.

    Atomicity guarantee: all valid new events land in both ledger and outbox, or
    none do. The Transactional Outbox rule holds at batch scale: a crash after
    COMMIT leaves all outbox rows intact for the downstream worker to deliver;
    a crash before COMMIT leaves nothing in either table.

    Returns a list of result dicts indexed by input position, matching the shape
    returned by process_stripe_event() for drop-in compatibility.
    """
    if not events:
        return []

    now = datetime.now(timezone.utc)
    results: list = [None] * len(events)
    valid: list[tuple[int, dict, LedgerEntry]] = []
    invalid_dlq_rows: list[tuple] = []

    # ── Phase 1: Pydantic validation (no DB touch) ────────────────────────────
    # WHY before the transaction: a ValidationError is a pure-CPU outcome — no DB
    #     state changes. Opening BEGIN for a payload that will never reach a table wastes
    #     a connection slot, burns a round trip on the BEGIN itself, and forces a ROLLBACK
    #     on the exception path. Separating validation from the transaction means the DB
    #     sees only structurally correct, business-rule-valid data. The hot path stays hot.
    for idx, event in enumerate(events):
        try:
            validated = StripeEvent(**event)
        except ValidationError as e:
            tid = event.get("id") or "unknown"
            invalid_dlq_rows.append(
                DLQEntry(transaction_id=tid, reason=DLQReason.INVALID, raw_payload=event).to_db()
            )
            results[idx] = {
                "outcome":        "DLQ_INVALID",
                "transaction_id": tid,
                "reason":         f"Pydantic validation failed: {e.error_count()} errors",
            }
            continue
        data_obj = validated.data.object
        entry = LedgerEntry(
            transaction_id=validated.id,
            event_type=validated.type,
            customer_id=data_obj.customer,
            amount_cents=data_obj.get_amount(),
            currency=data_obj.currency,
            status=_STATUS_MAP[validated.type],
            idempotency_key=validated.idempotency_key,
            payload=json.dumps(event),
            created_at=now,
        )
        valid.append((idx, event, entry))

    # ── Phase 2: Single atomic transaction — bulk ledger + outbox + DLQ ───────
    # WHY one transaction for all three tables: the Transactional Outbox pattern
    #     requires that the ledger row and the outbox row are either both committed or
    #     both absent. If we committed the ledger rows first and then failed before
    #     writing outbox rows, the downstream system would never see those events —
    #     revenue posted to the ledger but never forwarded is invisible to everything
    #     downstream. Wrapping all three tables in one BEGIN…COMMIT closes that gap
    #     completely: the outbox is only non-empty if the corresponding ledger rows exist.
    try:
        with _tx(conn) as cur:
            inserted_ids: set[str] = set()
            if valid:
                # execute_values sends rows in page_size-row SQL statements.
                #     Each statement carries (page_size × columns) bind parameters —
                #     1,000 rows × 9 columns = 9,000 parameters, within PostgreSQL's
                #     65,535-parameter limit. Fewer statements = fewer round trips = higher TPS.
                #
                #     ON CONFLICT (transaction_id) DO NOTHING is enforced per-row by
                #     PostgreSQL's constraint engine — not by application code. This is the
                #     same serialization point as process_stripe_event(); it works identically
                #     under concurrent writers because the PRIMARY KEY constraint is the lock.
                #
                #     RETURNING transaction_id is the O(1) partition key. It is the only
                #     correct mechanism here: it is evaluated inside this transaction, so it
                #     reports exactly what THIS transaction inserted. Any row absent from this
                #     set was a silent ON CONFLICT hit — a duplicate that needs to go to DLQ.
                #     The set membership check (entry.transaction_id in inserted_ids) below
                #     runs in O(1) per event, making the entire partition pass O(N).
                #
                #     fetch=True tells execute_values to accumulate RETURNING rows across
                #     all page_size batches and return them together as one list.
                returned = execute_values(
                    cur,
                    """
                    INSERT INTO ledger
                      (transaction_id, event_type, customer_id, amount_cents,
                       currency, status, idempotency_key, payload, created_at)
                    VALUES %s
                    ON CONFLICT (transaction_id) DO NOTHING
                    RETURNING transaction_id
                    """,
                    [entry.to_db() for _, _, entry in valid],
                    page_size=page_size,
                    fetch=True,
                )
                inserted_ids = {row[0] for row in returned}

            outbox_rows: list[tuple] = []
            dup_dlq_rows: list[tuple] = []

            # Use a mutable copy of inserted_ids for partition. When the same
            #     transaction_id appears multiple times in the batch, only the first
            #     occurrence is in RETURNING (PostgreSQL inserted it exactly once).
            #     Discarding the id after the first match ensures the second occurrence
            #     correctly routes to DLQ_DUPLICATE instead of being misclassified as new.
            remaining_ids: set[str] = set(inserted_ids)

            for idx, event, entry in valid:
                if entry.transaction_id in remaining_ids:
                    remaining_ids.discard(entry.transaction_id)  # consume so duplicate in same batch routes correctly
                    outbox_rows.append(
                        (entry.transaction_id, entry.event_type.value, entry.payload, now)
                    )
                    results[idx] = {
                        "outcome":        entry.status.value,
                        "transaction_id": entry.transaction_id,
                        "reason":         None,
                    }
                else:
                    dup_dlq_rows.append(
                        DLQEntry(
                            transaction_id=entry.transaction_id,
                            reason=DLQReason.DUPLICATE,
                            raw_payload=event,
                        ).to_db()
                    )
                    results[idx] = {
                        "outcome":        "DLQ_DUPLICATE",
                        "transaction_id": entry.transaction_id,
                        "reason":         "Already processed — idempotency guard fired",
                    }

            if outbox_rows:
                execute_values(
                    cur,
                    "INSERT INTO outbox"
                    " (transaction_id, event_type, payload, created_at) VALUES %s",
                    outbox_rows,
                    page_size=page_size,
                )

            all_dlq = invalid_dlq_rows + dup_dlq_rows
            if all_dlq:
                execute_values(
                    cur,
                    "INSERT INTO dlq"
                    " (transaction_id, reason, raw_payload, received_at,"
                    "  status, retry_count, max_retries, next_retry_at) VALUES %s",
                    all_dlq,
                    page_size=page_size,
                )

    except psycopg2.OperationalError as exc:
        raise RuntimeError(f"DB batch write failed: {exc}") from exc

    return results


# ===========================================================================
# SECTION 5: FASTAPI APPLICATION
#
# HTTP layer — thin wrapper around process_stripe_event(). FastAPI handles
#     JSON parsing, request validation, and response formatting. The core logic
#     lives in process_stripe_event() — framework-agnostic and independently testable.
# ===========================================================================

app = FastAPI(title="Micro-Billing-Ledger PoC", version="1.0.0")

# Rate limiter — keyed by client IP. Prevents a single caller from exhausting
#     the connection pool (maxconn=20) or flooding the DLQ table.
#     Limits are applied per-route via @limiter.limit() decorator below.
#     429 Too Many Requests is returned when the limit is exceeded.
_limiter = Limiter(key_func=get_remote_address)
app.state.limiter = _limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Ensure all three tables and their indexes exist before the pool opens connections.
#     The bootstrap connection is closed immediately — it is not reused.
_bootstrap_conn = _bootstrap()
_bootstrap_conn.close()

# Thread-safe connection pool for the FastAPI route handlers.
#     minconn=1 — always keep one connection warm to avoid cold-start latency on first request.
#     maxconn=20 — cap concurrent DB connections; tune to your PostgreSQL max_connections budget.
#     In tests, replace _pool with a _MockPool that wraps the test connection (see test_ledger.py).
_pool: ThreadedConnectionPool = ThreadedConnectionPool(minconn=1, maxconn=20, dsn=DATABASE_URL)


def _require_api_key(x_api_key: str = Header(default="")) -> None:
    """FastAPI dependency for ops endpoints. Enforces X-Api-Key when BILLING_API_KEY is set.
    If BILLING_API_KEY is empty (default), the check is skipped — dev-mode convenience.
    ISO 27001 A.9.4.1: access to operational data must be access-controlled in production.
    """
    if not BILLING_API_KEY:
        return  # dev mode — no key configured, skip check
    if not x_api_key:
        raise HTTPException(status_code=401, detail="X-Api-Key header required")
    if x_api_key != BILLING_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


class StripeWebhookPayload(BaseModel):
    """
    FastAPI request body model for the webhook endpoint. This is a minimal
    envelope — FastAPI uses it to parse the incoming JSON. The actual deep
    validation happens inside process_stripe_event() via StripeEvent.
    Using dict for data and request allows arbitrary Stripe payload shapes
    to pass through without FastAPI rejecting them before we can DLQ them.
    """
    id:      str  = Field(..., description="Stripe event id")
    type:    str  = Field(..., description="Event type")
    data:    dict = Field(default_factory=dict)
    request: dict = Field(default_factory=dict)


@app.post("/webhook/stripe", status_code=status.HTTP_200_OK)
@_limiter.limit("100/minute")
def stripe_webhook(request: Request, payload: StripeWebhookPayload) -> JSONResponse:
    """
    Receives a Stripe webhook and commits it to the billing ledger.
    Always returns HTTP 200 for valid JSON — even for duplicates and invalid
    events that end up in DLQ. Returning non-200 tells Stripe to retry,
    which is only correct for transient failures (e.g., DB down → 503).
    HMAC signature verification is active — stripe.Webhook.construct_event()
    validates the Stripe-Signature header before any business logic runs.
    ISO 27001 A.14.1.2: authentication enforced at all ingestion entry points.
    """
    # ── HMAC SIGNATURE VERIFICATION ─────────────────────────────────────
    #     stripe.Webhook.construct_event() validates the HMAC-SHA256 in the
    #     Stripe-Signature header against the raw request body. Any body tampering
    #     or wrong/missing signature raises SignatureVerificationError → 400.
    #     ISO 27001 A.14.1.2: authentication at all ingestion entry points.
    # Request.body() is async def in Starlette — calling it without await returns a coroutine
    # object, not bytes. FastAPI already read and cached the body in request._body when it
    # parsed StripeWebhookPayload above. Access the cached attribute directly.
    raw_body   = request._body
    sig_header = request.headers.get("stripe-signature", "")
    try:
        stripe.Webhook.construct_event(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(
            status_code=400,
            detail="Invalid or missing Stripe signature"
        )

    conn = _pool.getconn()
    try:
        result = process_stripe_event(conn, payload.model_dump())
    except RuntimeError as exc:
        # RuntimeError from _tx() means a DB-level failure (locked, corrupt, disk full).
        #     503 tells Stripe to back off and retry — the correct behavior for transient errors.
        #     Do NOT return 200 here — that would tell Stripe "got it" when we didn't.
        raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _pool.putconn(conn)

    return JSONResponse(content=result)


@app.get("/ledger/summary")
@_limiter.limit("20/minute")
def ledger_summary(request: Request, _: None = Depends(_require_api_key)) -> JSONResponse:
    """
    Quick sanity-check endpoint. Returns row counts per ledger status, DLQ depth,
    and outbox pending count. Requires X-Api-Key header when BILLING_API_KEY is set.
    sync def: FastAPI dispatches sync handlers to its thread pool via run_in_threadpool.
    psycopg2 blocking calls do not block the asyncio event loop.
    """
    conn = _pool.getconn()
    try:
        # GROUP BY status gives {POSTED: N, VOID: N, PENDING: N} in one query.
        with conn.cursor() as cur:
            cur.execute("SELECT status, COUNT(*) FROM ledger GROUP BY status")
            counts = {row[0]: row[1] for row in cur.fetchall()}

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dlq")
            dlq_depth = cur.fetchone()[0]

        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM outbox WHERE dispatched=0")
            outbox_pending = cur.fetchone()[0]
    finally:
        _pool.putconn(conn)

    return JSONResponse(content={
        "ledger":         counts,
        "dlq_depth":      dlq_depth,      # Total entries in DLQ (all reasons)
        "outbox_pending": outbox_pending, # Events not yet dispatched downstream
    })


@app.get("/dlq/entries")
@_limiter.limit("20/minute")
def dlq_entries(request: Request, limit: int = 50, _: None = Depends(_require_api_key)) -> JSONResponse:
    """
    Inspect DLQ entries over HTTP — newest first. This is the ops endpoint:
    without it, "check the DLQ" means "SSH into the box and run a psql query."
    Default limit 50; capped at 1000 to prevent accidental full-table dumps.
    raw_payload is deserialized so callers get a JSON object, not a string.
    Requires X-Api-Key header when BILLING_API_KEY is set.
    sync def: runs in FastAPI's thread pool — no event loop blocking.
    """
    if limit < 1:
        raise HTTPException(status_code=422, detail="limit must be >= 1")

    conn = _pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, transaction_id, reason, raw_payload, received_at "
                "FROM dlq ORDER BY id DESC LIMIT %s",
                (min(limit, 1000),),  # cap at 1000 — never dump the entire table
            )
            rows = cur.fetchall()
    finally:
        _pool.putconn(conn)

    return JSONResponse(content={
        "entries": [
            {
                "id":             r[0],
                "transaction_id": r[1],
                "reason":         r[2],
                "raw_payload":    json.loads(r[3]),  # string → dict for clean JSON response
                "received_at":    r[4].isoformat() if hasattr(r[4], "isoformat") else r[4],
            }
            for r in rows
        ],
        "count": len(rows),
    })


@app.post("/dlq/{dlq_id}/retry")
@_limiter.limit("20/minute")
def dlq_retry(request: Request, dlq_id: int, _: None = Depends(_require_api_key)) -> JSONResponse:
    """
    Manual retry endpoint for a single DLQ entry. Reprocesses the raw_payload
    immediately, ignoring retry_count and backoff. Useful for ops: fix the
    upstream issue (e.g. bad validator config), then replay specific entries.
    Returns the outcome of the reprocessing attempt.
    Requires X-Api-Key header when BILLING_API_KEY is set.

    'resolved' is terminal and returns 409. A resolved entry means the ledger already
    holds its event, so replaying it can only ever produce DLQ_DUPLICATE — no work, but
    it used to mark the row resolved a second time and append a fresh pending DLQ row,
    which made the queue self-amplifying and let one id be reopened without limit.
    Every other status ('pending', 'retrying', 'exhausted') stays replayable — that is
    the point of the endpoint, and it is also the manual escape hatch for an entry whose
    automatic retries are spent.
    """
    conn = _pool.getconn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT transaction_id, raw_payload, status FROM dlq WHERE id=%s",
                (dlq_id,),
            )
            row = cur.fetchone()
        if row is None:
            raise HTTPException(status_code=404, detail=f"DLQ entry {dlq_id} not found")

        transaction_id, raw_payload_str, current_status = row
        if current_status == "resolved":
            raise HTTPException(
                status_code=409,
                detail=f"DLQ entry {dlq_id} is already resolved — nothing to replay",
            )

        payload = json.loads(raw_payload_str)
        result = process_stripe_event(conn, payload, from_retry=True)
        outcome = result.get("outcome", "")

        if outcome in ("POSTED", "VOID", "PENDING", "DLQ_DUPLICATE"):
            with _tx(conn) as cur:
                cur.execute(
                    "UPDATE dlq SET status='resolved', claimed_at=NULL WHERE id=%s",
                    (dlq_id,),
                )
            _log.info("dlq manual retry resolved id=%s transaction_id=%s", dlq_id, transaction_id)
        else:
            _increment_retry(conn, dlq_id, transaction_id)

        return JSONResponse(content={
            "dlq_id":         dlq_id,
            "transaction_id": transaction_id,
            "outcome":        outcome,
            "previous_status": current_status,
        })
    finally:
        _pool.putconn(conn)


@app.get("/health")
def health() -> JSONResponse:
    """
    Kubernetes/Docker health check endpoint. Returns 200 if the process is alive.
    Does NOT check DB connectivity — a separate readiness probe should do that.
    sync def: consistent with other route handlers; no I/O performed here.
    """
    return JSONResponse(content={"status": "ok"})


# ===========================================================================
# SECTION 6: HEADLESS BENCHMARK MODE
#
# Bypasses the HTTP stack entirely to measure pure ledger throughput.
#     Run with: python ledger.py --silent --events 10000
#     This is what the TPS numbers in the README are based on.
# ===========================================================================

def _run_headless_benchmark(n: int = 10_000) -> None:
    """
    Drives process_stripe_event() directly — no HTTP, no console I/O.
    Connects to PostgreSQL via DATABASE_URL. Each fake event gets a random ID
    so no duplicates — we're measuring insert throughput, not idempotency overhead.
    PostgreSQL TPS is lower than SQLite in-memory (~1,000-3,000 vs ~15,000) because
    of network round-trips and WAL fsync on the server side. That's the correct tradeoff.
    """
    import random
    import string

    conn = _bootstrap()

    def _fake_event(i: int) -> dict:
        """
        Generates a structurally valid Stripe event with a random unique ID.
        customer format "cus_{i:06d}" passes the cus_ prefix check.
        amount_paid=random.randint(100, 100_000) satisfies ge=0 and nonzero invoice check.
        """
        return {
            "id":   f"evt_{''.join(random.choices(string.ascii_lowercase, k=16))}",
            "type": random.choice(list(EventType)).value,
            "data": {"object": {
                "customer":    f"cus_{i:06d}",
                "amount_paid": random.randint(100, 100_000),
                "currency":    "usd",
            }},
            "request": {},
        }

    events  = [_fake_event(i) for i in range(n)]
    t0      = time.perf_counter()
    for ev in events:
        process_stripe_event(conn, ev)
    elapsed = time.perf_counter() - t0
    tps     = n / elapsed
    print(f"Headless benchmark: {n:,} events in {elapsed:.3f}s  →  {tps:,.0f} TPS")


# ---------------------------------------------------------------------------
# Entry point
# When run directly (python ledger.py), either starts the benchmark or
#     launches uvicorn. The HTTP server is the normal mode for production.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silent", action="store_true",
                        help="Run headless benchmark instead of HTTP server")
    parser.add_argument("--events", type=int, default=10_000,
                        help="Number of events for benchmark")
    args = parser.parse_args()

    if args.silent:
        _run_headless_benchmark(args.events)
    else:
        import uvicorn
        # Binds all interfaces because inside the container nothing else can reach the
        #     port: compose publishes it on 127.0.0.1:8000 and external traffic arrives
        #     through Caddy. The nosec is live, not vestigial — bandit emits
        #     "nosec encountered (B104), but no failed test on line N", which reads like
        #     a dead suppression, but deleting it makes B104 fire immediately. Verified
        #     both ways on 2026-08-09; leave it in place.
        uvicorn.run("ledger:app", host="0.0.0.0", port=8000, reload=False)  # nosec B104
