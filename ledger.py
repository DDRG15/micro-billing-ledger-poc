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
import hashlib        # SHA-256 fingerprints of live secrets and API-key lookups
import hmac           # compare_digest for constant-time API key comparison
import ipaddress      # is the public ingress bound to loopback? (_is_loopback)
import json           # JSON serialization for raw payload archiving
import logging        # Structured log output to file and stderr
from logging.handlers import RotatingFileHandler  # Rotating file handler — caps log file size to prevent disk fill
import os             # DATABASE_URL, STRIPE_WEBHOOK_SECRET, BILLING_API_KEY env var lookup
import time           # perf_counter for benchmark timing
import threading      # serialises the /metrics set-then-render pair (see metrics())
import stripe         # Stripe SDK for HMAC-SHA256 webhook signature verification
import psycopg2       # PostgreSQL driver — production-grade relational storage
from datetime import datetime, timezone  # Timezone-aware timestamps for TIMESTAMPTZ columns — financial audit trail requires tz
from psycopg2.extras import execute_values  # Bulk INSERT — collapses N round trips to ceil(N/page_size)
from psycopg2.pool import ThreadedConnectionPool  # Thread-safe connection pool — replaces single module-level connection
from contextlib import asynccontextmanager, contextmanager  # app lifespan; _tx() BEGIN/COMMIT wrapper
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
#
# Format is JSON when LOG_FORMAT=json (the default in Docker), plain text otherwise.
#     Plain text is unparseable at the point you most need it: "which transaction_id
#     failed, how often, and how slow was it" becomes a regex against a human sentence,
#     and the answer changes whenever someone rewords a log line. Emitting one JSON
#     object per line makes those fields queryable in any log backend without parsing
#     rules, and it survives rewording because consumers read keys, not prose.
# ---------------------------------------------------------------------------
LOG_FORMAT = os.getenv("LOG_FORMAT", "text").strip().lower()

# Attributes LogRecord always carries. Anything outside this set was attached by the
#     caller via extra={...} and belongs in the JSON output as a top-level field.
_LOGRECORD_BUILTINS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", None, None)))


class JsonLogFormatter(logging.Formatter):
    """Render each record as a single-line JSON object.

    One line per record, no embedded newlines: log shippers split on newlines, so a
    multi-line record becomes several unparseable fragments. Exception tracebacks are
    folded into a single string field for that reason.

    Any key passed through extra={...} is promoted to a top-level field, which is what
    makes transaction_id, outcome and duration_ms queryable instead of buried in prose.
    """

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "ts":      datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level":   record.levelname,
            "logger":  record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key not in _LOGRECORD_BUILTINS and not key.startswith("_"):
                payload[key] = value
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # default=str so a stray datetime or Decimal degrades to its string form
        #     instead of raising inside the logging call and losing the record.
        return json.dumps(payload, default=str)


def _build_log_handlers() -> list:
    formatter: logging.Formatter = (
        JsonLogFormatter() if LOG_FORMAT == "json"
        else logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    )
    stream_handler = logging.StreamHandler()               # stderr
    file_handler = RotatingFileHandler(
        "billing_ledger.log",
        maxBytes=10_485_760,   # 10 MB per file
        backupCount=5,          # keep .1 through .5 → 50 MB max
    )
    for handler in (stream_handler, file_handler):
        handler.setFormatter(formatter)
    return [stream_handler, file_handler]


logging.basicConfig(level=logging.INFO, handlers=_build_log_handlers())
_log = logging.getLogger("ledger")

# ---------------------------------------------------------------------------
# Third-party imports
# FastAPI for HTTP routing, Pydantic for validation, starlette for responses.
#     All pinned in requirements.txt — no surprise version changes.
# ---------------------------------------------------------------------------
from fastapi import Depends, FastAPI, Header, HTTPException, Request, status  # noqa: E402
from fastapi.responses import JSONResponse, Response  # noqa: E402
from pydantic import BaseModel, Field, ValidationError, field_validator, model_validator  # noqa: E402
from slowapi import Limiter, _rate_limit_exceeded_handler  # noqa: E402
from slowapi.util import get_remote_address  # noqa: E402
from slowapi.errors import RateLimitExceeded  # noqa: E402
from prometheus_client import (  # noqa: E402
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
)

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

# The tenant that owns rows written before multi-tenancy existed, and the tenant a
#     single-operator deployment runs as. Every table defaults to it, so a deployment
#     that never creates a second org behaves exactly as it did before Phase 13.
DEFAULT_ORG_ID: str = "default"

# Opt-in token for work that is legitimately cross-tenant: the outbox drain, the DLQ
#     retry claim, retention pruning. Spelled out so `grep ALL_ORGS` lists every place
#     that steps outside a single tenant.
ALL_ORGS: str = "*"


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


# Subscription events record that an account changed state; they do not record money.
#     Stripe's Subscription object has no top-level amount field at all — prices live in
#     items.data[].price — so requiring one sent every real customer.subscription.* event
#     to the DLQ, where each one inflated the dlq_depth operators read as ingestion
#     health. Deriving an amount from the items would be worse: it posts a list price as
#     revenue on top of the invoice.paid that actually collects it, and it is wrong for
#     trials, metered and tiered prices, coupons and proration. So these events are
#     ledgered with NO amount (amount_cents NULL, status LIFECYCLE) and still reach the
#     outbox, because downstream systems act on plan changes and cancellations. The
#     database enforces the same split (migration 0007, ledger_amount_matches_kind).
_LIFECYCLE_TYPES: frozenset = frozenset({
    EventType.SUB_CREATED, EventType.SUB_DELETED, EventType.SUB_UPDATED,
})


# Which field carries the billable figure, per invoice event type, most specific first.
#     Stripe does not use one field for every event: a paid invoice reports
#     amount_paid, a failed one reports amount_paid=0 with the real figure in
#     amount_due. Reading amount_paid for every type made each invoice.payment_failed
#     look like a $0 invoice, so check_invoice_amount_nonzero rejected it and a real
#     failed payment — the kind of event dunning depends on — landed in the DLQ.
#     An invoice whose fields are all absent still routes to the DLQ, which is the
#     intended outcome: an unknown amount must never be recorded as $0. Subscription
#     types have no entry: no amount is read for them at all (_LIFECYCLE_TYPES).
_AMOUNT_FIELDS_BY_TYPE: dict = {
    EventType.INVOICE_PAID:   ("amount_paid", "amount", "amount_due"),
    EventType.INVOICE_FAILED: ("amount_due", "amount_paid", "amount"),
}
_AMOUNT_FIELDS_DEFAULT = ("amount_paid", "amount", "amount_due")


class StripeObject(BaseModel):
    """
    Represents the `data.object` payload inside a Stripe webhook: an Invoice or a
    Subscription, depending on the event. Field constraints run here. Whether an
    amount must be present depends on the event type, which this model cannot see,
    so that rule lives on StripeEvent (check_amount_present).
    """

    # Stripe customer IDs look like "cus_ABC123". min_length=4 catches empty
    #     strings and single-char placeholders. The cus_ prefix check is in StripeEvent.
    customer: str = Field(
        min_length=4,
        description="Stripe customer ID (e.g., 'cus_ABC123')"
    )

    # Stripe puts the paid amount here on invoice.paid events. Optional because which
    #     field carries the amount depends on the event type, and a Subscription object
    #     carries none of them.
    amount_paid: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount paid in cents, non-negative"
    )

    # What an invoice was billed for, as opposed to what was collected. A failed
    #     payment carries amount_paid=0 and the real figure here, so reading only
    #     amount_paid made every invoice.payment_failed event look like a $0 invoice
    #     and get rejected by check_invoice_amount_nonzero. See _AMOUNT_FIELDS_BY_TYPE.
    amount_due: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount owed in cents — the figure on a failed or open invoice"
    )

    # Fallback amount field — Stripe uses different field names depending on
    #     the event type. _AMOUNT_FIELDS_BY_TYPE picks the right one.
    amount: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount in cents (generic fallback)"
    )

    # ISO 4217 currency code. Regex enforces exactly 3 lowercase letters; the
    #     validator below normalizes case before the pattern runs, so "USD" is
    #     accepted and stored as "usd" rather than rejected as malformed.
    currency: str = Field(
        default="usd",
        pattern=r"^[a-z]{3}$",
        description="ISO 4217 currency code, lowercase 3 chars"
    )

    @field_validator("currency", mode="before")
    @classmethod
    def normalize_currency(cls, v: object) -> object:
        """
        Lowercase the currency before the pattern constraint sees it.

        ISO 4217 codes are case-insensitive and upstream systems disagree about
        which case to send. Storing both "USD" and "usd" splits every
        GROUP BY currency in a finance report into two rows that look like two
        currencies, and the split is silent — the totals still add up per row, so
        nothing looks broken until someone reconciles against the bank.
        Normalizing at the entry boundary means the database only ever holds one
        spelling, and mode="before" is what lets the pattern validate the
        normalized value instead of the raw one.
        """
        return v.lower() if isinstance(v, str) else v

    def get_amount(self, field_priority: tuple = ("amount_paid", "amount", "amount_due")) -> Optional[int]:
        """
        Returns the effective billing amount in cents, reading the candidate fields
        in the given order and taking the first that is present.

        The default order preserves the original behaviour for callers that have no
        event type to hand. Callers that do have one should go through
        StripeEvent.get_billing_amount(), which selects the order that matches how
        Stripe populates that particular event.

        Returns None only if every candidate is None. StripeEvent.check_amount_present
        rules that out for invoice events; a bare StripeObject() has no event type to
        apply it with, so Optional[int] is the honest type.
        """
        for name in field_priority:
            value = getattr(self, name, None)
            if value is not None:
                return value
        return None


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
        # `or`, not .get()'s default. Stripe sends the key PRESENT and null on every event
        #     no API request caused — renewals, automatic retries, its own subscription
        #     handling: "request": {"id": null, "idempotency_key": null}. .get(k, default)
        #     falls back only when the key is absent, so it returned None, LedgerEntry
        #     rejected that outside the ValidationError handler, and Stripe got a 500 for
        #     exactly the recurring events a billing ledger exists to record.
        if self.idempotency_key is None:
            self.idempotency_key = (self.request or {}).get("idempotency_key") or self.id
        return self

    @model_validator(mode='after')
    def check_amount_present(self):
        """
        Cross-field rule: an invoice event must carry an amount; a subscription event
        is not asked for one.

        This used to live on StripeObject, which cannot see the event type, so it
        demanded an amount from every object — and a Stripe Subscription has none.
        Every real customer.subscription.* event failed here and went to the DLQ.
        For invoice events the rule is unchanged: no amount field at all is a data
        quality failure, never a $0 row.
        """
        if self.type not in _LIFECYCLE_TYPES and self.get_billing_amount() is None:
            raise ValueError(
                "One of amount_paid, amount_due or amount must be provided and non-null"
            )
        return self

    @model_validator(mode='after')
    def check_invoice_amount_nonzero(self):
        """
        Cross-field rule: invoice events must carry a non-zero amount.
            - invoice.paid with $0 = no revenue received. Data quality failure.
            - invoice.payment_failed with $0 = nothing to retry. Data quality failure.
            - Subscription events are exempt: they carry no amount at all
              (see _LIFECYCLE_TYPES).
        Field(ge=0) cannot express this — it allows zero for all types.
        This validator runs after Field checks, so it has access to self.type.
        """
        invoice_types = {EventType.INVOICE_PAID, EventType.INVOICE_FAILED}
        if self.type in invoice_types:
            amount = self.get_billing_amount()
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

    def get_billing_amount(self) -> Optional[int]:
        """
        The billable amount in cents, read from whichever field this event type
        actually populates (see _AMOUNT_FIELDS_BY_TYPE).

        Returns None for a subscription event, always: it records a state change, not
        money, and is ledgered with no amount (see _LIFECYCLE_TYPES). That holds even
        when the object happens to carry an `amount` field — reading it would post a
        figure nothing collected.

        For an invoice event, None means the event carries no amount field at all.
        Callers must route that to the DLQ rather than defaulting it: a missing amount
        is an unknown amount, and recording an unknown amount as $0 turns a data-quality
        problem into a silent revenue loss that reconciles cleanly and is therefore
        invisible. check_amount_present already rejects that case at validation time.
        """
        if self.type in _LIFECYCLE_TYPES:
            return None
        priority = _AMOUNT_FIELDS_BY_TYPE.get(self.type, _AMOUNT_FIELDS_DEFAULT)
        return self.data.object.get_amount(priority)


class LedgerStatus(str, Enum):
    """
    The states a ledger row can carry. Using an enum instead
    of raw strings prevents typos like "POSETD" from silently entering the DB.
    The _STATUS_MAP below uses EventType keys and LedgerStatus values — both
    are enums, so mypy can verify the mapping is complete.
    """
    POSTED    = "POSTED"     # Revenue confirmed — invoice.paid
    VOID      = "VOID"       # Payment failed — invoice.payment_failed
    LIFECYCLE = "LIFECYCLE"  # Account state changed, no money moved — customer.subscription.*
    # No event type maps to PENDING since Cycle 6 (it was subscription.updated). Kept
    #     because rows written before then carry it, and an enum that cannot name a
    #     stored value turns a historical row into a validation error.
    PENDING   = "PENDING"


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
    one of these. The raw_payload holds every field of the event as received — never
    corrected, never truncated. Humans review DLQ entries; the system never
    assumes a DLQ entry is unimportant.
    to_db() serializes to a tuple matching the dlq table column order exactly.
    """
    # Owning tenant. Defaults so a single-operator deployment behaves exactly as it
    #     did before Phase 13, and so every existing caller keeps working unchanged.
    org_id: str = Field(default=DEFAULT_ORG_ID, min_length=1,
                        description="Owning organisation")

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
        # org_id leads, matching the column order in every dlq INSERT.
        return (
            self.org_id,
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
    # Owning tenant. Defaults so a single-operator deployment behaves exactly as it
    #     did before Phase 13, and so every existing caller keeps working unchanged.
    org_id: str = Field(default=DEFAULT_ORG_ID, min_length=1,
                        description="Owning organisation")

    transaction_id:  str          = Field(min_length=1)           # Stripe event ID — PK
    event_type:      EventType                                     # Validated enum — not a raw string
    customer_id:     str          = Field(min_length=4)            # cus_ prefix validated upstream in StripeEvent
    amount_cents:    Optional[int] = Field(default=None, ge=0)     # Integer cents; None for a lifecycle row
    currency:        str          = Field(pattern=r"^[a-z]{3}$")  # ISO 4217 lowercase
    status:          LedgerStatus                                  # POSTED / VOID / LIFECYCLE
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
            self.org_id,
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

# The DSN the SERVICE runs its traffic on. DATABASE_URL stays the owner connection and
#     is used for one thing only: DDL (alembic, and _bootstrap on a local run).
#
#     Splitting them is what puts row-level security in force. Migration 0006 created
#     the policies and proved they bite, but the application was still connecting as
#     `postgres` — a SUPERUSER, which bypasses RLS entirely regardless of ENABLE or
#     FORCE. Policies that exist and do not apply are worse than no policies: the repo
#     documents a guarantee the database does not provide, and the manual org_id
#     predicates that are the only real defence start to look redundant.
#
#     Defaults to DATABASE_URL so a fresh clone and the test suite keep working with no
#     new configuration. A deployment points it at billing_app, and the startup log says
#     which of the two worlds the process actually came up in (rls_enforced / rls_inert).
APP_DATABASE_URL: str = os.environ.get("APP_DATABASE_URL", "").strip() or DATABASE_URL

# Password migration 0006 assigns to billing_app at creation. A migration cannot carry a
#     secret, so it bootstraps with a known value and the deployment rotates it. Same
#     failure shape as the Stripe placeholder below — a deploy that skips the rotation
#     runs on a credential published in this repository — so it gets the same treatment:
#     loud at startup, not silent at runtime.
_PLACEHOLDER_APP_DB_PASSWORD = "billing_app"


def _read_secret(name: str, default: str = "") -> str:
    """Resolve a secret from `<NAME>_FILE` if set, otherwise from `<NAME>`.

    The _FILE form is the Docker/Kubernetes secrets convention: the orchestrator mounts
    the value as a file and the process reads it. It exists here for a specific reason —
    a container's environment is IMMUTABLE for its lifetime, so a secret delivered by env
    var cannot be rotated without replacing the container. A mounted file can change under
    a running process, which is what makes rotation possible at all.

    A configured but unreadable _FILE raises instead of falling back to the env var.
    Falling back would start the service on the wrong credential and log nothing, which is
    the failure this whole section exists to prevent.
    """
    path = os.environ.get(f"{name}_FILE", "").strip()
    if path:
        try:
            with open(path, encoding="utf-8") as handle:
                return handle.read().strip()
        except OSError as exc:
            raise RuntimeError(
                f"{name}_FILE is set to {path!r} but could not be read: {exc}. "
                f"Refusing to fall back to ${name} — starting on the wrong secret silently "
                f"is worse than not starting."
            ) from exc
    return os.environ.get(name, default)


def _secret_fingerprint(secret: str) -> str:
    """First 12 hex chars of SHA-256, or 'unset'.

    Answers one operational question: is the secret this process is running with the same
    one I think I deployed? Compare against `sha256(value)` computed locally.

    Why it is needed at all: `docker compose restart` does NOT re-read `.env`. It restarts
    the existing container with its original environment, so an operator who edits .env and
    restarts believes the new secret is live when it is not — `docker compose up -d`, which
    recreates the container, is the command that applies it. Without a fingerprint that
    mistake produces no error, no log line and no metric. With one it is visible in both.

    The fingerprint is safe to log and to expose on the key-protected /metrics endpoint for
    a HIGH-ENTROPY secret, which is what the docs demand (openssl rand -hex 32). It is NOT
    safe for a guessable one: publishing a hash of a low-entropy value invites an offline
    dictionary attack. Treating a weak secret as confidential was never going to work
    anyway, but the trade-off belongs in the open.
    """
    if not secret:
        return "unset"
    return hashlib.sha256(secret.encode("utf-8")).hexdigest()[:12]


# Stripe webhook signing secret — sourced from environment or a mounted file, never hardcoded.
#     Get from: Stripe Dashboard → Developers → Webhooks → your endpoint → Signing secret
#     Format: whsec_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
STRIPE_WEBHOOK_SECRET: str = _read_secret("STRIPE_WEBHOOK_SECRET", "whsec_YOUR_SECRET_HERE")

# The default above is published in this repository, and it is a perfectly valid HMAC
#     key: construct_event() verifies against whatever string it is handed. A process
#     running on it does not reject webhooks — it ACCEPTS any webhook signed with this
#     string, which anyone who has read this file can produce. Earlier versions of this
#     comment said the opposite ("will reject ALL incoming requests", "fail hard at
#     startup"), the warning repeated it, and the code only warned.
#
#     The warning below fires in every process that imports this module. The refusal to
#     SERVE traffic on it lives in _enforce_no_demo_credentials(), which runs at
#     application startup — see BILLING_ENV and INGRESS_BIND_ADDR below.
_PLACEHOLDER_SECRET = "whsec_YOUR_SECRET_HERE"
if STRIPE_WEBHOOK_SECRET == _PLACEHOLDER_SECRET:
    import warnings
    warnings.warn(
        "STRIPE_WEBHOOK_SECRET is the placeholder published in this repository. Signatures "
        "made with it VERIFY, so anyone can forge events into the default org. Development "
        "only: unless BILLING_ENV=dev and the ingress is bound to loopback, the service "
        "refuses to start on it.",
        stacklevel=1,
    )

# API key for ops endpoints (/ledger/summary, /dlq/entries).
#     If empty (env var not set), the check is skipped — dev-mode convenience.
#     In production: set to a strong random value, e.g., openssl rand -hex 32.
#     Operational data (customer IDs, amounts, raw payloads) must not be world-readable.
BILLING_API_KEY: str = _read_secret("BILLING_API_KEY", "")

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

# May this process serve traffic on credentials published in this repository?
#
#     Every default above is a working credential: the placeholder signing secret verifies
#     signatures, `dev_key_change_in_production` opens the ops endpoints, and migration
#     0006's `billing_app` password opens the database. docker-compose supplies them so a
#     clone runs with no setup, and the same file used to publish Caddy on 0.0.0.0:80/443 —
#     so the zero-setup path and the exposed path were the same path. The docs said to
#     change them first. Nothing made anyone.
#
#     Two declarations, both required to allow it:
#       BILLING_ENV=dev           the operator says this is a development run. Unset means
#                                 NOT dev: a deployment that forgets to say what it is gets
#                                 the strict behaviour, not the permissive one.
#       INGRESS_BIND_ADDR         where docker-compose binds Caddy's host ports. Compose
#                                 passes the same value here, so publishing the ingress
#                                 beyond loopback revokes dev mode without a second edit.
#     Anything else refuses to start — see _enforce_no_demo_credentials().
BILLING_ENV: str = os.environ.get("BILLING_ENV", "").strip().lower()
INGRESS_BIND_ADDR: str = os.environ.get("INGRESS_BIND_ADDR", "127.0.0.1").strip()

# Values that ship as defaults in docker-compose.yml or .env.example. Length alone does
#     not catch these: a published 40-character key is no more secret than a short one.
_DEMO_API_KEYS = frozenset({"dev_key_change_in_production"})
_MIN_API_KEY_LENGTH = 32  # openssl rand -hex 16 is the floor; the docs ask for -hex 32


def _is_loopback(addr: str) -> bool:
    """True if a bind address is reachable from this host only."""
    addr = addr.strip().strip("[]").lower()
    if addr == "localhost":
        return True
    try:
        return ipaddress.ip_address(addr).is_loopback
    except ValueError:
        return False  # empty, a hostname, garbage: none of them is provably local


def _demo_credentials_allowed(env: str, bind_addr: str) -> bool:
    """Dev mode needs BOTH a declared dev run and an ingress nothing else can reach."""
    return env.strip().lower() == "dev" and _is_loopback(bind_addr)


def _demo_credential_problems(webhook_secret: str, api_key: str, app_dsn: str,
                              any_org_keys: bool, placeholder_orgs: list) -> list:
    """Every credential this process would serve traffic with that is not a secret.

    Pure: the caller supplies the values, so each rule is testable on its own. Returns
    human-readable reasons, never the values themselves.
    """
    problems = []
    if webhook_secret == _PLACEHOLDER_SECRET:
        problems.append("STRIPE_WEBHOOK_SECRET is the placeholder published in this repository")
    elif not webhook_secret.startswith("whsec_"):
        # Empty included: HMAC with an empty key is as forgeable as a published one.
        problems.append("STRIPE_WEBHOOK_SECRET is not a Stripe signing secret (whsec_...)")
    if placeholder_orgs:
        # The secret that verifies a webhook is the org registry's, not the env var's.
        #     Migration 0004 seeds the default org with the placeholder and provision.py
        #     overwrites it — so a deploy that migrates without provisioning still
        #     verifies the legacy route against the published string.
        problems.append("org registry still holds the placeholder signing secret for: "
                        + ", ".join(placeholder_orgs))
    if not api_key and not any_org_keys:
        problems.append("no API key is configured anywhere, so /ledger/summary, /dlq/* "
                        "and /metrics are unauthenticated")
    elif api_key in _DEMO_API_KEYS:
        problems.append("BILLING_API_KEY is the default published in this repository")
    elif api_key and len(api_key) < _MIN_API_KEY_LENGTH:
        problems.append(f"BILLING_API_KEY is shorter than {_MIN_API_KEY_LENGTH} characters")
    if _dsn_password(app_dsn) == _PLACEHOLDER_APP_DB_PASSWORD:
        problems.append("APP_DATABASE_URL uses the bootstrap password migration 0006 publishes")
    return problems

# Which secrets this process actually came up with. An operator can compare these against
#     `sha256(value)` computed locally to confirm a rotation landed, without either value
#     ever appearing in a log. See _secret_fingerprint for why this is worth the line.
_SECRET_FINGERPRINTS = {
    "stripe_webhook_secret": _secret_fingerprint(STRIPE_WEBHOOK_SECRET),
    "billing_api_key":       _secret_fingerprint(BILLING_API_KEY),
    "stripe_secret_source":  "file" if os.environ.get("STRIPE_WEBHOOK_SECRET_FILE") else "env",
    "billing_key_source":    "file" if os.environ.get("BILLING_API_KEY_FILE") else "env",
}
_log.info(
    "secrets loaded stripe=%s(%s) billing_api_key=%s(%s)",
    _SECRET_FINGERPRINTS["stripe_webhook_secret"], _SECRET_FINGERPRINTS["stripe_secret_source"],
    _SECRET_FINGERPRINTS["billing_api_key"],       _SECRET_FINGERPRINTS["billing_key_source"],
    extra={"event": "secrets_loaded", **_SECRET_FINGERPRINTS},
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
        # org registry — mirrors Alembic migration 0004. Created first: ledger references it.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS org (
                org_id                TEXT        PRIMARY KEY,
                name                  TEXT        NOT NULL,
                api_key_sha256        TEXT        UNIQUE,
                stripe_webhook_secret TEXT        NOT NULL,
                active                BOOLEAN     NOT NULL DEFAULT TRUE,
                created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)
        cur.execute(
            # DO UPDATE, not DO NOTHING. For the default org the signing secret IS
            #     STRIPE_WEBHOOK_SECRET by definition, so a stale row would make the
            #     legacy /webhook/stripe route verify against a secret the operator
            #     already rotated away from — and the failure would look like Stripe
            #     sending bad signatures. Other orgs are never touched here; their
            #     secrets live only in the registry.
            "INSERT INTO org (org_id, name, stripe_webhook_secret) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (org_id) DO UPDATE SET stripe_webhook_secret = EXCLUDED.stripe_webhook_secret",
            (DEFAULT_ORG_ID, "Default organisation", STRIPE_WEBHOOK_SECRET),
        )

        # ledger — PRIMARY KEY is (org_id, transaction_id), not transaction_id alone.
        #     transaction_id IS the idempotency guard, and Stripe event ids are unique
        #     per account rather than globally, so a single-column key would let one
        #     tenant's event silently discard another's as a duplicate. Composite key =
        #     per-tenant idempotency, which is what the guard always meant.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ledger (
                org_id          TEXT    NOT NULL DEFAULT 'default' REFERENCES org(org_id),
                transaction_id  TEXT    NOT NULL,
                event_type      TEXT    NOT NULL,
                customer_id     TEXT    NOT NULL,
                amount_cents    BIGINT,
                currency        TEXT    NOT NULL DEFAULT 'usd',
                status          TEXT    NOT NULL,
                idempotency_key TEXT    NOT NULL,
                payload         TEXT    NOT NULL CHECK (length(payload) < 50000),
                created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                CONSTRAINT ledger_amount_matches_kind
                    CHECK ((amount_cents IS NULL) = (event_type LIKE 'customer.subscription.%')),
                PRIMARY KEY (org_id, transaction_id)
            )
        """)
        # amount_cents NULL exactly for subscription rows (migration 0007, _LIFECYCLE_TYPES).
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
                org_id          TEXT        NOT NULL DEFAULT 'default',
                transaction_id  TEXT        NOT NULL,
                FOREIGN KEY (org_id, transaction_id)
                    REFERENCES ledger(org_id, transaction_id) ON DELETE CASCADE,
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
        #     raw_payload is the full event JSON as received (every field; re-serialized,
        #     so not byte-for-byte), kept for replay.
        #     status/retry columns added in Phase 11 — ADD COLUMN IF NOT EXISTS is
        #     idempotent for databases that already existed before Phase 11.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id              BIGSERIAL   PRIMARY KEY,
                org_id          TEXT        NOT NULL DEFAULT 'default',
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
        # Per-tenant delivery destination (migration 0005). NULL = use the worker's
        #     process-wide DOWNSTREAM_URL, so existing deployments are unaffected.
        cur.execute("ALTER TABLE org ADD COLUMN IF NOT EXISTS downstream_url TEXT")
        # org_id on databases created before migration 0004.
        for _t in ("ledger", "outbox", "dlq"):
            cur.execute(f"ALTER TABLE {_t} ADD COLUMN IF NOT EXISTS org_id TEXT "
                        f"NOT NULL DEFAULT '{DEFAULT_ORG_ID}'")

        # Indexes for the two production query patterns:
        #     idx_ledger_customer_id — used by revenue-by-customer GROUP BY queries.
        #     idx_outbox_dispatched_id — partial index covering only undispatched rows;
        #       the outbox worker always queries WHERE dispatched=0 ORDER BY id.
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_ledger_org_customer
            ON ledger(org_id, customer_id)
        """)
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_outbox_org_dispatched
            ON outbox(org_id, dispatched, id)
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
def _tx(conn: "psycopg2.extensions.connection",
        org_scope: str) -> Generator["psycopg2.extensions.cursor", None, None]:
    """
    Explicit transaction context manager for PostgreSQL. Issues BEGIN, yields
    a cursor for the caller's statements, then COMMIT on success or ROLLBACK
    on any exception. Because the connection runs with autocommit=True, we need
    explicit BEGIN to group multiple statements into one atomic transaction.
    This is what makes the transactional outbox atomic: the ledger INSERT and
    the outbox INSERT both succeed or both roll back — no half-written state.

    org_scope establishes the row-level security context for this transaction
    (migration 0006). Pass a tenant to read and write only that tenant's rows; pass
    ALL_ORGS for genuinely cross-tenant jobs. It has no default ON PURPOSE. A default
    would make "scope omitted" indistinguishable from "scope considered and chosen",
    which is the shape of the mistake this whole mechanism exists to catch — the
    2026-08-11 batch bug was a forgotten org_id, not a wrong one. Required means the
    omission is visible at the call site instead of at runtime.

    SET LOCAL, never SET. The setting dies with the transaction. A plain SET would
    outlive it and leak one request's tenant into whatever borrows the same pooled
    connection next — a cross-tenant read caused by the mechanism meant to prevent one.

    Reads go through here too, and not only because SET LOCAL needs a transaction to
    be local to: a multi-query report wrapped in one _tx returns one snapshot instead
    of three readings taken at three different instants.
    """
    cur = conn.cursor()
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
    EventType.INVOICE_PAID:   LedgerStatus.POSTED,     # Revenue confirmed
    EventType.INVOICE_FAILED: LedgerStatus.VOID,       # Payment failed, recorded at amount_due
    EventType.SUB_CREATED:    LedgerStatus.LIFECYCLE,  # No money — see _LIFECYCLE_TYPES
    EventType.SUB_DELETED:    LedgerStatus.LIFECYCLE,
    EventType.SUB_UPDATED:    LedgerStatus.LIFECYCLE,
}

# Outcomes that settle a DLQ entry on replay: the event is in the ledger now, because
#     this replay wrote it or because an earlier delivery already had. Derived from
#     LedgerStatus, not listed. The two retry paths each kept a literal tuple, so adding
#     LIFECYCLE to the status map alone would have let a replayed subscription event
#     reach the ledger while its DLQ entry went on retrying — the ledger and the DLQ
#     each telling a different story about the same event.
_RESOLVING_OUTCOMES: frozenset = frozenset({s.value for s in LedgerStatus} | {"DLQ_DUPLICATE"})


def process_stripe_event(
    conn: "psycopg2.extensions.connection",
    event: dict,
    from_retry: bool = False,
    org_id: str = DEFAULT_ORG_ID,
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
      POSTED     — new invoice.paid, written to the ledger with its amount
      VOID       — new invoice.payment_failed, written at its amount_due
      LIFECYCLE  — new customer.subscription.*, written with no amount
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
                org_id=org_id,
                transaction_id=transaction_id,
                reason=DLQReason.INVALID,
                raw_payload=event,
            )
            if not _write_dlq(conn, dlq_entry):
                # The DLQ row is the only place this event would exist: it failed
                #     validation, so the ledger never saw it. Answering 200 tells Stripe to
                #     stop delivering an event nothing recorded, and the log line
                #     _write_dlq left lives in a container file nothing mounts. Raising
                #     makes the handler answer 503, so Stripe redelivers once the database
                #     is back.
                raise RuntimeError(f"DLQ write failed for invalid event {transaction_id}")
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
    # get_billing_amount() reads the field this event type actually populates,
    #     so a failed invoice is recorded at its amount_due, not at amount_paid=0.
    amount_cents  = validated_event.get_billing_amount()
    customer_id   = data_object.customer
    currency      = data_object.currency     # already lowercased at the entry boundary
    ledger_status = _STATUS_MAP[event_type]   # type-safe — KeyError impossible here
    payload_json  = json.dumps(event)

    ledger_entry = LedgerEntry(
        org_id=org_id,
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
        with _tx(conn, org_id) as cur:
            cur.execute(
                """
                INSERT INTO ledger
                  (org_id, transaction_id, event_type, customer_id, amount_cents,
                   currency, status, idempotency_key, payload, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (org_id, transaction_id) DO NOTHING
                """,
                ledger_entry.to_db(),
            )
            inserted = cur.rowcount  # 1=new row inserted, 0=ON CONFLICT fired (duplicate)

            if inserted == 1:
                # Only write outbox row for genuinely new events — not for duplicates.
                #     Same transaction = zero dual-write gap between ledger and outbox.
                cur.execute(
                    """
                    INSERT INTO outbox (org_id, transaction_id, event_type, payload, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                    """,
                    (org_id, transaction_id, event_type.value, payload_json, now),
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
                org_id=org_id,
                transaction_id=transaction_id,
                reason=DLQReason.DUPLICATE,
                raw_payload=event,
            )
            # A lost DUPLICATE row costs an audit line, not an event: the ledger already
            #     holds this one. So a failed write stays log-only and the answer stays 200 —
            #     a 503 here would only make Stripe redeliver what is already recorded.
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


def _write_dlq(conn: "psycopg2.extensions.connection", entry: DLQEntry) -> bool:
    """
    Append a DLQ row. Returns True if it was written, False if it was not. Never raises:
    whether a lost DLQ row is fatal depends on what else holds the event, and only the
    caller knows that.

    On failure the full raw payload is logged at ERROR ("DLQ write failed"), to stderr
    and billing_ledger.log. That log is a last resort, not a guarantee: in the compose
    deployment the file lives inside the container and nothing mounts it, so recreating
    the container deletes it. This function used to call itself the "zero data loss"
    guarantee and return nothing, so an invalid event whose DLQ write failed still got
    HTTP 200 — Stripe stopped delivering an event that existed only in that file.
    process_stripe_event now turns False into a 503 for exactly that case.
    """
    try:
        # entry.org_id, not a caller-supplied scope: the row being written is the only
        #     authority on which tenant it belongs to. Under RLS a mismatch between the
        #     two is refused by WITH CHECK rather than silently written to the wrong
        #     tenant, which is precisely what the batch path did on 2026-08-11.
        with _tx(conn, entry.org_id) as cur:
            cur.execute(
                "INSERT INTO dlq (org_id, transaction_id, reason, raw_payload, received_at,"
                " status, retry_count, max_retries, next_retry_at)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                entry.to_db(),
            )
        return True
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
        return False


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
    # ALL_ORGS: the claim is the one genuinely cross-tenant step here. A worker drains
    #     every tenant's backlog, so scoping this to one org would leave the rest
    #     unclaimed forever. Everything the loop does WITH a claimed row is scoped back
    #     down to that row's own tenant below.
    with _tx(conn, ALL_ORGS) as cur:
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
            RETURNING id, transaction_id, raw_payload, org_id
            """,
            (claim_timeout_seconds, batch_size),
        )
        rows = cur.fetchall()

    attempted = 0
    for row in rows:
        dlq_id, transaction_id, raw_payload_str, row_org_id = row[0], row[1], row[2], row[3]
        attempted += 1
        try:
            payload = json.loads(raw_payload_str)
            # from_retry=True: this payload is being replayed, so a DLQ_DUPLICATE outcome
            #     must not append another DLQ row. Without it every retry of an entry whose
            #     event already reached the ledger left one more pending row behind than it
            #     consumed, and each of those was itself retry-eligible.
            # org_id comes from the DLQ row, not from a default. Replaying without it
            #     writes the ledger row under 'default' while the entry is marked
            #     resolved — the tenant's ledger stays empty and the DLQ claims the
            #     event was reprocessed. Wrong tenant credited, and a resolved entry
            #     asserting a side effect that never happened for its owner.
            result = process_stripe_event(conn, payload, from_retry=True, org_id=row_org_id)
            outcome = result.get("outcome", "")
            if outcome in _RESOLVING_OUTCOMES:
                # row_org_id, not ALL_ORGS. The claim above had to see every tenant;
                #     resolving one row does not, and narrowing back means a bug that
                #     resolved the wrong row updates zero rows instead of somebody
                #     else's entry.
                with _tx(conn, row_org_id) as cur:
                    cur.execute(
                        "UPDATE dlq SET status='resolved', claimed_at=NULL WHERE id=%s",
                        (dlq_id,),
                    )
                _log.info("dlq_retry resolved id=%s transaction_id=%s", dlq_id, transaction_id)
            else:
                _increment_retry(conn, dlq_id, transaction_id, row_org_id)
        except Exception as exc:
            _log.error("dlq_retry error id=%s error=%r", dlq_id, exc)
            # The recovery UPDATE can fail for the same reason the retry did — the database
            #     is still unreachable. Unguarded, that exception escaped this loop and left
            #     every remaining claimed row of the batch stuck in 'retrying'. Contain it
            #     per row so one bad row cannot abort the rest of the batch.
            try:
                _increment_retry(conn, dlq_id, transaction_id, row_org_id)
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
    org_scope: str,
) -> None:
    """Atomically increment retry_count and set status/next_retry_at in one UPDATE.

    org_scope is the tenant that owns dlq_id. It is a parameter rather than ALL_ORGS
    because both callers already know it — retry_dlq_batch reads it back from the claim,
    the manual endpoint derives it from the API key — and a function that writes to a
    single row by id has no reason to hold a cross-tenant scope while it does so.

    Single statement eliminates the TOCTOU race: the old SELECT + UPDATE pattern
    let two concurrent callers both read retry_count=N and both write N+1 instead of N+2.

    The WHERE clause carries `AND status <> 'resolved'` because this is the one place
    that writes dlq.status without checking what it currently is, which made 'resolved'
    reversible. The trigger is narrow but real: in retry_dlq_batch, process_stripe_event
    can succeed and the following `UPDATE ... status='resolved'` can still fail on a
    transient database error - precisely the failure the TRANSIENT reason exists for.
    The except handler then calls this function unconditionally, and without the guard it
    reopened an entry whose money was already in the ledger, pushing it back to 'pending'
    or, at the retry ceiling, to 'exhausted' - the state documented as needing human
    intervention. An operator would then investigate an event that was never broken.
    A no-op returns row=None, the same path already used for "row not found".
    """
    with _tx(conn, org_scope) as cur:
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
            WHERE id = %s AND status <> 'resolved'
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
    # ALL_ORGS: retention applies to every tenant. Scoped to one org this DELETE would
    #     silently stop pruning the moment a second tenant existed, and the table it is
    #     supposed to bound would grow without one.
    with _tx(conn, ALL_ORGS) as cur:
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
    org_id: str = DEFAULT_ORG_ID,
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
                # org_id=org_id, not the model default. A DLQ row written under the
                #     wrong tenant is not merely mislabelled: the retry paths trust
                #     dlq.org_id to decide where a replay lands, so a row stamped
                #     'default' lets the default tenant see another tenant's raw
                #     payload and replay it into its own ledger — the same Stripe
                #     event POSTED under two tenants.
                DLQEntry(org_id=org_id, transaction_id=tid,
                         reason=DLQReason.INVALID, raw_payload=event).to_db()
            )
            results[idx] = {
                "outcome":        "DLQ_INVALID",
                "transaction_id": tid,
                "reason":         f"Pydantic validation failed: {e.error_count()} errors",
            }
            continue
        data_obj = validated.data.object
        entry = LedgerEntry(
            org_id=org_id,
            transaction_id=validated.id,
            event_type=validated.type,
            customer_id=data_obj.customer,
            amount_cents=validated.get_billing_amount(),  # per-event-type field, see above
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
        with _tx(conn, org_id) as cur:
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
                      (org_id, transaction_id, event_type, customer_id, amount_cents,
                       currency, status, idempotency_key, payload, created_at)
                    VALUES %s
                    ON CONFLICT (org_id, transaction_id) DO NOTHING
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
                        (entry.org_id, entry.transaction_id, entry.event_type.value,
                         entry.payload, now)
                    )
                    results[idx] = {
                        "outcome":        entry.status.value,
                        "transaction_id": entry.transaction_id,
                        "reason":         None,
                    }
                else:
                    dup_dlq_rows.append(
                        DLQEntry(
                            org_id=org_id,          # see the INVALID branch above
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
                    " (org_id, transaction_id, event_type, payload, created_at) VALUES %s",
                    outbox_rows,
                    page_size=page_size,
                )

            all_dlq = invalid_dlq_rows + dup_dlq_rows
            if all_dlq:
                execute_values(
                    cur,
                    "INSERT INTO dlq"
                    " (org_id, transaction_id, reason, raw_payload, received_at,"
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

@asynccontextmanager
async def _lifespan(_app: FastAPI):
    """Startup checks that must pass before the first request. Resolved at call time:
    _enforce_no_demo_credentials needs the pool, which is built further down."""
    _enforce_no_demo_credentials()
    yield


app = FastAPI(title="Micro-Billing-Ledger PoC", version="1.0.0", lifespan=_lifespan)

# Rate limiter — keyed by client IP. Prevents a single caller from exhausting
#     the connection pool (maxconn=20) or flooding the DLQ table.
#     Limits are applied per-route via @limiter.limit() decorator below.
#     429 Too Many Requests is returned when the limit is exceeded.
_limiter = Limiter(key_func=get_remote_address)
app.state.limiter = _limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# Limits are configurable so they can be tuned per deployment — and so a load test can
#     measure the ingest path instead of measuring the limiter. The defaults are the
#     values that were previously hardcoded, so behaviour is unchanged unless someone
#     opts in. Raising RATE_LIMIT_WEBHOOK on a network-reachable deployment removes the
#     protection against a flood exhausting the connection pool; benchmark.py sets it
#     deliberately and locally, which is the only use that makes sense.
RATE_LIMIT_WEBHOOK = os.getenv("RATE_LIMIT_WEBHOOK", "100/minute")
RATE_LIMIT_OPS     = os.getenv("RATE_LIMIT_OPS",     "20/minute")
RATE_LIMIT_METRICS = os.getenv("RATE_LIMIT_METRICS", "60/minute")

if RATE_LIMIT_WEBHOOK != "100/minute":
    _log.warning(
        "RATE_LIMIT_WEBHOOK is %s, not the 100/minute default — intended for local "
        "benchmarking. A raised limit on a reachable deployment lets a flood exhaust "
        "the connection pool and grow outbox_pending without bound.",
        RATE_LIMIT_WEBHOOK,
    )

# ---------------------------------------------------------------------------
# Metrics
# Counters and a latency histogram, exported at GET /metrics.
#     Counters only ever increase, so a scrape that is missed or arrives late loses
#     no information — the next one still carries the true total. A gauge sampled at
#     scrape time cannot say that, which is why dlq_depth and outbox_pending are read
#     from the database on each scrape rather than tracked incrementally in memory:
#     an in-memory counter of queue depth drifts from the table the moment anything
#     else writes to it, including the worker, and a drifting depth is worse than none.
# ---------------------------------------------------------------------------
_WEBHOOK_EVENTS = Counter(
    "webhook_events_total",
    "Webhook events by terminal outcome",
    ["outcome"],
)
_HTTP_REQUESTS = Counter(
    "http_requests_total",
    "HTTP requests by route template and status code",
    ["method", "path", "status"],
)
_HTTP_LATENCY = Histogram(
    "http_request_duration_seconds",
    "Request latency by route template",
    ["method", "path"],
)
_DLQ_DEPTH = Gauge("dlq_depth", "DLQ rows still needing action (pending, retrying or exhausted)")
_DLQ_TOTAL = Gauge("dlq_total", "All DLQ rows ever written, resolved ones included")

# Fingerprints of the secrets this process is running with, and where each came from.
#     Constant for the life of the process — that is the point. Alert on it changing
#     unexpectedly (a deploy you did not make) or failing to change after a rotation you
#     did make. The second case is the silent one: `docker compose restart` does not
#     re-read .env, so a rotation can appear applied while the old secret stays live.
#     Safe to expose here because /metrics requires the API key and a SHA-256 prefix of a
#     high-entropy secret is not reversible. Not safe if the secret is guessable — see
#     _secret_fingerprint.
_SECRETS_INFO = Info("billing_secrets", "Fingerprint and source of the secrets in use")
_SECRETS_INFO.info(_SECRET_FINGERPRINTS)
_OUTBOX_PENDING = Gauge("outbox_pending", "Outbox rows not yet dispatched downstream")

# The three gauges above hold PER-TENANT values in process-wide objects, so writing them
#     and rendering them has to be one indivisible step. Without this, two tenants
#     scraping at once could each be served the other's queue depth. See metrics().
_METRICS_RENDER_LOCK = threading.Lock()


@app.middleware("http")
async def _observability(request: Request, call_next):
    """Emit one structured log line and one metrics sample per request.

    Labels use the route TEMPLATE (/dlq/{dlq_id}/retry), never the resolved path.
    Labelling by resolved path gives every dlq_id its own time series, which turns a
    handful of routes into unbounded cardinality and is the standard way to take down
    a Prometheus server with your own instrumentation.
    """
    start = time.perf_counter()
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - start) * 1000, 2)

    route = request.scope.get("route")
    path = getattr(route, "path", None) or "unmatched"
    status_code = str(response.status_code)

    _HTTP_REQUESTS.labels(request.method, path, status_code).inc()
    _HTTP_LATENCY.labels(request.method, path).observe(duration_ms / 1000.0)

    _log.info(
        "request %s %s -> %s", request.method, path, status_code,
        extra={
            "event":       "http_request",
            "method":      request.method,
            "path":        path,
            "status":      response.status_code,
            "duration_ms": duration_ms,
            "client_ip":   get_remote_address(request),
        },
    )
    return response

# Ensure all three tables and their indexes exist before the pool opens connections.
#     The bootstrap connection is closed immediately — it is not reused.
#
#     Skipped when the service runs as the restricted role. _bootstrap issues DDL, which
#     billing_app deliberately cannot do; running it would fail on the first CREATE. More
#     to the point, a deployment that separates the two DSNs should not ship owner
#     credentials to the application container at all — a process that cannot authenticate
#     as the owner cannot drop a table or bypass RLS even if its own credentials leak.
#     The schema is alembic's job there (the `migrate` service), which is already a
#     completion gate for the billing service in docker-compose.yml.
if APP_DATABASE_URL == DATABASE_URL:
    _bootstrap_conn = _bootstrap()
    _bootstrap_conn.close()
else:
    _log.info(
        "skipping bootstrap DDL — running as the application role; schema is alembic's",
        extra={"event": "bootstrap_skipped"},
    )

# Thread-safe connection pool for the FastAPI route handlers.
#     minconn=1 — always keep one connection warm to avoid cold-start latency on first request.
#     maxconn=20 — cap concurrent DB connections; tune to your PostgreSQL max_connections budget.
#     In tests, replace _pool with a _MockPool that wraps the test connection (see test_ledger.py).
#     minconn EQUALS maxconn deliberately. psycopg2 returns a connection to the idle pool
#     only while `len(self._pool) < self.minconn` — every other release calls conn.close()
#     (see AbstractConnectionPool._putconn). With the previous minconn=1 the pool retained
#     exactly ONE connection: under concurrency each request opened a fresh PostgreSQL
#     connection and closed it again on release, and because _connect() runs inside
#     getconn while the pool lock is held, that connect/close storm serialized every
#     caller. Measured on this machine: 23 TPS at 1 client, 15 at 2, then 1 TPS at 4 with
#     p50 latency of 4.2 SECONDS — and the app's own middleware confirmed the time was
#     spent inside the process, not in the network. After the change: 61 TPS at 4 and
#     143 TPS at 16, p50 under 100ms throughout.
#     Size to the PostgreSQL max_connections budget: every worker process holds this many.
#     APP_DATABASE_URL, not DATABASE_URL: every connection that serves traffic runs as the
#     restricted role, which is what makes the row-level security policies apply at all.
class _AutocommitPool(ThreadedConnectionPool):
    """A ThreadedConnectionPool whose connections run with autocommit=True.

    _tx() opens its transactions with a literal BEGIN and states in its own docstring
    that it does so "because the connection runs with autocommit=True". That was true of
    _bootstrap() and of worker._connect(), and false of every connection this pool has
    ever handed out: psycopg2 defaults autocommit to False, so the driver opened an
    implicit transaction on the first statement and _tx's BEGIN landed inside one that
    was already running. PostgreSQL answers that with

        WARNING:  there is already a transaction in progress

    on every request that reads before it writes — the webhook path does exactly that,
    resolving the tenant's signing secret before processing the event. Nothing was
    corrupted, because psycopg2's putconn rolls a connection back before re-pooling and
    SET LOCAL still applies to the transaction that is open. But the transaction started
    earlier than the code believes, so it spanned the org lookup and the HMAC
    verification, and the warning stream shares the log channel the RLS acceptance check
    greps — a genuine rls_inert CRITICAL is harder to see buried in it.

    ThreadedConnectionPool offers no hook for connection setup, so this overrides the one
    method that creates a raw connection. It runs for every connection the pool opens,
    including replacements, which a one-off loop over the initial connections would miss.
    """

    def _connect(self, key=None):
        conn = super()._connect(key)
        conn.autocommit = True
        return conn


DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "20"))
_pool: ThreadedConnectionPool = _AutocommitPool(
    minconn=DB_POOL_SIZE, maxconn=DB_POOL_SIZE, dsn=APP_DATABASE_URL
)


def _any_org_keys() -> bool:
    """True if any active org has registered an API key.

    Decides between dev mode (no credential configured anywhere, everything is the
    default org) and enforced auth. Read once at startup rather than per request: an
    org is provisioned at deploy time, and a query on every call would put the auth
    check on the hot path. Restart after provisioning the first tenant — the startup
    log line says which mode the process came up in.

    No org_scope, and not by omission: migration 0006 puts policies on ledger, outbox and
    dlq only. The registry is the map FROM a credential TO a tenant, so a session would
    have to already know which tenant it is to read it — the scope would have to come from
    the answer. billing_app holds SELECT and nothing else on this table.
    """
    _c = _pool.getconn()
    try:
        with _c.cursor() as cur:
            cur.execute("SELECT EXISTS (SELECT 1 FROM org WHERE api_key_sha256 IS NOT NULL AND active)")
            return bool(cur.fetchone()[0])
    except Exception:  # noqa: BLE001 — a missing org table means a pre-0004 database
        return False
    finally:
        _pool.putconn(_c)


def _rls_status() -> dict:
    """Report whether row-level security is actually in force for THIS connection.

    A policy that exists but does not apply is the worst of both worlds: the repository
    documents a guarantee, the manual org_id predicates start to look redundant, and
    nothing enforces anything. That is not hypothetical — the first attempt at migration
    0006 shipped exactly that state and the full suite passed, because a SUPERUSER
    bypasses RLS regardless of ENABLE or FORCE.

    So the process states, out loud and at startup, which of the two worlds it is in.

    Reads pg_user and pg_policies, never a tenant table, so it needs no scope of its own.
    """
    _c = _pool.getconn()
    try:
        with _c.cursor() as cur:
            cur.execute("SELECT current_user, "
                        "(SELECT usesuper FROM pg_user WHERE usename = current_user)")
            user, is_super = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM pg_policies "
                        "WHERE tablename IN ('ledger','outbox','dlq')")
            policies = cur.fetchone()[0]
    except Exception:  # noqa: BLE001 — a pre-0006 database has no policies to report
        return {"db_user": "unknown", "policies": 0, "enforced": False}
    finally:
        _pool.putconn(_c)
    return {"db_user": user, "superuser": bool(is_super),
            "policies": policies, "enforced": bool(policies) and not is_super}


_ANY_ORG_KEYS: bool = _any_org_keys()


def _dsn_password(dsn: str) -> str:
    """The password embedded in a libpq URI, or '' if there is none to read.

    A malformed DSN is not this function's problem — psycopg2 will have failed to
    connect long before anyone asks. Returning '' means "nothing to warn about".
    """
    from urllib.parse import urlsplit
    try:
        return urlsplit(dsn).password or ""
    except ValueError:
        return ""


_RLS = _rls_status()
if _dsn_password(APP_DATABASE_URL) == _PLACEHOLDER_APP_DB_PASSWORD:
    _log.warning(
        "APP_DATABASE_URL still carries the bootstrap password migration 0006 assigns to "
        "%s. That value is published in this repository's migration file, so anyone who "
        "can reach PostgreSQL can authenticate as the application role and read every "
        "tenant's rows — RLS scopes what a session may see, it does not decide who may "
        "open one. Rotate it before this database is reachable from a network: "
        "ALTER ROLE %s PASSWORD '<openssl rand -hex 32>', then update APP_DATABASE_URL.",
        _PLACEHOLDER_APP_DB_PASSWORD, _PLACEHOLDER_APP_DB_PASSWORD,
        extra={"event": "app_db_password_is_placeholder"},
    )
if _RLS["policies"] and not _RLS["enforced"]:
    _log.critical(
        "row-level security policies exist but DO NOT APPLY — connected as %r, which is "
        "a superuser and bypasses RLS. Tenant isolation currently depends entirely on "
        "every query carrying its own org_id predicate. Connect as a non-superuser role "
        "(migration 0006 creates billing_app) to make the database enforce it.",
        _RLS.get("db_user"),
        extra={"event": "rls_inert", **_RLS},
    )
elif _RLS["enforced"]:
    _log.info("row-level security in force as %s", _RLS.get("db_user"),
              extra={"event": "rls_enforced", **_RLS})
_log.info(
    "tenancy mode=%s", "multi-tenant" if _ANY_ORG_KEYS else "single-org",
    extra={"event": "tenancy_mode", "multi_tenant": _ANY_ORG_KEYS,
           "default_org": DEFAULT_ORG_ID},
)


def _orgs_with_placeholder_secret(conn: "psycopg2.extensions.connection") -> list:
    """Active orgs whose signing secret is the published placeholder or empty.

    Inactive orgs are left out on purpose: _org_webhook_secret() refuses them, so a
    placeholder on one cannot verify anything. Unscoped for the same reason as
    _any_org_keys — `org` carries no RLS policy and billing_app may read it.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT org_id FROM org WHERE active "
            "AND (stripe_webhook_secret = %s OR stripe_webhook_secret = '') ORDER BY org_id",
            (_PLACEHOLDER_SECRET,),
        )
        return [row[0] for row in cur.fetchall()]


def _enforce_no_demo_credentials() -> None:
    """Refuse to serve traffic on published credentials, unless this is declared dev.

    Runs from the application lifespan, not at import: worker.py and the test suite
    import this module too, and neither serves HTTP — the worker has no use for an API
    key, so refusing it one would stop the wrong process. uvicorn runs the lifespan
    before it accepts a connection, and a raise here makes it exit ("Application startup
    failed"), so an exposed deployment on demo values never answers a single request.
    """
    conn = _pool.getconn()
    try:
        placeholder_orgs = _orgs_with_placeholder_secret(conn)
    finally:
        _pool.putconn(conn)
    problems = _demo_credential_problems(STRIPE_WEBHOOK_SECRET, BILLING_API_KEY,
                                         APP_DATABASE_URL, _ANY_ORG_KEYS, placeholder_orgs)
    if not problems:
        return
    if _demo_credentials_allowed(BILLING_ENV, INGRESS_BIND_ADDR):
        _log.warning(
            "running on demo credentials (BILLING_ENV=dev, ingress on %s): %s",
            INGRESS_BIND_ADDR, "; ".join(problems),
            extra={"event": "demo_credentials_in_use", "problems": problems},
        )
        return
    _log.critical(
        "refusing to serve traffic on demo credentials: %s", "; ".join(problems),
        extra={"event": "demo_credentials_refused", "problems": problems,
               "billing_env": BILLING_ENV or "unset", "ingress_bind_addr": INGRESS_BIND_ADDR},
    )
    raise RuntimeError(
        "refusing to start: " + "; ".join(problems) + ". Set real secrets (see "
        ".env.example), or declare a local run with BILLING_ENV=dev and INGRESS_BIND_ADDR "
        "on a loopback address."
    )


def _lookup_org_by_key(conn: "psycopg2.extensions.connection", api_key: str) -> Optional[str]:
    """Return the org that owns this API key, or None.

    Compares SHA-256 digests: the registry never stores the key itself, so a database
    dump does not hand over working credentials.

    Unscoped for the reason given in _any_org_keys: `org` carries no RLS policy, and this
    lookup is what establishes the scope every other query then runs under.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT org_id FROM org WHERE api_key_sha256 = %s AND active",
            (hashlib.sha256(api_key.encode("utf-8")).hexdigest(),),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _org_webhook_secret(conn: "psycopg2.extensions.connection", org_id: str) -> Optional[str]:
    """The Stripe signing secret registered for this tenant, or None if unknown/inactive.

    Read per request rather than cached. A cache would save one indexed lookup on the
    ingest path, but it would also mean a rotated signing secret keeps verifying against
    the old value until the entry expires — and the whole point of resolving the secret
    per tenant is that the signature is the proof of which tenant sent the event. Correct
    first; benchmark.py exists to say whether the lookup is worth optimising.

    Unscoped: `org` carries no RLS policy (see _any_org_keys). The org_id here comes from
    the URL and is an untrusted routing hint until the signature checks out — which is
    exactly what this read exists to make possible.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT stripe_webhook_secret FROM org WHERE org_id = %s AND active",
            (org_id,),
        )
        row = cur.fetchone()
    return row[0] if row else None


def _require_api_key(x_api_key: str = Header(default="")) -> str:
    """FastAPI dependency for ops endpoints. Returns the caller's org_id.

    The tenant is DERIVED from the credential; it is never accepted from the caller.
    An X-Org-Id header validated against a shared key would not be an isolation
    boundary at all — anyone holding that key could read another tenant's data by
    editing a header. Whoever presents the key IS the org, and that is the only claim
    about identity this system trusts.

    Resolution order:
      1. BILLING_API_KEY empty and no org has registered a key — dev mode, everything
         is DEFAULT_ORG_ID. The startup warning already says the endpoints are open.
      2. The presented key matches a registered org — that org.
      3. The presented key matches BILLING_API_KEY — DEFAULT_ORG_ID. This keeps
         single-operator deployments working exactly as before Phase 13.
      4. Otherwise 401.

    Operational endpoints expose customer IDs, amounts and raw payloads, so the key is
    what stands between a tenant and everyone else's revenue data.
    """
    if not BILLING_API_KEY and not _ANY_ORG_KEYS:
        return DEFAULT_ORG_ID  # dev mode — no credential configured anywhere
    if not x_api_key:
        raise HTTPException(status_code=401, detail="X-Api-Key header required")

    if _ANY_ORG_KEYS:
        conn = _pool.getconn()
        try:
            org_id = _lookup_org_by_key(conn, x_api_key)
        finally:
            _pool.putconn(conn)
        if org_id:
            return org_id

    # compare_digest, not ==. String equality short-circuits on the first differing
    #     byte, which leaks the length of a correct prefix to anyone timing the response.
    if BILLING_API_KEY and hmac.compare_digest(x_api_key, BILLING_API_KEY):
        return DEFAULT_ORG_ID

    raise HTTPException(status_code=401, detail="Invalid API key")


def _require_operator_key(x_api_key: str = Header(default="")) -> None:
    """FastAPI dependency for /metrics: the operator's key, never a tenant's.

    /metrics renders process-wide state — webhook and HTTP counters summed over every
    tenant, and billing_secrets_info, a fingerprint of the operator's own secrets — so
    it cannot be scoped to a caller. It used to accept any key _require_api_key
    accepted: every tenant read every other tenant's traffic volume and the operator's
    secret fingerprints. The operator credential is BILLING_API_KEY. A tenant key gets
    403 (authenticated, not authorised); no key, or a wrong one, gets 401.

    Dev mode (no key configured anywhere) stays open, like every ops endpoint. A
    deployment where tenants hold keys but BILLING_API_KEY is empty has no operator
    credential, so /metrics answers 403 to everyone until one is set.
    """
    if not BILLING_API_KEY and not _ANY_ORG_KEYS:
        return  # dev mode — the startup warning already says the endpoints are open
    if not x_api_key:
        raise HTTPException(status_code=401, detail="X-Api-Key header required")
    if BILLING_API_KEY and hmac.compare_digest(x_api_key, BILLING_API_KEY):
        return
    if _ANY_ORG_KEYS:
        conn = _pool.getconn()
        try:
            is_tenant = _lookup_org_by_key(conn, x_api_key) is not None
        finally:
            _pool.putconn(conn)
        if is_tenant:
            raise HTTPException(status_code=403, detail="/metrics requires the operator key")
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
    # Optional, because Stripe documents it as nullable. `dict` with a default only covers
    #     an ABSENT key; an explicit null failed validation here, before the signature
    #     check and before anything could reach the DLQ — a 422 Stripe retries for three
    #     days and that no counter in this service records.
    request: Optional[dict] = None


def _ingest_webhook(request: Request, payload: StripeWebhookPayload, org_id: str) -> JSONResponse:
    """Verify the signature with the tenant's own secret, then ingest under that tenant.

    Resolving the org from the URL and verifying with THAT org's secret is
    self-validating: a signature only checks out against the account that produced the
    event, so a caller cannot post into a tenant it does not hold the secret for. The
    path segment is a routing hint, not a trusted claim.

    An unknown or inactive org returns exactly what a bad signature returns. Anything
    else — a 404, a different message — turns this endpoint into an org enumerator for
    anyone on the internet. The distinction is recorded in the log, where operators can
    see it and attackers cannot.
    """
    raw_body   = request._body
    sig_header = request.headers.get("stripe-signature", "")

    conn = _pool.getconn()
    try:
        secret = _org_webhook_secret(conn, org_id)
        if secret is None:
            _log.warning(
                "webhook for unknown or inactive org org_id=%s", org_id,
                extra={"event": "webhook_unknown_org", "org_id": org_id},
            )
            _WEBHOOK_EVENTS.labels("UNKNOWN_ORG").inc()
            raise HTTPException(status_code=400, detail="Invalid or missing Stripe signature")

        try:
            stripe.Webhook.construct_event(raw_body, sig_header, secret)
        except stripe.error.SignatureVerificationError:
            raise HTTPException(status_code=400, detail="Invalid or missing Stripe signature")

        try:
            # The body Stripe signed, parsed — not payload.model_dump(). The HTTP model
            #     declares four fields and drops every other one, so `created`, `livemode`,
            #     `api_version`, `object` and `pending_webhooks` never reached the ledger,
            #     the outbox or the DLQ, while the docs said the raw payload was preserved.
            #     The signature was verified over these exact bytes, and FastAPI has
            #     already parsed them as a JSON object to build `payload`, so this cannot
            #     fail where that succeeded. Stored via json.dumps: every field, though
            #     not byte-for-byte (whitespace and escaping are re-rendered).
            result = process_stripe_event(conn, json.loads(raw_body), org_id=org_id)
        except RuntimeError as exc:
            # A DB-level failure — including a DLQ row that could not be written for an
            #     invalid event (see process_stripe_event). 503 tells Stripe to retry,
            #     which is correct for a transient error; 200 would say "got it" when
            #     nothing kept it.
            _WEBHOOK_EVENTS.labels("DB_ERROR").inc()
            raise HTTPException(status_code=503, detail=str(exc))
    finally:
        _pool.putconn(conn)

    outcome = result.get("outcome", "UNKNOWN")
    _WEBHOOK_EVENTS.labels(outcome).inc()
    _log.info(
        "webhook processed %s", outcome,
        extra={
            "event":          "webhook_processed",
            "org_id":         org_id,
            "transaction_id": result.get("transaction_id"),
            "outcome":        outcome,
            "reason":         result.get("reason"),
        },
    )
    return JSONResponse(content=result)


@app.post("/webhook/stripe/{org_id}", status_code=status.HTTP_200_OK)
@_limiter.limit(RATE_LIMIT_WEBHOOK)
def stripe_webhook_for_org(request: Request, org_id: str,
                           payload: StripeWebhookPayload) -> JSONResponse:
    """
    Per-tenant ingest. Point each Stripe account's webhook at its own path; the
    signature is verified with that tenant's registered signing secret, so the URL
    cannot be used to write into someone else's ledger.
    """
    return _ingest_webhook(request, payload, org_id)


@app.post("/webhook/stripe", status_code=status.HTTP_200_OK)
@_limiter.limit(RATE_LIMIT_WEBHOOK)
def stripe_webhook(request: Request, payload: StripeWebhookPayload) -> JSONResponse:
    """
    Single-tenant ingest, kept so existing Stripe endpoints keep working unchanged.
    Routes to DEFAULT_ORG_ID, whose registered secret is seeded from
    STRIPE_WEBHOOK_SECRET at bootstrap.

    Always returns HTTP 200 for valid JSON — including duplicates and invalid events
    that end up in the DLQ. A non-200 tells Stripe to retry, which is only correct for
    transient failures (DB down -> 503).
    """
    return _ingest_webhook(request, payload, DEFAULT_ORG_ID)


@app.get("/ledger/summary")
@_limiter.limit(RATE_LIMIT_OPS)
def ledger_summary(request: Request, org_id: str = Depends(_require_api_key)) -> JSONResponse:
    """
    Quick sanity-check endpoint. Returns row counts per ledger status, DLQ depth,
    and outbox pending count. Requires X-Api-Key header when BILLING_API_KEY is set.
    sync def: FastAPI dispatches sync handlers to its thread pool via run_in_threadpool.
    psycopg2 blocking calls do not block the asyncio event loop.
    """
    conn = _pool.getconn()
    try:
        # One transaction for all three reads, not three autocommit statements. Two
        #     reasons, and both are load-bearing. RLS: SET LOCAL is local to a
        #     transaction, so a read outside one carries no scope and default-deny
        #     returns nothing. Consistency: the three numbers now come from a single
        #     snapshot, where before the worker could drain the outbox between the
        #     second query and the third and produce a summary describing no instant
        #     that ever existed.
        with _tx(conn, org_id) as cur:
            # GROUP BY status gives {POSTED: N, VOID: N, LIFECYCLE: N} in one query.
            cur.execute("SELECT status, COUNT(*) FROM ledger WHERE org_id=%s GROUP BY status",
                        (org_id,))
            counts = {row[0]: row[1] for row in cur.fetchall()}

            # Two questions, two numbers. dlq_depth answers "how much still needs
            #     attention" - what an operator reads to judge ingestion health - so it
            #     excludes resolved rows. Counting every row ever written made it grow
            #     monotonically with historical traffic and never come back down, because
            #     prune_dlq only ages out DUPLICATE rows: a resolved INVALID or TRANSIENT
            #     entry inflated "queue depth" forever. dlq_total keeps the audit count.
            cur.execute(
                "SELECT COUNT(*) FILTER (WHERE status IN ('pending','retrying','exhausted')), "
                "       COUNT(*) FROM dlq WHERE org_id=%s", (org_id,)
            )
            dlq_depth, dlq_total = cur.fetchone()

            cur.execute("SELECT COUNT(*) FROM outbox WHERE dispatched=0 AND org_id=%s", (org_id,))
            outbox_pending = cur.fetchone()[0]
    finally:
        _pool.putconn(conn)

    return JSONResponse(content={
        "org_id":         org_id,
        "ledger":         counts,
        "dlq_depth":      dlq_depth,      # Entries still needing action
        "dlq_total":      dlq_total,      # Every DLQ row ever written, resolved included
        "outbox_pending": outbox_pending, # Events not yet dispatched downstream
    })


@app.get("/metrics")
@_limiter.limit(RATE_LIMIT_METRICS)
def metrics(request: Request, _operator: None = Depends(_require_operator_key)) -> Response:
    """
    Prometheus exposition endpoint, for the operator (see _require_operator_key).

    Point Prometheus at it with a custom header. `authorization:` does NOT work here:
    Prometheus sends it as `Authorization: <type> <credentials>`, and this service
    reads X-Api-Key, so the example this docstring used to give got a 401 on every
    scrape. `http_headers` sends an arbitrary header:

        scrape_configs:
          - job_name: billing
            http_headers:
              X-Api-Key:
                files: [/etc/prom/billing.key]

    The two queue gauges are read from the database on each scrape rather than kept
    in memory. The worker drains the outbox and prunes the DLQ from a different
    process, so any in-memory count would silently diverge from the tables it claims
    to describe — and a queue depth that is confidently wrong is worse than absent.
    """
    # Operator-wide: every tenant's backlog. A per-tenant view is /ledger/summary.
    conn = _pool.getconn()
    try:
        with _tx(conn, ALL_ORGS) as cur:
            cur.execute(
                "SELECT COUNT(*) FILTER (WHERE status IN ('pending','retrying','exhausted')), "
                "       COUNT(*) FROM dlq"
            )
            _actionable, _total = cur.fetchone()
            cur.execute("SELECT COUNT(*) FROM outbox WHERE dispatched=0")
            _pending = cur.fetchone()[0]
    finally:
        _pool.putconn(conn)

    # The three queue gauges are per-tenant values living in PROCESS-WIDE objects, and
    #     generate_latest() renders whatever they hold at render time — not what this
    #     request computed. Setting them and rendering as two unsynchronised steps let a
    #     second tenant's scrape land in between, so a tenant asking for its own numbers
    #     got somebody else's: reproduced with tenants of depth 2 and 9, where the first
    #     was served 9. The SQL was correctly scoped the whole time; the leak was one
    #     line later, in shared process state that row-level security cannot reach.
    #
    #     The lock makes set-then-render atomic with respect to other requests, which is
    #     the whole of the defect. It is held across no I/O — the database work is done
    #     above and the render is pure CPU over in-memory counters — and /metrics is
    #     rate-limited to RATE_LIMIT_METRICS, so serialising it costs nothing measurable.
    #
    #     Labelling the gauges by org_id would NOT fix this. generate_latest() emits every
    #     series in the registry, so each tenant would then read every other tenant's
    #     depth from its own response — the same leak, made deterministic.
    #
    #     The modelling question this comment used to leave open — operator endpoint or
    #     tenant one — is settled: operator (_require_operator_key). The gauges are now
    #     operator-wide, so the race the lock prevents can no longer cross tenants; the
    #     lock stays because two scrapes still share these process-wide objects.
    with _METRICS_RENDER_LOCK:
        _DLQ_DEPTH.set(_actionable)
        _DLQ_TOTAL.set(_total)
        _OUTBOX_PENDING.set(_pending)
        body = generate_latest()

    return Response(content=body, media_type=CONTENT_TYPE_LATEST)


@app.get("/dlq/entries")
@_limiter.limit(RATE_LIMIT_OPS)
def dlq_entries(request: Request, limit: int = 50,
                org_id: str = Depends(_require_api_key)) -> JSONResponse:
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
        with _tx(conn, org_id) as cur:
            cur.execute(
                "SELECT id, transaction_id, reason, raw_payload, received_at "
                "FROM dlq WHERE org_id=%s ORDER BY id DESC LIMIT %s",
                (org_id, min(limit, 1000)),  # cap at 1000 — never dump the entire table
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
@_limiter.limit(RATE_LIMIT_OPS)
def dlq_retry(request: Request, dlq_id: int,
              org_id: str = Depends(_require_api_key)) -> JSONResponse:
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
        with _tx(conn, org_id) as cur:
            cur.execute(
                # org_id in the WHERE clause, not checked after the fetch. A tenant must
                #     not be able to learn that another tenant's dlq id exists, and a
                #     retry is a WRITE — it replays a payload into the ledger. Scoping the
                #     lookup makes another tenant's entry indistinguishable from a
                #     nonexistent one: both are 404.
                "SELECT transaction_id, raw_payload, status FROM dlq WHERE id=%s AND org_id=%s",
                (dlq_id, org_id),
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
        # Same reason as retry_dlq_batch: the replay must land in the tenant that
        #     owns the entry, which the scoped SELECT above has already established.
        result = process_stripe_event(conn, payload, from_retry=True, org_id=org_id)
        outcome = result.get("outcome", "")

        if outcome in _RESOLVING_OUTCOMES:
            with _tx(conn, org_id) as cur:
                cur.execute(
                    "UPDATE dlq SET status='resolved', claimed_at=NULL WHERE id=%s",
                    (dlq_id,),
                )
            _log.info("dlq manual retry resolved id=%s transaction_id=%s", dlq_id, transaction_id)
        else:
            _increment_retry(conn, dlq_id, transaction_id, org_id)

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
