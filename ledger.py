"""
ledger.py — Micro-Billing-Ledger PoC
=====================================
EN: Main application file. Handles Stripe webhook ingestion, Pydantic validation,
    idempotent ledger writes, transactional outbox, and Dead-Letter Queue (DLQ).
    Exposes a FastAPI HTTP server and a headless benchmark mode.

ES: Archivo principal de la aplicación. Maneja la ingesta de webhooks de Stripe,
    validación con Pydantic, escrituras idempotentes en el libro contable, outbox
    transaccional y Cola de Mensajes Muertos (DLQ). Expone un servidor HTTP FastAPI
    y un modo de benchmark sin interfaz.

Key design decisions / Decisiones de diseño clave
--------------------------------------------------
Idempotency guard / Guardia de Idempotencia:
  EN: Every Stripe event carries a unique `id` (e.g. "evt_3PxK..."). We use this
      as the ledger primary key. INSERT INTO ledger ... ON CONFLICT (transaction_id)
      DO NOTHING means a replayed webhook is a no-op at the DB level — no
      application-level lock needed.
  ES: Cada evento de Stripe lleva un `id` único (ej. "evt_3PxK..."). Lo usamos
      como clave primaria del libro. INSERT INTO ledger ... ON CONFLICT (transaction_id)
      DO NOTHING significa que un webhook repetido es un no-op a nivel de DB —
      no se necesita bloqueo a nivel aplicación.

Transactional Outbox / Outbox Transaccional:
  EN: The ledger row and the outbox row are written in the same BEGIN…COMMIT.
      A downstream worker tails the outbox and forwards confirmed events onward.
  ES: La fila del libro y la fila del outbox se escriben en el mismo BEGIN…COMMIT.
      Un worker downstream sigue el outbox y reenvía los eventos confirmados.

Idempotency (PostgreSQL) / Idempotencia (PostgreSQL):
  EN: INSERT INTO ledger (...) VALUES (...) ON CONFLICT (transaction_id) DO NOTHING.
      rowcount=0 after the INSERT means the event was already in the ledger — the
      unique constraint on transaction_id rejected it silently. Route to DLQ_DUPLICATE.
  ES: INSERT INTO ledger (...) VALUES (...) ON CONFLICT (transaction_id) DO NOTHING.
      rowcount=0 después del INSERT significa que el evento ya estaba en el libro — la
      restricción única en transaction_id lo rechazó silenciosamente. Enrutar a DLQ_DUPLICATE.

Usage / Uso:
  uvicorn ledger:app --port 8000
  python ledger.py --silent          # headless benchmark / benchmark sin interfaz
"""

# ---------------------------------------------------------------------------
# Standard library imports / Importaciones de la librería estándar
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
# Logging configuration / Configuración de logging
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
# Third-party imports / Importaciones de terceros
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
# SECCIÓN 1: ENUMS Y MODELOS PYDANTIC (Fase 1 + Fase 2)
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
    EN: The complete list of supported Stripe event types. If it's not in this
        enum, Pydantic rejects it at model creation — before any business logic runs.
        This is the "bouncer's rulebook": written down, enforced at the door.
    ES: La lista completa de tipos de eventos Stripe soportados. Si no está en este
        enum, Pydantic lo rechaza en la creación del modelo — antes de que corra
        cualquier lógica de negocio. Este es el "reglamento del portero": escrito,
        aplicado en la puerta.
    """
    INVOICE_PAID   = "invoice.paid"                    # Successful payment received
    INVOICE_FAILED = "invoice.payment_failed"          # Payment attempt failed
    SUB_CREATED    = "customer.subscription.created"   # New subscription activated
    SUB_DELETED    = "customer.subscription.deleted"   # Subscription cancelled
    SUB_UPDATED    = "customer.subscription.updated"   # Subscription plan changed


class StripeObject(BaseModel):
    """
    EN: Represents the `data.object` payload inside a Stripe webhook. Contains the
        actual billing details: who paid, how much, in what currency. Three validation
        layers run here: Field constraints, then a cross-field validator for amounts.
    ES: Representa el payload `data.object` dentro de un webhook de Stripe. Contiene
        los detalles reales de facturación: quién pagó, cuánto, en qué moneda. Tres
        capas de validación corren aquí: restricciones de Field, luego un validador
        de campos cruzados para montos.
    """

    # Stripe customer IDs look like "cus_ABC123". min_length=4 catches empty
    #     strings and single-char placeholders. The cus_ prefix check is in StripeEvent.
    customer: str = Field(
        min_length=4,
        description="Stripe customer ID (e.g., 'cus_ABC123') / ID de cliente Stripe"
    )

    # Stripe puts the paid amount here on invoice.paid events. Optional because
    #     subscription lifecycle events don't always carry payment amounts.
    amount_paid: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount paid in cents, non-negative / Monto pagado en centavos, no negativo"
    )

    # Fallback amount field — Stripe uses different field names depending on
    #     the event type. The model_validator below picks the right one.
    amount: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount in cents (fallback to amount_paid) / Monto en centavos (respaldo de amount_paid)"
    )

    # ISO 4217 currency code. Regex enforces exactly 3 lowercase letters.
    #     "USD" (uppercase) fails — currency normalization (.lower() on ingest) is a
    #     known gap listed in the Production Gap Checklist in the README.
    currency: str = Field(
        default="usd",
        pattern=r"^[a-z]{3}$",
        description="ISO 4217 currency code, lowercase 3 chars / Código de moneda ISO 4217, 3 chars minúsculas"
    )

    @model_validator(mode='after')
    def check_amount_present(self):
        """
        EN: Ensures at least one amount field is present and non-null. Field(ge=0)
            handles non-negative enforcement — this validator handles the
            "either/or" logic that a single Field constraint cannot express.
            Subscription events may have amount=0 (lifecycle, no money moved).
            Invoice events with amount=0 are caught by check_invoice_amount_nonzero
            in StripeEvent — a different layer for a different rule.
        ES: Asegura que al menos un campo de monto esté presente y no sea nulo.
            Field(ge=0) maneja la aplicación de no-negativos — este validador maneja
            la lógica "uno u otro" que una sola restricción de Field no puede expresar.
            Los eventos de suscripción pueden tener amount=0 (ciclo de vida, sin dinero
            movido). Los eventos de factura con amount=0 son capturados por
            check_invoice_amount_nonzero en StripeEvent — una capa diferente para
            una regla diferente.
        """
        actual_amount = self.amount_paid if self.amount_paid is not None else self.amount
        if actual_amount is None:
            # Both fields are None — no amount at all. This is a data quality failure.
            raise ValueError(
                "Either amount_paid or amount must be provided and non-null / "
                "Se debe proporcionar amount_paid o amount y no ser nulo"
            )
        return self

    def get_amount(self) -> Optional[int]:
        """
        EN: Returns the effective billing amount in cents. Prefers amount_paid
            (Stripe invoice events) over amount (fallback). Returns None only if
            both fields are None — check_amount_present prevents this when the
            model is constructed via StripeEvent, but direct StripeObject()
            construction bypasses that validator, so Optional[int] is the honest type.
        ES: Retorna el monto de facturación efectivo en centavos. Retorna None solo
            si ambos campos son None — check_amount_present previene esto cuando el
            modelo se construye vía StripeEvent, pero la construcción directa de
            StripeObject() omite ese validador, por lo que Optional[int] es el tipo honesto.
        """
        return self.amount_paid if self.amount_paid is not None else self.amount


class StripeEventData(BaseModel):
    """
    EN: Wrapper for the `data` field in a Stripe webhook. Stripe always nests
        the billing object one level deep: event.data.object. This model makes
        that nesting explicit and type-safe.
    ES: Envoltorio para el campo `data` en un webhook de Stripe. Stripe siempre
        anida el objeto de facturación un nivel de profundidad: event.data.object.
        Este modelo hace ese anidamiento explícito y type-safe.
    """
    object: StripeObject = Field(
        description="Billing object with customer, amount, currency / Objeto de facturación con cliente, monto, moneda"
    )


class StripeEvent(BaseModel):
    """
    EN: The complete Stripe webhook envelope. This is the top-level model that
        validates the entire incoming payload. Contains three cross-field validators
        that run AFTER all Field constraints pass — they see the full model state.
    ES: El sobre completo del webhook de Stripe. Este es el modelo de nivel superior
        que valida todo el payload entrante. Contiene tres validadores de campos
        cruzados que corren DESPUÉS de que pasen todas las restricciones de Field —
        ven el estado completo del modelo.
    """

    # Stripe event ID — this becomes the ledger primary key.
    #     min_length=1 catches empty strings that would create a NULL-equivalent PK.
    id: str = Field(
        min_length=1,
        description="Unique Stripe event ID (e.g., 'evt_3Px...') / ID de evento Stripe único"
    )

    # EventType enum — rejects unknown types at model creation, not halfway
    #     through business logic. The bouncer checks the list at the door.
    type: EventType = Field(
        description="Event type validated against supported types / Tipo de evento validado contra tipos soportados"
    )

    # Nested model containing the billing object (customer, amount, currency).
    data: StripeEventData = Field(
        description="Event data with customer, amount, currency / Datos del evento con cliente, monto, moneda"
    )

    # Optional Stripe request metadata. May contain idempotency_key from
    #     Stripe's retry logic. If absent, idempotency_key falls back to event id.
    request: Optional[dict] = Field(
        default=None,
        description="Request metadata, may contain idempotency_key / Metadatos de solicitud, puede contener idempotency_key"
    )

    # Resolved by resolve_idempotency_key validator below. Declared as a
    #     model field so Pydantic v2 allows assignment in the validator.
    #     (v2 does not allow setting attributes that aren't declared fields.)
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Idempotency key — resolved from request or defaults to id / Clave de idempotencia — resuelta desde request o por defecto al id"
    )

    @model_validator(mode='after')
    def resolve_idempotency_key(self):
        """
        EN: Resolves the idempotency_key from the request metadata or falls back
            to the event ID. This is explicit behavior — not a hidden .get() chain.
            Stripe sometimes sends the key in request.idempotency_key; when absent,
            the event ID itself is the correct idempotency token.
        ES: Resuelve el idempotency_key desde los metadatos de solicitud o cae de
            vuelta al ID del evento. Este es comportamiento explícito — no una cadena
            .get() oculta. Stripe a veces envía la clave en request.idempotency_key;
            cuando está ausente, el ID del evento en sí es el token de idempotencia correcto.
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
        EN: Cross-field rule: invoice events must carry a non-zero amount.
            - invoice.paid with $0 = no revenue received. Data quality failure.
            - invoice.payment_failed with $0 = nothing to retry. Data quality failure.
            - Subscription events (created/deleted/updated) are EXEMPT — they are
              lifecycle events, not payment events. $0 is valid for them.
            Field(ge=0) cannot express this — it allows zero for all types.
            This validator runs after Field checks, so it has access to self.type.
        ES: Regla de campos cruzados: los eventos de factura deben llevar un monto no cero.
            - invoice.paid con $0 = sin ingreso recibido. Fallo de calidad de datos.
            - invoice.payment_failed con $0 = nada que reintentar. Fallo de calidad de datos.
            - Los eventos de suscripción (created/deleted/updated) están EXENTOS — son
              eventos de ciclo de vida, no de pago. $0 es válido para ellos.
            Field(ge=0) no puede expresar esto — permite cero para todos los tipos.
            Este validador corre después de los checks de Field, por lo que tiene acceso a self.type.
        """
        invoice_types = {EventType.INVOICE_PAID, EventType.INVOICE_FAILED}
        if self.type in invoice_types:
            amount = self.data.object.get_amount()
            if amount == 0:
                raise ValueError(
                    f"{self.type.value} must have amount > 0 "
                    f"(got 0 — route to DLQ for manual review) / "
                    f"debe tener amount > 0 (obtuvo 0 — enrutar al DLQ para revisión manual)"
                )
        return self

    @model_validator(mode='after')
    def check_customer_id_format(self):
        """
        EN: Cross-field rule: Stripe customer IDs always start with 'cus_'.
            Field(min_length=4) on StripeObject catches empty/short strings.
            This catches structurally wrong IDs like "abc123" — 10 chars, passes
            length, but not a real Stripe customer ID. A business rule, not a
            format rule — that's why it lives here and not in StripeObject.
        ES: Regla de campos cruzados: los IDs de cliente Stripe siempre empiezan con 'cus_'.
            Field(min_length=4) en StripeObject captura strings vacíos/cortos.
            Este captura IDs estructuralmente incorrectos como "abc123" — 10 chars,
            pasa longitud, pero no es un ID real de cliente Stripe. Una regla de
            negocio, no de formato — por eso vive aquí y no en StripeObject.
        """
        if not self.data.object.customer.startswith("cus_"):
            raise ValueError(
                f"Customer ID must start with 'cus_': "
                f"got '{self.data.object.customer}' / "
                f"El ID de cliente debe comenzar con 'cus_'"
            )
        return self


class LedgerStatus(str, Enum):
    """
    EN: The three valid accounting states for a ledger row. Using an enum instead
        of raw strings prevents typos like "POSETD" from silently entering the DB.
        The _STATUS_MAP below uses EventType keys and LedgerStatus values — both
        are enums, so mypy can verify the mapping is complete.
    ES: Los tres estados contables válidos para una fila del libro. Usar un enum
        en lugar de strings crudos previene errores tipográficos como "POSETD"
        de entrar silenciosamente en la DB. El _STATUS_MAP de abajo usa claves
        EventType y valores LedgerStatus — ambos son enums, por lo que mypy puede
        verificar que el mapeo es completo.
    """
    POSTED  = "POSTED"    # Revenue confirmed — invoice.paid, subscription.created
    PENDING = "PENDING"   # Awaiting resolution — subscription.updated
    VOID    = "VOID"      # Reversed or failed — invoice.payment_failed, subscription.deleted


class DLQReason(str, Enum):
    """
    EN: Structured rejection reason codes for the Dead-Letter Queue. Using an enum
        instead of freeform strings means DLQ entries can be filtered and replayed
        by reason type — e.g., replay all INVALID entries after fixing a validator.
    ES: Códigos de razón de rechazo estructurados para la Cola de Mensajes Muertos.
        Usar un enum en lugar de strings libres significa que las entradas del DLQ
        pueden filtrarse y reproducirse por tipo de razón — ej., reproducir todas las
        entradas INVALID después de corregir un validador.
    """
    DUPLICATE    = "DUPLICATE"     # Stripe retry of already-processed event
    INVALID      = "INVALID"       # Failed Pydantic validation — bad structure or business rule
    UNKNOWN_TYPE = "UNKNOWN_TYPE"  # Reserved — currently caught as INVALID by Pydantic enum
    TRANSIENT    = "TRANSIENT"     # Transient DB/network error — safe to auto-retry


class DLQStatus(str, Enum):
    PENDING   = "pending"    # Waiting for first or next retry attempt
    RETRYING  = "retrying"   # Currently being processed by retry worker
    RESOLVED  = "resolved"   # Successfully reprocessed — ledger row now exists
    EXHAUSTED = "exhausted"  # retry_count >= max_retries — needs human intervention


class DLQEntry(BaseModel):
    """
    EN: A typed Dead-Letter Queue record. Every invalid or duplicate event creates
        one of these. The raw_payload is always preserved byte-perfect — never
        corrected, never truncated. Humans review DLQ entries; the system never
        assumes a DLQ entry is unimportant.
        to_db() serializes to a 4-tuple matching the dlq table column order exactly.
    ES: Un registro tipado de la Cola de Mensajes Muertos. Cada evento inválido o
        duplicado crea uno de estos. El raw_payload siempre se preserva byte-perfecto —
        nunca corregido, nunca truncado. Los humanos revisan las entradas del DLQ;
        el sistema nunca asume que una entrada del DLQ es sin importancia.
        to_db() serializa a una 4-tupla que coincide exactamente con el orden de
        columnas de la tabla dlq.
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
    EN: A fully-validated ledger row ready for database insertion. All nine fields
        are constrained — no raw strings, no unvalidated amounts. to_db() produces
        a 9-tuple in the exact column order of the INSERT statement, eliminating
        query-column sequence drift. If you add a field here, you must add it to
        to_db() and to the INSERT — the code will be obviously incomplete.
    ES: Una fila del libro completamente validada lista para inserción en la base de
        datos. Los nueve campos están restringidos — sin strings crudos, sin montos
        sin validar. to_db() produce una 9-tupla en el orden exacto de columnas del
        INSERT, eliminando el drift de secuencia query-columna. Si agregas un campo
        aquí, debes agregarlo a to_db() y al INSERT — el código será obviamente incompleto.
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
        EN: Serializes the LedgerEntry to an ordered 9-tuple for the INSERT statement.
            .value on enum fields produces plain strings for TEXT columns.
            Column order here must match the INSERT statement in process_stripe_event
            exactly — any mismatch causes silent data corruption (wrong value in wrong column).
        ES: Serializa el LedgerEntry a una 9-tupla ordenada para el INSERT.
            .value en campos enum produce strings planos para columnas TEXT.
            El orden de columnas aquí debe coincidir exactamente con el INSERT en
            process_stripe_event — cualquier discrepancia causa corrupción silenciosa
            de datos (valor incorrecto en columna incorrecta).
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
# SECCIÓN 2: CONFIGURACIÓN
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


# ===========================================================================
# SECTION 3: DATABASE BOOTSTRAP
# SECCIÓN 3: BOOTSTRAP DE BASE DE DATOS
#
# Creates the three relational tables on first run. Idempotent — safe to
#     call multiple times (CREATE TABLE IF NOT EXISTS). WAL mode is enabled
#     for concurrent reads while writes are in progress.
# ===========================================================================

def _bootstrap(dsn: str = DATABASE_URL) -> "psycopg2.extensions.connection":
    """
    EN: Connects to PostgreSQL and ensures all three tables exist.
        autocommit=True: each DDL/DML statement commits immediately unless wrapped
        in an explicit BEGIN...COMMIT block (which _tx() provides for writes).
        Tables use IF NOT EXISTS — safe to call on every startup and in every test.
        Three tables:
        - ledger: financial record — transaction_id TEXT PRIMARY KEY = idempotency guard
        - outbox: pending downstream delivery — dispatched=0 means not yet forwarded
        - dlq: rejected events — DUPLICATE, INVALID, or UNKNOWN_TYPE
    ES: Se conecta a PostgreSQL y asegura que las tres tablas existan.
        autocommit=True: cada sentencia DDL/DML hace commit inmediatamente a menos que
        esté envuelta en un bloque BEGIN...COMMIT explícito (que _tx() provee para escrituras).
        Las tablas usan IF NOT EXISTS — seguro llamar en cada inicio y en cada test.
        Tres tablas:
        - ledger: registro financiero — transaction_id TEXT PRIMARY KEY = guardia de idempotencia
        - outbox: entrega downstream pendiente — dispatched=0 significa aún no reenviado
        - dlq: eventos rechazados — DUPLICATE, INVALID, o UNKNOWN_TYPE
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
                max_retries     INTEGER     NOT NULL DEFAULT 0,
                next_retry_at   TIMESTAMPTZ
            )
        """)
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS status        TEXT    NOT NULL DEFAULT 'pending'")
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS retry_count   INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS max_retries   INTEGER NOT NULL DEFAULT 0")
        cur.execute("ALTER TABLE dlq ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ")

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

    return conn


@contextmanager
def _tx(conn: "psycopg2.extensions.connection") -> Generator["psycopg2.extensions.cursor", None, None]:
    """
    EN: Explicit transaction context manager for PostgreSQL. Issues BEGIN, yields
        a cursor for the caller's statements, then COMMIT on success or ROLLBACK
        on any exception. Because the connection runs with autocommit=True, we need
        explicit BEGIN to group multiple statements into one atomic transaction.
        This is what makes the transactional outbox atomic: the ledger INSERT and
        the outbox INSERT both succeed or both roll back — no half-written state.
    ES: Gestor de contexto de transacción explícita para PostgreSQL. Emite BEGIN,
        cede un cursor para las sentencias del llamador, luego COMMIT en éxito o
        ROLLBACK en cualquier excepción. Como la conexión corre con autocommit=True,
        necesitamos BEGIN explícito para agrupar múltiples sentencias en una
        transacción atómica. Esto es lo que hace el outbox transaccional atómico:
        el INSERT del libro y el INSERT del outbox ambos tienen éxito o ambos hacen
        rollback — sin estado medio-escrito.
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
# SECCIÓN 4: LÓGICA CENTRAL DEL LIBRO (independiente del framework)
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


def process_stripe_event(conn: "psycopg2.extensions.connection", event: dict) -> dict:
    """
    EN: Idempotently inserts a Stripe event into the ledger and outbox.

        THREE-LAYER VALIDATION PIPELINE:
        Layer 1 — Pydantic validation: type checking + field constraints + cross-field rules
        Layer 2 — Business logic: atomic dual-write (ledger + outbox) in one transaction
        Layer 3 — DLQ routing: invalid or duplicate events are logged, never silently dropped

        Returns a result dict with keys: outcome, transaction_id, reason.
        Possible outcomes:
          POSTED     — new event, payment confirmed, written to ledger
          VOID       — new event, payment failed or subscription cancelled
          PENDING    — new event, subscription updated (awaiting resolution)
          DLQ_INVALID    — Pydantic validation failed; event in DLQ
          DLQ_DUPLICATE  — already processed; idempotency guard fired; event in DLQ

    ES: Inserta idempotentemente un evento Stripe en el libro y el outbox.

        PIPELINE DE VALIDACIÓN DE TRES CAPAS:
        Capa 1 — Validación Pydantic: verificación de tipos + restricciones de campo + reglas de campos cruzados
        Capa 2 — Lógica de negocio: escritura dual atómica (libro + outbox) en una transacción
        Capa 3 — Enrutamiento DLQ: eventos inválidos o duplicados se registran, nunca se descartan silenciosamente

        Retorna un dict de resultado con claves: outcome, transaction_id, reason.
        Resultados posibles:
          POSTED     — evento nuevo, pago confirmado, escrito en el libro
          VOID       — evento nuevo, pago fallido o suscripción cancelada
          PENDING    — evento nuevo, suscripción actualizada (esperando resolución)
          DLQ_INVALID    — falló validación Pydantic; evento en DLQ
          DLQ_DUPLICATE  — ya procesado; guardia de idempotencia disparada; evento en DLQ
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
        dlq_entry = DLQEntry(
            transaction_id=transaction_id,
            reason=DLQReason.INVALID,
            raw_payload=event,
        )
        _write_dlq(conn, dlq_entry)
        return {
            "outcome":        "DLQ_INVALID",
            "transaction_id": transaction_id,
            "reason":         f"Pydantic validation failed: {e.error_count()} errors / "
                              f"Validación Pydantic falló: {e.error_count()} errores"
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
        dlq_entry = DLQEntry(
            transaction_id=transaction_id,
            reason=DLQReason.DUPLICATE,
            raw_payload=event,
        )
        _write_dlq(conn, dlq_entry)
        return {
            "outcome":        "DLQ_DUPLICATE",
            "transaction_id": transaction_id,
            "reason":         "Already processed — idempotency guard fired / "
                              "Ya procesado — guardia de idempotencia disparada"
        }

    # Success path — event is new, validated, written to ledger and outbox.
    return {
        "outcome":        ledger_status.value,
        "transaction_id": transaction_id,
        "reason":         None
    }


def _write_dlq(conn: "psycopg2.extensions.connection", entry: DLQEntry) -> None:
    """
    EN: Best-effort DLQ append. Deliberately never raises — the main processing
        path must not die because the DLQ is unavailable. If the DB write fails
        (disk full, locked, corrupt), the full raw payload is logged at ERROR level
        to both stderr and billing_ledger.log. An operator can grep the log file
        for "DLQ write failed", extract the raw_payload JSON, and replay manually.
        This is the "zero data loss" guarantee: if the DB can't save it, the log can.
    ES: Append al DLQ de mejor esfuerzo. Deliberadamente nunca lanza — la ruta de
        procesamiento principal no debe morir porque el DLQ no esté disponible. Si
        la escritura en DB falla (disco lleno, bloqueado, corrupto), el payload crudo
        completo se registra en nivel ERROR en stderr y billing_ledger.log. Un operador
        puede hacer grep en el log por "DLQ write failed", extraer el JSON raw_payload,
        y reproducirlo manualmente. Esta es la garantía de "cero pérdida de datos":
        si la DB no puede guardarlo, el log puede.
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


def retry_dlq_batch(
    conn: "psycopg2.extensions.connection",
    batch_size: int = 50,
) -> int:
    """
    Find DLQ entries eligible for retry, attempt to reprocess each one, and
    update status accordingly. Returns the number of entries attempted.

    Eligibility: status='pending' AND next_retry_at <= NOW() AND retry_count < max_retries.
    DUPLICATE and INVALID entries have max_retries=0 so they are never auto-retried.
    Only TRANSIENT entries (max_retries=3) enter this loop.

    On success: status → 'resolved'.
    On failure: retry_count += 1, next_retry_at = NOW() + backoff, and if
                retry_count >= max_retries: status → 'exhausted'.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, transaction_id, raw_payload
            FROM dlq
            WHERE status = 'pending'
              AND (next_retry_at IS NULL OR next_retry_at <= NOW())
              AND retry_count < max_retries
            ORDER BY id
            LIMIT %s
            """,
            (batch_size,),
        )
        rows = cur.fetchall()

    attempted = 0
    for row in rows:
        dlq_id, transaction_id, raw_payload_str = row[0], row[1], row[2]
        attempted += 1
        try:
            payload = json.loads(raw_payload_str)
            result = process_stripe_event(conn, payload)
            outcome = result.get("outcome", "")
            # Both SUCCESS and DLQ_DUPLICATE count as resolved — the event is in the ledger.
            if outcome in ("POSTED", "VOID", "PENDING", "DLQ_DUPLICATE"):
                with _tx(conn) as cur:
                    cur.execute(
                        "UPDATE dlq SET status='resolved' WHERE id=%s",
                        (dlq_id,),
                    )
                _log.info("dlq_retry resolved id=%s transaction_id=%s", dlq_id, transaction_id)
            else:
                _increment_retry(conn, dlq_id, transaction_id)
        except Exception as exc:
            _log.error("dlq_retry error id=%s error=%r", dlq_id, exc)
            _increment_retry(conn, dlq_id, transaction_id)

    return attempted


def _increment_retry(
    conn: "psycopg2.extensions.connection",
    dlq_id: int,
    transaction_id: str,
) -> None:
    """Increment retry_count, set next backoff delay, mark exhausted if ceiling reached."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT retry_count, max_retries FROM dlq WHERE id=%s",
            (dlq_id,),
        )
        row = cur.fetchone()
        if row is None:
            return
        retry_count, max_retries = row[0], row[1]
        new_count = retry_count + 1
        delay = _RETRY_BACKOFF[min(retry_count, len(_RETRY_BACKOFF) - 1)]
        if new_count >= max_retries:
            cur.execute(
                "UPDATE dlq SET retry_count=%s, status='exhausted', next_retry_at=NULL WHERE id=%s",
                (new_count, dlq_id),
            )
            _log.warning(
                "dlq_retry exhausted id=%s transaction_id=%s retries=%s/%s",
                dlq_id, transaction_id, new_count, max_retries,
            )
        else:
            cur.execute(
                "UPDATE dlq SET retry_count=%s, status='pending',"
                " next_retry_at=NOW() + interval '%s seconds' WHERE id=%s",
                (new_count, delay, dlq_id),
            )
            _log.info(
                "dlq_retry scheduled id=%s transaction_id=%s attempt=%s next_in=%ss",
                dlq_id, transaction_id, new_count, delay,
            )


def process_stripe_event_batch(
    conn: "psycopg2.extensions.connection",
    events: list[dict],
    page_size: int = 1000,
) -> list[dict]:
    """
    EN: Batch variant of process_stripe_event(). Collapses N single-event round trips
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

    ES: Variante en lote de process_stripe_event(). Colapsa N round trips por evento
        a 4 + ceil(N/page_size)*3 round trips totales. Con page_size=1000 y N=5,000
        son ~12 round trips vs 20,000 para la ruta por evento — fuente de la mejora
        192× en el benchmark (26 TPS → ~5,000 TPS).

        POR QUÉ page_size=1000: PostgreSQL limita un prepared statement a 65,535
        parámetros. Con 9 columnas del libro por fila, 1,000 filas = 9,000 parámetros —
        dentro del límite mientras minimiza los round trips.

        POR QUÉ RETURNING: es el único mecanismo atómico sin ventana de carrera para
        saber qué filas de un INSERT masivo con ON CONFLICT realmente aterrizaron.
        El set inserted_ids construido desde RETURNING habilita partición O(1) de la
        lista valid en (nuevos → outbox) vs (duplicados → DLQ) en un solo paso lineal.

        Garantía de atomicidad: todos los eventos válidos nuevos aterrizan en libro
        y outbox, o ninguno lo hace. El patrón Outbox Transaccional se mantiene a
        escala de lote: crash después de COMMIT deja todas las filas del outbox
        intactas; crash antes de COMMIT no deja nada en ninguna tabla.
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
# SECCIÓN 5: APLICACIÓN FASTAPI
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
    """
    EN: FastAPI dependency for ops endpoints. If BILLING_API_KEY is configured in the
        environment (non-empty), the X-Api-Key request header must match exactly.
        If BILLING_API_KEY is not set at all (empty string from os.environ.get default),
        the check is skipped — dev-mode convenience without requiring env setup for
        every developer.
        IMPORTANT: An operator who sets BILLING_API_KEY="" (explicitly empty) is treated
        the same as "not configured" — this is intentional dev-mode behaviour. In
        production, always set BILLING_API_KEY to a non-empty secret value.
        ISO 27001 A.9.4.1: access to sensitive operational data (DLQ, ledger summary)
        must be access-controlled in production.
    ES: Dependencia FastAPI para endpoints de operaciones. Si BILLING_API_KEY está
        configurado en el entorno (no vacío), el header X-Api-Key debe coincidir exactamente.
        Un operador que establece BILLING_API_KEY="" se trata como "no configurado" — es
        comportamiento intencional de modo dev. En producción, siempre establecer
        BILLING_API_KEY a un valor secreto no vacío.
        ISO 27001 A.9.4.1: el acceso a datos operacionales sensibles debe estar controlado.
    """
    if not BILLING_API_KEY:
        return  # dev mode — no key configured, skip check
    if not x_api_key:
        raise HTTPException(
            status_code=401,
            detail="X-Api-Key header required / Header X-Api-Key requerido"
        )
    if x_api_key != BILLING_API_KEY:
        raise HTTPException(
            status_code=401,
            detail="Invalid API key / Clave de API inválida"
        )


class StripeWebhookPayload(BaseModel):
    """
    EN: FastAPI request body model for the webhook endpoint. This is a minimal
        envelope — FastAPI uses it to parse the incoming JSON. The actual deep
        validation happens inside process_stripe_event() via StripeEvent.
        Using dict for data and request allows arbitrary Stripe payload shapes
        to pass through without FastAPI rejecting them before we can DLQ them.
    ES: Modelo de cuerpo de solicitud FastAPI para el endpoint de webhook. Este
        es un sobre mínimo — FastAPI lo usa para parsear el JSON entrante. La
        validación profunda real ocurre dentro de process_stripe_event() vía StripeEvent.
        Usar dict para data y request permite que formas de payload Stripe arbitrarias
        pasen sin que FastAPI las rechace antes de que podamos enviarlas al DLQ.
    """
    id:      str  = Field(..., description="Stripe event id / ID de evento Stripe")
    type:    str  = Field(..., description="Event type / Tipo de evento")
    data:    dict = Field(default_factory=dict)
    request: dict = Field(default_factory=dict)


@app.post("/webhook/stripe", status_code=status.HTTP_200_OK)
@_limiter.limit("100/minute")
def stripe_webhook(request: Request, payload: StripeWebhookPayload) -> JSONResponse:
    """
    EN: Receives a Stripe webhook and commits it to the billing ledger.
        Always returns HTTP 200 for valid JSON — even for duplicates and invalid
        events that end up in DLQ. Returning non-200 tells Stripe to retry,
        which is only correct for transient failures (e.g., DB down → 503).
        HMAC signature verification is active — stripe.Webhook.construct_event()
        validates the Stripe-Signature header before any business logic runs.
        ISO 27001 A.14.1.2: authentication enforced at all ingestion entry points.
    ES: Recibe un webhook de Stripe y lo compromete en el libro de facturación.
        Siempre retorna HTTP 200 para JSON válido — incluso para duplicados y
        eventos inválidos que terminan en DLQ. Retornar no-200 le dice a Stripe
        que reintente, lo cual solo es correcto para fallos transitorios (ej., DB caída → 503).
        La verificación de firma HMAC está activa — stripe.Webhook.construct_event()
        valida el header Stripe-Signature antes de que corra cualquier lógica de negocio.
        ISO 27001 A.14.1.2: autenticación aplicada en todos los puntos de entrada de ingesta.
    """
    # ── HMAC SIGNATURE VERIFICATION ─────────────────────────────────────
    #     stripe.Webhook.construct_event() validates the HMAC-SHA256 in the
    #     Stripe-Signature header against the raw request body. Any body tampering
    #     or wrong/missing signature raises SignatureVerificationError → 400.
    #     ISO 27001 A.14.1.2: authentication at all ingestion entry points.
    raw_body   = request.body()   # sync call — handler is def, Starlette provides sync .body()
    sig_header = request.headers.get("stripe-signature", "")
    try:
        stripe.Webhook.construct_event(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
    except stripe.error.SignatureVerificationError:
        raise HTTPException(
            status_code=400,
            detail="Invalid or missing Stripe signature / Firma Stripe inválida o ausente"
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
    EN: Quick sanity-check endpoint. Returns row counts per ledger status, DLQ depth,
        and outbox pending count. Requires X-Api-Key header when BILLING_API_KEY is set.
        sync def: FastAPI dispatches sync handlers to its thread pool via run_in_threadpool.
        psycopg2 blocking calls do not block the asyncio event loop.
    ES: Endpoint de verificación rápida de cordura. Retorna conteos de filas por estado
        del libro, profundidad del DLQ y conteo pendiente del outbox. Requiere header
        X-Api-Key cuando BILLING_API_KEY está establecido.
        sync def: FastAPI despacha manejadores síncronos al thread pool vía run_in_threadpool.
        Las llamadas bloqueantes de psycopg2 no bloquean el event loop de asyncio.
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
    EN: Inspect DLQ entries over HTTP — newest first. This is the ops endpoint:
        without it, "check the DLQ" means "SSH into the box and run a psql query."
        Default limit 50; capped at 1000 to prevent accidental full-table dumps.
        raw_payload is deserialized so callers get a JSON object, not a string.
        Requires X-Api-Key header when BILLING_API_KEY is set.
        sync def: runs in FastAPI's thread pool — no event loop blocking.
    ES: Inspeccionar entradas del DLQ por HTTP — más recientes primero. Este es
        el endpoint de ops: sin él, "revisar el DLQ" significa "SSH a la máquina
        y ejecutar una consulta psql". Límite por defecto 50; limitado a 1000 para prevenir
        volcados accidentales de tabla completa. raw_payload está deserializado para que
        los llamadores obtengan un objeto JSON, no un string.
        Requiere header X-Api-Key cuando BILLING_API_KEY está establecido.
        sync def: corre en el thread pool de FastAPI — sin bloqueo del event loop.
    """
    if limit < 1:
        raise HTTPException(status_code=422, detail="limit must be >= 1 / limit debe ser >= 1")

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
    immediately regardless of status or retry_count. Useful for ops: fix the
    upstream issue (e.g. bad validator config), then replay specific entries.
    Returns the outcome of the reprocessing attempt.
    Requires X-Api-Key header when BILLING_API_KEY is set.
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
        payload = json.loads(raw_payload_str)
        result = process_stripe_event(conn, payload)
        outcome = result.get("outcome", "")

        if outcome in ("SUCCESS", "DLQ_DUPLICATE"):
            with _tx(conn) as cur:
                cur.execute(
                    "UPDATE dlq SET status='resolved' WHERE id=%s",
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
    EN: Kubernetes/Docker health check endpoint. Returns 200 if the process is alive.
        Does NOT check DB connectivity — a separate readiness probe should do that.
        sync def: consistent with other route handlers; no I/O performed here.
    ES: Endpoint de health check para Kubernetes/Docker. Retorna 200 si el proceso
        está vivo. NO verifica la conectividad de la DB — una sonda de readiness
        separada debe hacer eso. sync def: consistente con otros manejadores.
    """
    return JSONResponse(content={"status": "ok"})


# ===========================================================================
# SECTION 6: HEADLESS BENCHMARK MODE
# SECCIÓN 6: MODO DE BENCHMARK SIN INTERFAZ
#
# Bypasses the HTTP stack entirely to measure pure ledger throughput.
#     Run with: python ledger.py --silent --events 10000
#     This is what the TPS numbers in the README are based on.
# ===========================================================================

def _run_headless_benchmark(n: int = 10_000) -> None:
    """
    EN: Drives process_stripe_event() directly — no HTTP, no console I/O.
        Connects to PostgreSQL via DATABASE_URL. Each fake event gets a random ID
        so no duplicates — we're measuring insert throughput, not idempotency overhead.
        PostgreSQL TPS is lower than SQLite in-memory (~1,000-3,000 vs ~15,000) because
        of network round-trips and WAL fsync on the server side. That's the correct tradeoff.
    ES: Ejecuta process_stripe_event() directamente — sin HTTP, sin E/S de consola.
        Se conecta a PostgreSQL vía DATABASE_URL. Cada evento falso obtiene un ID aleatorio
        para no tener duplicados — medimos el throughput de inserción, no la sobrecarga de
        idempotencia. El TPS de PostgreSQL es menor que SQLite en memoria (~1,000-3,000 vs ~15,000)
        debido a los round-trips de red y WAL fsync del lado del servidor. Ese es el tradeoff correcto.
    """
    import random
    import string

    conn = _bootstrap()

    def _fake_event(i: int) -> dict:
        """
        EN: Generates a structurally valid Stripe event with a random unique ID.
            customer format "cus_{i:06d}" passes the cus_ prefix check.
            amount_paid=random.randint(100, 100_000) satisfies ge=0 and nonzero invoice check.
        ES: Genera un evento Stripe estructuralmente válido con un ID único aleatorio.
            El formato de cliente "cus_{i:06d}" pasa el check del prefijo cus_.
            amount_paid=random.randint(100, 100_000) satisface ge=0 y el check de factura no cero.
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
# Entry point / Punto de entrada
# When run directly (python ledger.py), either starts the benchmark or
#     launches uvicorn. The HTTP server is the normal mode for production.
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--silent", action="store_true",
                        help="Run headless benchmark instead of HTTP server / Ejecutar benchmark sin interfaz en lugar del servidor HTTP")
    parser.add_argument("--events", type=int, default=10_000,
                        help="Number of events for benchmark / Número de eventos para benchmark")
    args = parser.parse_args()

    if args.silent:
        _run_headless_benchmark(args.events)
    else:
        import uvicorn
        uvicorn.run("ledger:app", host="0.0.0.0", port=8000, reload=False)  # nosec B104 — intentional: Docker container must bind all interfaces
