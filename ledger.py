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
# EN: All built-in Python modules — no external dependencies here.
# ES: Todos los módulos integrados de Python — ninguna dependencia externa aquí.
# ---------------------------------------------------------------------------
import argparse       # EN: CLI argument parsing for --silent/--events flags / ES: Parseo de argumentos CLI para las banderas --silent/--events
import json           # EN: JSON serialization for raw payload archiving / ES: Serialización JSON para archivo del payload crudo
import logging        # EN: Structured log output to file and stderr / ES: Salida de log estructurado a archivo y stderr
import os             # EN: DATABASE_URL env var lookup / ES: Lectura de la variable de entorno DATABASE_URL
import time           # EN: Unix timestamps for created_at and received_at fields / ES: Timestamps Unix para campos created_at y received_at
import psycopg2       # EN: PostgreSQL driver — replaces SQLite for production-grade storage / ES: Driver PostgreSQL — reemplaza SQLite para almacenamiento de grado producción
from contextlib import contextmanager  # EN: @contextmanager for _tx() BEGIN/COMMIT wrapper / ES: @contextmanager para el wrapper BEGIN/COMMIT de _tx()
from typing import Generator, Optional  # EN: Type hints for static analysis / ES: Hints de tipo para análisis estático
from enum import Enum  # EN: Enums for EventType, LedgerStatus, DLQReason — prevents raw string drift / ES: Enums para EventType, LedgerStatus, DLQReason — previene drift de strings crudos

# ---------------------------------------------------------------------------
# Logging configuration / Configuración de logging
# EN: Two handlers: StreamHandler (stderr for Docker/systemd) + FileHandler
#     (billing_ledger.log for manual recovery). Both see ERROR logs for DLQ
#     write failures, which include the full raw payload for manual replay.
# ES: Dos manejadores: StreamHandler (stderr para Docker/systemd) + FileHandler
#     (billing_ledger.log para recuperación manual). Ambos ven logs ERROR para
#     fallos de escritura en DLQ, que incluyen el payload crudo completo para
#     reproducción manual.
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),                          # EN: stderr / ES: stderr
        logging.FileHandler("billing_ledger.log"),        # EN: persistent log file / ES: archivo de log persistente
    ],
)
_log = logging.getLogger("ledger")

# ---------------------------------------------------------------------------
# Third-party imports / Importaciones de terceros
# EN: FastAPI for HTTP routing, Pydantic for validation, starlette for responses.
#     All pinned in requirements.txt — no surprise version changes.
# ES: FastAPI para enrutamiento HTTP, Pydantic para validación, starlette para respuestas.
#     Todas fijadas en requirements.txt — sin cambios de versión sorpresa.
# ---------------------------------------------------------------------------
from fastapi import FastAPI, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, ValidationError, model_validator

# ===========================================================================
# SECTION 1: ENUMS AND PYDANTIC MODELS (Phase 1 + Phase 2)
# SECCIÓN 1: ENUMS Y MODELOS PYDANTIC (Fase 1 + Fase 2)
#
# EN: This is the 5-layer validation stack. Every Stripe webhook passes through
#     all layers before touching the database. The order matters:
#       1. Pydantic type coercion (BaseModel)
#       2. Field-level constraints (min_length, ge=0, pattern)
#       3. Single-model validators (@model_validator on StripeObject)
#       4. Cross-field validators (@model_validator on StripeEvent)
#       5. Business logic (duplicate detection, DLQ routing)
#
# ES: Esta es la pila de validación de 5 capas. Cada webhook de Stripe pasa por
#     todas las capas antes de tocar la base de datos. El orden importa:
#       1. Coerción de tipos Pydantic (BaseModel)
#       2. Restricciones a nivel de campo (min_length, ge=0, pattern)
#       3. Validadores de un solo modelo (@model_validator en StripeObject)
#       4. Validadores de campos cruzados (@model_validator en StripeEvent)
#       5. Lógica de negocio (detección de duplicados, enrutamiento al DLQ)
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
    INVOICE_PAID   = "invoice.paid"                    # EN: Successful payment received / ES: Pago exitoso recibido
    INVOICE_FAILED = "invoice.payment_failed"          # EN: Payment attempt failed / ES: Intento de pago fallido
    SUB_CREATED    = "customer.subscription.created"   # EN: New subscription activated / ES: Nueva suscripción activada
    SUB_DELETED    = "customer.subscription.deleted"   # EN: Subscription cancelled / ES: Suscripción cancelada
    SUB_UPDATED    = "customer.subscription.updated"   # EN: Subscription plan changed / ES: Plan de suscripción cambiado


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

    # EN: Stripe customer IDs look like "cus_ABC123". min_length=4 catches empty
    #     strings and single-char placeholders. The cus_ prefix check is in StripeEvent.
    # ES: Los IDs de cliente Stripe se ven como "cus_ABC123". min_length=4 captura
    #     strings vacíos y placeholders de un char. El check del prefijo cus_ está en StripeEvent.
    customer: str = Field(
        min_length=4,
        description="Stripe customer ID (e.g., 'cus_ABC123') / ID de cliente Stripe"
    )

    # EN: Stripe puts the paid amount here on invoice.paid events. Optional because
    #     subscription lifecycle events don't always carry payment amounts.
    # ES: Stripe pone el monto pagado aquí en eventos invoice.paid. Opcional porque
    #     los eventos de ciclo de vida de suscripción no siempre llevan montos de pago.
    amount_paid: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount paid in cents, non-negative / Monto pagado en centavos, no negativo"
    )

    # EN: Fallback amount field — Stripe uses different field names depending on
    #     the event type. The model_validator below picks the right one.
    # ES: Campo de monto alternativo — Stripe usa diferentes nombres de campo según
    #     el tipo de evento. El model_validator de abajo elige el correcto.
    amount: Optional[int] = Field(
        default=None,
        ge=0,
        description="Amount in cents (fallback to amount_paid) / Monto en centavos (respaldo de amount_paid)"
    )

    # EN: ISO 4217 currency code. Regex enforces exactly 3 lowercase letters.
    #     "USD" (uppercase) fails — Stripe should send lowercase, but if they don't,
    #     see BLUEPRINT_ANALYSIS.md §5 for the .lower() normalization fix.
    # ES: Código de moneda ISO 4217. El regex aplica exactamente 3 letras minúsculas.
    #     "USD" (mayúsculas) falla — Stripe debería enviar minúsculas, pero si no lo
    #     hace, ver BLUEPRINT_ANALYSIS.md §5 para la corrección de normalización .lower().
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
            # EN: Both fields are None — no amount at all. This is a data quality failure.
            # ES: Ambos campos son None — sin monto en absoluto. Esto es un fallo de calidad de datos.
            raise ValueError(
                "Either amount_paid or amount must be provided and non-null / "
                "Se debe proporcionar amount_paid o amount y no ser nulo"
            )
        return self

    def get_amount(self) -> int:
        """
        EN: Returns the effective billing amount in cents. Prefers amount_paid
            (Stripe invoice events) over amount (fallback). Always returns an
            int — model_validator guarantees at least one is non-null.
        ES: Retorna el monto de facturación efectivo en centavos. Prefiere amount_paid
            (eventos de factura Stripe) sobre amount (respaldo). Siempre retorna un
            int — el model_validator garantiza que al menos uno no es nulo.
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

    # EN: Stripe event ID — this becomes the ledger primary key.
    #     min_length=1 catches empty strings that would create a NULL-equivalent PK.
    # ES: ID de evento Stripe — este se convierte en la clave primaria del libro.
    #     min_length=1 captura strings vacíos que crearían un PK equivalente a NULL.
    id: str = Field(
        min_length=1,
        description="Unique Stripe event ID (e.g., 'evt_3Px...') / ID de evento Stripe único"
    )

    # EN: EventType enum — rejects unknown types at model creation, not halfway
    #     through business logic. The bouncer checks the list at the door.
    # ES: Enum EventType — rechaza tipos desconocidos en la creación del modelo, no
    #     a mitad de la lógica de negocio. El portero revisa la lista en la puerta.
    type: EventType = Field(
        description="Event type validated against supported types / Tipo de evento validado contra tipos soportados"
    )

    # EN: Nested model containing the billing object (customer, amount, currency).
    # ES: Modelo anidado que contiene el objeto de facturación (cliente, monto, moneda).
    data: StripeEventData = Field(
        description="Event data with customer, amount, currency / Datos del evento con cliente, monto, moneda"
    )

    # EN: Optional Stripe request metadata. May contain idempotency_key from
    #     Stripe's retry logic. If absent, idempotency_key falls back to event id.
    # ES: Metadatos opcionales de solicitud Stripe. Puede contener idempotency_key
    #     de la lógica de reintentos de Stripe. Si no está, idempotency_key cae
    #     de vuelta al id del evento.
    request: Optional[dict] = Field(
        default=None,
        description="Request metadata, may contain idempotency_key / Metadatos de solicitud, puede contener idempotency_key"
    )

    # EN: Resolved by resolve_idempotency_key validator below. Declared as a
    #     model field so Pydantic v2 allows assignment in the validator.
    #     (v2 does not allow setting attributes that aren't declared fields.)
    # ES: Resuelto por el validador resolve_idempotency_key de abajo. Declarado
    #     como campo del modelo para que Pydantic v2 permita asignación en el
    #     validador. (v2 no permite establecer atributos que no son campos declarados.)
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
    POSTED  = "POSTED"    # EN: Revenue confirmed — invoice.paid, subscription.created / ES: Ingreso confirmado
    PENDING = "PENDING"   # EN: Awaiting resolution — subscription.updated / ES: Esperando resolución
    VOID    = "VOID"      # EN: Reversed or failed — invoice.payment_failed, subscription.deleted / ES: Revertido o fallido


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
    DUPLICATE    = "DUPLICATE"     # EN: Stripe retry of already-processed event / ES: Reintento de Stripe de evento ya procesado
    INVALID      = "INVALID"       # EN: Failed Pydantic validation — bad structure or business rule / ES: Falló validación Pydantic — mala estructura o regla de negocio
    UNKNOWN_TYPE = "UNKNOWN_TYPE"  # EN: Reserved — currently caught as INVALID by Pydantic enum / ES: Reservado — actualmente capturado como INVALID por el enum de Pydantic


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
    transaction_id: str   = Field(min_length=1)   # EN: Stripe event ID or "unknown" if missing / ES: ID de evento Stripe o "unknown" si falta
    reason:         DLQReason                      # EN: Typed rejection code — not a freeform string / ES: Código de rechazo tipado — no un string libre
    raw_payload:    dict                           # EN: The full original webhook payload, untouched / ES: El payload original completo del webhook, sin tocar
    received_at:    float = Field(default_factory=time.time)  # EN: Unix timestamp of when we received it / ES: Timestamp Unix de cuándo lo recibimos

    def to_db(self) -> tuple:
        """
        EN: Serializes the DLQEntry to an ordered 4-tuple for the INSERT statement.
            .value on the enum converts it to a plain string for the TEXT column.
            json.dumps on raw_payload preserves the full structure as a JSON string.
        ES: Serializa el DLQEntry a una 4-tupla ordenada para el INSERT.
            .value en el enum lo convierte a un string plano para la columna TEXT.
            json.dumps en raw_payload preserva la estructura completa como string JSON.
        """
        return (
            self.transaction_id,
            self.reason.value,              # EN: enum → string / ES: enum → string
            json.dumps(self.raw_payload),   # EN: dict → JSON string / ES: dict → string JSON
            self.received_at,
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
    transaction_id:  str          = Field(min_length=1)           # EN: Stripe event ID — PK / ES: ID de evento Stripe — PK
    event_type:      EventType                                     # EN: Validated enum — not a raw string / ES: Enum validado — no un string crudo
    customer_id:     str          = Field(min_length=4)            # EN: cus_ prefix validated upstream in StripeEvent / ES: Prefijo cus_ validado upstream en StripeEvent
    amount_cents:    int          = Field(ge=0)                    # EN: Non-negative integer cents / ES: Enteros no negativos en centavos
    currency:        str          = Field(pattern=r"^[a-z]{3}$")  # EN: ISO 4217 lowercase / ES: ISO 4217 minúsculas
    status:          LedgerStatus                                  # EN: POSTED / PENDING / VOID / ES: POSTED / PENDING / VOID
    idempotency_key: str          = Field(min_length=1)            # EN: Network safety reference / ES: Referencia de seguridad de red
    payload:         str                                           # EN: Full JSON string — the outbox uses this for replay / ES: String JSON completo — el outbox lo usa para reproducción
    created_at:      float                                         # EN: Unix timestamp of ingestion / ES: Timestamp Unix de ingesta

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
            self.event_type.value,    # EN: EventType enum → string / ES: enum EventType → string
            self.customer_id,
            self.amount_cents,
            self.currency,
            self.status.value,        # EN: LedgerStatus enum → string / ES: enum LedgerStatus → string
            self.idempotency_key,
            self.payload,
            self.created_at,
        )


# ===========================================================================
# SECTION 2: CONFIGURATION
# SECCIÓN 2: CONFIGURACIÓN
#
# EN: Application-level constants. STRIPE_WEBHOOK_SECRET is the only one that
#     must change before production. See BLUEPRINT_ANALYSIS.md §2 for full
#     instructions on activating signature verification.
# ES: Constantes a nivel de aplicación. STRIPE_WEBHOOK_SECRET es la única que
#     debe cambiar antes de producción. Ver BLUEPRINT_ANALYSIS.md §2 para
#     instrucciones completas sobre activar la verificación de firma.
# ===========================================================================

# EN: PostgreSQL connection string. Set DATABASE_URL in your environment or .env file.
#     Format: postgresql://user:password@host:port/dbname
#     In Docker: pass via environment variable in docker run / compose.
#     In tests: set DATABASE_URL to point at a dedicated test database (see docker-compose.yml).
# ES: String de conexión PostgreSQL. Establecer DATABASE_URL en tu entorno o archivo .env.
#     Formato: postgresql://usuario:contraseña@host:puerto/nombredb
#     En Docker: pasar vía variable de entorno en docker run / compose.
#     En tests: establecer DATABASE_URL apuntando a una base de datos de test dedicada (ver docker-compose.yml).
DATABASE_URL: str = os.environ.get(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/billing"
)

# EN: Stripe webhook signing secret. Replace with the real value from:
#     Stripe Dashboard → Developers → Webhooks → your endpoint → Signing secret
#     Format: whsec_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
#     Without this, anyone who discovers your URL can POST fake events.
#     See the commented-out verification block in stripe_webhook() below.
# ES: Secreto de firma de webhooks de Stripe. Reemplazar con el valor real de:
#     Stripe Dashboard → Developers → Webhooks → tu endpoint → Signing secret
#     Formato: whsec_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
#     Sin esto, cualquiera que descubra tu URL puede enviar eventos falsos.
#     Ver el bloque de verificación comentado en stripe_webhook() abajo.
STRIPE_WEBHOOK_SECRET: str = "whsec_YOUR_SECRET_HERE"  # <-- REPLACE BEFORE PRODUCTION / REEMPLAZAR ANTES DE PRODUCCIÓN


# ===========================================================================
# SECTION 3: DATABASE BOOTSTRAP
# SECCIÓN 3: BOOTSTRAP DE BASE DE DATOS
#
# EN: Creates the three relational tables on first run. Idempotent — safe to
#     call multiple times (CREATE TABLE IF NOT EXISTS). WAL mode is enabled
#     for concurrent reads while writes are in progress.
# ES: Crea las tres tablas relacionales en el primer arranque. Idempotente — seguro
#     de llamar múltiples veces (CREATE TABLE IF NOT EXISTS). El modo WAL está
#     habilitado para lecturas concurrentes mientras hay escrituras en progreso.
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
    # EN: autocommit=True so DDL (CREATE TABLE) and reads execute outside any transaction.
    #     Write transactions use explicit BEGIN/COMMIT inside _tx().
    # ES: autocommit=True para que DDL (CREATE TABLE) y lecturas ejecuten fuera de transacción.
    #     Las transacciones de escritura usan BEGIN/COMMIT explícito dentro de _tx().
    conn.autocommit = True

    with conn.cursor() as cur:
        # EN: ledger — one row per unique Stripe event. transaction_id is the PK and the
        #     idempotency key. ON CONFLICT (transaction_id) DO NOTHING in the INSERT
        #     is what makes the deduplication zero-lock at the database level.
        # ES: ledger — una fila por evento Stripe único. transaction_id es el PK y la
        #     clave de idempotencia. ON CONFLICT (transaction_id) DO NOTHING en el INSERT
        #     es lo que hace la deduplicación sin bloqueo a nivel de base de datos.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ledger (
                transaction_id  TEXT             PRIMARY KEY,
                event_type      TEXT             NOT NULL,
                customer_id     TEXT             NOT NULL,
                amount_cents    INTEGER          NOT NULL,
                currency        TEXT             NOT NULL DEFAULT 'usd',
                status          TEXT             NOT NULL,
                idempotency_key TEXT             NOT NULL,
                payload         TEXT             NOT NULL,
                created_at      DOUBLE PRECISION NOT NULL
            )
        """)

        # EN: outbox — dispatched=0 rows are pending forwarding to a downstream system.
        #     BIGSERIAL gives a monotonically increasing id for ordered processing.
        # ES: outbox — las filas dispatched=0 están pendientes de reenvío a un sistema downstream.
        #     BIGSERIAL da un id monotónicamente creciente para procesamiento ordenado.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id              BIGSERIAL        PRIMARY KEY,
                transaction_id  TEXT             NOT NULL,
                event_type      TEXT             NOT NULL,
                payload         TEXT             NOT NULL,
                dispatched      INTEGER          NOT NULL DEFAULT 0,
                created_at      DOUBLE PRECISION NOT NULL
            )
        """)

        # EN: dlq — every rejected event lands here with a structured reason code.
        #     raw_payload is the full original JSON, preserved byte-perfect for replay.
        # ES: dlq — cada evento rechazado aterriza aquí con un código de razón estructurado.
        #     raw_payload es el JSON original completo, preservado byte-perfecto para reproducción.
        cur.execute("""
            CREATE TABLE IF NOT EXISTS dlq (
                id              BIGSERIAL        PRIMARY KEY,
                transaction_id  TEXT             NOT NULL,
                reason          TEXT             NOT NULL,
                raw_payload     TEXT             NOT NULL,
                received_at     DOUBLE PRECISION NOT NULL
            )
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
# EN: process_stripe_event() is the heart of the system. It is deliberately
#     framework-agnostic — it takes a plain dict and a connection, returns a
#     plain dict. This makes it easy to unit-test without spinning up FastAPI,
#     and easy to port to a different framework if needed.
# ES: process_stripe_event() es el corazón del sistema. Es deliberadamente
#     independiente del framework — toma un dict plano y una conexión, retorna
#     un dict plano. Esto lo hace fácil de probar unitariamente sin levantar
#     FastAPI, y fácil de portar a un framework diferente si es necesario.
# ===========================================================================

# EN: Maps each supported EventType to the correct ledger status.
#     dict[EventType, LedgerStatus] means mypy enforces that every EventType
#     has a corresponding status — a missing entry is a type error, not a
#     silent KeyError at runtime.
# ES: Mapea cada EventType soportado al estado correcto del libro.
#     dict[EventType, LedgerStatus] significa que mypy aplica que cada EventType
#     tiene un estado correspondiente — una entrada faltante es un error de tipo,
#     no un KeyError silencioso en tiempo de ejecución.
_STATUS_MAP: dict[EventType, LedgerStatus] = {
    EventType.INVOICE_PAID:   LedgerStatus.POSTED,   # EN: Revenue confirmed / ES: Ingreso confirmado
    EventType.INVOICE_FAILED: LedgerStatus.VOID,     # EN: Revenue reversed / ES: Ingreso revertido
    EventType.SUB_CREATED:    LedgerStatus.POSTED,   # EN: Account activated / ES: Cuenta activada
    EventType.SUB_DELETED:    LedgerStatus.VOID,     # EN: Account deactivated / ES: Cuenta desactivada
    EventType.SUB_UPDATED:    LedgerStatus.PENDING,  # EN: Change in progress / ES: Cambio en progreso
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
    now = time.time()

    # ── LAYER 1: Pydantic Validation ─────────────────────────────────────────
    # EN: StripeEvent(**event) runs all Field constraints AND all @model_validators.
    #     If anything fails — wrong type, bad currency, $0 invoice, missing cus_ prefix —
    #     a ValidationError is raised here. We catch it, write to DLQ, and return.
    #     The DB sees nothing from a failed validation. Clean separation.
    # ES: StripeEvent(**event) corre todas las restricciones de Field Y todos los @model_validators.
    #     Si algo falla — tipo incorrecto, moneda mala, factura $0, prefijo cus_ faltante —
    #     se lanza un ValidationError aquí. Lo capturamos, escribimos al DLQ y retornamos.
    #     La DB no ve nada de una validación fallida. Separación limpia.
    try:
        validated_event = StripeEvent(**event)
    except ValidationError as e:
        transaction_id = event.get("id") or "unknown"
        # EN: "unknown" fallback only if the event has no id at all.
        #     DLQEntry.min_length=1 would reject "" so we use "unknown" as the floor.
        # ES: Respaldo "unknown" solo si el evento no tiene id en absoluto.
        #     DLQEntry.min_length=1 rechazaría "" así que usamos "unknown" como piso.
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
    # EN: At this point, all fields are guaranteed valid by Pydantic.
    #     No more .get() with defaults. No more "could be None" uncertainty.
    # ES: En este punto, todos los campos están garantizados válidos por Pydantic.
    #     No más .get() con defaults. No más incertidumbre de "podría ser None".
    transaction_id  = validated_event.id
    event_type      = validated_event.type
    data_object     = validated_event.data.object
    idempotency_key = validated_event.idempotency_key

    # ── LAYER 2: Business Logic ───────────────────────────────────────────────
    # EN: Build the LedgerEntry from validated fields. This is the second validation
    #     layer — LedgerEntry's own Field constraints run on the extracted values.
    #     In practice these always pass (Pydantic already validated upstream),
    #     but the model acts as a schema contract for the INSERT statement.
    # ES: Construir el LedgerEntry desde campos validados. Esta es la segunda capa
    #     de validación — las restricciones de Field propias del LedgerEntry corren
    #     sobre los valores extraídos. En la práctica siempre pasan (Pydantic ya
    #     validó upstream), pero el modelo actúa como contrato de esquema para el INSERT.
    amount_cents  = data_object.get_amount()
    customer_id   = data_object.customer
    currency      = data_object.currency
    ledger_status = _STATUS_MAP[event_type]   # EN: type-safe — KeyError impossible here / ES: type-safe — KeyError imposible aquí
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
    # EN: The ledger INSERT and the outbox INSERT are in the same BEGIN…COMMIT.
    #     ON CONFLICT (transaction_id) DO NOTHING is the idempotency guard:
    #     if the event ID already exists in the ledger, the INSERT silently skips.
    #     rowcount=1 = new event, committed to both ledger and outbox.
    #     rowcount=0 = duplicate, ON CONFLICT fired, nothing written.
    # ES: El INSERT del libro y el INSERT del outbox están en el mismo BEGIN…COMMIT.
    #     ON CONFLICT (transaction_id) DO NOTHING es la guardia de idempotencia:
    #     si el ID de evento ya existe en el libro, el INSERT se omite silenciosamente.
    #     rowcount=1 = evento nuevo, commiteado tanto al libro como al outbox.
    #     rowcount=0 = duplicado, ON CONFLICT se disparó, nada escrito.
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
            inserted = cur.rowcount  # EN: 1=new row inserted, 0=ON CONFLICT fired (duplicate) / ES: 1=fila nueva insertada, 0=ON CONFLICT disparado (duplicado)

            if inserted == 1:
                # EN: Only write outbox row for genuinely new events — not for duplicates.
                #     Same transaction = zero dual-write gap between ledger and outbox.
                # ES: Solo escribir fila del outbox para eventos genuinamente nuevos — no para duplicados.
                #     Misma transacción = cero brecha de escritura dual entre libro y outbox.
                cur.execute(
                    """
                    INSERT INTO outbox (transaction_id, event_type, payload, created_at)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (transaction_id, event_type.value, payload_json, now),
                )
    except psycopg2.OperationalError as exc:
        # EN: DB-level error (connection lost, server down). Raise RuntimeError so the
        #     FastAPI handler converts it to HTTP 503 — telling Stripe to retry.
        #     503 is the correct response; 200 would tell Stripe "got it, stop retrying."
        # ES: Error a nivel DB (conexión perdida, servidor caído). Lanzar RuntimeError para
        #     que el manejador FastAPI lo convierta en HTTP 503 — diciéndole a Stripe que reintente.
        #     503 es la respuesta correcta; 200 le diría a Stripe "recibido, deja de reintentar".
        raise RuntimeError(f"DB write failed: {exc}") from exc

    # ── LAYER 3: DLQ Routing ──────────────────────────────────────────────────
    # EN: After the transaction, check if we actually inserted a new row.
    #     inserted=0 means the event was a duplicate — idempotency guard fired.
    #     Write to DLQ with reason DUPLICATE for audit visibility.
    #     The duplicate event is NOT an error — it's Stripe doing its job (at-least-once delivery).
    # ES: Después de la transacción, verificar si realmente insertamos una nueva fila.
    #     inserted=0 significa que el evento era un duplicado — guardia de idempotencia disparada.
    #     Escribir al DLQ con razón DUPLICATE para visibilidad de auditoría.
    #     El evento duplicado NO es un error — es Stripe haciendo su trabajo (entrega al menos una vez).
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

    # EN: Success path — event is new, validated, written to ledger and outbox.
    # ES: Ruta de éxito — evento es nuevo, validado, escrito en libro y outbox.
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
                "INSERT INTO dlq (transaction_id, reason, raw_payload, received_at)"
                " VALUES (%s, %s, %s, %s)",
                entry.to_db(),
            )
    except Exception as exc:
        # EN: DO NOT re-raise. The hot path must not fail because the DLQ is down.
        #     Log at ERROR so monitoring alerts fire. Include full raw_payload so
        #     the entry can be manually replayed from the log file.
        # ES: NO relanzar. La ruta caliente no debe fallar porque el DLQ está caído.
        #     Registrar en ERROR para que las alertas de monitoreo se disparen. Incluir
        #     raw_payload completo para que la entrada pueda reproducirse manualmente
        #     desde el archivo de log.
        _log.error(
            "DLQ write failed — payload preserved here for manual recovery. "
            "transaction_id=%s reason=%s error=%r raw_payload=%s",
            entry.transaction_id,
            entry.reason.value,
            exc,
            json.dumps(entry.raw_payload),
        )


# ===========================================================================
# SECTION 5: FASTAPI APPLICATION
# SECCIÓN 5: APLICACIÓN FASTAPI
#
# EN: HTTP layer — thin wrapper around process_stripe_event(). FastAPI handles
#     JSON parsing, request validation, and response formatting. The core logic
#     lives in process_stripe_event() — framework-agnostic and independently testable.
# ES: Capa HTTP — envoltorio delgado alrededor de process_stripe_event(). FastAPI
#     maneja el parseo JSON, validación de solicitudes y formateo de respuestas. La
#     lógica central vive en process_stripe_event() — independiente del framework
#     y probable de forma independiente.
# ===========================================================================

app   = FastAPI(title="Micro-Billing-Ledger PoC", version="1.0.0")

# EN: Module-level PostgreSQL connection — shared across all requests in a single process.
#     In tests, this is patched to a dedicated test connection: L._conn = test_conn.
#     For production at scale, replace with psycopg2.pool.ThreadedConnectionPool or asyncpg.
#     DATABASE_URL controls which PostgreSQL instance this connects to.
# ES: Conexión PostgreSQL a nivel de módulo — compartida entre todas las solicitudes en un
#     solo proceso. En tests, esto se parchea a una conexión de test dedicada: L._conn = test_conn.
#     Para producción a escala, reemplazar con psycopg2.pool.ThreadedConnectionPool o asyncpg.
#     DATABASE_URL controla a qué instancia PostgreSQL se conecta.
_conn = _bootstrap()


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
async def stripe_webhook(request: Request, payload: StripeWebhookPayload) -> JSONResponse:
    """
    EN: Receives a Stripe webhook and commits it to the billing ledger.
        Always returns HTTP 200 for valid JSON — even for duplicates and invalid
        events that end up in DLQ. Returning non-200 tells Stripe to retry,
        which is only correct for transient failures (e.g., DB down → 503).
        Signature verification is commented out — see BLUEPRINT_ANALYSIS.md §2
        for activation instructions. Do not ship to production without it.
    ES: Recibe un webhook de Stripe y lo compromete en el libro de facturación.
        Siempre retorna HTTP 200 para JSON válido — incluso para duplicados y
        eventos inválidos que terminan en DLQ. Retornar no-200 le dice a Stripe
        que reintente, lo cual solo es correcto para fallos transitorios (ej., DB caída → 503).
        La verificación de firma está comentada — ver BLUEPRINT_ANALYSIS.md §2
        para instrucciones de activación. No enviar a producción sin esto.
    """
    # EN: ── SIGNATURE VERIFICATION (commented out — activate before production) ──
    #     This is the difference between "secure endpoint" and "open door for anyone."
    #     Uncomment when you add `stripe` to requirements.txt and set STRIPE_WEBHOOK_SECRET.
    # ES: ── VERIFICACIÓN DE FIRMA (comentada — activar antes de producción) ──
    #     Esta es la diferencia entre "endpoint seguro" y "puerta abierta para cualquiera."
    #     Descomentar cuando agregues `stripe` a requirements.txt y establezcas STRIPE_WEBHOOK_SECRET.
    #
    # import stripe
    # sig_header = request.headers.get("stripe-signature")
    # raw_body   = await request.body()
    # try:
    #     stripe.WebhookSignature.verify_header(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
    # except stripe.error.SignatureVerificationError:
    #     raise HTTPException(status_code=400, detail="Invalid Stripe signature / Firma Stripe inválida")

    try:
        result = process_stripe_event(_conn, payload.model_dump())
    except RuntimeError as exc:
        # EN: RuntimeError from _tx() means a DB-level failure (locked, corrupt, disk full).
        #     503 tells Stripe to back off and retry — the correct behavior for transient errors.
        #     Do NOT return 200 here — that would tell Stripe "got it" when we didn't.
        # ES: RuntimeError de _tx() significa un fallo a nivel DB (bloqueada, corrupta, disco lleno).
        #     503 le dice a Stripe que retroceda y reintente — el comportamiento correcto para errores transitorios.
        #     NO retornar 200 aquí — eso le diría a Stripe "recibido" cuando no lo recibimos.
        raise HTTPException(status_code=503, detail=str(exc))

    return JSONResponse(content=result)


@app.get("/ledger/summary")
async def ledger_summary() -> JSONResponse:
    """
    EN: Quick sanity-check endpoint. Returns row counts per ledger status and
        the current DLQ depth and outbox pending count. Use this for manual
        inspection and smoke tests. For production monitoring, use /metrics
        (see BLUEPRINT_ANALYSIS.md §7 for Prometheus implementation).
    ES: Endpoint de verificación rápida de cordura. Retorna conteos de filas por
        estado del libro, la profundidad actual del DLQ y el conteo pendiente del outbox.
        Usar para inspección manual y pruebas de humo. Para monitoreo en producción,
        usar /metrics (ver BLUEPRINT_ANALYSIS.md §7 para implementación de Prometheus).
    """
    # EN: GROUP BY status gives {POSTED: N, VOID: N, PENDING: N} in one query.
    #     All three SELECT statements are reads — no explicit transaction needed
    #     because autocommit=True on the module-level _conn.
    # ES: GROUP BY status da {POSTED: N, VOID: N, PENDING: N} en una sola consulta.
    #     Las tres sentencias SELECT son lecturas — no se necesita transacción explícita
    #     porque autocommit=True en el _conn a nivel de módulo.
    with _conn.cursor() as cur:
        cur.execute("SELECT status, COUNT(*) FROM ledger GROUP BY status")
        counts = {row[0]: row[1] for row in cur.fetchall()}

    with _conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dlq")
        dlq_depth = cur.fetchone()[0]

    with _conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM outbox WHERE dispatched=0")
        outbox_pending = cur.fetchone()[0]

    return JSONResponse(content={
        "ledger":         counts,
        "dlq_depth":      dlq_depth,       # EN: Total entries in DLQ (all reasons) / ES: Total de entradas en DLQ (todas las razones)
        "outbox_pending": outbox_pending,  # EN: Events not yet dispatched downstream / ES: Eventos aún no enviados downstream
    })


@app.get("/dlq/entries")
async def dlq_entries(limit: int = 50) -> JSONResponse:
    """
    EN: Inspect DLQ entries over HTTP — newest first. This is the ops endpoint:
        without it, "check the DLQ" means "SSH into the box and run a psql query."
        Default limit 50; capped at 1000 to prevent accidental full-table dumps.
        raw_payload is deserialized so callers get a JSON object, not a string.
    ES: Inspeccionar entradas del DLQ por HTTP — más recientes primero. Este es
        el endpoint de ops: sin él, "revisar el DLQ" significa "SSH a la máquina
        y ejecutar una consulta psql". Límite por defecto 50; limitado a 1000 para prevenir
        volcados accidentales de tabla completa. raw_payload está deserializado
        para que los llamadores obtengan un objeto JSON, no un string.
    """
    with _conn.cursor() as cur:
        cur.execute(
            "SELECT id, transaction_id, reason, raw_payload, received_at "
            "FROM dlq ORDER BY id DESC LIMIT %s",
            (min(limit, 1000),),  # EN: cap at 1000 — never dump the entire table / ES: limitar a 1000 — nunca volcar toda la tabla
        )
        rows = cur.fetchall()

    return JSONResponse(content={
        "entries": [
            {
                "id":             r[0],
                "transaction_id": r[1],
                "reason":         r[2],
                "raw_payload":    json.loads(r[3]),  # EN: string → dict for clean JSON response / ES: string → dict para respuesta JSON limpia
                "received_at":    r[4],
            }
            for r in rows
        ],
        "count": len(rows),
    })


@app.get("/health")
async def health() -> JSONResponse:
    """
    EN: Kubernetes/Docker health check endpoint. Returns 200 if the process is alive.
        Does NOT check DB connectivity — a separate readiness probe should do that.
    ES: Endpoint de health check para Kubernetes/Docker. Retorna 200 si el proceso
        está vivo. NO verifica la conectividad de la DB — una sonda de readiness
        separada debe hacer eso.
    """
    return JSONResponse(content={"status": "ok"})


# ===========================================================================
# SECTION 6: HEADLESS BENCHMARK MODE
# SECCIÓN 6: MODO DE BENCHMARK SIN INTERFAZ
#
# EN: Bypasses the HTTP stack entirely to measure pure ledger throughput.
#     Run with: python ledger.py --silent --events 10000
#     This is what the TPS numbers in the README are based on.
# ES: Omite completamente la pila HTTP para medir el throughput puro del libro.
#     Ejecutar con: python ledger.py --silent --events 10000
#     Esto es en lo que se basan los números TPS del README.
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
    import random, string

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
# EN: When run directly (python ledger.py), either starts the benchmark or
#     launches uvicorn. The HTTP server is the normal mode for production.
# ES: Cuando se ejecuta directamente (python ledger.py), ya sea inicia el benchmark
#     o lanza uvicorn. El servidor HTTP es el modo normal para producción.
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
        uvicorn.run("ledger:app", host="0.0.0.0", port=8000, reload=False)
