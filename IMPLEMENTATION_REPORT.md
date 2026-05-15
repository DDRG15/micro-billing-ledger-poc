# Implementation Report — Personal Reading / Reporte de Implementación — Lectura Personal
## PostHog Billing PoC: Pydantic Validation Phases 1, 2 & 3 / Fases de Validación Pydantic 1, 2 y 3
### Diego Alonso Del Río García — Mayo 2026

---

## Why This File Exists / Por qué Existe Este Archivo

EN: The README is public. This file is for you.
    This is the unfiltered version: what we built, why each decision was made,
    where the bodies are buried, and how it maps to what you studied in the PDF.

ES: El README es público. Este archivo es para ti.
    Esta es la versión sin filtros: qué construimos, por qué se tomó cada decisión,
    dónde están los cuerpos enterrados, y cómo mapea con lo que estudiaste en el PDF.

---

## The Architecture Before We Started / La Arquitectura Antes de Empezar

EN: The original `process_stripe_event()` was doing this:

```python
event_type   = event.get("type", "")                          # string, unvalidated
customer_id  = data_object.get("customer", "unknown")         # silent default
amount_cents = data_object.get("amount_paid") or data_object.get("amount", 0)  # no validation
```

Every `.get()` with a default is a silent assumption. In fintech, silent assumptions
are the ones that cause a bad month-end close and a very uncomfortable conversation
with your auditor. Quoting you directly: "asumir en un fintech es la abuela de todos
los problemas." This code was full of grandmas.

ES: El `process_stripe_event()` original hacía esto:

```python
event_type   = event.get("type", "")                          # string, sin validar
customer_id  = data_object.get("customer", "unknown")         # default silencioso
amount_cents = data_object.get("amount_paid") or data_object.get("amount", 0)  # sin validación
```

Cada `.get()` con un default es una suposición silenciosa. En fintech, las suposiciones
silenciosas son las que causan un cierre de mes malo y una conversación muy incómoda
con tu auditor. Citándote directamente: "asumir en un fintech es la abuela de todos
los problemas." Este código estaba lleno de abuelas.

---

## Phase 1 — Entry Boundary (The Bouncer Gets a Rulebook) / Fase 1 — Frontera de Entrada (El Portero Tiene un Reglamento)

**Commit:** `0dd866c`
**Tests:** 34/34

### What the PDF Said / Lo que Dijo el PDF

EN: - BaseModel validates TYPE. Field validates VALUE. You need both.
    - JSON accepts everything. Jason doesn't care. Pydantic does.
    - The bouncer needs a written rulebook, not just vibes.

ES: - BaseModel valida TIPOS. Field valida VALORES. Necesitas ambos.
    - JSON acepta todo. Jason no le importa. A Pydantic sí.
    - El portero necesita un reglamento escrito, no solo intuición.

### What We Built / Lo que Construimos

**EventType Enum**

EN: The supported list. Anything not on this list gets caught by Pydantic before it touches
    a line of business logic.

ES: La lista de soporte. Cualquier cosa que no esté en esta lista es capturada por Pydantic
    antes de tocar una línea de lógica de negocio.

```python
class EventType(str, Enum):
    INVOICE_PAID = "invoice.paid"
    INVOICE_FAILED = "invoice.payment_failed"
    SUB_CREATED = "customer.subscription.created"
    SUB_DELETED = "customer.subscription.deleted"
    SUB_UPDATED = "customer.subscription.updated"
```

**StripeObject**

EN: The billing data inside the webhook. Three things validated here:
    - `customer: str = Field(min_length=4)` — No empty strings, no "unknown" placeholder
    - `currency: str = Field(pattern=r"^[a-z]{3}$")` — Exactly ISO 4217, lowercase
    - `amount_paid / amount: Optional[int] = Field(ge=0)` — Non-negative, or None

ES: Los datos de facturación dentro del webhook. Tres cosas validadas aquí:
    - `customer: str = Field(min_length=4)` — Sin strings vacíos, sin placeholder "unknown"
    - `currency: str = Field(pattern=r"^[a-z]{3}$")` — Exactamente ISO 4217, minúsculas
    - `amount_paid / amount: Optional[int] = Field(ge=0)` — No negativo, o None

**@model_validator for amount / @model_validator para amount**

EN: This is the PDF's "Validator" section in action.
    Field can't do "either A or B must exist" — you need a cross-model check for that.
    The validator also handles the fallback: prefer `amount_paid`, fall back to `amount`.

ES: Esto es la sección "Validator" del PDF en acción.
    Field no puede hacer "A o B debe existir" — necesitas una validación cruzada para eso.
    El validador también maneja el fallback: preferir `amount_paid`, caer en `amount`.

**StripeEvent**

EN: The full webhook. `type: EventType` means unknown types are rejected at model creation,
    not halfway through the business logic. The bouncer at the door, not the bouncer at the VIP area.

ES: El webhook completo. `type: EventType` significa que los tipos desconocidos son rechazados
    en la creación del modelo, no a mitad de la lógica de negocio. El portero en la puerta,
    no el portero en el área VIP.

### The Bug We Hit / El Bug que Encontramos

EN: The `idempotency_key` resolver tried to `self.idempotency_key = ...` but the field
    wasn't declared in the model. Pydantic v2 doesn't let you set attributes that aren't
    declared fields. Fixed by adding `idempotency_key: Optional[str] = None` to the model.
    This is a Pydantic v2 thing — v1 was more permissive. Read the docs before you assume.

ES: El resolver de `idempotency_key` intentó `self.idempotency_key = ...` pero el campo
    no estaba declarado en el modelo. Pydantic v2 no permite establecer atributos que no
    sean campos declarados. Corregido agregando `idempotency_key: Optional[str] = None` al modelo.
    Esto es algo de Pydantic v2 — v1 era más permisivo. Lee la documentación antes de asumir.

### Result / Resultado

EN: - Unknown types → `DLQ_INVALID` (not `DLQ_UNKNOWN_TYPE` — Pydantic catches first)
    - `"USD"` → `DLQ_INVALID` (regex is strict about lowercase)
    - `"unknown"` customer → `DLQ_INVALID` (min_length catches it)
    - 34 tests. Clean.

ES: - Tipos desconocidos → `DLQ_INVALID` (no `DLQ_UNKNOWN_TYPE` — Pydantic atrapa primero)
    - `"USD"` → `DLQ_INVALID` (el regex es estricto con minúsculas)
    - Cliente `"unknown"` → `DLQ_INVALID` (min_length lo atrapa)
    - 34 tests. Limpio.

---

## Phase 2 — Output Quality (The Exit Also Gets a Bouncer) / Fase 2 — Calidad de Salida (La Salida También Tiene Portero)

**Commits:** `d2e2e9a` (models), `d2a0ef7` (tests), `db5d79f` (README)
**Tests:** 60/60

### What the PDF Said / Lo que Dijo el PDF

EN: - model_dump() replaces dict() in Pydantic v2.
    - Enum values should be strings in DB, not enum instances.
    - Dead Letter Queue: raw payload always preserved. Never discarded. Never corrected
      without authorization.

ES: - model_dump() reemplaza dict() en Pydantic v2.
    - Los valores de Enum deben ser strings en la DB, no instancias de enum.
    - Dead Letter Queue: el payload crudo siempre preservado. Nunca descartado. Nunca
      corregido sin autorización.

### What We Built / Lo que Construimos

**LedgerStatus and DLQReason enums / Enums LedgerStatus y DLQReason**

EN: Before Phase 2, the status was a raw string `"POSTED"` assembled manually. A typo would
    sail into the database with no error. Now it's `LedgerStatus.POSTED` — the enum is the contract.

ES: Antes de la Fase 2, el status era un string crudo `"POSTED"` ensamblado manualmente. Un error
    tipográfico navegaría a la base de datos sin error. Ahora es `LedgerStatus.POSTED` — el enum es el contrato.

**DLQEntry model / Modelo DLQEntry**:

```python
class DLQEntry(BaseModel):
    transaction_id: str = Field(min_length=1)
    reason: DLQReason          # enum — not a free-form string
    raw_payload: dict          # always preserved (PDF rule)
    received_at: float = Field(default_factory=time.time)

    def to_db(self) -> tuple:
        return (self.transaction_id, self.reason.value,
                json.dumps(self.raw_payload), self.received_at)
```

EN: `to_db()` is the PDF's `model_dump()` concept applied to DB serialization.
    The `.value` on the enum converts it to a string for the database — PostgreSQL doesn't know
    what a Python enum is.

ES: `to_db()` es el concepto `model_dump()` del PDF aplicado a la serialización de DB.
    El `.value` en el enum lo convierte a un string para la base de datos — PostgreSQL no sabe
    qué es un enum de Python.

**LedgerEntry model / Modelo LedgerEntry**

EN: Same pattern. Nine validated fields, one `to_db()` method.
    The `INSERT` statement gets `ledger_entry.to_db()` and stops caring about field order.

ES: El mismo patrón. Nueve campos validados, un método `to_db()`.
    La sentencia `INSERT` recibe `ledger_entry.to_db()` y deja de preocuparse por el orden de los campos.

### The Windows Bug / El Bug de Windows

EN: The test runner used `─` (Unicode box-drawing characters) in `print()`.
    On Windows, stdout defaults to CP-1252. CP-1252 doesn't have `─`.
    `UnicodeEncodeError` at line 60 before a single test ran.

    Fix: `sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")` at the top
    of the test file. One line. Always add this when writing test runners on Windows.

ES: El ejecutor de tests usaba `─` (caracteres de dibujo de caja Unicode) en `print()`.
    En Windows, stdout por defecto es CP-1252. CP-1252 no tiene `─`.
    `UnicodeEncodeError` en la línea 60 antes de que corriera un solo test.

    Solución: `sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")` al inicio
    del archivo de test. Una línea. Siempre añade esto cuando escribas ejecutores de test en Windows.

### Result / Resultado

EN: - DLQ entries have structured, typed reasons (not raw strings)
    - Ledger rows built from validated models, not manual variable lists
    - `to_db()` handles all serialization — business logic is clean
    - 60 tests. Clean.

ES: - Las entradas de DLQ tienen razones estructuradas y tipadas (no strings crudos)
    - Las filas del ledger se construyen desde modelos validados, no listas de variables manuales
    - `to_db()` maneja toda la serialización — la lógica de negocio está limpia
    - 60 tests. Limpio.

---

## Phase 3 — Cross-Field Validators (The Rules That Field Can't Do) / Fase 3 — Validadores de Campos Cruzados (Las Reglas que Field No Puede Hacer)

**Commit:** `d82e729`
**Tests:** 69/69

### What the PDF Said / Lo que Dijo el PDF

EN: - Field validates individual values. Validators validate relationships between fields.
    - "La hot girl en el VIP que no importa si estás vestido correctamente — si tienes cara
      de problemas, no bailas con ella." The rule that isn't written on the paper but exists.
    - `@model_validator(mode='after')` runs after all Field checks pass. Has access to all
      fields simultaneously.

ES: - Field valida valores individuales. Los Validators validan relaciones entre campos.
    - "La hot girl en el VIP que no importa si estás vestido correctamente — si tienes cara
      de problemas, no bailas con ella." La regla que no está escrita en el papel pero existe.
    - `@model_validator(mode='after')` corre después de que pasen todos los checks de Field.
      Tiene acceso a todos los campos simultáneamente.

### What We Built / Lo que Construimos

**Validator 1: Invoice amount > 0 / Validador 1: Monto de factura > 0**

```python
@model_validator(mode='after')
def check_invoice_amount_nonzero(self):
    invoice_types = {EventType.INVOICE_PAID, EventType.INVOICE_FAILED}
    if self.type in invoice_types:
        amount = self.data.object.get_amount()
        if amount == 0:
            raise ValueError(f"{self.type.value} must have amount > 0")
    return self
```

EN: Why: `invoice.paid` with $0 = no revenue received. `invoice.payment_failed` with $0 =
    nothing to retry. Both are data quality failures. Subscription events (created/deleted/
    updated) are lifecycle events — they don't carry payment amounts and are exempt.

    Field alone can't do this. `ge=0` allows zero. The rule "zero is only invalid for invoice
    types" requires knowing `self.type`, which is a different field.

ES: Por qué: `invoice.paid` con $0 = no se recibieron ingresos. `invoice.payment_failed` con $0 =
    nada que reintentar. Ambos son fallas de calidad de datos. Los eventos de suscripción
    (created/deleted/updated) son eventos de ciclo de vida — no llevan montos de pago y están exentos.

    Field solo no puede hacer esto. `ge=0` permite cero. La regla "cero solo es inválido para tipos
    de factura" requiere conocer `self.type`, que es un campo diferente.

**Validator 2: Customer ID prefix / Validador 2: Prefijo de ID de cliente**

```python
@model_validator(mode='after')
def check_customer_id_format(self):
    if not self.data.object.customer.startswith("cus_"):
        raise ValueError(f"Customer ID must start with 'cus_': got '...'")
    return self
```

EN: Why: `min_length=4` on StripeObject catches empty/short IDs. It doesn't catch
    `"abc1234567"` — which is 10 characters but not a Stripe customer ID. Stripe
    customer IDs always start with `"cus_"`. This is a business rule, not a format rule.

    Note: `"cus_"` alone (exactly 4 chars) passes — the prefix is correct even if there's
    no suffix. You could debate this, but the alternative is setting `min_length=5` on the
    prefix check, which feels arbitrary. Business decision: prefix required, suffix length
    not enforced at this layer.

ES: Por qué: `min_length=4` en StripeObject atrapa IDs vacíos/cortos. No atrapa
    `"abc1234567"` — que tiene 10 caracteres pero no es un ID de cliente Stripe. Los IDs
    de cliente Stripe siempre empiezan con `"cus_"`. Esta es una regla de negocio, no una regla de formato.

    Nota: `"cus_"` solo (exactamente 4 chars) pasa — el prefijo es correcto incluso si no hay sufijo.
    Podrías debatir esto, pero la alternativa es `min_length=5` en el check de prefijo, lo cual
    se siente arbitrario. Decisión de negocio: prefijo requerido, longitud del sufijo no aplicada en esta capa.

### How Pydantic Collects Multiple Errors / Cómo Pydantic Recolecta Múltiples Errores

EN: When both validators fail (bad customer + $0 invoice), Pydantic runs all validators
    and collects ALL errors before raising `ValidationError`. The DLQ entry gets the count
    of failures, not just the first one. This is the PDF's "Pydantic recolecta TODOS los
    errores antes de lanzar la excepción."

ES: Cuando ambos validadores fallan (cliente malo + factura de $0), Pydantic corre todos los
    validadores y recolecta TODOS los errores antes de lanzar `ValidationError`. La entrada de
    DLQ obtiene el conteo de fallas, no solo la primera. Esto es el "Pydantic recolecta TODOS
    los errores antes de lanzar la excepción" del PDF.

### Result / Resultado

EN: - $0 invoices → `DLQ_INVALID`
    - Customers without `"cus_"` prefix → `DLQ_INVALID`
    - Subscription lifecycle events with $0 → `POSTED` (correct behavior)
    - Both validators fire together when both fail
    - 69 tests. Clean.

ES: - Facturas de $0 → `DLQ_INVALID`
    - Clientes sin prefijo `"cus_"` → `DLQ_INVALID`
    - Eventos de ciclo de vida de suscripción con $0 → `POSTED` (comportamiento correcto)
    - Ambos validadores se activan juntos cuando ambos fallan
    - 69 tests. Limpio.

---

## Audit Trail Hardening / Endurecimiento del Rastro de Auditoría

**Commit:** `3e33ed4`
**Tests:** 101/101 — unchanged / sin cambios

### What the PDF Said / Lo que Dijo el PDF

EN: Nothing. This isn't in the PDF. This is production discipline applied to a PoC.
    The rule: in fintech, missing data is a liability. Silent failures are fraud-adjacent.
    If something went wrong, there must always be a log line that proves it happened.

ES: Nada. Esto no está en el PDF. Esta es disciplina de producción aplicada a un PoC.
    La regla: en fintech, los datos faltantes son una responsabilidad. Los fallos silenciosos
    están cerca del fraude. Si algo salió mal, siempre debe haber una línea de log que lo pruebe.

### What We Fixed / Lo que Corregimos

**1. `_write_dlq` bare `except: pass` → structured ERROR log / `except: pass` desnudo → log ERROR estructurado**

EN: The old code:
    ```python
    except Exception:
        pass
    ```
    That's "I know something failed and I chose not to tell anyone." In a billing system
    that's not acceptable. The fix:
    ```python
    except Exception as exc:
        _log.error(
            "DLQ write failed — payload preserved here for manual recovery. "
            "transaction_id=%s reason=%s error=%r raw_payload=%s",
            entry.transaction_id, entry.reason.value, exc,
            json.dumps(entry.raw_payload),
        )
    ```
    The full raw payload is in the log line as JSON. An operator can grep `billing_ledger.log`
    for "DLQ write failed", extract the `raw_payload` field, and replay every entry manually.
    Nothing disappears. The hot path still doesn't raise. Both requirements satisfied.

ES: El código anterior:
    ```python
    except Exception:
        pass
    ```
    Eso es "sé que algo falló y elegí no decírselo a nadie." En un sistema de facturación
    eso no es aceptable. La corrección:
    ```python
    except Exception as exc:
        _log.error(
            "DLQ write failed — payload preserved here for manual recovery. "
            "transaction_id=%s reason=%s error=%r raw_payload=%s",
            entry.transaction_id, entry.reason.value, exc,
            json.dumps(entry.raw_payload),
        )
    ```
    El payload crudo completo está en la línea de log como JSON. Un operador puede hacer grep en
    `billing_ledger.log` por "DLQ write failed", extraer el campo `raw_payload`, y reproducir
    cada entrada manualmente. Nada desaparece. El hot path todavía no lanza excepciones.
    Ambos requisitos satisfechos.

**2. `GET /dlq/entries` endpoint**

EN: Before: DLQ was write-only from an HTTP perspective. You could see the depth.
    You could not see what was in there without querying the database directly.
    After: `GET /dlq/entries?limit=N` returns newest entries first, raw payload deserialized.

    For the AltScore demo: this is the endpoint the ops team would use to triage failures.
    Without it, "check the DLQ" means "SSH into the box and open a database prompt."

ES: Antes: El DLQ era de solo escritura desde una perspectiva HTTP. Podías ver la profundidad.
    No podías ver qué había dentro sin consultar la base de datos directamente.
    Después: `GET /dlq/entries?limit=N` devuelve las entradas más recientes primero, payload crudo deserializado.

    Para la demo de AltScore: este es el endpoint que el equipo de ops usaría para triagear fallas.
    Sin él, "revisar el DLQ" significa "SSH al servidor y abrir un prompt de base de datos."

**3. Dead code: `SUPPORTED_EVENT_TYPES` removed / Código muerto: `SUPPORTED_EVENT_TYPES` eliminado**

EN: ```python
    # was:
    SUPPORTED_EVENT_TYPES = {e.value for e in EventType}
    ```
    Never referenced after the Pydantic refactor. The benchmark and test were updated to use
    `random.choice(list(EventType)).value` directly. The set was a landmine — a future
    reader might assume it was authoritative and not realize the enum is.

ES: ```python
    # era:
    SUPPORTED_EVENT_TYPES = {e.value for e in EventType}
    ```
    Nunca referenciado después del refactor de Pydantic. El benchmark y el test fueron actualizados
    para usar `random.choice(list(EventType)).value` directamente. El set era una mina terrestre —
    un lector futuro podría asumir que era autoritativo sin darse cuenta de que el enum lo es.

**4. Unreachable branch removed from `check_amount_present` / Rama inalcanzable eliminada de `check_amount_present`**

EN: ```python
    # was:
    if actual_amount < 0:
        raise ValueError(...)
    ```
    `ge=0` on the Field definitions means Pydantic rejects negatives before the validator
    runs. This branch could never fire. Keeping it is technically wrong (it implies it
    can happen) and practically misleading (reviewer spends time verifying it can't).

ES: ```python
    # era:
    if actual_amount < 0:
        raise ValueError(...)
    ```
    `ge=0` en las definiciones de Field significa que Pydantic rechaza los negativos antes de que
    corra el validador. Esta rama nunca podría activarse. Mantenerla es técnicamente incorrecto
    (implica que puede ocurrir) y prácticamente engañoso (un revisor pasa tiempo verificando que no puede).

---

## API Key — Where It Lives and Why It Matters / Clave de API — Dónde Vive y Por qué Importa

EN: Added after Phase 3. Two spots in [ledger.py](ledger.py):

    **Spot 1 — Configuration section:**
    ```python
    STRIPE_WEBHOOK_SECRET: str = "whsec_YOUR_SECRET_HERE"  # <-- replace this
    ```
    This is where you paste the signing secret from your Stripe Dashboard.
    Stripe Dashboard → Developers → Webhooks → your endpoint → Signing secret.
    Looks like: `whsec_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

    **Spot 2 — The webhook handler (commented out):**
    ```python
    # stripe.WebhookSignature.verify_header(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
    ```
    This is the actual verification call. It's commented out because `stripe` isn't in
    `requirements.txt` yet. For the Makers Challenge demo this is fine to leave commented.
    For anything touching real Stripe data, uncomment it and add `stripe` to requirements.

    **Why this matters for AltScore:**
    Without signature verification, anyone who discovers your endpoint URL can replay arbitrary
    events. That's not a theoretical risk — it's the first thing a security review will flag.
    The fix is one `pip install stripe` and three lines of code. No excuse to skip it in production.

ES: Agregada después de la Fase 3. Dos lugares en [ledger.py](ledger.py):

    **Lugar 1 — Sección de configuración:**
    ```python
    STRIPE_WEBHOOK_SECRET: str = "whsec_YOUR_SECRET_HERE"  # <-- reemplaza esto
    ```
    Aquí es donde pegas el secreto de firma de tu Dashboard de Stripe.
    Stripe Dashboard → Developers → Webhooks → tu endpoint → Signing secret.
    Se ve como: `whsec_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

    **Lugar 2 — El manejador de webhook (comentado):**
    ```python
    # stripe.WebhookSignature.verify_header(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
    ```
    Esta es la llamada de verificación real. Está comentada porque `stripe` no está en
    `requirements.txt` todavía. Para la demo del Makers Challenge está bien dejarlo comentado.
    Para cualquier cosa que toque datos reales de Stripe, descomentar y agregar `stripe` a requirements.

    **Por qué esto importa para AltScore:**
    Sin verificación de firma, cualquiera que descubra la URL de tu endpoint puede reproducir eventos
    arbitrarios. Ese no es un riesgo teórico — es lo primero que señalará una revisión de seguridad.
    La solución es un `pip install stripe` y tres líneas de código. Sin excusa para omitirlo en producción.

---

## The Validation Stack (Complete Picture) / La Pila de Validación (Imagen Completa)

EN: Every webhook now passes through this stack in order:

```
1. Pydantic type checking            — is the structure correct?
   EventType enum                    — is this a known event?
   StripeObject Field(min_length=4)  — is the customer ID long enough?
   StripeObject Field(pattern=...)   — is the currency ISO format?
   StripeObject Field(ge=0)          — is the amount non-negative?

2. @model_validator on StripeObject
   check_amount_present              — is at least one amount field present?

3. @model_validator on StripeEvent (cross-field)
   resolve_idempotency_key           — what's the idempotency key?
   check_invoice_amount_nonzero      — for invoices: is amount > 0?
   check_customer_id_format          — does customer start with "cus_"?

4. Business logic
   _STATUS_MAP[event_type]           — what ledger status does this event get?
   INSERT ... ON CONFLICT DO NOTHING — DB-level idempotency guard (PostgreSQL)

5. DLQ routing
   ValidationError                   → DLQ_INVALID
   absent from RETURNING set         → DLQ_DUPLICATE
```

If any layer throws, the event goes to DLQ. The raw payload is always preserved.
Humans review. Humans decide. Humans contact. The system never assumes.

ES: Cada webhook ahora pasa por esta pila en orden:

```
1. Verificación de tipos Pydantic    — ¿es correcta la estructura?
   Enum EventType                    — ¿es este un evento conocido?
   StripeObject Field(min_length=4)  — ¿es el ID de cliente suficientemente largo?
   StripeObject Field(pattern=...)   — ¿está la moneda en formato ISO?
   StripeObject Field(ge=0)          — ¿es el monto no negativo?

2. @model_validator en StripeObject
   check_amount_present              — ¿hay al menos un campo de monto presente?

3. @model_validator en StripeEvent (cross-field)
   resolve_idempotency_key           — ¿cuál es la clave de idempotencia?
   check_invoice_amount_nonzero      — para facturas: ¿es monto > 0?
   check_customer_id_format          — ¿empieza customer con "cus_"?

4. Lógica de negocio
   _STATUS_MAP[event_type]           — ¿qué status de ledger tiene este evento?
   INSERT ... ON CONFLICT DO NOTHING — guardia de idempotencia a nivel DB (PostgreSQL)

5. Enrutamiento DLQ
   ValidationError                   → DLQ_INVALID
   ausente del conjunto RETURNING    → DLQ_DUPLICATE
```

Si cualquier capa lanza, el evento va al DLQ. El payload crudo siempre se preserva.
Los humanos revisan. Los humanos deciden. Los humanos contactan. El sistema nunca asume.

---

## Phase 5 — Integration Tests / Fase 5 — Tests de Integración

**Commit:** `d5b56df`
**Tests:** 101/101

### What the PDF Said / Lo que Dijo el PDF

EN: The PDF doesn't cover FastAPI testing — this is production discipline, not study material.
    The principle is the same though: "testa en la capa donde vive el bug." The concurrency bug
    doesn't live in `process_stripe_event()`. It lives in the thread-connection boundary.

ES: El PDF no cubre testing de FastAPI — esta es disciplina de producción, no material de estudio.
    El principio es el mismo: "testa en la capa donde vive el bug." El bug de concurrencia
    no vive en `process_stripe_event()`. Vive en el límite thread-conexión.

### What We Built / Lo que Construimos

**HTTP layer (18 tests) / Capa HTTP (18 tests)**

EN: TestClient fires actual HTTP requests at the FastAPI app. Not `process_stripe_event()`
    directly. Not a mock. A real database wired through the module connection reference
    (`L._conn = http_conn`), which means the full stack gets exercised:
    routing → handler → validation → ledger write → response.

    This is how you know the HTTP 200/503 split is correct, not just theoretically correct.

ES: TestClient lanza solicitudes HTTP reales a la app FastAPI. No `process_stripe_event()`
    directamente. No un mock. Una base de datos real conectada a través de la referencia de
    conexión del módulo (`L._conn = http_conn`), lo que significa que se ejercita la pila completa:
    routing → handler → validación → escritura ledger → respuesta.

    Así es como sabes que el split HTTP 200/503 es correcto, no solo teóricamente correcto.

**Concurrent insertion (4 tests) / Inserción concurrente (4 tests)**

EN: 5 threads, same event ID, simultaneous fire.

    ```python
    posted = results.count("POSTED")
    dupes  = results.count("DLQ_DUPLICATE")
    assert posted == 1
    assert dupes  == 4
    ```

    This is the whole reason for `INSERT ... ON CONFLICT DO NOTHING`. Either the math is exact
    or the test fails. No fudge factor. One POSTED, four DUPLICATE, nothing else.

ES: 5 threads, el mismo ID de evento, disparo simultáneo.

    ```python
    posted = results.count("POSTED")
    dupes  = results.count("DLQ_DUPLICATE")
    assert posted == 1
    assert dupes  == 4
    ```

    Esta es la razón completa de `INSERT ... ON CONFLICT DO NOTHING`. O la matemática es exacta
    o el test falla. Sin margen de error. Un POSTED, cuatro DUPLICATE, nada más.

**Outbox dispatch simulation (4 tests) / Simulación de despacho de outbox (4 tests)**

EN: Flip `dispatched=1`, verify `pending=0`.
    This is the state machine for the Temporal activity that doesn't exist yet in this PoC.
    Testing the state machine without the worker is legitimate — the state is the contract,
    and the worker just needs to honor it.

ES: Cambiar `dispatched=1`, verificar `pending=0`.
    Esta es la máquina de estados para la actividad Temporal que no existe todavía en este PoC.
    Testear la máquina de estados sin el worker es legítimo — el estado es el contrato,
    y el worker solo necesita cumplirlo.

**DLQ queryability (6 tests) / Consultabilidad del DLQ (6 tests)**

EN: Raw payload preserved byte-perfect. The DLQ is only useful if you can query it and trust
    what you get back. Tested: reason codes correct, payloads round-trip without corruption,
    event type routing maps correctly to DLQ outcomes.

ES: Payload crudo preservado byte a byte. El DLQ solo es útil si puedes consultarlo y confiar
    en lo que recibes. Probado: códigos de razón correctos, los payloads van y vienen sin
    corrupción, el enrutamiento de tipo de evento mapea correctamente a los resultados del DLQ.

### The Two Bugs We Hit / Los Dos Bugs que Encontramos

**Bug 1: DLQ count wrong in summary test / Bug 1: Conteo DLQ incorrecto en test de resumen**

EN: ```python
    # Expected 3, got 4
    assert dlq_depth == 3   # WRONG
    assert dlq_depth == 4   # RIGHT
    ```
    3 INVALID (bad currency + $0 invoice + bad customer) + 1 DUPLICATE (the replay event).
    I miscounted. The test caught it. This is why you write assertions before you assume.

ES: ```python
    # Esperado 3, obtenido 4
    assert dlq_depth == 3   # INCORRECTO
    assert dlq_depth == 4   # CORRECTO
    ```
    3 INVALID (moneda mala + factura de $0 + cliente malo) + 1 DUPLICATE (el evento de replay).
    Conté mal. El test lo atrapó. Por eso escribes assertions antes de asumir.

**Bug 2: "cannot start a transaction within a transaction" / Bug 2: "no se puede iniciar una transacción dentro de una transacción"**

EN: This one took a minute. The concurrent test shared one connection object across 5 threads.
    `_tx()` calls `conn.execute("BEGIN")`. When two threads call this on the same connection
    object simultaneously, the database complains about nested transactions.

    The fix: each thread gets its own connection to a shared temp file database.

    ```python
    tmp_db = pathlib.Path(tempfile.mktemp(suffix=".db"))
    L._bootstrap(tmp_db).close()   # initialize schema once

    def _fire():
        conn = L._bootstrap(tmp_db)    # each thread: own connection
        result = L.process_stripe_event(conn, ev)
        results.append(result["outcome"])
        conn.close()
    ```

    In-memory databases can't be shared across threads — each connection sees its own memory.
    A file is required for shared concurrent access. The AltScore production environment needs
    PostgreSQL — this was also good evidence for that migration.

ES: Este tomó un momento. El test concurrente compartía un objeto de conexión entre 5 threads.
    `_tx()` llama a `conn.execute("BEGIN")`. Cuando dos threads llaman esto en el mismo objeto
    de conexión simultáneamente, la base de datos se queja de transacciones anidadas.

    La solución: cada thread obtiene su propia conexión a una base de datos de archivo temporal compartido.

    ```python
    tmp_db = pathlib.Path(tempfile.mktemp(suffix=".db"))
    L._bootstrap(tmp_db).close()   # inicializar esquema una vez

    def _fire():
        conn = L._bootstrap(tmp_db)    # cada thread: su propia conexión
        result = L.process_stripe_event(conn, ev)
        results.append(result["outcome"])
        conn.close()
    ```

    Las bases de datos en memoria no pueden ser compartidas entre threads — cada conexión ve su propia memoria.
    Se requiere un archivo para acceso concurrente compartido. El entorno de producción de AltScore necesita
    PostgreSQL — esto también fue buena evidencia para esa migración.

### Result / Resultado

EN: - 101/101 passing
    - Full stack tested: HTTP → validation → ledger → DLQ → outbox
    - Concurrency guard verified with exact math
    - DLQ raw payload preservation verified
    - 32 new tests across 4 categories

ES: - 101/101 pasando
    - Pila completa probada: HTTP → validación → ledger → DLQ → outbox
    - Guardia de concurrencia verificada con matemática exacta
    - Preservación del payload crudo del DLQ verificada
    - 32 nuevos tests en 4 categorías

---

## Git Log Summary / Resumen del Log de Git

```
3e33ed4  fix: audit trail hardening — no silent failures, no dead code
d5b56df  Phase 5: Integration tests — HTTP layer, concurrency, outbox dispatch, DLQ
b2dac5b  docs: Add Phase 3 section and API key to both README and private report
d82e729  Phase 3: Cross-field business logic validators
db5d79f  docs: Add Phase 2 implementation report to README
d2a0ef7  Phase 2 chunk 2: Expand test coverage for DLQEntry and LedgerEntry models
d2e2e9a  Phase 2 chunk 1: Add DLQEntry and LedgerEntry Pydantic models
0dd866c  Phase 1: Pydantic Validation Pipeline - Entry Boundary Guardrails
619305f  docs: finalized high-integrity README with Amateur Hour sass
e4724e7  chore: add .gitignore and cleanup local artifacts
36c5ca0  feat: high-throughput idempotent billing ledger poc
```

---

## Numbers / Números

| Phase / Fase | Tests | TPS | Engine / Motor |
|---|---|---|---|
| Before / Antes | ~25 | ~12,000 | SQLite in-memory / SQLite en memoria |
| After Phase 1 / Tras Fase 1 | 34 | 12,412 | SQLite in-memory / SQLite en memoria |
| After Phase 2 / Tras Fase 2 | 60 | 16,628 | SQLite in-memory / SQLite en memoria |
| After Phase 3 / Tras Fase 3 | 69 | 16,609 | SQLite in-memory / SQLite en memoria |
| After Phase 5 / Tras Fase 5 | 101 | 16,600+ | SQLite in-memory / SQLite en memoria |
| After PostgreSQL migration / Tras migración PostgreSQL | 101 | **26** | PostgreSQL (per-event, Docker-WSL2) |
| After batch processing / Tras procesamiento en lote | 101 | **~5,000** | PostgreSQL (execute_values batch) |

EN: The jump from 16,600 to 26 is not a regression — it is the cost of switching from
    an in-memory SQLite engine (no network, no fsync) to a real PostgreSQL server (network
    round trips + WAL fsync). The 26 TPS number is honest. The ~5,000 TPS number is the
    engineered answer to it.

ES: El salto de 16,600 a 26 no es una regresión — es el costo de cambiar de un motor
    SQLite en memoria (sin red, sin fsync) a un servidor PostgreSQL real (round trips de red
    + WAL fsync). El número de 26 TPS es honesto. El número de ~5,000 TPS es la respuesta
    ingenieril a eso.

---

## PostgreSQL Migration (May 14, 2026) / Migración a PostgreSQL (14 de Mayo, 2026)

**Commits:** `6554181` (migration / migración), `c8f1b30` (batch)
**Tests:** 101/101 — real PostgreSQL, no mocks / PostgreSQL real, sin mocks

### Why We Migrated / Por qué Migramos

EN: SQLite is a single-writer engine. Under concurrent load, all writes serialize behind a
    file-level lock. `check_same_thread=False` suppresses the Python warning — it does not
    fix the underlying problem. More importantly, SQLite's `INSERT OR IGNORE` does not give
    us `RETURNING` support, which is required for O(1) duplicate partitioning in batch mode.
    PostgreSQL gives us true concurrent writers, WAL, MVCC, and `ON CONFLICT DO NOTHING RETURNING`.

ES: SQLite es un motor de escritor único. Bajo carga concurrente, todas las escrituras se
    serializan detrás de un bloqueo a nivel de archivo. `check_same_thread=False` suprime la
    advertencia de Python — no corrige el problema subyacente. Más importante, `INSERT OR IGNORE`
    de SQLite no nos da soporte de `RETURNING`, que es requerido para la partición O(1) de
    duplicados en modo batch. PostgreSQL nos da escritores verdaderamente concurrentes, WAL,
    MVCC, y `ON CONFLICT DO NOTHING RETURNING`.

### What Changed / Lo que Cambió

**`sqlite3` → `psycopg2`**

EN: Every import, every connection, every placeholder. `?` → `%s`. `conn.execute()` →
    `conn.cursor(); cur.execute()`. `INSERT OR IGNORE` → `INSERT ... ON CONFLICT (transaction_id) DO NOTHING`.

ES: Cada import, cada conexión, cada placeholder. `?` → `%s`. `conn.execute()` →
    `conn.cursor(); cur.execute()`. `INSERT OR IGNORE` → `INSERT ... ON CONFLICT (transaction_id) DO NOTHING`.

**`_bootstrap(path)` → `_bootstrap(dsn)`**

EN: Instead of a file path, the function takes a DSN string from `DATABASE_URL`. The same
    three `CREATE TABLE IF NOT EXISTS` blocks — just PostgreSQL DDL syntax instead of SQLite.

ES: En lugar de una ruta de archivo, la función toma un string DSN de `DATABASE_URL`. Los mismos
    tres bloques `CREATE TABLE IF NOT EXISTS` — solo sintaxis DDL de PostgreSQL en lugar de SQLite.

**Infrastructure: Docker Compose / Infraestructura: Docker Compose**

EN: A `docker-compose.yml` was introduced using the `postgres:16-alpine` image to run the
    database. This guarantees development/production parity — every developer runs the exact
    same PostgreSQL version without polluting the host system with a local installation.

ES: Se introdujo un `docker-compose.yml` usando la imagen `postgres:16-alpine` para correr
    la base de datos. Esto garantiza paridad desarrollo/producción — cada desarrollador corre
    exactamente la misma versión de PostgreSQL sin contaminar el sistema host con una instalación local.

**`_tx()` unchanged in structure / `_tx()` sin cambios en estructura**

EN: `BEGIN` / yield cursor / `COMMIT` or `ROLLBACK` on exception. The PostgreSQL version uses
    `conn.autocommit = True` at the connection level, so `BEGIN` starts an explicit transaction
    that overrides autocommit for the duration of the block. Identical semantics, different driver.

ES: `BEGIN` / yield cursor / `COMMIT` o `ROLLBACK` en excepción. La versión PostgreSQL usa
    `conn.autocommit = True` a nivel de conexión, entonces `BEGIN` inicia una transacción explícita
    que anula el autocommit durante el bloque. Semántica idéntica, driver diferente.

**Test isolation: `TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY` / Aislamiento de tests**

EN: SQLite used `DROP TABLE / CREATE TABLE`. PostgreSQL uses `TRUNCATE ... RESTART IDENTITY`
    which also resets BIGSERIAL sequences. Each test section calls `fresh_conn()` to start
    from a guaranteed empty state. No mocking. Real database. Real truncation.

ES: SQLite usaba `DROP TABLE / CREATE TABLE`. PostgreSQL usa `TRUNCATE ... RESTART IDENTITY`
    que también reinicia las secuencias BIGSERIAL. Cada sección de test llama `fresh_conn()` para
    empezar desde un estado vacío garantizado. Sin mocking. Base de datos real. Truncado real.

### The Concurrency Test Difference / La Diferencia en el Test de Concurrencia

EN: SQLite required each thread to open its own connection to a **shared temp file** — in-memory
    SQLite is per-connection and can't be shared across threads. PostgreSQL is a server:
    multiple clients connect to the same instance independently. Each thread calls
    `L._bootstrap()` to get its own `psycopg2` connection to the same server. The idempotency
    guard (`ON CONFLICT DO NOTHING`) handles the race at the DB level. The test result is
    identical: exactly 1 POSTED, 4 DLQ_DUPLICATE.

ES: SQLite requería que cada thread abriera su propia conexión a un **archivo temporal compartido** —
    SQLite en memoria es por conexión y no puede ser compartido entre threads. PostgreSQL es un servidor:
    múltiples clientes se conectan a la misma instancia independientemente. Cada thread llama
    `L._bootstrap()` para obtener su propia conexión `psycopg2` al mismo servidor. La guardia de
    idempotencia (`ON CONFLICT DO NOTHING`) maneja la carrera a nivel DB. El resultado del test
    es idéntico: exactamente 1 POSTED, 4 DLQ_DUPLICATE.

---

## Batch Performance Engineering (May 14, 2026) / Ingeniería de Rendimiento en Lotes (14 de Mayo, 2026)

**Commit:** `c8f1b30`
**Tests:** 101/101 — 500 TPS floor enforced as a hard assertion / piso de 500 TPS aplicado como aserción dura

### The Problem / El Problema

EN: After the PostgreSQL migration, the benchmark failed at 26 TPS (floor: 500 TPS).

    Root cause diagnosis:
    - Each `process_stripe_event()` call opens one `_tx()` block
    - One `_tx()` block = `BEGIN` + `INSERT ledger` + `INSERT outbox` + `COMMIT` = 4 round trips
    - Docker-on-Windows WSL2 ≈ 8ms per round trip (loopback through the WSL2 network stack)
    - 5,000 events × 4 round trips × 8ms = **160 seconds → 26 TPS**

    Lowering the threshold was not an option. The 500 TPS floor is a hard business requirement:
    the system must absorb massive transactional spikes — like a Black Friday event burst —
    without dropping or silently discarding a single event. Engineering our way out was the
    only acceptable path.

ES: Después de la migración a PostgreSQL, el benchmark falló a 26 TPS (piso: 500 TPS).

    Diagnóstico de causa raíz:
    - Cada llamada a `process_stripe_event()` abre un bloque `_tx()`
    - Un bloque `_tx()` = `BEGIN` + `INSERT ledger` + `INSERT outbox` + `COMMIT` = 4 round trips
    - Docker-on-Windows WSL2 ≈ 8ms por round trip (loopback a través del stack de red WSL2)
    - 5,000 eventos × 4 round trips × 8ms = **160 segundos → 26 TPS**

    Bajar el umbral no era una opción. El piso de 500 TPS es un requisito de negocio duro:
    el sistema debe absorber picos de transacciones masivos — como una ráfaga de eventos de
    Black Friday — sin perder ni descartar silenciosamente un solo evento. Salir de esto con
    ingeniería era el único camino aceptable.

### The Solution: `process_stripe_event_batch()` / La Solución: `process_stripe_event_batch()`

EN: **Key insight:** The bottleneck is not CPU. It is not the PostgreSQL constraint check.
    It is not Pydantic validation. It is the number of synchronous network round trips.
    Every `BEGIN` / `COMMIT` pair is two round trips. Collapsing 5,000 transactions into
    one transaction collapses 10,000 round trips into 2.

ES: **Insight clave:** El cuello de botella no es la CPU. No es la verificación de restricciones de PostgreSQL.
    No es la validación de Pydantic. Es el número de round trips de red síncronos.
    Cada par `BEGIN` / `COMMIT` son dos round trips. Colapsar 5,000 transacciones en
    una transacción colapsa 10,000 round trips en 2.

**Why `execute_values` and not a loop / Por qué `execute_values` y no un bucle**

EN: `psycopg2.extras.execute_values` generates a multi-row `INSERT INTO ledger VALUES (row1), (row2), ...`
    statement. At `page_size=1000`, 5,000 rows become 5 SQL statements instead of 5,000.
    Combined with a single `BEGIN...COMMIT`, the round trip count drops from 20,000 to ~12.

    The page_size math: 9 ledger columns × 1,000 rows = 9,000 bind parameters — safely within
    PostgreSQL's 65,535-parameter statement limit. `page_size=1000` is not arbitrary.

ES: `psycopg2.extras.execute_values` genera una sentencia multi-fila `INSERT INTO ledger VALUES (fila1), (fila2), ...`.
    Con `page_size=1000`, 5,000 filas se convierten en 5 sentencias SQL en lugar de 5,000.
    Combinado con un único `BEGIN...COMMIT`, el conteo de round trips cae de 20,000 a ~12.

    La matemática del page_size: 9 columnas del ledger × 1,000 filas = 9,000 parámetros de bind —
    dentro del límite de 65,535 parámetros de PostgreSQL. `page_size=1000` no es arbitrario.

**Why `RETURNING transaction_id` and not a post-INSERT SELECT / Por qué `RETURNING transaction_id` y no un SELECT post-INSERT**

EN: After a bulk `INSERT ... ON CONFLICT DO NOTHING`, we need to know which rows actually
    landed vs were silently skipped. Two alternatives were considered and rejected:

    - **Alternative A (post-INSERT SELECT):** Adds a round trip AND has a race window.
      Between our `INSERT` and our `SELECT`, another concurrent connection could have inserted
      the same ID. We'd misclassify it as new when it's actually a duplicate.

    - **Alternative B (pre-INSERT tracking):** Track which IDs we're about to insert.
      Loses atomicity — between our tracking step and our INSERT, another connection could
      insert the same ID. Same race, different direction.

    `RETURNING` is evaluated inside the same transaction: it reports exactly what THIS
    transaction inserted — atomic, no race window, no extra round trip. The resulting
    `inserted_ids` set enables O(1) partition of the valid events into
    `(new → write to outbox)` vs `(duplicate → write to DLQ)` in a single linear pass.

ES: Después de un `INSERT ... ON CONFLICT DO NOTHING` masivo, necesitamos saber qué filas
    realmente aterrizaron vs fueron silenciosamente omitidas. Dos alternativas fueron consideradas y rechazadas:

    - **Alternativa A (SELECT post-INSERT):** Agrega un round trip Y tiene una ventana de carrera.
      Entre nuestro `INSERT` y nuestro `SELECT`, otra conexión concurrente podría haber insertado
      el mismo ID. Lo clasificaríamos mal como nuevo cuando en realidad es un duplicado.

    - **Alternativa B (tracking pre-INSERT):** Rastrear qué IDs estamos a punto de insertar.
      Pierde atomicidad — entre nuestro paso de tracking y nuestro INSERT, otra conexión podría
      insertar el mismo ID. La misma carrera, dirección diferente.

    `RETURNING` se evalúa dentro de la misma transacción: reporta exactamente lo que ESTA
    transacción insertó — atómico, sin ventana de carrera, sin round trip extra. El conjunto
    `inserted_ids` resultante habilita la partición O(1) de los eventos válidos en
    `(nuevo → escribir al outbox)` vs `(duplicado → escribir al DLQ)` en un solo pase lineal.

**Atomicity at batch scale / Atomicidad a escala de lote**

EN: The Transactional Outbox rule holds: all three tables (ledger, outbox, DLQ) are written
    in one `BEGIN...COMMIT`. A crash before `COMMIT` leaves nothing. A crash after `COMMIT`
    leaves all outbox rows intact for the downstream worker. The guarantee is identical to
    the single-event path — just applied to N events instead of 1.

ES: La regla del Outbox Transaccional se mantiene: las tres tablas (ledger, outbox, DLQ) se
    escriben en un único `BEGIN...COMMIT`. Un crash antes del `COMMIT` no deja nada. Un crash
    después del `COMMIT` deja todas las filas del outbox intactas para el worker downstream.
    La garantía es idéntica a la ruta de evento único — solo aplicada a N eventos en lugar de 1.

### Result / Resultado

```
Before / Antes:   5,000 events in ~195s → 26 TPS    (per-event, 4 round trips each / por evento, 4 round trips cada uno)
After / Después:  5,000 events in  ~1.0s → ~5,000 TPS  (batch, 12 round trips total / lote, 12 round trips en total)
```

EN: 192× throughput improvement. Same idempotency guarantee. Same atomicity guarantee.
    Same DLQ semantics. Same 101 tests. All passing.

ES: 192× de mejora en throughput. Misma garantía de idempotencia. Misma garantía de atomicidad.
    Misma semántica de DLQ. Los mismos 101 tests. Todos pasando.

---

## Bilingual Comments + Blueprint Analysis Session (May 2026) / Sesión de Comentarios Bilingües + Análisis del Blueprint (Mayo 2026)

**Commit:** `docs: bilingual comments, BLUEPRINT_ANALYSIS, README/IMPLEMENTATION_REPORT updates`
**Tests:** 101/101 — unchanged / sin cambios

### What We Did / Lo que Hicimos

EN: This session added zero new features and changed zero behavior. What it did:

ES: Esta sesión no agregó nuevas características y no cambió ningún comportamiento. Lo que hizo:

**1. `instrucciones 1.1.txt` — deleted / eliminado**

EN: Historical Copilot session transcript. Everything in it had already been implemented
    in Phases 1–5. Keeping it was misleading (it ended mid-sentence at 92% token limit)
    and added no value. Not tracked in git — deleted cleanly.

ES: Transcripción histórica de sesión de Copilot. Todo lo que contenía ya había sido
    implementado en las Fases 1–5. Mantenerlo era engañoso (terminaba a mitad de oración
    al 92% del límite de tokens) y no agregaba valor. No rastreado en git — eliminado limpiamente.

**2. Bilingual `#` comments — all source files / Comentarios `#` bilingües — todos los archivos fuente**

EN: Every meaningful block in every source file now has two comment lines: one English,
    one Spanish. Each comment covers WHAT the code does AND WHY. The goal: any reader —
    recruiter, engineer, auditor — can read a line number and need zero follow-up questions.

ES: Cada bloque significativo en cada archivo fuente ahora tiene dos líneas de comentario:
    una en inglés, una en español. Cada comentario cubre QUÉ hace el código Y POR QUÉ.
    El objetivo: cualquier lector — reclutador, ingeniero, auditor — puede leer un número
    de línea y no necesitar preguntas de seguimiento.

Files updated / Archivos actualizados:
- `ledger.py` — 6 full sections, module docstring, all models, all functions, all routes
- `test_ledger.py` — every test section header, every chk() call group, all helpers
- `Dockerfile` — every RUN/COPY/ENV/CMD instruction
- `requirements.txt` — every dependency with version rationale + future deps commented
- `.gitignore` — every rule group with security rationale (especially .env)

### What Did NOT Change / Lo que NO Cambió

EN: The 101 tests still pass. The TPS numbers are unchanged. The API surface is identical.
    The database schema is identical. This was a documentation and readability session,
    not a feature session. If something broke, it's a comment formatting issue — not logic.

ES: Los 101 tests todavía pasan. Los números TPS no cambiaron. La superficie de la API
    es idéntica. El esquema de la base de datos es idéntico. Esta fue una sesión de
    documentación y legibilidad, no una sesión de características. Si algo se rompió,
    es un problema de formato de comentario — no de lógica.

---

*Diego Alonso Del Río García — PostHog Billing PoC — Mayo 2026*
