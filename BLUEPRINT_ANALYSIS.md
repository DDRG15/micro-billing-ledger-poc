# ⚠️ DIEGO — LEE ESTO PRIMERO. ES IMPORTANTE.
# ⚠️ DIEGO — READ THIS FIRST. THIS IS IMPORTANT.

---

# BLUEPRINT ANALYSIS — MICRO-BILLING-LEDGER
## Production Gap Feasibility Report / Reporte de Factibilidad de Brechas de Producción
### Diego Alonso Del Río García — Mayo 2026

---

> **EN:** This document is the companion to the Architectural Blueprint PDF. It takes every
> production gap identified in that document and answers the real questions: *can we build it
> here, how hard is it, what breaks, and in what order do we do it.*
>
> **ES:** Este documento es el complemento del PDF de Arquitectura. Toma cada brecha de
> producción identificada y responde las preguntas reales: *¿podemos construirlo aquí, qué tan
> difícil es, qué se rompe, y en qué orden lo hacemos?*

---

## 0. Current State — Lo Que Ya Está Hecho

Before analyzing gaps, let's be honest about what this PoC already does correctly.
Antes de analizar las brechas, seamos honestos sobre lo que el PoC ya hace correctamente.

| Capability / Capacidad | Status / Estado |
|---|---|
| Idempotent INSERT OR IGNORE at DB level | ✅ Done / Hecho |
| Transactional Outbox (ledger + outbox in one commit) | ✅ Done / Hecho |
| Pydantic 5-layer validation stack | ✅ Done / Hecho |
| EventType enum — unknown types rejected at entry | ✅ Done / Hecho |
| Currency ISO-4217 regex validation | ✅ Done / Hecho |
| Customer ID `cus_` prefix cross-field validator | ✅ Done / Hecho |
| $0 invoice cross-field validator | ✅ Done / Hecho |
| DLQ with structured reason codes (DUPLICATE, INVALID) | ✅ Done / Hecho |
| DLQ write failure → full payload preserved in log | ✅ Done / Hecho |
| `GET /dlq/entries` HTTP inspection endpoint | ✅ Done / Hecho |
| 101 integration tests — HTTP, concurrency, outbox, DLQ | ✅ Done / Hecho |
| 16,600+ TPS on core ledger path | ✅ Done / Hecho |
| Multi-stage Docker build, non-root user | ✅ Done / Hecho |

Everything above was built before this document was written.
Todo lo anterior fue construido antes de que se escribiera este documento.

---

## 1. CRITICAL — PostgreSQL Migration / Migración a PostgreSQL

### What is it? / ¿Qué es?

**EN:** SQLite is a single-writer engine. Under concurrent load, all writes serialize behind
one lock. `check_same_thread=False` suppresses the Python warning but does not fix the
underlying bottleneck. For any production deployment handling real Stripe traffic, this
is the first thing that kills you.

**ES:** SQLite es un motor de escritura única. Bajo carga concurrente, todas las escrituras
se serializan detrás de un solo bloqueo. `check_same_thread=False` suprime la advertencia
de Python pero no resuelve el cuello de botella subyacente. Para cualquier despliegue en
producción manejando tráfico real de Stripe, esto es lo primero que te mata.

### How to implement / Cómo implementar

**Step 1 — Add dependency / Paso 1 — Agregar dependencia:**
```
# requirements.txt
psycopg2-binary==2.9.9   # sync driver — matches current sqlite3 usage pattern
# OR for full async:
asyncpg==0.29.0
```

**Step 2 — Replace `_bootstrap()` / Paso 2 — Reemplazar `_bootstrap()`:**
```python
# Current (SQLite) / Actual (SQLite):
conn = sqlite3.connect(str(path), check_same_thread=False, timeout=10)

# New (PostgreSQL) / Nuevo (PostgreSQL):
import psycopg2
conn = psycopg2.connect(os.environ["DATABASE_URL"])
```

**Step 3 — Replace `INSERT OR IGNORE` / Paso 3 — Reemplazar `INSERT OR IGNORE`:**
```python
# Current / Actual:
"INSERT OR IGNORE INTO ledger (...) VALUES (...)"

# New / Nuevo:
"INSERT INTO ledger (...) VALUES (...) ON CONFLICT (transaction_id) DO NOTHING"
```

**Step 4 — Replace `_tx()` context manager / Paso 4 — Reemplazar `_tx()`:**
```python
# psycopg2 uses connection.cursor() differently — autocommit must be False
# psycopg2 usa connection.cursor() de forma diferente — autocommit debe ser False
@contextmanager
def _tx(conn):
    cur = conn.cursor()
    try:
        yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
```

**Step 5 — Schema migration / Paso 5 — Migración de esquema:**
```sql
-- SQLite uses REAL for timestamps; PostgreSQL prefers DOUBLE PRECISION or TIMESTAMPTZ
-- SQLite usa REAL para timestamps; PostgreSQL prefiere DOUBLE PRECISION o TIMESTAMPTZ
-- SQLite TEXT PRIMARY KEY becomes TEXT PRIMARY KEY (same syntax, native behavior)
-- The outbox and dlq tables are identical in structure
```

### What breaks / Qué se rompe

**EN:**
- `_bootstrap()` — entire function needs rewrite for psycopg2
- `executescript()` — not available in psycopg2; run each CREATE TABLE separately
- `PRAGMA journal_mode=WAL` — SQLite-only; remove it
- `PRAGMA synchronous=NORMAL` — SQLite-only; remove it
- In-memory DB for tests (`:memory:`) — not available in PostgreSQL; use a dedicated test DB
- Concurrent test setup (temp file DB) — replace with a test transaction that rolls back

**ES:**
- `_bootstrap()` — toda la función necesita reescritura para psycopg2
- `executescript()` — no disponible en psycopg2; ejecutar cada CREATE TABLE por separado
- `PRAGMA journal_mode=WAL` — solo SQLite; eliminarlo
- `PRAGMA synchronous=NORMAL` — solo SQLite; eliminarlo
- DB en memoria para tests (`:memory:`) — no disponible en PostgreSQL; usar DB de test dedicada
- Setup de test concurrente (archivo temp) — reemplazar con transacción de test que haga rollback

### What improves / Qué mejora

**EN:** True horizontal scaling. Multiple worker processes can write simultaneously.
`ON CONFLICT DO NOTHING` is native and atomic at the engine level — same idempotency
guarantee, but without SQLite's single-writer serialization. The outbox pattern and
all business logic are unchanged.

**ES:** Escala horizontal real. Múltiples procesos worker pueden escribir simultáneamente.
`ON CONFLICT DO NOTHING` es nativo y atómico a nivel del motor — misma garantía de
idempotencia, pero sin la serialización de escritura única de SQLite. El patrón outbox y
toda la lógica de negocio no cambian.

### Files touched / Archivos afectados
`ledger.py` (bootstrap, _tx, all SQL), `requirements.txt`, `Dockerfile`, `test_ledger.py`

### Effort / Esfuerzo
**~6–8 hours.** The SQL logic is identical. The pain is in the connection layer and test infrastructure.
**~6–8 horas.** La lógica SQL es idéntica. El dolor está en la capa de conexión e infraestructura de tests.

### Priority / Prioridad
🔴 **CRITICAL** — Do this before any real Stripe traffic hits the endpoint.

---

## 2. CRITICAL — Stripe Webhook Signature Verification / Verificación de Firma

### What is it? / ¿Qué es?

**EN:** Right now, anyone who discovers your `/webhook/stripe` URL can POST arbitrary
fake events. Your server accepts them, validates them through Pydantic, and — if they're
structurally valid — writes them to the ledger. That is a direct attack vector for
injecting fake revenue records or triggering false subscription events.

**ES:** Ahora mismo, cualquier persona que descubra tu URL `/webhook/stripe` puede
enviar eventos falsos arbitrarios. Tu servidor los acepta, los valida a través de Pydantic
y — si son estructuralmente válidos — los escribe en el libro contable. Eso es un vector
de ataque directo para inyectar registros de ingresos falsos o disparar eventos de suscripción falsos.

### How to implement / Cómo implementar

**Step 1 — Add Stripe SDK / Paso 1 — Agregar Stripe SDK:**
```
# requirements.txt
stripe==10.12.0
```

**Step 2 — Uncomment the 8-line block already in `ledger.py` / Paso 2 — Descomentar el bloque ya en `ledger.py`:**
```python
# This block already exists at line ~476, just uncomment it:
# Este bloque ya existe alrededor de la línea 476, solo descomentarlo:
import stripe
sig_header = request.headers.get("stripe-signature")
raw_body   = await request.body()
try:
    stripe.WebhookSignature.verify_header(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
except stripe.error.SignatureVerificationError:
    raise HTTPException(status_code=400, detail="Invalid Stripe signature")
```

**Step 3 — Set the real secret / Paso 3 — Configurar el secreto real:**
```python
# ledger.py line ~245 — replace the placeholder:
# ledger.py línea ~245 — reemplazar el placeholder:
STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_YOUR_SECRET_HERE")
```

**Step 4 — Inject via environment / Paso 4 — Inyectar vía entorno:**
```bash
# Docker / Kubernetes:
docker run -e STRIPE_WEBHOOK_SECRET=whsec_xxx ...
# Or in a .env file for local dev (never commit .env):
# O en un archivo .env para desarrollo local (nunca hacer commit de .env):
STRIPE_WEBHOOK_SECRET=whsec_xxx
```

### What breaks / Qué se rompe

**EN:** Every existing test that POSTs to `/webhook/stripe` via TestClient will get a 400
because there is no `Stripe-Signature` header. Tests need a mock signature or a test mode
flag. The simplest fix: set `STRIPE_WEBHOOK_SECRET = ""` in test config and add a bypass
guard `if STRIPE_WEBHOOK_SECRET and STRIPE_WEBHOOK_SECRET != "whsec_YOUR_SECRET_HERE"`.

**ES:** Cada test existente que hace POST a `/webhook/stripe` via TestClient recibirá un 400
porque no hay header `Stripe-Signature`. Los tests necesitan una firma mock o un flag de
modo test. La solución más simple: establecer `STRIPE_WEBHOOK_SECRET = ""` en la config de
tests y agregar un guard de bypass `if STRIPE_WEBHOOK_SECRET and STRIPE_WEBHOOK_SECRET != "whsec_YOUR_SECRET_HERE"`.

### What improves / Qué mejora

**EN:** The endpoint goes from "open door" to "verified door." Anyone who knows the URL
but not the signing secret gets a 400. This is the difference between a billing system
and a wishful thinking system.

**ES:** El endpoint pasa de "puerta abierta" a "puerta verificada." Cualquiera que conozca
la URL pero no el secreto de firma recibe un 400. Esta es la diferencia entre un sistema
de facturación y un sistema de pensamiento positivo.

### Files touched / Archivos afectados
`ledger.py` (uncomment block + env var), `requirements.txt` (add stripe)

### Effort / Esfuerzo
**~2 hours** including test updates. The code is already written — it just needs uncommenting.
**~2 horas** incluyendo actualizaciones de tests. El código ya está escrito — solo necesita descomentarse.

### Priority / Prioridad
🔴 **CRITICAL** — No production deployment without this. Period. Sin esto no hay producción. Punto.

---

## 3. HIGH — Asynchronous Outbox Worker / Worker Asíncrono de Outbox

### What is it? / ¿Qué es?

**EN:** The outbox table stores events with `dispatched=0`. Right now, nothing reads them.
The outbox is a write-only audit log — not a delivery guarantee. Without a worker,
downstream feature provisioning (activating/deactivating subscriptions, sending
confirmation emails, etc.) never happens automatically.

**ES:** La tabla outbox almacena eventos con `dispatched=0`. Ahora mismo, nada los lee.
El outbox es un registro de auditoría de solo escritura — no una garantía de entrega.
Sin un worker, el aprovisionamiento de funciones downstream (activar/desactivar
suscripciones, enviar correos de confirmación, etc.) nunca ocurre automáticamente.

### How to implement / Cómo implementar

**Option A — FastAPI Lifespan Background Task (simplest for PoC):**
```python
import asyncio
from contextlib import asynccontextmanager

async def _outbox_worker(conn):
    """Poll outbox WHERE dispatched=0, process, flip to 1 atomically."""
    # Sondear outbox WHERE dispatched=0, procesar, cambiar a 1 atómicamente.
    while True:
        rows = conn.execute(
            "SELECT id, transaction_id, event_type, payload "
            "FROM outbox WHERE dispatched=0 ORDER BY id LIMIT 100"
        ).fetchall()
        for row in rows:
            try:
                # TODO: send to downstream system (HTTP call, message queue, etc.)
                # TODO: enviar al sistema downstream (llamada HTTP, cola de mensajes, etc.)
                _dispatch_event(row)
                conn.execute(
                    "UPDATE outbox SET dispatched=1 WHERE id=?", (row[0],)
                )
                conn.commit()
            except Exception as exc:
                _log.error("Outbox dispatch failed: id=%s error=%r", row[0], exc)
        await asyncio.sleep(5)  # Poll every 5 seconds / Sondear cada 5 segundos

@asynccontextmanager
async def lifespan(app):
    task = asyncio.create_task(_outbox_worker(_conn))
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan, ...)
```

**Option B — Temporal Activity Worker (production-grade):**
```python
# Separate process; Temporal handles retries, backoff, and delivery guarantees
# Proceso separado; Temporal maneja reintentos, backoff y garantías de entrega
# See: https://docs.temporal.io/develop/python
```

### What breaks / Qué se rompe

**EN:** Nothing in the existing code breaks — this is additive. The risk is in `_dispatch_event()`:
if it crashes without proper error handling, the worker loop dies silently. The loop must
be wrapped in a try/except that logs and continues, never dies.

**ES:** Nada en el código existente se rompe — esto es aditivo. El riesgo está en `_dispatch_event()`:
si falla sin manejo de errores adecuado, el ciclo del worker muere silenciosamente. El ciclo
debe estar envuelto en un try/except que registre y continúe, que nunca muera.

### What improves / Qué mejora

**EN:** The outbox becomes a real delivery guarantee, not a storage table. Events are
actually forwarded to downstream systems. The pattern survives process restarts: on
restart, the worker picks up any `dispatched=0` rows left behind.

**ES:** El outbox se convierte en una garantía de entrega real, no una tabla de almacenamiento.
Los eventos se reenvían realmente a sistemas downstream. El patrón sobrevive reinicios del
proceso: al reiniciar, el worker recoge cualquier fila `dispatched=0` que quedó pendiente.

### Files touched / Archivos afectados
`ledger.py` (new worker function + lifespan hook)

### Effort / Esfuerzo
**~4–6 hours** for Option A (FastAPI lifespan). **~2–3 days** for Option B (Temporal).
**~4–6 horas** para Opción A (FastAPI lifespan). **~2–3 días** para Opción B (Temporal).

### Priority / Prioridad
🟠 **HIGH** — Without this, the outbox is decorative. Sin esto, el outbox es decorativo.

---

## 4. HIGH — DLQ Backoff Retry Budget Engine / Motor de Reintento con Backoff en DLQ

### What is it? / ¿Qué es?

**EN:** Events in the DLQ stay there forever. There is no mechanism to replay a batch of
`INVALID` events after you fix a validation bug, or to retry `UNKNOWN_TYPE` events after
you add support for a new event type. Right now the DLQ is a graveyard, not a quarantine.

**ES:** Los eventos en el DLQ se quedan ahí para siempre. No hay mecanismo para reproducir
un lote de eventos `INVALID` después de corregir un bug de validación, o para reintentar
eventos `UNKNOWN_TYPE` después de agregar soporte para un nuevo tipo de evento.
Ahora mismo el DLQ es un cementerio, no una cuarentena.

### How to implement / Cómo implementar

**Step 1 — Extend the DLQ schema / Paso 1 — Extender el esquema del DLQ:**
```sql
ALTER TABLE dlq ADD COLUMN retry_count   INTEGER NOT NULL DEFAULT 0;
ALTER TABLE dlq ADD COLUMN next_retry_at REAL;
ALTER TABLE dlq ADD COLUMN max_retries   INTEGER NOT NULL DEFAULT 3;
ALTER TABLE dlq ADD COLUMN retryable     INTEGER NOT NULL DEFAULT 1;
-- 1 = can be retried; 0 = permanently dead
-- 1 = puede reintentarse; 0 = permanentemente muerto
```

**Step 2 — Extend DLQEntry model / Paso 2 — Extender el modelo DLQEntry:**
```python
class DLQEntry(BaseModel):
    ...  # existing fields / campos existentes
    retry_count:   int   = 0
    next_retry_at: Optional[float] = None
    max_retries:   int   = 3
    retryable:     bool  = True
```

**Step 3 — Add retry worker / Paso 3 — Agregar worker de reintentos:**
```python
async def _dlq_retry_worker(conn):
    """Pick retryable DLQ entries past their next_retry_at, attempt reprocessing."""
    # Tomar entradas retryable del DLQ pasado su next_retry_at, intentar reprocesar.
    while True:
        now = time.time()
        rows = conn.execute(
            "SELECT id, transaction_id, raw_payload, retry_count, max_retries "
            "FROM dlq WHERE retryable=1 AND (next_retry_at IS NULL OR next_retry_at <= ?) "
            "ORDER BY id LIMIT 50",
            (now,)
        ).fetchall()
        for row in rows:
            raw_payload = json.loads(row[2])
            result = process_stripe_event(conn, raw_payload)
            if result["outcome"] not in ("DLQ_INVALID", "DLQ_DUPLICATE"):
                # Success — mark as non-retryable (it's been processed)
                # Éxito — marcar como no retryable (ya fue procesado)
                conn.execute("UPDATE dlq SET retryable=0 WHERE id=?", (row[0],))
            else:
                new_count = row[3] + 1
                if new_count >= row[4]:  # max_retries exceeded
                    conn.execute(
                        "UPDATE dlq SET retryable=0, retry_count=? WHERE id=?",
                        (new_count, row[0])
                    )
                else:
                    # Exponential backoff: 2^retry_count minutes
                    # Backoff exponencial: 2^retry_count minutos
                    backoff = (2 ** new_count) * 60
                    conn.execute(
                        "UPDATE dlq SET retry_count=?, next_retry_at=? WHERE id=?",
                        (new_count, now + backoff, row[0])
                    )
            conn.commit()
        await asyncio.sleep(30)
```

### What breaks / Qué se rompe

**EN:** The dlq table schema changes — existing rows won't have the new columns unless you
run the ALTER TABLE statements. In PostgreSQL this is a proper migration; in SQLite you can
add nullable columns without rebuilding the table. The DLQEntry model and `to_db()` need
updating to include the new fields.

**ES:** El esquema de la tabla dlq cambia — las filas existentes no tendrán las nuevas
columnas a menos que ejecutes los ALTER TABLE. En PostgreSQL esto es una migración
adecuada; en SQLite puedes agregar columnas nullable sin reconstruir la tabla. El modelo
DLQEntry y `to_db()` necesitan actualización para incluir los nuevos campos.

### What improves / Qué mejora

**EN:** The DLQ becomes a true quarantine: events wait for human review or automatic
retry, not permanent death. After a Pydantic schema fix, you can replay all `INVALID`
entries and recover lost transactions without touching the raw logs.

**ES:** El DLQ se convierte en una cuarentena real: los eventos esperan revisión humana
o reintento automático, no muerte permanente. Después de una corrección de esquema Pydantic,
puedes reproducir todas las entradas `INVALID` y recuperar transacciones perdidas sin
tocar los logs crudos.

### Files touched / Archivos afectados
`ledger.py` (schema, DLQEntry model, new worker)

### Effort / Esfuerzo
**~4–6 hours** including schema migration and tests.
**~4–6 horas** incluyendo migración de esquema y tests.

### Priority / Prioridad
🟠 **HIGH** — Required for production ops. Requerido para operaciones en producción.

---

## 5. LOW — Currency Lowercase Normalization / Normalización de Moneda a Minúsculas

### What is it? / ¿Qué es?

**EN:** Stripe can send `"currency": "USD"` (uppercase) in some webhook variants. Right
now, uppercase currency goes to DLQ with reason INVALID because the regex `^[a-z]{3}$`
is strict. The fix is one line: normalize to lowercase before Pydantic sees it.

**ES:** Stripe puede enviar `"currency": "USD"` (mayúsculas) en algunas variantes de
webhook. Ahora mismo, la moneda en mayúsculas va al DLQ con razón INVALID porque el
regex `^[a-z]{3}$` es estricto. La corrección es una línea: normalizar a minúsculas
antes de que Pydantic lo vea.

### How to implement / Cómo implementar

**Option A — In StripeObject (recommended):**
```python
class StripeObject(BaseModel):
    currency: str = Field(default="usd", pattern=r"^[a-z]{3}$")

    @model_validator(mode='before')
    @classmethod
    def normalize_currency(cls, values):
        # Normalize currency to lowercase before field validation
        # Normalizar moneda a minúsculas antes de la validación de campos
        if isinstance(values, dict) and "currency" in values:
            values["currency"] = values["currency"].lower()
        return values
```

**Option B — In process_stripe_event (simpler but less clean):**
```python
# Before StripeEvent(**event):
if "data" in event and "object" in event["data"]:
    currency = event["data"]["object"].get("currency", "")
    event["data"]["object"]["currency"] = currency.lower()
```

### What breaks / Qué se rompe

**EN:** Nothing breaks. Currently `"USD"` → DLQ. After this fix, `"USD"` → normalized
to `"usd"` → POSTED. This is a behavior change but in the correct direction.

**ES:** Nada se rompe. Actualmente `"USD"` → DLQ. Después de esta corrección, `"USD"` →
normalizado a `"usd"` → POSTED. Es un cambio de comportamiento pero en la dirección correcta.

Test update needed: the test `"currency 'USD' (uppercase) → DLQ_INVALID"` becomes
`"currency 'USD' (uppercase) → normalized to 'usd' → POSTED"`.
Actualización de test necesaria: el test `"currency 'USD' (uppercase) → DLQ_INVALID"` pasa a
`"currency 'USD' (uppercase) → normalizado a 'usd' → POSTED"`.

### What improves / Qué mejora

**EN:** Resilient to Stripe inconsistencies. No valid payment gets DLQ'd because of
case sensitivity. Revenue stays in the ledger where it belongs.

**ES:** Resistente a inconsistencias de Stripe. Ningún pago válido va al DLQ por
sensibilidad de mayúsculas/minúsculas. Los ingresos se quedan en el libro contable donde pertenecen.

### Files touched / Archivos afectados
`ledger.py` (StripeObject validator), `test_ledger.py` (update currency test expectation)

### Effort / Esfuerzo
**~30 minutes** including test update.
**~30 minutos** incluyendo actualización de tests.

### Priority / Prioridad
🟢 **LOW** — But it's a 30-minute fix. Do it. Pero son 30 minutos. Hazlo.

---

## 6. MEDIUM — Structured JSON Logging / Logging Estructurado en JSON

### What is it? / ¿Qué es?

**EN:** The current log output looks like:
```
2026-05-14 12:00:01 INFO ledger: DLQ write failed — payload preserved...
```
This is human-readable but machine-unreadable. A production billing service needs
structured JSON logs with consistent fields so alerting systems can parse them.

**ES:** La salida de log actual se ve así:
```
2026-05-14 12:00:01 INFO ledger: DLQ write failed — payload preserved...
```
Esto es legible para humanos pero no para máquinas. Un servicio de facturación en producción
necesita logs JSON estructurados con campos consistentes para que los sistemas de alertas
puedan analizarlos.

### How to implement / Cómo implementar

```python
import json as _json

class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log line with consistent fields."""
    # Emitir un objeto JSON por línea de log con campos consistentes.
    def format(self, record):
        payload = {
            "ts":      self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level":   record.levelname,
            "logger":  record.name,
            "msg":     record.getMessage(),
        }
        # Merge any extra fields passed via extra={} in log calls
        # Mezclar cualquier campo extra pasado vía extra={} en llamadas de log
        for key in ("transaction_id", "event_type", "outcome", "duration_ms"):
            if hasattr(record, key):
                payload[key] = getattr(record, key)
        return _json.dumps(payload)

# Replace basicConfig with:
handler = logging.StreamHandler()
handler.setFormatter(_JsonFormatter())
file_handler = logging.FileHandler("billing_ledger.log")
file_handler.setFormatter(_JsonFormatter())
logging.basicConfig(level=logging.INFO, handlers=[handler, file_handler])
```

**Usage in process_stripe_event / Uso en process_stripe_event:**
```python
_log.info("event processed", extra={
    "transaction_id": transaction_id,
    "event_type":     event_type.value,
    "outcome":        result["outcome"],
    "duration_ms":    round((time.time() - start) * 1000, 2),
})
```

### What breaks / Qué se rompe

**EN:** Nothing functionally breaks. Log files switch from human-readable to JSON.
Anyone using `grep "DLQ write failed" billing_ledger.log` needs to switch to
`jq '.msg | select(contains("DLQ write failed"))' billing_ledger.log`.

**ES:** Nada falla funcionalmente. Los archivos de log cambian de legible por humanos a JSON.
Cualquiera que use `grep "DLQ write failed" billing_ledger.log` necesita cambiar a
`jq '.msg | select(contains("DLQ write failed"))' billing_ledger.log`.

### What improves / Qué mejora

**EN:** Grafana, Datadog, CloudWatch, and every modern log aggregation system can parse
JSON natively. You can build dashboards and alerts on `outcome`, `event_type`, and
`duration_ms` without regex hacks.

**ES:** Grafana, Datadog, CloudWatch y todo sistema moderno de agregación de logs puede
parsear JSON nativamente. Puedes construir dashboards y alertas sobre `outcome`,
`event_type` y `duration_ms` sin hacks de regex.

### Files touched / Archivos afectados
`ledger.py` (logging config + log call sites)

### Effort / Esfuerzo
**~2–3 hours** including adding `extra={}` to all existing log calls.
**~2–3 horas** incluyendo agregar `extra={}` a todas las llamadas de log existentes.

### Priority / Prioridad
🟡 **MEDIUM** — Required before any real monitoring is set up.
🟡 **MEDIO** — Requerido antes de configurar cualquier monitoreo real.

---

## 7. MEDIUM — Prometheus Metrics Endpoint / Endpoint de Métricas Prometheus

### What is it? / ¿Qué es?

**EN:** `/ledger/summary` is a manual inspection tool. Production needs machine-readable
counters that Prometheus can scrape on a schedule and Grafana can graph. Specifically:
`webhooks_received_total`, `webhooks_posted_total`, `webhooks_dlq_total`,
`outbox_pending_depth`.

**ES:** `/ledger/summary` es una herramienta de inspección manual. Producción necesita
contadores legibles por máquinas que Prometheus pueda hacer scrape periódicamente y
Grafana pueda graficar. Específicamente: `webhooks_received_total`, `webhooks_posted_total`,
`webhooks_dlq_total`, `outbox_pending_depth`.

### How to implement / Cómo implementar

```
# requirements.txt
prometheus-client==0.21.0
```

```python
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response

# Define counters at module level — thread-safe by design
# Definir contadores a nivel de módulo — thread-safe por diseño
WEBHOOKS_RECEIVED = Counter("webhooks_received_total", "Total webhooks received")
WEBHOOKS_POSTED   = Counter("webhooks_posted_total",   "Total webhooks posted to ledger")
WEBHOOKS_DLQ      = Counter("webhooks_dlq_total",      "Total webhooks routed to DLQ",
                             ["reason"])  # label: DUPLICATE, INVALID
OUTBOX_PENDING    = Gauge("outbox_pending_depth", "Current outbox pending count")

@app.get("/metrics")
async def metrics():
    # Update the gauge from DB on every scrape
    # Actualizar el gauge desde la DB en cada scrape
    pending = _conn.execute(
        "SELECT COUNT(*) FROM outbox WHERE dispatched=0"
    ).fetchone()[0]
    OUTBOX_PENDING.set(pending)
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
```

**Increment in process_stripe_event:**
```python
WEBHOOKS_RECEIVED.inc()
# ... after result:
if result["outcome"] in ("POSTED", "VOID", "PENDING"):
    WEBHOOKS_POSTED.inc()
elif "DLQ" in result["outcome"]:
    WEBHOOKS_DLQ.labels(reason=result["outcome"].replace("DLQ_", "")).inc()
```

### What breaks / Qué se rompe

**EN:** Nothing breaks. Additive change. The `/metrics` endpoint is new.

**ES:** Nada se rompe. Cambio aditivo. El endpoint `/metrics` es nuevo.

### What improves / Qué mejora

**EN:** Real-time visibility into webhook throughput and DLQ accumulation. You can
set a Grafana alert on "DLQ depth growing faster than X per minute" — the canary
for a Stripe schema change or validation bug.

**ES:** Visibilidad en tiempo real del throughput de webhooks y acumulación del DLQ.
Puedes configurar una alerta en Grafana sobre "profundidad del DLQ creciendo más rápido
que X por minuto" — el canario para un cambio de esquema de Stripe o bug de validación.

### Files touched / Archivos afectados
`ledger.py` (new counters + new route), `requirements.txt`

### Effort / Esfuerzo
**~2–3 hours** including counter placement in all relevant code paths.
**~2–3 horas** incluyendo colocación de contadores en todas las rutas de código relevantes.

### Priority / Prioridad
🟡 **MEDIUM** — Required before oncall rotation. Requerido antes de rotación de guardia.

---

## 8. LOW — WAL Checkpoint Every N Commits / Checkpoint WAL Cada N Commits

### What is it? / ¿Qué es?

**EN:** SQLite in WAL mode accumulates write-ahead log entries without checkpointing.
On a long-running process ingesting millions of events, the WAL file (`billing_ledger.db-wal`)
grows unbounded and degrades read performance as SQLite must scan more of the WAL on
every read. `PRAGMA wal_checkpoint(PASSIVE)` flushes WAL pages back to the main DB file.

**ES:** SQLite en modo WAL acumula entradas del write-ahead log sin hacer checkpoint.
En un proceso de larga ejecución que ingiere millones de eventos, el archivo WAL
(`billing_ledger.db-wal`) crece sin límite y degrada el rendimiento de lectura ya que
SQLite debe escanear más del WAL en cada lectura. `PRAGMA wal_checkpoint(PASSIVE)`
vacía las páginas WAL de vuelta al archivo DB principal.

### How to implement / Cómo implementar

```python
# Add a commit counter to the module level
# Agregar un contador de commits a nivel de módulo
_commit_count = 0
_CHECKPOINT_EVERY = 1_000  # checkpoint every 1000 commits / checkpoint cada 1000 commits

@contextmanager
def _tx(conn):
    global _commit_count
    cur = conn.cursor()
    conn.execute("BEGIN")
    try:
        yield cur
        conn.commit()
        _commit_count += 1
        if _commit_count % _CHECKPOINT_EVERY == 0:
            # PASSIVE: checkpoint without blocking readers or writers
            # PASSIVE: checkpoint sin bloquear lectores ni escritores
            conn.execute("PRAGMA wal_checkpoint(PASSIVE)")
    except Exception:
        conn.rollback()
        raise
```

### What breaks / Qué se rompe

**EN:** Nothing breaks. PASSIVE checkpoint is non-blocking — it only checkpoints pages
that are not being read. The checkpoint is a no-op if nothing needs flushing.

**ES:** Nada se rompe. El checkpoint PASSIVE no es bloqueante — solo hace checkpoint de
páginas que no están siendo leídas. El checkpoint es un no-op si no hay nada que vaciar.

### What improves / Qué mejora

**EN:** WAL file stays bounded. Read performance stays flat over long runs instead of
degrading as the WAL grows.

**ES:** El archivo WAL permanece acotado. El rendimiento de lectura se mantiene plano
durante ejecuciones largas en lugar de degradarse conforme crece el WAL.

### Files touched / Archivos afectados
`ledger.py` (_tx function, module-level counter)

### Effort / Esfuerzo
**~30 minutes.** ~30 minutos.

### Priority / Prioridad
🟢 **LOW** — Only relevant at SQLite scale and high volume. Only matters before PostgreSQL migration.
🟢 **BAJO** — Solo relevante a escala SQLite y alto volumen. Solo importa antes de la migración a PostgreSQL.

---

## 9. HIGH — Per-Event-Type Strict Amount Extraction / Extracción Estricta de Monto por Tipo de Evento

### What is it? / ¿Qué es?

**EN:** The current `check_amount_present` validator raises a `ValueError` if both
`amount_paid` and `amount` are `None`. But if one is present and is `0`, it is allowed
through (except for invoice types, which are caught by `check_invoice_amount_nonzero`).
The gap: a subscription event with a genuinely missing amount silently defaults to 0,
which is technically allowed but represents missing data — not a $0 transaction.

**ES:** El validador actual `check_amount_present` lanza un `ValueError` si tanto
`amount_paid` como `amount` son `None`. Pero si uno está presente y es `0`, se permite
pasar (excepto para tipos de factura, que son capturados por `check_invoice_amount_nonzero`).
La brecha: un evento de suscripción con un monto genuinamente faltante silenciosamente
toma por defecto 0, lo cual es técnicamente permitido pero representa datos faltantes,
no una transacción de $0.

### How to implement / Cómo implementar

```python
# In StripeObject, separate the concepts:
# En StripeObject, separar los conceptos:
AMOUNT_REQUIRED_TYPES = {EventType.INVOICE_PAID, EventType.INVOICE_FAILED}
AMOUNT_OPTIONAL_TYPES = {EventType.SUB_CREATED, EventType.SUB_DELETED, EventType.SUB_UPDATED}

# Pass event_type down to StripeObject validator context, or handle at StripeEvent level:
# Pasar event_type al contexto del validador StripeObject, o manejar a nivel StripeEvent:
@model_validator(mode='after')
def check_amount_by_type(self):
    amount = self.data.object.get_amount()
    if self.type in AMOUNT_REQUIRED_TYPES and amount is None:
        raise ValueError(
            f"{self.type.value} requires an explicit amount — "
            f"route to DLQ with reason MISSING_AMOUNT"
        )
    return self
```

### What breaks / Qué se rompe

**EN:** Subscription events that previously posted with `amount=0` (missing data silently
defaulted) will now route to DLQ if the amount field is genuinely absent. This is
correct behavior — the data was always missing, we just weren't enforcing it.

**ES:** Los eventos de suscripción que anteriormente se publicaban con `amount=0` (datos
faltantes que silenciosamente tomaban valor por defecto) ahora irán al DLQ si el campo
monto está genuinamente ausente. Este es el comportamiento correcto — los datos siempre
faltaron, solo no lo estábamos aplicando.

### What improves / Qué mejora

**EN:** No silent $0 records in the ledger that represent missing data rather than
genuine $0 transactions. The auditor is happy. The month-end close is clean.

**ES:** Sin registros $0 silenciosos en el libro contable que representen datos faltantes
en lugar de transacciones genuinas de $0. El auditor está contento. El cierre de mes está limpio.

### Files touched / Archivos afectados
`ledger.py` (StripeEvent validator), `test_ledger.py` (new test cases)

### Effort / Esfuerzo
**~2–3 hours** including test coverage.
**~2–3 horas** incluyendo cobertura de tests.

### Priority / Prioridad
🟠 **HIGH** — Financial data integrity. Integridad de datos financieros.

---

## 10. Implementation Order / Orden de Implementación

**EN:** If you had two sprints and infinite motivation, this is the order that minimizes
risk and maximizes production readiness.

**ES:** Si tuvieras dos sprints y motivación infinita, este es el orden que minimiza
el riesgo y maximiza la preparación para producción.

```
Sprint 1 — Make it safe / Hacerlo seguro:
  1. Stripe Signature Verification (2h) — security first
  2. Currency .lower() normalization (30m) — trivial, do it now
  3. WAL Checkpoint (30m) — trivial, do it now
  4. Structured JSON Logging (3h) — needed before any monitoring

Sprint 2 — Make it scale / Hacerlo escalar:
  5. PostgreSQL Migration (8h) — the big one
  6. Asynchronous Outbox Worker (6h) — makes outbox useful
  7. DLQ Retry Budget Engine (6h) — makes DLQ useful
  8. Per-event-type Amount Validation (3h) — financial integrity
  9. Prometheus Metrics (3h) — close the loop on observability
```

**Total estimated effort / Esfuerzo total estimado:** ~32–38 hours of focused engineering.
This is a 2-week solo sprint or a 1-week paired sprint.
Esto es un sprint individual de 2 semanas o un sprint en pareja de 1 semana.

---

## 11. What Is NOT in Scope / Lo Que NO Está en Alcance

**EN:** The following are real production concerns but are out of scope for this PoC.
They are mentioned so you don't get surprised in a technical interview.

**ES:** Los siguientes son preocupaciones reales de producción pero están fuera del
alcance de este PoC. Se mencionan para que no te sorprendan en una entrevista técnica.

| Concern / Preocupación | Why out of scope / Por qué fuera de alcance |
|---|---|
| Rate limiting on `/webhook/stripe` | Stripe is the only caller; signature verification is sufficient |
| mTLS / API key auth | Stripe uses HMAC signatures, not mutual TLS |
| Multi-tenancy | Single Stripe account assumed for PoC |
| Event schema versioning | Stripe API versions handled at account level |
| GDPR / data retention | Out of scope for billing PoC; requires legal review |
| Currency conversion | Single-currency assumption; multi-currency needs FX rate source |

---

## 12. The Honest Summary / El Resumen Honesto

**EN:** The PoC is production-grade in its *patterns*: idempotency, transactional outbox,
DLQ, Pydantic validation stack, audit trail. The gaps are in *infrastructure*: PostgreSQL,
signature verification, outbox worker, and observability. None of the gaps require
rethinking the architecture — they are all additive. The skeleton is titanium.
You just need to put the rest of the house around it.

**ES:** El PoC es de grado de producción en sus *patrones*: idempotencia, outbox
transaccional, DLQ, pila de validación Pydantic, rastro de auditoría. Las brechas están
en la *infraestructura*: PostgreSQL, verificación de firma, worker de outbox y
observabilidad. Ninguna de las brechas requiere repensar la arquitectura — todas son
aditivas. El esqueleto es de titanio. Solo necesitas poner el resto de la casa a su alrededor.

---

*Diego Alonso Del Río García — PostHog Billing PoC Blueprint Analysis — Mayo 2026*
*Claude Sonnet 4.6 — Generated as companion to Micro-Billing-Ledger Architectural Blueprint PDF*
