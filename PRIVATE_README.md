# PRIVATE README — Micro-Billing-Ledger PoC
## Diego Alonso Del Río García · Mayo 2026
### Documento personal de estudio y referencia

> **Este documento es privado.** No es para el repositorio público.
> Está escrito en 3 niveles para que puedas enviárselo a diferentes personas:
> - **Nivel Simple** → Tu mamá, tu primo, un amigo no técnico
> - **Nivel Intermedio** → Alguien que entiende tecnología pero no programa
> - **Nivel Técnico** → Un desarrollador o ingeniero de software

---

# ═══════════════════════════════════════════════════════
# NIVEL SIMPLE — Para alguien que nunca ha programado
# ═══════════════════════════════════════════════════════

## ¿Qué construiste?

Imagina que tienes una tienda en línea y usas Stripe para cobrar a tus clientes.
Cada vez que alguien paga, Stripe te manda un "aviso" automático diciéndote:
*"Hey, acaba de haber un pago."*

El problema es: ¿qué pasa si ese aviso llega dos veces? ¿O si llega corrupto?
¿O si alguien malintencionado manda avisos falsos?

**Este programa es el guardia de seguridad y el contador que recibe esos avisos.**

Hace tres cosas:
1. **Verifica** que el aviso viene realmente de Stripe (no de alguien haciéndose pasar por Stripe)
2. **Registra** el pago en el libro de contabilidad, pero si llega dos veces, solo lo anota UNA vez
3. **Guarda** los avisos malos o duplicados en una carpeta especial para revisarlos después

---

## ¿Qué problemas encontramos y arreglamos?

Piensa en esto como revisar una caja fuerte:

### Problema 1: La cerradura estaba puesta pero la llave era de mentira
El programa tenía una función para verificar que los avisos venían de Stripe.
Pero nunca se había puesto la llave de verdad. Si alguien arrancaba el programa
sin configurar esa llave, aceptaba TODO sin verificar.

**Arreglo:** Ahora el programa te avisa inmediatamente si olvidaste poner la llave real.

### Problema 2: El libro de contabilidad podía recibir a cualquiera sin cita
Los reportes financieros (cuánto dinero entró, qué errores hubo) eran visibles
para cualquier persona que supiera la dirección web. Como dejar los estados de
cuenta del banco en la recepción de una oficina para que cualquiera los vea.

**Arreglo:** Ahora necesitas una contraseña para ver esos reportes.

### Problema 3: El archivador de logs crecía sin límite
Cada vez que pasaba algo, el programa lo escribía en un archivo de texto.
Ese archivo nunca se borraba ni dividía. Como llenar cuadernos sin límite —
eventualmente llenarías todo el cuarto.

**Arreglo:** Ahora el archivo se divide automáticamente cuando llega a 10 páginas,
y solo guarda los últimos 5 cuadernos. Máximo 50 páginas en total.

### Problema 4: La puerta de la base de datos estaba abierta hacia la calle
El sistema de almacenamiento de datos estaba configurado para que cualquier
computadora en la misma red WiFi pudiera conectarse directamente.

**Arreglo:** Ahora solo acepta conexiones desde la misma computadora.

### Problema 5: No había límite de cuántas veces podías tocar el timbre
Alguien podría tocar el timbre 10,000 veces por minuto para saturar el sistema.
Como si alguien llenara tu buzón de cartas basura hasta bloquearlo.

**Arreglo:** Ahora el sistema solo acepta 100 toques por minuto de la misma persona.

### Problema 6: No había letrero de "Aquí estamos abiertos"
Los sistemas de monitoreo no podían verificar si el programa estaba funcionando bien
o si solo parecía estar encendido.

**Arreglo:** Se agregó una señal de "Estoy vivo y funcionando" que los monitores revisan cada 10 segundos.

---

## ¿Qué queda pendiente?

Como cualquier negocio, siempre hay cosas en la lista de mejoras:
- ~~El programa registra los pagos, pero todavía no los reenvía a otros sistemas automáticamente~~ — **Implementado (Fase 7):** hay un "mensajero" separado que revisa los pagos registrados y los reenvía automáticamente cada pocos segundos
- No tiene un tablero con gráficas de cuántos pagos entran por hora
- Los datos de los clientes se guardan tal cual — en un futuro habría que protegerlos mejor (LGPD/GDPR)
- Si un pago llega al "buzón de errores" (DLQ), no hay reintento automático todavía — requiere intervención manual

---

# ═══════════════════════════════════════════════════════
# NIVEL INTERMEDIO — Para alguien que entiende tecnología
# ═══════════════════════════════════════════════════════

## ¿Qué es este sistema?

Un microservicio Python que recibe webhooks de Stripe, los valida con múltiples capas
de reglas, los escribe en una base de datos PostgreSQL de forma idempotente, y guarda
una copia en una "bandeja de salida" transaccional para reenvío downstream.

**Stack:** Python 3.12 · FastAPI · Pydantic v2 · PostgreSQL 16 · Docker

---

## Arquitectura en palabras simples

```
Stripe → [POST /webhook/stripe] → Validación Pydantic → PostgreSQL
                                       ↓ (si falla o duplicado)
                                    Dead-Letter Queue (DLQ)
```

1. **Stripe** manda un JSON con datos del pago
2. **FastAPI** recibe la petición HTTP
3. **Pydantic** valida 5 capas de reglas (tipos, formatos, montos, prefijos de cliente)
4. **PostgreSQL** guarda el registro con una guardia de idempotencia (no duplicados)
5. **Outbox** guarda una copia para que otro sistema lo procese después
6. **DLQ** (Dead-Letter Queue) captura los eventos inválidos o duplicados

---

## Fases del proyecto y qué se arregló en cada una

### Fases 1–7 (sesiones anteriores)
Estas fases construyeron la funcionalidad base:
- Validación con Pydantic (modelos, enums, reglas cruzadas)
- Migración de SQLite a PostgreSQL
- Pool de conexiones, índices, BIGINT para montos grandes
- Suite de 108 pruebas automatizadas

### Fase de Re-Auditoría (esta sesión — Mayo 2026)

Se encontraron 17 vulnerabilidades o mejoras pendientes clasificadas así:

| Nivel | Cantidad | Qué significa |
|-------|----------|---------------|
| CRÍTICO | 2 | Para antes de mostrar el código a cualquiera |
| ALTO | 5 | Para antes de usar el sistema con datos reales |
| MEDIO | 6 | Para antes de lanzar a producción |
| BAJO | 4 | Mejoras de calidad de código |

**Las 6 fases implementadas en esta sesión:**

#### Fase 1 — Documentación corrupta + guardia de secreto
- Se corrigió un documento (`LOGIC_AUDIT.md`) que decía cosas falsas sobre el código
- Se agregó una advertencia automática si el sistema arranca sin el secreto real de Stripe

#### Fase 2 — Seguridad y estabilidad del servidor
- El endpoint principal se convirtió de **asíncrono a síncrono** (los dos hacen lo mismo aquí, pero el síncrono es el correcto para llamadas a base de datos)
- Los logs ahora rotan automáticamente (máx 50 MB)
- El puerto de PostgreSQL ahora solo acepta conexiones locales
- Se agregó un "health check" para que Docker sepa si el servicio está sano

#### Fase 3 — Limitación de tráfico + protección adicional
- Se implementó **rate limiting**: máximo 100 peticiones/minuto al endpoint de webhooks
- Se pusieron límites de memoria y CPU al contenedor Docker (256 MB RAM, 0.5 CPU)
- Se agregó una restricción de integridad referencial en la base de datos (FK constraint)
- Se corrigió un bug donde una API key vacía omitía la autenticación

#### Fase 4 — Pruebas que faltaban
- Test para verificar que los resultados del procesamiento en lote están en el orden correcto
- Test para verificar que un error de base de datos devuelve HTTP 503
- Test para verificar que el procesamiento en lote realmente escribió filas en la DB
- Se cerraron conexiones de base de datos que se filtraban en las pruebas

#### Fase 5 — Calidad de código
- Se corrigió una anotación de tipo incorrecta en una función
- Se creó un archivo `requirements-dev.txt` con herramientas de análisis: `ruff` (lint), `mypy` (tipos), `bandit` (seguridad)

---

## ¿Qué queda por hacer?

| Feature | Estado |
|---------|--------|
| Worker del Outbox (reenvío automático) | **Completado — Fase 7:** `worker.py` servicio Docker independiente, drena la cola cada N segundos |
| Secrets management (.env) + HTTPS (Caddy) | **Completado — Fase 8:** credenciales fuera del código, HTTPS automático con Let's Encrypt |
| CI/CD con GitHub Actions | **Completado — Fase 9:** 108 tests + lint en cada push y PR |
| Endpoint de métricas Prometheus | Pendiente |
| Logs estructurados en JSON | Pendiente |
| Mecanismo de reintento para el DLQ | Pendiente — sin esto el DLQ es un cementerio, no una cuarentena |
| Migraciones de schema (Alembic) | Pendiente — `_bootstrap()` hace DDL al arrancar, peligroso en producción |

**El riesgo más alto en producción hoy:** Si un evento llega al DLQ (por error de validación o duplicado), no tiene reintento automático. Requiere intervención manual para reproducirlo. Monitorear `SELECT COUNT(*) FROM dlq WHERE received_at > NOW() - INTERVAL '1 hour'`.

---

## Glosario — Nivel Intermedio

| Término | Significado |
|---------|-------------|
| **Webhook** | Un aviso automático que un servicio manda a otro cuando pasa algo (como una notificación push, pero para servidores) |
| **API** | Una puerta de entrada digital donde los sistemas se comunican. Como el mostrador de un banco donde pides servicios |
| **Endpoint** | Una URL específica que hace una función. Como `/webhook/stripe` = "recibe avisos de Stripe" |
| **PostgreSQL** | Una base de datos. Como una hoja de cálculo muy poderosa y segura que vive en un servidor |
| **Idempotencia** | Que puedes hacer la misma operación 100 veces y el resultado final es el mismo que hacerla 1 vez |
| **DLQ (Dead-Letter Queue)** | La bandeja de mensajes que fallaron o llegaron duplicados. Como la bandeja de "correo no entregable" en una oficina postal |
| **Pool de conexiones** | Como tener 20 líneas telefónicas abiertas con la base de datos en lugar de llamar y colgar cada vez |
| **Rate limiting** | Poner un límite de cuántas veces alguien puede usar el servicio por minuto |
| **Pydantic** | Una librería que verifica que los datos que llegan tienen el formato correcto. Como un aduanero que revisa los documentos |
| **Docker** | Un sistema que empaqueta el programa con todo lo que necesita para correr igual en cualquier computadora. Como una caja de envío estándar |
| **HMAC / Firma digital** | Una firma matemática que prueba que el mensaje no fue alterado y viene del remitente correcto |
| **Health check** | Una verificación automática periódica de que el sistema está funcionando. Como tomarle el pulso a un paciente |
| **Outbox transaccional** | Una "bandeja de salida" que se llena al mismo tiempo que se registra el pago — garantizando que ambas cosas pasan o ninguna |
| **FK constraint** | Una regla en la base de datos que dice "esta columna debe apuntar a algo que existe en otra tabla" |

---

# ═══════════════════════════════════════════════════════
# NIVEL TÉCNICO — Para desarrolladores e ingenieros
# ═══════════════════════════════════════════════════════

## Stack

| Componente | Versión | Rol |
|-----------|---------|-----|
| Python | 3.12 | Runtime |
| FastAPI | 0.115.0 | HTTP layer |
| Pydantic v2 | 2.9.2 | Validation stack |
| psycopg2-binary | 2.9.9 | PostgreSQL driver (sync) |
| PostgreSQL | 16-alpine | Persistent storage |
| stripe | 10.12.0 | Webhook HMAC verification |
| slowapi | 0.1.9 | Rate limiting (new — Phase 3) |
| uvicorn[standard] | 0.30.6 | ASGI server |

---

## Arquitectura de componentes

```
Stripe (external)
    │ POST /webhook/stripe
    │ Header: Stripe-Signature: t=...,v1=...
    ▼
FastAPI (sync def — threadpool via run_in_threadpool)
    │
    ├─ stripe.Webhook.construct_event()    ← HMAC-SHA256 verification
    │
    ├─ StripeEvent(**payload)              ← Pydantic 5-layer validation
    │       1. Type coercion (BaseModel)
    │       2. Field constraints (min_length, ge=0, pattern=^[a-z]{3}$)
    │       3. Model validator: check_amount_present
    │       4. Model validator: check_invoice_amount_nonzero
    │       5. Model validator: check_customer_id_format
    │
    ├─ BEGIN (autocommit=False via _tx())
    │       INSERT INTO ledger ... ON CONFLICT (transaction_id) DO NOTHING
    │       RETURNING transaction_id
    │       → rowcount=1: INSERT INTO outbox ...
    │       → rowcount=0: INSERT INTO dlq (reason=DUPLICATE)
    │   COMMIT
    │
    └─ JSONResponse(outcome=POSTED|VOID|PENDING|DLQ_DUPLICATE|DLQ_INVALID)
```

---

## Resumen de todas las fases de implementación

### Fases 1–7 (sesiones anteriores, Mayo 2026)

#### Fase 1 — Entry Validation (commit: `0dd866c`)
- `EventType(str, Enum)`: 5 tipos de eventos Stripe
- `StripeObject`: `customer` min_length=4, `amount_paid/amount` ge=0, `currency` pattern `^[a-z]{3}$`
- `@model_validator(mode='after')` `check_amount_present`: OR lógico entre amount_paid/amount
- 34 tests, ~12,412 TPS benchmark (SQLite in-memory)

#### Fase 2 — Output Quality (commit: `d2e2e9a`)
- `LedgerStatus(str, Enum)`: POSTED / PENDING / VOID
- `DLQReason(str, Enum)`: DUPLICATE / INVALID / UNKNOWN_TYPE
- `DLQEntry(BaseModel)` con `to_db()` → 4-tuple
- `LedgerEntry(BaseModel)` con `to_db()` → 9-tuple
- `process_stripe_event()` refactorizado para usar ambos modelos

#### Fase 3 — Cross-field Validators (commit: `d82e729`)
- `check_invoice_amount_nonzero`: invoice.paid + invoice.payment_failed necesitan amount > 0
- `check_customer_id_format`: customer debe empezar con `cus_`
- Suscripciones exentas de la regla de amount > 0
- 69 tests en total

#### Fase 4 — Migración PostgreSQL (commit: `6554181`)
- Eliminación completa de SQLite
- `psycopg2.connect()` con `autocommit=True`
- `_tx()` context manager: `BEGIN` / `COMMIT` / `ROLLBACK` explícitos
- `ON CONFLICT (transaction_id) DO NOTHING` para idempotencia
- `ledger.transaction_id TEXT PRIMARY KEY`
- `amount_cents BIGINT` (PostgreSQL INTEGER max = 2.1B; BIGINT = 9.2T)

#### Fase 5 — Batch Processing (commit: `c8f1b30`)
- `process_stripe_event_batch()`: `execute_values` + `RETURNING transaction_id`
- Reducción de N round trips a `ceil(N/1000) × 3` round trips
- 26 TPS (per-event, Docker-WSL2) → ~5,000 TPS (batch)
- Duplicate partitioning via `remaining_ids: set[str]` — consume on first match
- Bug fix: `remaining_ids.discard()` en commit `457ebee`

#### Fase 6 — Docs (commit: `5e47362`)
- Comentarios bilingües (EN/ES) en todo el código
- `LOGIC_AUDIT.md`: estado arquitectónico authoritative
- `IMPLEMENTATION_REPORT.md`: reporte privado completo
- `README.md`: reporte público

#### Fase 7 — Integration Tests (commit: `d5b56df`)
- 18 tests HTTP via `TestClient` con PostgreSQL real (no mocks de DB)
- 4 tests de concurrencia: `threading.Barrier(5)` → exactamente 1 POSTED, 4 DUPLICATE
- 4 tests de outbox dispatch simulation
- 6 tests DLQ queryability
- 101 tests en total

#### Remediation (commits: `03a4226`, `48c9779`)
- `ThreadedConnectionPool(minconn=1, maxconn=20)` reemplaza `_conn` módulo-nivel
- `_require_api_key` dependency en `/ledger/summary` y `/dlq/entries`
- `BIGINT` para `amount_cents`
- Índices: `idx_ledger_customer_id`, `idx_outbox_dispatched_id WHERE dispatched=0`
- `threading.Barrier(5)` en test de concurrencia
- `if limit < 1: raise HTTPException(422)`
- `chown -R billing:billing /app` en Dockerfile

---

### Re-Auditoría — Mayo 2026 (esta sesión)

#### Fase 1 CRÍTICO (commit: `8dfe1cf`)

**C1 — `LOGIC_AUDIT.md:105`:**
Doc decía "Stripe verification commented out — stripe SDK not in requirements". Falso.
`stripe==10.12.0` está en `requirements.txt` y `stripe.Webhook.construct_event()` se llama
en cada request. Actualizado a "ACTIVE".

**C2 — `ledger.py:514`:**
`STRIPE_WEBHOOK_SECRET = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_YOUR_SECRET_HERE")`
Sin guard de startup. Deploy misconfigured silenciosamente aceptaría todos los webhooks
(construct_event compararía contra el literal). Solución: `warnings.warn` si el valor
es igual al placeholder. Se usó `warnings.warn` en lugar de `RuntimeError` para no romper
tests y entornos dev que no configuran la variable.

#### Fase 2 HIGH (commit: `e4703be`)

**H1 — Event loop blocking:**
`async def stripe_webhook` llamaba `_pool.getconn()` y `process_stripe_event()` (sync
psycopg2) sin `run_in_executor`. Bloqueaba el event loop de asyncio en cada request.
Fix: `def stripe_webhook` (sync) — FastAPI despacha automáticamente via `run_in_threadpool`.
También `await request.body()` → `request.body()` (Starlette provee `.body()` sync en
handlers sync). Mismo cambio para `health`.

**H2 — Log rotation:**
`logging.FileHandler("billing_ledger.log")` → `RotatingFileHandler(maxBytes=10_485_760, backupCount=5)`.
A 500 TPS con DLQ write failures loggeando full raw payload: disco lleno en horas.

**H3 — PG port:**
`"5432:5432"` → `"127.0.0.1:5432:5432"`. Bind a `0.0.0.0` exponía PostgreSQL a toda la LAN
con credenciales `postgres:postgres`.

**H4 — Billing healthcheck:**
```yaml
healthcheck:
  test: ["CMD-SHELL", "wget -qO- http://localhost:8000/health || exit 1"]
  interval: 10s; timeout: 5s; retries: 5; start_period: 10s
```
Sin healthcheck, Docker/K8s no puede distinguir liveness de readiness.

#### Fase 3 HIGH+MEDIUM (commit: `346a6bd`)

**H5 — Rate limiting:**
`slowapi==0.1.9` + `Limiter(key_func=get_remote_address)`.
- `/webhook/stripe`: `@_limiter.limit("100/minute")`
- `/dlq/entries`: `@_limiter.limit("20/minute")`
- `/ledger/summary`: `@_limiter.limit("20/minute")`
Sin esto: flood de IPs → pool exhaustion (maxconn=20) + DLQ unbounded growth.

**M1 — Resource limits:**
```yaml
deploy:
  resources:
    limits:
      memory: 256M
      cpus: "0.50"
```

**M2 — Outbox FK:**
```sql
transaction_id TEXT NOT NULL REFERENCES ledger(transaction_id) ON DELETE CASCADE
```
Antes: invariante del Transactional Outbox garantizada solo por código de aplicación.
Ahora: garantizada también por constraint de base de datos.

**M6 — Empty API key bypass:**
Antes: `if BILLING_API_KEY and x_api_key != BILLING_API_KEY` → si `BILLING_API_KEY=""`,
el `and` hace short-circuit y el check se omite silenciosamente.
Ahora: tres ramas explícitas:
```python
if not BILLING_API_KEY: return       # dev mode
if not x_api_key: raise 401          # missing header
if x_api_key != BILLING_API_KEY: raise 401  # wrong key
```

#### Fase 4 MEDIUM+LOW tests (commit: `49f5bd2`)

**M3 — Batch position indexing:**
```python
pos_results = L.process_stripe_event_batch(conn, [ev_a, ev_b, ev_c])
assert pos_results[0]["transaction_id"] == "evt_pos_a"
assert pos_results[1]["transaction_id"] == "evt_pos_b"
assert pos_results[2]["transaction_id"] == "evt_pos_c"
```
Sin esto: un bug que devuelve resultados en orden de RETURNING (≠ input order) pasa todos los tests.

**M4 — DB failure → 503:**
```python
with patch("ledger.process_stripe_event", side_effect=RuntimeError("DB write failed")):
    r = client.post("/webhook/stripe", json=ev)
    assert r.status_code == 503
```

**M5 — Batch data integrity:**
```python
bench_row_count = _q1(conn4, "SELECT COUNT(*) FROM ledger")
assert bench_row_count == N
```
Sin esto: una función batch que devuelve dicts sin tocar la DB pasaba el test de TPS.

**L3 — Connection leaks:**
`conn.close()` agregado explícitamente en: `conn4`, `large_conn`, `dispatch_conn`,
`dlq_conn`, `batch_conn`. ~12 conexiones abiertas por ejecución → CI exhaust
`max_connections=100` al correr la suite repetidamente.

**L4 — Empty batch:**
```python
assert L.process_stripe_event_batch(conn, []) == []
```

#### Fase 5 LOW (commit: `bb6479c`)

**L1 — Return type annotation:**
`get_amount(self) -> int` → `get_amount(self) -> Optional[int]`.
`StripeEvent.check_amount_present` previene `None` en el path normal, pero
`StripeObject()` puede construirse directamente sin pasar por ese validador.
La anotación anterior era una mentira al type checker.

**L2 — Dev toolchain:**
`requirements-dev.txt`:
```
ruff==0.4.4      # lint + format (replaces flake8 + isort + pyupgrade)
mypy==1.10.0     # static type checking
bandit==1.7.8    # security lint (OWASP Top 10 patterns)
```

---

## Gaps de producción pendientes

| Gap | Impacto | Esfuerzo | Estado |
|-----|---------|---------|--------|
| Outbox worker (dispatch loop) | CRÍTICO — revenue no forwarded | Alto | **Completado** — `worker.py` Fase 7 |
| Secrets management + HTTPS | CRÍTICO — credenciales hardcodeadas | Bajo | **Completado** — `.env` + Caddy Fase 8 |
| CI/CD pipeline | ALTO — sin badge no hay confianza | Bajo | **Completado** — GitHub Actions Fase 9 |
| Schema migrations (Alembic) | ALTO — `_bootstrap()` destruye datos en prod | Medio | Pendiente |
| DLQ retry budget (`retry_count`, `max_retries`) | Medio — DLQ es cementerio sin esto | Medio | Pendiente |
| Structured JSON logging | Alto — observabilidad en cloud | Bajo | Pendiente |
| Prometheus `/metrics` endpoint | Medio | Bajo (ya comentado en requirements.txt) | Pendiente |
| PII redaction en `payload`/`raw_payload` | Legal (LGPD/GDPR) | Alto | Pendiente |

---

## Comandos de referencia

```bash
# Instalar dependencias de producción
pip install -r requirements.txt

# Instalar herramientas de desarrollo
pip install -r requirements-dev.txt

# Correr tests (requiere PostgreSQL corriendo)
docker-compose up -d postgres
python test_ledger.py

# Lint
ruff check ledger.py
ruff format ledger.py

# Type check
mypy ledger.py

# Security scan
bandit -r ledger.py

# Levantar stack completo
docker-compose up

# Ver estado del sistema (requiere BILLING_API_KEY)
curl -H "X-Api-Key: $BILLING_API_KEY" http://localhost:8000/ledger/summary
curl -H "X-Api-Key: $BILLING_API_KEY" http://localhost:8000/dlq/entries
```

---

## Glosario — Nivel Técnico

| Término | Definición técnica |
|---------|-------------------|
| **Webhook** | HTTP callback — una petición POST que un servicio externo (Stripe) envía a tu endpoint cuando ocurre un evento |
| **HMAC-SHA256** | Hash-based Message Authentication Code. Stripe firma el body del request con un secreto compartido. `stripe.Webhook.construct_event()` verifica que la firma coincide antes de procesar |
| **Idempotencia** | Propiedad de una operación donde ejecutarla N veces produce el mismo resultado que ejecutarla 1 vez. Implementada aquí vía `ON CONFLICT (transaction_id) DO NOTHING` |
| **Transactional Outbox** | Patrón de arquitectura: el row de ledger y el row de outbox se escriben en la misma transacción `BEGIN/COMMIT`. Garantiza que nunca habrá un ledger row sin su correspondiente outbox row |
| **DLQ (Dead-Letter Queue)** | Cola de mensajes muertos — destino de eventos que fallaron validación (INVALID) o llegaron duplicados (DUPLICATE). Preserva el `raw_payload` byte-perfect para replay manual |
| **psycopg2** | Driver Python para PostgreSQL (síncrono). Usa el protocolo de red binary de PG |
| **ThreadedConnectionPool** | Pool de conexiones thread-safe de psycopg2. `getconn()`/`putconn()` para borrow/return. maxconn=20 limita conexiones concurrentes a PostgreSQL |
| **run_in_threadpool** | Mecanismo de Starlette (subyace FastAPI) que despacha handlers `def` síncronos a un ThreadPoolExecutor, evitando que bloqueen el event loop de asyncio |
| **execute_values** | Helper de `psycopg2.extras` para bulk INSERT. Colapsa N INSERTs individuales en `ceil(N/page_size)` statements. Con page_size=1000: 9,000 bind params por statement (dentro del límite 65,535 de PostgreSQL) |
| **RETURNING** | Cláusula PostgreSQL que devuelve valores de filas afectadas por un INSERT/UPDATE/DELETE. Usado aquí para identificar qué rows de un bulk INSERT con ON CONFLICT realmente se insertaron (sin race window) |
| **slowapi** | Rate limiting para FastAPI. Wrap sobre la librería `limits`. `Limiter(key_func=get_remote_address)` identifica callers por IP. Decorador `@limiter.limit("100/minute")` en el endpoint |
| **RotatingFileHandler** | Handler de logging de stdlib Python. Cuando el archivo alcanza `maxBytes`, lo renombra a `.1`, `.2`, etc., hasta `backupCount`. Limita storage total a `maxBytes × backupCount` |
| **Pydantic v2 model_validator** | Decorator `@model_validator(mode='after')` — corre después de que todos los Field constraints pasaron. Recibe el modelo completo (`self`) — puede leer cualquier campo para validaciones cruzadas |
| **TIMESTAMPTZ** | Tipo PostgreSQL timezone-aware. Correcto para ledgers financieros — `date_trunc('month', created_at)` y `AT TIME ZONE` funcionan correctamente. `DOUBLE PRECISION` (Unix timestamp) no tiene estas propiedades |
| **BIGINT** | PostgreSQL 8 bytes, rango ±9.2 × 10¹⁸. Necesario para `amount_cents` en contexto B2B enterprise (INTEGER max = 2.1B = ~$21M USD) |
| **FK constraint** | Foreign Key constraint en PostgreSQL. `REFERENCES ledger(transaction_id) ON DELETE CASCADE` — un outbox row no puede existir sin su ledger row correspondiente. Error a nivel DB, no solo aplicación |
| **`ON DELETE CASCADE`** | Cuando se elimina un row de `ledger`, todos los rows de `outbox` con el mismo `transaction_id` se eliminan automáticamente |
| **ruff** | Linter/formatter Python escrito en Rust. Reemplaza flake8, isort, pyupgrade. ~100× más rápido que flake8 |
| **mypy** | Type checker estático para Python. Detecta `get_amount() -> int` cuando la función puede devolver `None` |
| **bandit** | Security linter para Python. Detecta patrones OWASP Top 10: hardcoded passwords, unsafe deserialization, SQL injection vectors, etc. |
| **Optional[int]** | En Python typing: `Union[int, None]`. Un valor que puede ser un entero o `None` |
| **rate limiting** | Control de cuántas peticiones acepta un endpoint por unidad de tiempo. Previene DoS, abuso, y agotamiento de recursos (connection pool exhaustion) |

---

*Diego Alonso Del Río García — posthog-billing-poc — Mayo 2026*
*Documento privado — no publicar en repositorio público*
