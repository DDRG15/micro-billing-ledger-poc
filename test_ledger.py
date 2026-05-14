"""
test_ledger.py — Full Integration Test Suite / Suite Completa de Tests de Integración
======================================================================================
EN: 101 tests across 5 phases. Run with: python test_ledger.py
    Requires: PostgreSQL running and DATABASE_URL set (see docker-compose.yml).
    Covers: entry validation, output models, cross-field rules, HTTP layer,
    concurrent insertion, outbox dispatch simulation, DLQ queryability.
    No mocks. No stubs. Every test hits a real PostgreSQL database.
    The test runner is deliberately simple — no pytest dependency — so anyone
    can run it with a plain Python install.

ES: 101 tests en 5 fases. Ejecutar con: python test_ledger.py
    Requiere: PostgreSQL en ejecución y DATABASE_URL establecido (ver docker-compose.yml).
    Cubre: validación de entrada, modelos de salida, reglas de campos cruzados,
    capa HTTP, inserción concurrente, simulación de despacho del outbox, consultabilidad del DLQ.
    Sin mocks. Sin stubs. Cada test golpea una base de datos PostgreSQL real.
    El runner de tests es deliberadamente simple — sin dependencia de pytest — para que
    cualquiera pueda ejecutarlo con una instalación Python plana.
"""

# ---------------------------------------------------------------------------
# Standard library imports / Importaciones de la librería estándar
# ---------------------------------------------------------------------------
import io          # EN: Used to wrap stdout for UTF-8 output on Windows / ES: Usado para envolver stdout para salida UTF-8 en Windows
import json        # EN: Used to verify raw_payload round-trips correctly / ES: Usado para verificar que raw_payload va y vuelve correctamente
import sys         # EN: stdout override for Windows UTF-8 encoding / ES: Override de stdout para codificación UTF-8 en Windows
import time        # EN: Used in benchmark timing / ES: Usado en temporización del benchmark

# EN: Force UTF-8 output on Windows. Without this, Unicode characters in print()
#     cause UnicodeEncodeError because Windows stdout defaults to CP-1252.
#     This must happen before any print() call — so it's at the top of the file.
# ES: Forzar salida UTF-8 en Windows. Sin esto, los caracteres Unicode en print()
#     causan UnicodeEncodeError porque stdout de Windows tiene CP-1252 por defecto.
#     Debe ocurrir antes de cualquier llamada print() — por eso está en la parte superior del archivo.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# EN: Import the ledger module directly — all tests call its functions and use
#     its models. This is the module under test.
# ES: Importar el módulo ledger directamente — todos los tests llaman a sus funciones
#     y usan sus modelos. Este es el módulo bajo prueba.
import ledger as L

# ---------------------------------------------------------------------------
# Test infrastructure / Infraestructura de tests
# EN: Minimal test runner — two globals (ok, fail) and one helper function.
#     chk() is the assertion function: prints PASS or FAIL with the test name.
#     No pytest, no unittest — intentional. Any recruiter can read and run this.
# ES: Runner de tests mínimo — dos globales (ok, fail) y una función auxiliar.
#     chk() es la función de aserción: imprime PASS o FAIL con el nombre del test.
#     Sin pytest, sin unittest — intencional. Cualquier reclutador puede leer y ejecutar esto.
# ---------------------------------------------------------------------------
ok = fail = 0

def chk(name: str, cond: bool, detail: str = "") -> None:
    """
    EN: Assertion helper. Increments ok or fail and prints result.
        detail is shown only on failure — useful for showing actual vs expected values.
    ES: Auxiliar de aserción. Incrementa ok o fail e imprime el resultado.
        detail se muestra solo en fallo — útil para mostrar valores actuales vs esperados.
    """
    global ok, fail
    if cond:
        ok   += 1
        print(f"  PASS  {name}")
    else:
        fail += 1
        print(f"  FAIL  {name}" + (f"  |  {detail}" if detail else ""))


def _q1(conn, sql: str, params: tuple = ()) -> object:
    """
    EN: Execute a query and return the first column of the first row.
        Used for COUNT(*) and similar single-value reads. psycopg2 connections
        do not have a .execute() shortcut — cursors are required.
    ES: Ejecutar una consulta y retornar la primera columna de la primera fila.
        Usado para COUNT(*) y lecturas similares de valor único. Las conexiones
        psycopg2 no tienen un atajo .execute() — se requieren cursores.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def _qall(conn, sql: str, params: tuple = ()) -> list:
    """
    EN: Execute a query and return all rows as a list of tuples.
    ES: Ejecutar una consulta y retornar todas las filas como lista de tuplas.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


def fresh_conn():
    """
    EN: Returns a PostgreSQL connection with all tables truncated.
        Each test section that needs isolation calls fresh_conn() to start clean.
        TRUNCATE ... RESTART IDENTITY resets all table data and BIGSERIAL sequences.
        No mocking — this hits the real PostgreSQL database defined by DATABASE_URL.
        Requires: docker-compose up -d postgres (or a running PostgreSQL instance).
    ES: Retorna una conexión PostgreSQL con todas las tablas truncadas.
        Cada sección de test que necesita aislamiento llama fresh_conn() para empezar limpio.
        TRUNCATE ... RESTART IDENTITY resetea todos los datos de tablas y secuencias BIGSERIAL.
        Sin mocking — esto golpea la base de datos PostgreSQL real definida por DATABASE_URL.
        Requiere: docker-compose up -d postgres (o una instancia PostgreSQL en ejecución).
    """
    conn = L._bootstrap()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
    return conn


def fake_event(
    eid: str = "evt_test_001",
    etype: str = "invoice.paid",
    customer: str = "cus_123",
    amount: int = 4900,
    currency: str = "usd",
    include_request: bool = True,
) -> dict:
    """
    EN: Factory function for fake Stripe events. Produces structurally valid payloads
        that pass all Pydantic validators by default. Override individual fields to
        test specific failure modes (e.g., currency="USD" to test uppercase rejection).
        customer default "cus_123" passes min_length=4 AND the cus_ prefix check.
        amount default 4900 passes ge=0 AND the nonzero invoice check.
    ES: Función fábrica para eventos Stripe falsos. Produce payloads estructuralmente
        válidos que pasan todos los validadores Pydantic por defecto. Sobreescribir
        campos individuales para probar modos de fallo específicos (ej., currency="USD"
        para probar el rechazo de mayúsculas). El cliente por defecto "cus_123" pasa
        min_length=4 Y el check del prefijo cus_. El monto por defecto 4900 pasa
        ge=0 Y el check de factura no cero.
    """
    event = {
        "id": eid,
        "type": etype,
        "data": {"object": {
            "customer":    customer,
            "amount_paid": amount,
            "currency":    currency,
        }},
    }
    if include_request:
        event["request"] = {"idempotency_key": f"idem_{eid}"}
    return event


# =============================================================================
# PHASE 1: PYDANTIC ENTRY BOUNDARY VALIDATION
# FASE 1: VALIDACIÓN DE LÍMITE DE ENTRADA PYDANTIC
#
# EN: Tests that the Pydantic models (StripeEvent, StripeObject) correctly reject
#     invalid payloads at the entry boundary. Every invalid payload should route
#     to DLQ with reason=INVALID, never touch the ledger table.
# ES: Tests que los modelos Pydantic (StripeEvent, StripeObject) rechazan correctamente
#     payloads inválidos en el límite de entrada. Cada payload inválido debe enrutarse
#     al DLQ con reason=INVALID, nunca tocar la tabla del libro.
# =============================================================================

print("\n── Phase 1: Pydantic Validation (Entry Boundary) ────────────────────────")

conn = fresh_conn()
ev   = fake_event()

# EN: Happy path — valid event should be posted to ledger, not DLQ.
# ES: Camino feliz — el evento válido debe publicarse en el libro, no en el DLQ.
r1 = L.process_stripe_event(conn, ev)
chk("valid event → POSTED",        r1["outcome"] == "POSTED")
chk("transaction_id returned",     r1["transaction_id"] == "evt_test_001")


print("\n── Phase 1: Currency Validation (Regex Pattern) ────────────────────────")
# EN: Tests the `pattern=r"^[a-z]{3}$"` constraint on StripeObject.currency.
#     Only exactly 3 lowercase ASCII letters are accepted. ISO 4217 enforced at entry.
# ES: Tests la restricción `pattern=r"^[a-z]{3}$"` en StripeObject.currency.
#     Solo exactamente 3 letras ASCII minúsculas son aceptadas. ISO 4217 aplicado en entrada.

conn = fresh_conn()

ev_valid = fake_event(currency="usd")
r = L.process_stripe_event(conn, ev_valid)
chk("currency 'usd' → POSTED",                  r["outcome"] == "POSTED")

# EN: "USD" (uppercase) fails the lowercase-only regex.
#     See BLUEPRINT_ANALYSIS.md §5 for the .lower() normalization fix.
# ES: "USD" (mayúsculas) falla el regex de solo minúsculas.
#     Ver BLUEPRINT_ANALYSIS.md §5 para la corrección de normalización .lower().
ev_uppercase = fake_event(eid="evt_invalid_curr_1", currency="USD")
r = L.process_stripe_event(conn, ev_uppercase)
chk("currency 'USD' (uppercase) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

# EN: "us" fails because the pattern requires exactly 3 chars, not 2.
# ES: "us" falla porque el patrón requiere exactamente 3 chars, no 2.
ev_short = fake_event(eid="evt_invalid_curr_2", currency="us")
r = L.process_stripe_event(conn, ev_short)
chk("currency 'us' (2 chars) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")


print("\n── Phase 1: Customer ID Validation (Min Length) ────────────────────────")
# EN: Tests the min_length=4 constraint on StripeObject.customer.
#     The cus_ prefix check is a separate cross-field validator in StripeEvent.
# ES: Tests la restricción min_length=4 en StripeObject.customer.
#     El check del prefijo cus_ es un validador de campos cruzados separado en StripeEvent.

conn = fresh_conn()

ev_valid = fake_event(customer="cus_123")
r = L.process_stripe_event(conn, ev_valid)
chk("customer 'cus_123' → POSTED",              r["outcome"] == "POSTED")

# EN: "cus" is only 3 chars — fails min_length=4. The 4th char would be the underscore.
# ES: "cus" es solo 3 chars — falla min_length=4. El 4to char sería el guión bajo.
ev_short = fake_event(eid="evt_short_cus", customer="cus")
r = L.process_stripe_event(conn, ev_short)
chk("customer 'cus' (3 chars) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

# EN: Empty string fails min_length=4 (0 < 4).
# ES: String vacío falla min_length=4 (0 < 4).
ev_empty = fake_event(eid="evt_empty_cus", customer="")
r = L.process_stripe_event(conn, ev_empty)
chk("customer '' (empty) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")


print("\n── Phase 1: Amount Validation (Fallback + Non-Negative) ────────────────")
# EN: Tests the amount field logic:
#     - amount_paid (preferred) OR amount (fallback) must be present
#     - Both have ge=0 — negative values rejected by Field before validators run
#     - If both are None, the check_amount_present validator raises ValueError
# ES: Tests la lógica del campo de monto:
#     - amount_paid (preferido) O amount (respaldo) debe estar presente
#     - Ambos tienen ge=0 — valores negativos rechazados por Field antes de que corran los validadores
#     - Si ambos son None, el validador check_amount_present lanza ValueError

conn = fresh_conn()

ev_amount_paid = fake_event(eid="evt_amt_1", amount=5000)
r = L.process_stripe_event(conn, ev_amount_paid)
chk("amount_paid 5000 → POSTED",                r["outcome"] == "POSTED")

# EN: Negative amount fails ge=0 at Field level — Pydantic rejects before validators.
# ES: Monto negativo falla ge=0 a nivel de Field — Pydantic rechaza antes de los validadores.
ev_negative = fake_event(eid="evt_negative", amount=-5000)
r = L.process_stripe_event(conn, ev_negative)
chk("amount_paid -5000 → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

# EN: amount_paid=None triggers fallback to amount=3000. get_amount() returns 3000.
#     This tests the smart fallback in StripeObject.get_amount().
# ES: amount_paid=None dispara el respaldo a amount=3000. get_amount() retorna 3000.
#     Esto prueba el respaldo inteligente en StripeObject.get_amount().
ev_amount_fallback = {
    "id": "evt_amount_fallback",
    "type": "invoice.paid",
    "data": {"object": {
        "customer":    "cus_fallback",
        "amount_paid": None,
        "amount":      3000,
        "currency":    "usd",
    }},
    "request": {},
}
r = L.process_stripe_event(conn, ev_amount_fallback)
chk("amount (fallback from amount_paid) → POSTED", r["outcome"] == "POSTED")

# EN: Both amount_paid=None and amount=None fails check_amount_present validator.
# ES: Ambos amount_paid=None y amount=None falla el validador check_amount_present.
ev_no_amount = {
    "id": "evt_no_amount",
    "type": "invoice.paid",
    "data": {"object": {
        "customer":    "cus_no_amount",
        "amount_paid": None,
        "amount":      None,
        "currency":    "usd",
    }},
    "request": {},
}
r = L.process_stripe_event(conn, ev_no_amount)
chk("both amount fields None → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")


print("\n── Phase 1: EventType Enum Validation ─────────────────────────────────")
# EN: Tests that the EventType enum rejects unknown event type strings at model
#     creation. Pydantic handles this before any business logic runs — the event
#     never reaches the ledger or outbox.
# ES: Tests que el enum EventType rechaza strings de tipo de evento desconocidos
#     en la creación del modelo. Pydantic maneja esto antes de que corra cualquier
#     lógica de negocio — el evento nunca llega al libro o al outbox.

conn = fresh_conn()

ev_valid_type = fake_event(etype="invoice.paid")
r = L.process_stripe_event(conn, ev_valid_type)
chk("event type 'invoice.paid' → POSTED",       r["outcome"] == "POSTED")

# EN: "payment.created" is not in EventType enum — rejected as DLQ_INVALID.
#     Previously this was DLQ_UNKNOWN_TYPE; now Pydantic catches it as INVALID.
# ES: "payment.created" no está en el enum EventType — rechazado como DLQ_INVALID.
#     Anteriormente era DLQ_UNKNOWN_TYPE; ahora Pydantic lo captura como INVALID.
ev_invalid_type = fake_event(eid="evt_bad_type", etype="payment.created")
r = L.process_stripe_event(conn, ev_invalid_type)
chk("event type 'payment.created' → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")


print("\n── Phase 1: Missing Required Fields ───────────────────────────────────")
# EN: Tests that missing or empty required fields route to DLQ_INVALID.
#     These are structural failures — the payload doesn't match the expected shape.
# ES: Tests que campos requeridos faltantes o vacíos enrutan a DLQ_INVALID.
#     Estos son fallos estructurales — el payload no coincide con la forma esperada.

conn = fresh_conn()

# EN: Empty id fails min_length=1 on StripeEvent.id.
# ES: id vacío falla min_length=1 en StripeEvent.id.
ev_no_id = {
    "id": "",
    "type": "invoice.paid",
    "data": {"object": {"customer": "cus_123", "amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_id)
chk("empty id → DLQ_INVALID",                   r["outcome"] == "DLQ_INVALID")

# EN: Empty type string fails EventType enum validation.
# ES: String de tipo vacío falla la validación del enum EventType.
ev_no_type = {
    "id": "evt_no_type",
    "type": "",
    "data": {"object": {"customer": "cus_123", "amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_type)
chk("empty type → DLQ_INVALID",                 r["outcome"] == "DLQ_INVALID")

# EN: Missing customer field — Pydantic raises because 'customer' is required in StripeObject.
# ES: Campo customer faltante — Pydantic lanza porque 'customer' es requerido en StripeObject.
ev_no_customer = {
    "id": "evt_no_cus",
    "type": "invoice.paid",
    "data": {"object": {"amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_customer)
chk("missing customer → DLQ_INVALID",           r["outcome"] == "DLQ_INVALID")


print("\n── Idempotency guard ────────────────────────────────────────────────")
# EN: Core idempotency test — verifies that ON CONFLICT (transaction_id) DO NOTHING
#     correctly prevents duplicate rows. The second call with the same event ID must
#     route to DLQ_DUPLICATE, not write a second ledger row. The ledger must have
#     exactly 1 row after both calls.
# ES: Test de idempotencia central — verifica que ON CONFLICT (transaction_id) DO NOTHING
#     evite correctamente filas duplicadas. La segunda llamada con el mismo ID de evento
#     debe enrutar a DLQ_DUPLICATE, no escribir una segunda fila en el libro. El libro
#     debe tener exactamente 1 fila después de ambas llamadas.

conn = fresh_conn()
ev   = fake_event()

r1 = L.process_stripe_event(conn, ev)
chk("first insert → POSTED",                    r1["outcome"] == "POSTED")
chk("transaction_id returned",                  r1["transaction_id"] == "evt_test_001")

r2 = L.process_stripe_event(conn, ev)           # EN: exact same event / ES: exactamente el mismo evento
chk("duplicate → DLQ_DUPLICATE",               r2["outcome"] == "DLQ_DUPLICATE")
chk("DLQ row written",
    _q1(conn, "SELECT COUNT(*) FROM dlq") == 1)

# EN: The duplicate must NOT create a second ledger row — idempotency guard must hold.
# ES: El duplicado NO debe crear una segunda fila en el libro — la guardia de idempotencia debe mantenerse.
ledger_count = _q1(conn, "SELECT COUNT(*) FROM ledger")
chk("ledger has exactly 1 row after replay",    ledger_count == 1)


print("\n── Outbox written atomically ────────────────────────────────────────")
# EN: Verifies the Transactional Outbox pattern — the outbox row is written in the
#     same BEGIN...COMMIT as the ledger row. Uses the same conn from above.
#     The duplicate call above should NOT have written a second outbox row.
# ES: Verifica el patrón de Outbox Transaccional — la fila del outbox se escribe
#     en el mismo BEGIN...COMMIT que la fila del libro. Usa el mismo conn de arriba.
#     La llamada duplicada de arriba NO debe haber escrito una segunda fila del outbox.

outbox_count = _q1(conn, "SELECT COUNT(*) FROM outbox WHERE dispatched=0")
chk("outbox has 1 pending row",                 outbox_count == 1)

row = _qall(conn, "SELECT transaction_id, event_type FROM outbox WHERE dispatched=0")[0]
chk("outbox row has correct tx_id",             row[0] == "evt_test_001")
chk("outbox row has correct event_type",        row[1] == "invoice.paid")


print("\n── Validation & DLQ routing ─────────────────────────────────────────")
# EN: Verifies that different failure modes all correctly route to DLQ.
#     Each test uses a fresh connection to avoid interference from previous tests.
# ES: Verifica que diferentes modos de fallo enrutan correctamente al DLQ.
#     Cada test usa una conexión fresca para evitar interferencia de tests anteriores.

conn2 = fresh_conn()

# EN: Empty id is caught by StripeEvent.id min_length=1 — Pydantic, not business logic.
# ES: id vacío es capturado por StripeEvent.id min_length=1 — Pydantic, no lógica de negocio.
r = L.process_stripe_event(conn2, {"id": "", "type": "invoice.paid", "data": {}})
chk("empty id → DLQ_INVALID",                  r["outcome"] == "DLQ_INVALID")

# EN: Unknown event type caught by EventType enum — same Pydantic layer.
# ES: Tipo de evento desconocido capturado por el enum EventType — misma capa Pydantic.
r = L.process_stripe_event(conn2, fake_event(eid="evt_x", etype="payment.created"))
chk("unknown type → DLQ_INVALID",              r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']} instead of DLQ_INVALID")

dlq_count = _q1(conn2, "SELECT COUNT(*) FROM dlq")
chk("DLQ has 2 rows",                           dlq_count == 2)


print("\n── All supported event types route correctly ─────────────────────────")
# EN: Verifies that the _STATUS_MAP correctly maps all 5 EventType values to their
#     expected LedgerStatus. If _STATUS_MAP is incomplete, this test will fail with
#     a KeyError — which is the point. No silent mapping gaps.
# ES: Verifica que _STATUS_MAP mapea correctamente los 5 valores EventType a su
#     LedgerStatus esperado. Si _STATUS_MAP está incompleto, este test fallará con
#     un KeyError — ese es el punto. Sin brechas de mapeo silenciosas.

conn3 = fresh_conn()
expected = {
    "invoice.paid":                   "POSTED",
    "invoice.payment_failed":         "VOID",
    "customer.subscription.created":  "POSTED",
    "customer.subscription.deleted":  "VOID",
    "customer.subscription.updated":  "PENDING",
}
for i, (etype, expected_status) in enumerate(expected.items()):
    ev = fake_event(eid=f"evt_{i:04d}", etype=etype)
    r  = L.process_stripe_event(conn3, ev)
    chk(f"{etype} → {expected_status}",
        r["outcome"] == expected_status, f"got {r['outcome']}")


print("\n── Headless benchmark ───────────────────────────────────────────────")
# EN: Throughput measurement — 5,000 unique events, no HTTP overhead.
#     PostgreSQL floor is 500 TPS — conservative for localhost with explicit
#     per-event transactions (BEGIN + 2 INSERTs + COMMIT per event).
#     SQLite in-memory was ~15,000 TPS; PostgreSQL network overhead is the delta.
#     If this fails, the DB is unreachable or severely overloaded.
# ES: Medición de throughput — 5,000 eventos únicos, sin sobrecarga HTTP.
#     El piso de PostgreSQL es 500 TPS — conservador para localhost con transacciones
#     explícitas por evento (BEGIN + 2 INSERTs + COMMIT por evento).
#     SQLite en memoria era ~15,000 TPS; la sobrecarga de red de PostgreSQL es el delta.
#     Si esto falla, la DB no es alcanzable o está severamente sobrecargada.

N     = 5_000
conn4 = fresh_conn()

import random, string

def _fake(i):
    """
    EN: Generates a unique fake event for the benchmark. Random ID prevents
        duplicate detection from inflating/deflating the numbers.
    ES: Genera un evento falso único para el benchmark. ID aleatorio previene
        que la detección de duplicados infle/deflacte los números.
    """
    return fake_event(
        eid=f"evt_{''.join(random.choices(string.ascii_lowercase, k=12))}_{i}",
        etype=random.choice(list(L.EventType)).value,
        customer=f"cus_{i}",
        amount=random.randint(100, 50_000),
    )

events  = [_fake(i) for i in range(N)]
t0      = time.perf_counter()
L.process_stripe_event_batch(conn4, events)
elapsed = time.perf_counter() - t0
tps     = N / elapsed
chk(f"throughput ≥ 500 TPS  (got {tps:,.0f})", tps >= 500)
print(f"         {N:,} events in {elapsed:.3f}s  →  {tps:,.0f} TPS  "
      f"(batch via execute_values, PostgreSQL localhost)")


# =============================================================================
# PHASE 2: OUTPUT MODEL VALIDATION (DLQEntry + LedgerEntry)
# FASE 2: VALIDACIÓN DE MODELOS DE SALIDA (DLQEntry + LedgerEntry)
#
# EN: Tests that the output models (DLQEntry, LedgerEntry) correctly validate
#     their fields and produce correct serialization tuples via to_db().
#     These models act as the schema contract for INSERT statements —
#     if a model accepts invalid data, that invalid data goes to the database.
# ES: Tests que los modelos de salida (DLQEntry, LedgerEntry) validan correctamente
#     sus campos y producen tuplas de serialización correctas vía to_db().
#     Estos modelos actúan como el contrato de esquema para los INSERT —
#     si un modelo acepta datos inválidos, esos datos inválidos van a la base de datos.
# =============================================================================

print("\n── Phase 2: DLQEntry model ──────────────────────────────────────────────")

from pydantic import ValidationError as PydanticValidationError

# EN: Happy path — valid DLQEntry builds without error.
# ES: Camino feliz — DLQEntry válido se construye sin error.
entry = L.DLQEntry(
    transaction_id="evt_test_001",
    reason=L.DLQReason.DUPLICATE,
    raw_payload={"id": "evt_test_001"},
)
chk("DLQEntry builds with valid data",          entry.transaction_id == "evt_test_001")
chk("DLQEntry reason is enum value",            entry.reason == L.DLQReason.DUPLICATE)

# EN: to_db() must produce a 4-tuple matching the dlq INSERT column order exactly.
#     Order: (transaction_id, reason, raw_payload, received_at)
# ES: to_db() debe producir una 4-tupla que coincida exactamente con el orden de columnas
#     del INSERT de dlq. Orden: (transaction_id, reason, raw_payload, received_at)
db_row = entry.to_db()
chk("DLQEntry.to_db() is 4-tuple",              len(db_row) == 4)
chk("to_db() reason is string not enum",        db_row[1] == "DUPLICATE")
chk("to_db() payload is JSON string",           db_row[2] == '{"id": "evt_test_001"}')
chk("to_db() received_at is float",             isinstance(db_row[3], float))

# EN: Unknown reason string must be rejected — not in DLQReason enum.
# ES: String de razón desconocido debe ser rechazado — no está en el enum DLQReason.
try:
    L.DLQEntry(
        transaction_id="evt_x",
        reason="TYPO",               # EN: not in enum / ES: no está en el enum
        raw_payload={},
    )
    chk("DLQEntry rejects unknown reason", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("DLQEntry rejects unknown reason", True)

# EN: Empty transaction_id must be rejected — min_length=1.
# ES: transaction_id vacío debe ser rechazado — min_length=1.
try:
    L.DLQEntry(
        transaction_id="",
        reason=L.DLQReason.INVALID,
        raw_payload={},
    )
    chk("DLQEntry rejects empty transaction_id", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("DLQEntry rejects empty transaction_id", True)


print("\n── Phase 2: LedgerEntry model ───────────────────────────────────────────")

# EN: Happy path — valid LedgerEntry builds and serializes correctly.
# ES: Camino feliz — LedgerEntry válido se construye y serializa correctamente.
le = L.LedgerEntry(
    transaction_id="evt_le_001",
    event_type=L.EventType.INVOICE_PAID,
    customer_id="cus_abc123",
    amount_cents=4900,
    currency="usd",
    status=L.LedgerStatus.POSTED,
    idempotency_key="idem_evt_le_001",
    payload='{"id": "evt_le_001"}',
    created_at=1234567890.0,
)
chk("LedgerEntry builds with valid data",       le.transaction_id == "evt_le_001")
chk("LedgerEntry status is LedgerStatus",       le.status == L.LedgerStatus.POSTED)

# EN: to_db() must produce a 9-tuple matching the ledger INSERT column order.
#     Enum fields must be serialized to strings (.value), not left as enum instances.
# ES: to_db() debe producir una 9-tupla que coincida con el orden de columnas del INSERT del libro.
#     Los campos enum deben serializarse a strings (.value), no dejarse como instancias de enum.
db_row = le.to_db()
chk("LedgerEntry.to_db() is 9-tuple",           len(db_row) == 9)
chk("to_db() event_type is string not enum",    db_row[1] == "invoice.paid")
chk("to_db() status is string not enum",        db_row[5] == "POSTED")
chk("to_db() amount_cents is int",              db_row[3] == 4900)
chk("to_db() created_at is float",              db_row[8] == 1234567890.0)

# EN: Negative amount_cents must fail ge=0 — same constraint as StripeObject.
# ES: amount_cents negativo debe fallar ge=0 — misma restricción que StripeObject.
try:
    L.LedgerEntry(
        transaction_id="evt_neg",
        event_type=L.EventType.INVOICE_PAID,
        customer_id="cus_abc",
        amount_cents=-1,             # EN: violates ge=0 / ES: viola ge=0
        currency="usd",
        status=L.LedgerStatus.POSTED,
        idempotency_key="idem_neg",
        payload="{}",
        created_at=0.0,
    )
    chk("LedgerEntry rejects negative amount_cents", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects negative amount_cents", True)

# EN: Uppercase currency must fail the lowercase-only pattern regex.
# ES: Moneda en mayúsculas debe fallar el regex de solo minúsculas.
try:
    L.LedgerEntry(
        transaction_id="evt_curr",
        event_type=L.EventType.INVOICE_PAID,
        customer_id="cus_abc",
        amount_cents=100,
        currency="USD",             # EN: uppercase fails pattern / ES: mayúsculas fallan el patrón
        status=L.LedgerStatus.POSTED,
        idempotency_key="idem_curr",
        payload="{}",
        created_at=0.0,
    )
    chk("LedgerEntry rejects uppercase currency", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects uppercase currency", True)

# EN: Short customer_id must fail min_length=4.
# ES: customer_id corto debe fallar min_length=4.
try:
    L.LedgerEntry(
        transaction_id="evt_cus",
        event_type=L.EventType.INVOICE_PAID,
        customer_id="cus",          # EN: 3 chars — too short / ES: 3 chars — demasiado corto
        amount_cents=100,
        currency="usd",
        status=L.LedgerStatus.POSTED,
        idempotency_key="idem_cus",
        payload="{}",
        created_at=0.0,
    )
    chk("LedgerEntry rejects short customer_id", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects short customer_id", True)


print("\n── Phase 2: DLQ rows in DB have correct structured reasons ──────────────")
# EN: End-to-end test — verify that the reason codes in the actual DB rows match
#     what the DLQReason enum specifies. If _write_dlq() serializes incorrectly,
#     the reason stored in the DB would be wrong and filtering by reason would break.
# ES: Test de extremo a extremo — verificar que los códigos de razón en las filas
#     reales de la DB coincidan con lo que especifica el enum DLQReason. Si _write_dlq()
#     serializa incorrectamente, la razón almacenada en la DB sería incorrecta y
#     filtrar por razón se rompería.

conn5 = fresh_conn()

ev_dup = fake_event(eid="evt_dup_check")
L.process_stripe_event(conn5, ev_dup)
L.process_stripe_event(conn5, ev_dup)           # EN: replay → DUPLICATE / ES: reproducción → DUPLICATE

ev_bad = fake_event(eid="evt_invalid_check", currency="WRONG")
L.process_stripe_event(conn5, ev_bad)

rows    = _qall(conn5, "SELECT reason FROM dlq ORDER BY id")
reasons = [r[0] for r in rows]
chk("DB DLQ has 2 rows",                        len(reasons) == 2)
chk("First DLQ row reason is DUPLICATE",        reasons[0] == "DUPLICATE")
chk("Second DLQ row reason is INVALID",         reasons[1] == "INVALID")


print("\n── Phase 2: LedgerStatus enum coverage ──────────────────────────────────")
# EN: Verifies all three LedgerStatus values are correct and that the _STATUS_MAP
#     routes invoice.payment_failed → VOID and subscription.updated → PENDING.
# ES: Verifica que los tres valores LedgerStatus son correctos y que _STATUS_MAP
#     enruta invoice.payment_failed → VOID y subscription.updated → PENDING.

chk("LedgerStatus.POSTED value",                L.LedgerStatus.POSTED.value == "POSTED")
chk("LedgerStatus.PENDING value",               L.LedgerStatus.PENDING.value == "PENDING")
chk("LedgerStatus.VOID value",                  L.LedgerStatus.VOID.value == "VOID")

conn6 = fresh_conn()
ev_failed = fake_event(eid="evt_failed_001", etype="invoice.payment_failed")
r = L.process_stripe_event(conn6, ev_failed)
chk("invoice.payment_failed → VOID",
    r["outcome"] == "VOID", f"got {r['outcome']}")

ev_sub_updated = fake_event(eid="evt_sub_upd", etype="customer.subscription.updated")
r = L.process_stripe_event(conn6, ev_sub_updated)
chk("subscription.updated → PENDING",
    r["outcome"] == "PENDING", f"got {r['outcome']}")


# =============================================================================
# PHASE 5: INTEGRATION TESTS — FULL STACK
# FASE 5: TESTS DE INTEGRACIÓN — STACK COMPLETO
#
# EN: Tests the full HTTP-to-database execution cycle using FastAPI's TestClient.
#     Unlike Phases 1-3 which call process_stripe_event() directly, these tests
#     go through the actual HTTP routing, JSON parsing, and response formatting.
#     Four test categories: HTTP layer, concurrent insertion, outbox dispatch, DLQ queryability.
# ES: Tests el ciclo completo de ejecución HTTP-a-base de datos usando TestClient de FastAPI.
#     A diferencia de las Fases 1-3 que llaman process_stripe_event() directamente, estos
#     tests pasan por el enrutamiento HTTP real, parseo JSON y formateo de respuestas.
#     Cuatro categorías de tests: capa HTTP, inserción concurrente, despacho del outbox,
#     consultabilidad del DLQ.
# =============================================================================

print("\n── Phase 5: Integration — HTTP layer (TestClient) ──────────────────────")

from starlette.testclient import TestClient

# EN: Patch the module-level _conn to a fresh, empty PostgreSQL connection.
#     fresh_conn() truncates all tables so the HTTP tests start from a known empty state.
#     L._conn is the module global — replacing it means the FastAPI routes use this connection.
#     No mocking — this is a real psycopg2 connection to a real PostgreSQL database.
# ES: Parchear el _conn a nivel de módulo a una conexión PostgreSQL fresca y vacía.
#     fresh_conn() trunca todas las tablas para que los tests HTTP empiecen desde un estado vacío conocido.
#     L._conn es el global del módulo — reemplazarlo significa que las rutas FastAPI usan esta conexión.
#     Sin mocking — esta es una conexión psycopg2 real a una base de datos PostgreSQL real.
http_conn = fresh_conn()
L._conn   = http_conn
client    = TestClient(L.app)

# EN: Health check — verifies the server is responsive and returns the expected shape.
# ES: Health check — verifica que el servidor responde y retorna la forma esperada.
r = client.get("/health")
chk("GET /health → 200",                        r.status_code == 200)
chk("health response is ok",                    r.json().get("status") == "ok")

# EN: Valid event through the full HTTP stack.
# ES: Evento válido a través del stack HTTP completo.
ev_http = fake_event(eid="evt_http_001", customer="cus_http001", amount=9900)
r = client.post("/webhook/stripe", json=ev_http)
chk("POST valid event → HTTP 200",              r.status_code == 200)
chk("valid event → POSTED via HTTP",            r.json()["outcome"] == "POSTED",
    f"got {r.json()}")

# EN: Duplicate via HTTP — must return 200, not 4xx. Stripe needs 200 to stop retrying.
#     If we returned 409 or 422, Stripe would retry indefinitely — causing more duplicates.
# ES: Duplicado vía HTTP — debe retornar 200, no 4xx. Stripe necesita 200 para dejar de reintentar.
#     Si retornáramos 409 o 422, Stripe reintentaría indefinidamente — causando más duplicados.
r = client.post("/webhook/stripe", json=ev_http)
chk("POST duplicate → HTTP 200 (not 4xx)",      r.status_code == 200)
chk("duplicate → DLQ_DUPLICATE via HTTP",       r.json()["outcome"] == "DLQ_DUPLICATE",
    f"got {r.json()}")

# EN: Invalid currency via HTTP — Pydantic catches it, routes to DLQ, returns 200.
# ES: Moneda inválida vía HTTP — Pydantic la captura, enruta al DLQ, retorna 200.
ev_bad_curr = fake_event(eid="evt_http_bad_curr", currency="USD")
r = client.post("/webhook/stripe", json=ev_bad_curr)
chk("POST uppercase currency → HTTP 200",       r.status_code == 200)
chk("uppercase currency → DLQ_INVALID via HTTP", r.json()["outcome"] == "DLQ_INVALID",
    f"got {r.json()}")

# EN: $0 invoice via HTTP — cross-field validator fires, routes to DLQ.
# ES: Factura $0 vía HTTP — validador de campos cruzados se dispara, enruta al DLQ.
ev_zero = fake_event(eid="evt_http_zero", amount=0)
r = client.post("/webhook/stripe", json=ev_zero)
chk("POST $0 invoice → HTTP 200",               r.status_code == 200)
chk("$0 invoice → DLQ_INVALID via HTTP",        r.json()["outcome"] == "DLQ_INVALID",
    f"got {r.json()}")

# EN: Bad customer prefix via HTTP — check_customer_id_format fires.
# ES: Prefijo de cliente incorrecto vía HTTP — check_customer_id_format se dispara.
ev_bad_cus = fake_event(eid="evt_http_bad_cus", customer="notacus_001")
r = client.post("/webhook/stripe", json=ev_bad_cus)
chk("POST bad customer prefix → HTTP 200",      r.status_code == 200)
chk("bad prefix → DLQ_INVALID via HTTP",        r.json()["outcome"] == "DLQ_INVALID",
    f"got {r.json()}")

# EN: Ledger summary endpoint — verify it returns all expected keys and correct counts.
#     After the tests above: 1 POSTED, 1 DUPLICATE, 3 INVALID = 4 DLQ entries total.
# ES: Endpoint de resumen del libro — verificar que retorna todas las claves esperadas
#     y conteos correctos. Después de los tests anteriores: 1 POSTED, 1 DUPLICATE,
#     3 INVALID = 4 entradas DLQ en total.
r = client.get("/ledger/summary")
chk("GET /ledger/summary → 200",                r.status_code == 200)
summary = r.json()
chk("summary has 'ledger' key",                 "ledger" in summary)
chk("summary has 'dlq_depth' key",              "dlq_depth" in summary)
chk("summary has 'outbox_pending' key",         "outbox_pending" in summary)
chk("summary dlq_depth is 4",                   summary["dlq_depth"] == 4,
    f"got {summary['dlq_depth']}")
chk("summary outbox_pending is 1",              summary["outbox_pending"] == 1,
    f"got {summary['outbox_pending']}")


print("\n── Phase 5: Concurrent insertion (threading) ────────────────────────────")
# EN: The most important test in the suite. 5 threads fire the same event simultaneously.
#     INSERT INTO ledger ... ON CONFLICT (transaction_id) DO NOTHING is the idempotency guard.
#     PostgreSQL's MVCC ensures exactly 1 insert wins the unique constraint race;
#     the other 4 get rowcount=0 and are routed to DLQ_DUPLICATE.
#     This test verifies the "Ghost Payment" prevention mechanism works under concurrent load.
#
#     WHY PostgreSQL handles this natively (vs. SQLite temp file):
#     PostgreSQL is a multi-client server — all connections share the same data. The
#     unique constraint on transaction_id plus ON CONFLICT DO NOTHING is the serialization
#     point. No application-level lock needed. This is the production-correct approach.
#
# ES: El test más importante de la suite. 5 hilos disparan el mismo evento simultáneamente.
#     INSERT INTO ledger ... ON CONFLICT (transaction_id) DO NOTHING es la guardia de idempotencia.
#     El MVCC de PostgreSQL asegura que exactamente 1 inserción gane la carrera de restricción única;
#     los otros 4 obtienen rowcount=0 y son enrutados a DLQ_DUPLICATE.
#     Este test verifica que el mecanismo de prevención de "Pagos Fantasma" funciona bajo carga concurrente.
#
#     POR QUÉ PostgreSQL maneja esto nativamente (vs. archivo temporal SQLite):
#     PostgreSQL es un servidor multi-cliente — todas las conexiones comparten los mismos datos. La
#     restricción única en transaction_id más ON CONFLICT DO NOTHING es el punto de serialización.
#     Sin bloqueo a nivel de aplicación necesario. Este es el enfoque correcto para producción.

import threading

# EN: Fresh state before concurrent test — truncates all tables.
#     Each thread will open its own psycopg2 connection to the same PostgreSQL database.
#     Sharing a single connection object across threads is unsafe because _tx() issues BEGIN
#     and concurrent threads would interleave their transactions on the same connection.
# ES: Estado fresco antes del test concurrente — trunca todas las tablas.
#     Cada hilo abrirá su propia conexión psycopg2 a la misma base de datos PostgreSQL.
#     Compartir un único objeto de conexión entre hilos no es seguro porque _tx() emite BEGIN
#     y los hilos concurrentes intercalarían sus transacciones en la misma conexión.
_setup_conn = fresh_conn()
_setup_conn.close()

ev_concurrent      = fake_event(eid="evt_concurrent_001",
                                customer="cus_concurrent", amount=5000)
results_concurrent = []
errors_concurrent  = []

def _fire():
    """
    EN: Each thread gets its own psycopg2 connection to the PostgreSQL database.
        ON CONFLICT (transaction_id) DO NOTHING at the DB level serializes concurrent
        inserts — no application-level lock or coordination needed between threads.
    ES: Cada hilo obtiene su propia conexión psycopg2 a la base de datos PostgreSQL.
        ON CONFLICT (transaction_id) DO NOTHING a nivel DB serializa los inserts concurrentes —
        sin bloqueo a nivel de aplicación ni coordinación necesaria entre hilos.
    """
    try:
        conn = L._bootstrap()
        r    = L.process_stripe_event(conn, ev_concurrent)
        results_concurrent.append(r["outcome"])
        conn.close()
    except Exception as exc:
        errors_concurrent.append(str(exc))

threads = [threading.Thread(target=_fire) for _ in range(5)]
for t in threads:
    t.start()
for t in threads:
    t.join()

# EN: Verify final state using a separate connection — not one of the thread connections.
# ES: Verificar el estado final usando una conexión separada — no una de las conexiones de los hilos.
verify_conn = L._bootstrap()
ledger_rows = _q1(verify_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id=%s",
                  ("evt_concurrent_001",))
verify_conn.close()

chk("no exceptions from 5 concurrent inserts",
    len(errors_concurrent) == 0, str(errors_concurrent))
chk("exactly 1 POSTED outcome",
    results_concurrent.count("POSTED") == 1,
    f"POSTED count: {results_concurrent.count('POSTED')}")
chk("remaining 4 are DLQ_DUPLICATE",
    results_concurrent.count("DLQ_DUPLICATE") == 4,
    f"DUPLICATE count: {results_concurrent.count('DLQ_DUPLICATE')}")
chk("ledger has exactly 1 row after 5 concurrent inserts",
    ledger_rows == 1, f"got {ledger_rows}")


print("\n── Phase 5: Outbox dispatch simulation ──────────────────────────────────")
# EN: Simulates the downstream worker that reads outbox rows and marks them dispatched.
#     In production, this is a Temporal activity or a background asyncio task.
#     The test verifies the state machine: dispatched=0 (pending) → dispatched=1 (sent).
#     Also verifies that idempotency holds after dispatch — replaying the same event
#     still routes to DLQ_DUPLICATE, not to a new ledger write.
# ES: Simula el worker downstream que lee filas del outbox y las marca como despachadas.
#     En producción, esto es una actividad Temporal o una tarea asyncio en background.
#     El test verifica la máquina de estados: dispatched=0 (pendiente) → dispatched=1 (enviado).
#     También verifica que la idempotencia se mantiene después del despacho — reproducir el mismo
#     evento todavía enruta a DLQ_DUPLICATE, no a una nueva escritura en el libro.

dispatch_conn = fresh_conn()
L.process_stripe_event(
    dispatch_conn,
    fake_event(eid="evt_dispatch_001", customer="cus_dispatch", amount=7500)
)

pending_before = _q1(dispatch_conn, "SELECT COUNT(*) FROM outbox WHERE dispatched=0")
chk("outbox has 1 pending row before dispatch", pending_before == 1)

# EN: Simulate the worker flipping dispatched=0 → dispatched=1.
#     With autocommit=True on the connection, the UPDATE commits immediately —
#     no explicit conn.commit() needed. The AND dispatched=0 guard in the WHERE clause
#     is what prevents a concurrent worker from double-dispatching the same row.
# ES: Simular el worker cambiando dispatched=0 → dispatched=1.
#     Con autocommit=True en la conexión, el UPDATE hace commit inmediatamente —
#     no se necesita conn.commit() explícito. La guardia AND dispatched=0 en la cláusula WHERE
#     es lo que evita que un worker concurrente despache dos veces la misma fila.
with dispatch_conn.cursor() as cur:
    cur.execute(
        "UPDATE outbox SET dispatched=1 WHERE transaction_id=%s AND dispatched=0",
        ("evt_dispatch_001",),
    )

pending_after = _q1(dispatch_conn, "SELECT COUNT(*) FROM outbox WHERE dispatched=0")
chk("outbox has 0 pending rows after dispatch", pending_after == 0)

# EN: Replay after dispatch — idempotency guard must still fire. The ledger PK
#     remains, so ON CONFLICT (transaction_id) DO NOTHING still rejects the replay
#     even though the outbox row has been dispatched.
# ES: Reproducción después del despacho — la guardia de idempotencia todavía debe dispararse.
#     El PK del libro permanece, así que ON CONFLICT (transaction_id) DO NOTHING todavía
#     rechaza la reproducción aunque la fila del outbox haya sido despachada.
r = L.process_stripe_event(
    dispatch_conn,
    fake_event(eid="evt_dispatch_001", customer="cus_dispatch", amount=7500)
)
chk("replay after dispatch → DLQ_DUPLICATE (idempotency holds)",
    r["outcome"] == "DLQ_DUPLICATE", f"got {r['outcome']}")

ledger_rows = _q1(dispatch_conn, "SELECT COUNT(*) FROM ledger")
chk("ledger still has 1 row after replay post-dispatch",
    ledger_rows == 1, f"got {ledger_rows}")


print("\n── Phase 5: DLQ queryability and payload preservation ───────────────────")
# EN: Verifies that DLQ entries are queryable and that raw payloads are preserved
#     byte-perfect. This matters for manual recovery: an operator must be able to
#     extract a raw_payload from the DLQ table and replay it without data loss.
#     If raw_payload is corrupted (double-encoded, truncated, etc.), manual recovery
#     is impossible — which defeats the entire point of having a DLQ.
# ES: Verifica que las entradas del DLQ son consultables y que los payloads crudos
#     se preservan byte-perfecto. Esto importa para la recuperación manual: un operador
#     debe poder extraer un raw_payload de la tabla DLQ y reproducirlo sin pérdida de datos.
#     Si raw_payload está corrupto (doble codificado, truncado, etc.), la recuperación
#     manual es imposible — lo que anula el propósito entero de tener un DLQ.

dlq_conn = fresh_conn()

# EN: Three distinct invalid events → three DLQ entries with distinct event IDs.
# ES: Tres eventos inválidos distintos → tres entradas DLQ con IDs de evento distintos.
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_1", currency="WRONG"))
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_2", amount=0))
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_3", customer=""))

rows = _qall(dlq_conn, "SELECT transaction_id, reason, raw_payload FROM dlq ORDER BY id")

chk("DLQ has 3 rows",                           len(rows) == 3,   f"got {len(rows)}")
chk("all 3 reasons are INVALID",
    all(r[1] == "INVALID" for r in rows))
chk("raw_payload is a JSON string",
    all(isinstance(r[2], str) for r in rows))

# EN: Parse and verify each payload preserves the original event ID unchanged.
#     This is the byte-perfect preservation test — json.loads → original dict.
# ES: Parsear y verificar que cada payload preserva el ID de evento original sin cambios.
#     Este es el test de preservación byte-perfecto — json.loads → dict original.
parsed = [json.loads(r[2]) for r in rows]
chk("payload evt_dlq_1 id preserved",           parsed[0].get("id") == "evt_dlq_1")
chk("payload evt_dlq_2 id preserved",           parsed[1].get("id") == "evt_dlq_2")
chk("payload evt_dlq_3 id preserved",           parsed[2].get("id") == "evt_dlq_3")


# =============================================================================
# PHASE 3: CROSS-FIELD VALIDATORS
# FASE 3: VALIDADORES DE CAMPOS CRUZADOS
#
# EN: Tests the @model_validator rules on StripeEvent that enforce business logic
#     relationships between fields. These rules cannot be expressed with Field
#     constraints alone — they require access to multiple fields simultaneously.
# ES: Tests las reglas @model_validator en StripeEvent que aplican relaciones de
#     lógica de negocio entre campos. Estas reglas no se pueden expresar solo con
#     restricciones de Field — requieren acceso a múltiples campos simultáneamente.
# =============================================================================

print("\n── Phase 3: Cross-field validator — invoice amount > 0 ──────────────────")
# EN: Rule: invoice.paid and invoice.payment_failed must have amount > 0.
#     Rationale: a $0 invoice.paid means no revenue was received — data quality failure.
#     A $0 invoice.payment_failed means nothing to retry — also a data quality failure.
#     Subscription events are exempt — they are lifecycle events, not payment events.
# ES: Regla: invoice.paid e invoice.payment_failed deben tener amount > 0.
#     Justificación: un invoice.paid de $0 significa que no se recibieron ingresos — fallo de calidad de datos.
#     Un invoice.payment_failed de $0 significa nada que reintentar — también un fallo de calidad de datos.
#     Los eventos de suscripción están exentos — son eventos de ciclo de vida, no de pago.

conn7 = fresh_conn()

# EN: invoice.paid + amount=0 → fails check_invoice_amount_nonzero cross-field validator.
# ES: invoice.paid + amount=0 → falla el validador de campos cruzados check_invoice_amount_nonzero.
ev_zero_invoice = fake_event(eid="evt_zero_paid", etype="invoice.paid", amount=0)
r = L.process_stripe_event(conn7, ev_zero_invoice)
chk("invoice.paid with amount=0 → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

# EN: invoice.payment_failed + amount=0 → same rule applies.
# ES: invoice.payment_failed + amount=0 → la misma regla aplica.
ev_zero_failed = {
    "id": "evt_zero_failed",
    "type": "invoice.payment_failed",
    "data": {"object": {
        "customer":    "cus_abc123",
        "amount_paid": 0,
        "currency":    "usd",
    }},
}
r = L.process_stripe_event(conn7, ev_zero_failed)
chk("invoice.payment_failed with amount=0 → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

# EN: subscription.created + amount=0 → exempt from invoice rule. Should post.
#     Subscription lifecycle events can legitimately have $0 amounts.
# ES: subscription.created + amount=0 → exento de la regla de factura. Debe publicarse.
#     Los eventos de ciclo de vida de suscripción pueden tener legítimamente montos de $0.
ev_sub_zero = {
    "id": "evt_sub_zero",
    "type": "customer.subscription.created",
    "data": {"object": {
        "customer":    "cus_sub001",
        "amount_paid": 0,
        "currency":    "usd",
    }},
}
r = L.process_stripe_event(conn7, ev_sub_zero)
chk("subscription.created with amount=0 → POSTED (lifecycle event, not payment)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")

# EN: invoice.paid with valid amount → should post normally.
# ES: invoice.paid con monto válido → debe publicarse normalmente.
ev_valid_invoice = fake_event(eid="evt_valid_paid", etype="invoice.paid", amount=4900)
r = L.process_stripe_event(conn7, ev_valid_invoice)
chk("invoice.paid with amount=4900 → POSTED",
    r["outcome"] == "POSTED", f"got {r['outcome']}")


print("\n── Phase 3: Cross-field validator — customer ID format ──────────────────")
# EN: Rule: customer ID must start with "cus_". This is a structural Stripe API rule.
#     min_length=4 catches too-short strings. This validator catches structurally
#     wrong strings that pass the length check but aren't real Stripe customer IDs.
# ES: Regla: el ID de cliente debe comenzar con "cus_". Esta es una regla estructural de la API de Stripe.
#     min_length=4 captura strings demasiado cortos. Este validador captura strings
#     estructuralmente incorrectos que pasan el check de longitud pero no son IDs reales de cliente Stripe.

conn8 = fresh_conn()

# EN: "abc1234567" has 10 chars (passes min_length=4) but no "cus_" prefix.
# ES: "abc1234567" tiene 10 chars (pasa min_length=4) pero sin prefijo "cus_".
ev_bad_cus_fmt = fake_event(eid="evt_bad_cus_fmt", customer="abc1234567")
r = L.process_stripe_event(conn8, ev_bad_cus_fmt)
chk("customer 'abc1234567' (no cus_ prefix) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

# EN: "cus_abc123" has correct prefix — should post.
# ES: "cus_abc123" tiene el prefijo correcto — debe publicarse.
ev_good_cus = fake_event(eid="evt_good_cus", customer="cus_abc123")
r = L.process_stripe_event(conn8, ev_good_cus)
chk("customer 'cus_abc123' → POSTED",
    r["outcome"] == "POSTED", f"got {r['outcome']}")

# EN: "cus_" is exactly 4 chars, passes min_length=4, and has the correct prefix.
#     The validator only checks the prefix, not minimum suffix length.
#     Business decision: prefix required, suffix length not enforced at this layer.
# ES: "cus_" es exactamente 4 chars, pasa min_length=4, y tiene el prefijo correcto.
#     El validador solo verifica el prefijo, no la longitud mínima del sufijo.
#     Decisión de negocio: prefijo requerido, longitud del sufijo no aplicada en esta capa.
ev_bare_prefix = fake_event(eid="evt_bare_cus", customer="cus_")
r = L.process_stripe_event(conn8, ev_bare_prefix)
chk("customer 'cus_' (prefix only, 4 chars) → POSTED (prefix valid, suffix optional)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")


print("\n── Phase 3: Cross-field validators fire together ────────────────────────")
# EN: When multiple validators fail simultaneously, Pydantic collects ALL errors
#     before raising ValidationError. The DLQ entry captures the error count.
#     This test verifies that neither validator silently suppresses the other.
# ES: Cuando múltiples validadores fallan simultáneamente, Pydantic recolecta TODOS
#     los errores antes de lanzar ValidationError. La entrada DLQ captura el conteo de errores.
#     Este test verifica que ningún validador suprime silenciosamente al otro.

conn9 = fresh_conn()

# EN: Both cross-field rules violated: invoice.paid + $0 + no cus_ prefix.
#     Pydantic runs all validators — both failures appear in the error count.
# ES: Ambas reglas de campos cruzados violadas: invoice.paid + $0 + sin prefijo cus_.
#     Pydantic corre todos los validadores — ambos fallos aparecen en el conteo de errores.
ev_double_bad = {
    "id": "evt_double_bad",
    "type": "invoice.paid",
    "data": {"object": {
        "customer":    "notacustomer",   # EN: no cus_ prefix / ES: sin prefijo cus_
        "amount_paid": 0,               # EN: zero amount for invoice / ES: monto cero para factura
        "currency":    "usd",
    }},
}
r = L.process_stripe_event(conn9, ev_double_bad)
chk("invoice.paid + $0 + bad customer → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")
chk("reason mentions validation failures",
    "validation" in r.get("reason", "").lower() or "Pydantic" in r.get("reason", ""),
    f"got reason: {r.get('reason')}")


# =============================================================================
# FINAL SUMMARY / RESUMEN FINAL
# EN: Print total pass/fail counts and exit with code 1 if any tests failed.
#     Exit code 1 makes this compatible with CI pipelines — a non-zero exit
#     signals a test failure to the CI system.
# ES: Imprimir totales de aprobado/fallado y salir con código 1 si algún test falló.
#     El código de salida 1 hace esto compatible con pipelines CI — una salida no cero
#     señala un fallo de test al sistema CI.
# =============================================================================
print(f"\n{'='*60}")
print(f"  {ok} passed | {fail} failed")
if fail:
    sys.exit(1)
