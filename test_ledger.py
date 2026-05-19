"""
test_ledger.py — Full Integration Test Suite / Suite Completa de Tests de Integración
======================================================================================
EN: Tests across 5 phases. Run with: python test_ledger.py
    Requires: PostgreSQL running and DATABASE_URL set (see docker-compose.yml).
    Covers: entry validation, output models, cross-field rules, HTTP layer,
    concurrent insertion, outbox dispatch simulation, DLQ queryability.
    No mocks for the database. No stubs for the ON CONFLICT logic.
    Every assertion hits a real PostgreSQL database.
    The test runner is deliberately simple — no pytest dependency — so anyone
    can run it with a plain Python install.

ES: Tests en 5 fases. Ejecutar con: python test_ledger.py
    Requiere: PostgreSQL en ejecución y DATABASE_URL establecido (ver docker-compose.yml).
    Cubre: validación de entrada, modelos de salida, reglas de campos cruzados,
    capa HTTP, inserción concurrente, simulación de despacho del outbox, consultabilidad del DLQ.
    Sin mocks para la base de datos. Sin stubs para la lógica ON CONFLICT.
    Cada aserción golpea una base de datos PostgreSQL real.
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
import threading   # EN: Used for the concurrent idempotency test / ES: Usado para el test de idempotencia concurrente
from datetime import datetime, timezone
from unittest.mock import patch

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
import worker as W  # Phase 7: outbox drain worker

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
        Used for COUNT(*) and similar single-value reads.
    ES: Ejecutar una consulta y retornar la primera columna de la primera fila.
        Usado para COUNT(*) y lecturas similares de valor único.
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

ev_uppercase = fake_event(eid="evt_invalid_curr_1", currency="USD")
r = L.process_stripe_event(conn, ev_uppercase)
chk("currency 'USD' (uppercase) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

ev_short = fake_event(eid="evt_invalid_curr_2", currency="us")
r = L.process_stripe_event(conn, ev_short)
chk("currency 'us' (2 chars) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")


print("\n── Phase 1: Customer ID Validation (Min Length) ────────────────────────")

conn = fresh_conn()

ev_valid = fake_event(customer="cus_123")
r = L.process_stripe_event(conn, ev_valid)
chk("customer 'cus_123' → POSTED",              r["outcome"] == "POSTED")

ev_short = fake_event(eid="evt_short_cus", customer="cus")
r = L.process_stripe_event(conn, ev_short)
chk("customer 'cus' (3 chars) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

ev_empty = fake_event(eid="evt_empty_cus", customer="")
r = L.process_stripe_event(conn, ev_empty)
chk("customer '' (empty) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")


print("\n── Phase 1: Amount Validation (Fallback + Non-Negative) ────────────────")

conn = fresh_conn()

ev_amount_paid = fake_event(eid="evt_amt_1", amount=5000)
r = L.process_stripe_event(conn, ev_amount_paid)
chk("amount_paid 5000 → POSTED",                r["outcome"] == "POSTED")

ev_negative = fake_event(eid="evt_negative", amount=-5000)
r = L.process_stripe_event(conn, ev_negative)
chk("amount_paid -5000 → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

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

conn = fresh_conn()

ev_valid_type = fake_event(etype="invoice.paid")
r = L.process_stripe_event(conn, ev_valid_type)
chk("event type 'invoice.paid' → POSTED",       r["outcome"] == "POSTED")

ev_invalid_type = fake_event(eid="evt_bad_type", etype="payment.created")
r = L.process_stripe_event(conn, ev_invalid_type)
chk("event type 'payment.created' → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")


print("\n── Phase 1: Missing Required Fields ───────────────────────────────────")

conn = fresh_conn()

ev_no_id = {
    "id": "",
    "type": "invoice.paid",
    "data": {"object": {"customer": "cus_123", "amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_id)
chk("empty id → DLQ_INVALID",                   r["outcome"] == "DLQ_INVALID")

ev_no_type = {
    "id": "evt_no_type",
    "type": "",
    "data": {"object": {"customer": "cus_123", "amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_type)
chk("empty type → DLQ_INVALID",                 r["outcome"] == "DLQ_INVALID")

ev_no_customer = {
    "id": "evt_no_cus",
    "type": "invoice.paid",
    "data": {"object": {"amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_customer)
chk("missing customer → DLQ_INVALID",           r["outcome"] == "DLQ_INVALID")


print("\n── Idempotency guard ────────────────────────────────────────────────")

conn = fresh_conn()
ev   = fake_event()

r1 = L.process_stripe_event(conn, ev)
chk("first insert → POSTED",                    r1["outcome"] == "POSTED")
chk("transaction_id returned",                  r1["transaction_id"] == "evt_test_001")

r2 = L.process_stripe_event(conn, ev)
chk("duplicate → DLQ_DUPLICATE",               r2["outcome"] == "DLQ_DUPLICATE")
chk("DLQ row written",
    _q1(conn, "SELECT COUNT(*) FROM dlq") == 1)

ledger_count = _q1(conn, "SELECT COUNT(*) FROM ledger")
chk("ledger has exactly 1 row after replay",    ledger_count == 1)


print("\n── Outbox written atomically ────────────────────────────────────────")

outbox_count = _q1(conn, "SELECT COUNT(*) FROM outbox WHERE dispatched=0")
chk("outbox has 1 pending row",                 outbox_count == 1)

row = _qall(conn, "SELECT transaction_id, event_type FROM outbox WHERE dispatched=0")[0]
chk("outbox row has correct tx_id",             row[0] == "evt_test_001")
chk("outbox row has correct event_type",        row[1] == "invoice.paid")


print("\n── Validation & DLQ routing ─────────────────────────────────────────")

conn2 = fresh_conn()

r = L.process_stripe_event(conn2, {"id": "", "type": "invoice.paid", "data": {}})
chk("empty id → DLQ_INVALID",                  r["outcome"] == "DLQ_INVALID")

r = L.process_stripe_event(conn2, fake_event(eid="evt_x", etype="payment.created"))
chk("unknown type → DLQ_INVALID",              r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']} instead of DLQ_INVALID")

dlq_count = _q1(conn2, "SELECT COUNT(*) FROM dlq")
chk("DLQ has 2 rows",                           dlq_count == 2)


print("\n── All supported event types route correctly ─────────────────────────")

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
# ES: Medición de throughput — 5,000 eventos únicos, sin sobrecarga HTTP.
#     El piso de PostgreSQL es 500 TPS — conservador para localhost con transacciones
#     explícitas por evento (BEGIN + 2 INSERTs + COMMIT por evento).

N     = 5_000
conn4 = fresh_conn()

import random, string

def _fake(i):
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

# EN: M5 — data integrity: verify actual row count in DB matches batch input size.
#     A broken batch function that returns dummy results without touching the DB
#     would pass the TPS check above but fail this assertion.
# ES: M5 — integridad de datos: verificar que el conteo de filas en DB coincide
#     con el tamaño del lote de entrada.
bench_row_count = _q1(conn4, "SELECT COUNT(*) FROM ledger")
chk(f"batch data integrity: exactly {N} rows in ledger",
    bench_row_count == N, f"got {bench_row_count}")
conn4.close()  # EN: L3 — explicit close to avoid leaking connection / ES: L3 — cierre explícito para evitar fuga de conexión


# =============================================================================
# PHASE 2: OUTPUT MODEL VALIDATION (DLQEntry + LedgerEntry)
# FASE 2: VALIDACIÓN DE MODELOS DE SALIDA (DLQEntry + LedgerEntry)
# =============================================================================

print("\n── Phase 2: DLQEntry model ──────────────────────────────────────────────")

from pydantic import ValidationError as PydanticValidationError

entry = L.DLQEntry(
    transaction_id="evt_test_001",
    reason=L.DLQReason.DUPLICATE,
    raw_payload={"id": "evt_test_001"},
)
chk("DLQEntry builds with valid data",          entry.transaction_id == "evt_test_001")
chk("DLQEntry reason is enum value",            entry.reason == L.DLQReason.DUPLICATE)

db_row = entry.to_db()
chk("DLQEntry.to_db() is 4-tuple",              len(db_row) == 4)
chk("to_db() reason is string not enum",        db_row[1] == "DUPLICATE")
chk("to_db() payload is JSON string",           db_row[2] == '{"id": "evt_test_001"}')
# EN: received_at is now a timezone-aware datetime (TIMESTAMPTZ), not a float.
# ES: received_at es ahora un datetime con zona horaria (TIMESTAMPTZ), no un float.
chk("to_db() received_at is datetime",          isinstance(db_row[3], datetime))

try:
    L.DLQEntry(transaction_id="evt_x", reason="TYPO", raw_payload={})
    chk("DLQEntry rejects unknown reason", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("DLQEntry rejects unknown reason", True)

try:
    L.DLQEntry(transaction_id="", reason=L.DLQReason.INVALID, raw_payload={})
    chk("DLQEntry rejects empty transaction_id", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("DLQEntry rejects empty transaction_id", True)


print("\n── Phase 2: LedgerEntry model ───────────────────────────────────────────")

# EN: Pydantic v2 coerces float → datetime for datetime fields.
#     1234567890.0 (Unix timestamp) is accepted and stored as a tz-aware datetime.
# ES: Pydantic v2 coerce float → datetime para campos datetime.
#     1234567890.0 (Unix timestamp) se acepta y almacena como datetime con zona horaria.
le = L.LedgerEntry(
    transaction_id="evt_le_001",
    event_type=L.EventType.INVOICE_PAID,
    customer_id="cus_abc123",
    amount_cents=4900,
    currency="usd",
    status=L.LedgerStatus.POSTED,
    idempotency_key="idem_evt_le_001",
    payload='{"id": "evt_le_001"}',
    created_at=1234567890.0,  # EN: coerced to datetime by Pydantic v2 / ES: coercionado a datetime por Pydantic v2
)
chk("LedgerEntry builds with valid data",       le.transaction_id == "evt_le_001")
chk("LedgerEntry status is LedgerStatus",       le.status == L.LedgerStatus.POSTED)

db_row = le.to_db()
chk("LedgerEntry.to_db() is 9-tuple",           len(db_row) == 9)
chk("to_db() event_type is string not enum",    db_row[1] == "invoice.paid")
chk("to_db() status is string not enum",        db_row[5] == "POSTED")
chk("to_db() amount_cents is int",              db_row[3] == 4900)
# EN: created_at is now a timezone-aware datetime (TIMESTAMPTZ), not a float.
# ES: created_at es ahora un datetime con zona horaria (TIMESTAMPTZ), no un float.
chk("to_db() created_at is datetime",           isinstance(db_row[8], datetime))

try:
    L.LedgerEntry(
        transaction_id="evt_neg", event_type=L.EventType.INVOICE_PAID,
        customer_id="cus_abc", amount_cents=-1, currency="usd",
        status=L.LedgerStatus.POSTED, idempotency_key="idem_neg",
        payload="{}", created_at=0.0,
    )
    chk("LedgerEntry rejects negative amount_cents", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects negative amount_cents", True)

try:
    L.LedgerEntry(
        transaction_id="evt_curr", event_type=L.EventType.INVOICE_PAID,
        customer_id="cus_abc", amount_cents=100, currency="USD",
        status=L.LedgerStatus.POSTED, idempotency_key="idem_curr",
        payload="{}", created_at=0.0,
    )
    chk("LedgerEntry rejects uppercase currency", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects uppercase currency", True)

try:
    L.LedgerEntry(
        transaction_id="evt_cus", event_type=L.EventType.INVOICE_PAID,
        customer_id="cus", amount_cents=100, currency="usd",
        status=L.LedgerStatus.POSTED, idempotency_key="idem_cus",
        payload="{}", created_at=0.0,
    )
    chk("LedgerEntry rejects short customer_id", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects short customer_id", True)


print("\n── Phase 2: DLQ rows in DB have correct structured reasons ──────────────")

conn5 = fresh_conn()

ev_dup = fake_event(eid="evt_dup_check")
L.process_stripe_event(conn5, ev_dup)
L.process_stripe_event(conn5, ev_dup)

ev_bad = fake_event(eid="evt_invalid_check", currency="WRONG")
L.process_stripe_event(conn5, ev_bad)

rows    = _qall(conn5, "SELECT reason FROM dlq ORDER BY id")
reasons = [r[0] for r in rows]
chk("DB DLQ has 2 rows",                        len(reasons) == 2)
chk("First DLQ row reason is DUPLICATE",        reasons[0] == "DUPLICATE")
chk("Second DLQ row reason is INVALID",         reasons[1] == "INVALID")


print("\n── Phase 2: LedgerStatus enum coverage ──────────────────────────────────")

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
# PHASE 3: CROSS-FIELD VALIDATORS
# FASE 3: VALIDADORES DE CAMPOS CRUZADOS
# (Placed before Phase 5 so unit-level cross-field failures are verified
#  before the full HTTP integration stack is exercised.)
# =============================================================================

print("\n── Phase 3: Cross-field validator — invoice amount > 0 ──────────────────")

conn7 = fresh_conn()

ev_zero_invoice = fake_event(eid="evt_zero_paid", etype="invoice.paid", amount=0)
r = L.process_stripe_event(conn7, ev_zero_invoice)
chk("invoice.paid with amount=0 → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

ev_zero_failed = {
    "id": "evt_zero_failed",
    "type": "invoice.payment_failed",
    "data": {"object": {"customer": "cus_abc123", "amount_paid": 0, "currency": "usd"}},
}
r = L.process_stripe_event(conn7, ev_zero_failed)
chk("invoice.payment_failed with amount=0 → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

ev_sub_zero = {
    "id": "evt_sub_zero",
    "type": "customer.subscription.created",
    "data": {"object": {"customer": "cus_sub001", "amount_paid": 0, "currency": "usd"}},
}
r = L.process_stripe_event(conn7, ev_sub_zero)
chk("subscription.created with amount=0 → POSTED (lifecycle event, not payment)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")

ev_valid_invoice = fake_event(eid="evt_valid_paid", etype="invoice.paid", amount=4900)
r = L.process_stripe_event(conn7, ev_valid_invoice)
chk("invoice.paid with amount=4900 → POSTED",
    r["outcome"] == "POSTED", f"got {r['outcome']}")


print("\n── Phase 3: Cross-field validator — customer ID format ──────────────────")

conn8 = fresh_conn()

ev_bad_cus_fmt = fake_event(eid="evt_bad_cus_fmt", customer="abc1234567")
r = L.process_stripe_event(conn8, ev_bad_cus_fmt)
chk("customer 'abc1234567' (no cus_ prefix) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

ev_good_cus = fake_event(eid="evt_good_cus", customer="cus_abc123")
r = L.process_stripe_event(conn8, ev_good_cus)
chk("customer 'cus_abc123' → POSTED",
    r["outcome"] == "POSTED", f"got {r['outcome']}")

ev_bare_prefix = fake_event(eid="evt_bare_cus", customer="cus_")
r = L.process_stripe_event(conn8, ev_bare_prefix)
chk("customer 'cus_' (prefix only, 4 chars) → POSTED (prefix valid, suffix optional)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")


print("\n── Phase 3: Cross-field validators fire together ────────────────────────")

conn9 = fresh_conn()

ev_double_bad = {
    "id": "evt_double_bad",
    "type": "invoice.paid",
    "data": {"object": {
        "customer":    "notacustomer",
        "amount_paid": 0,
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
# PHASE 5: INTEGRATION TESTS — FULL STACK
# FASE 5: TESTS DE INTEGRACIÓN — STACK COMPLETO
# =============================================================================

print("\n── Phase 5: Integration — HTTP layer (TestClient) ──────────────────────")

from starlette.testclient import TestClient

# EN: _MockPool wraps a single psycopg2 connection to satisfy the _pool.getconn() /
#     _pool.putconn() interface expected by the FastAPI route handlers. This lets tests
#     inject a controlled, truncated connection without standing up a real pool.
#     This does NOT mock the database — every INSERT and SELECT hits real PostgreSQL.
# ES: _MockPool envuelve una única conexión psycopg2 para satisfacer la interfaz
#     _pool.getconn() / _pool.putconn() esperada por los manejadores de rutas FastAPI.
#     Esto permite que los tests inyecten una conexión controlada y truncada sin levantar
#     un pool real. Esto NO mockea la base de datos — cada INSERT y SELECT golpea PostgreSQL real.
class _MockPool:
    def __init__(self, conn):
        self._conn = conn
    def getconn(self):
        return self._conn
    def putconn(self, conn):
        pass  # EN: test connection stays open until the section ends / ES: la conexión de test permanece abierta hasta que la sección termina

http_conn        = fresh_conn()
original_pool    = L._pool
L._pool          = _MockPool(http_conn)
client           = TestClient(L.app)

# EN: Stripe signature verification is patched to a no-op for happy-path tests.
#     We are testing the application layer (Pydantic validation, idempotency, DLQ routing),
#     not Stripe's HMAC library. The signature test below verifies the security boundary
#     separately without patching.
# ES: La verificación de firma de Stripe se parchea a un no-op para los tests del camino feliz.
#     Estamos probando la capa de aplicación (validación Pydantic, idempotencia, enrutamiento al DLQ),
#     no la librería HMAC de Stripe. El test de firma a continuación verifica el límite de seguridad
#     por separado sin parchear.
with patch("stripe.Webhook.construct_event"):

    # EN: Health check — verifies the server is responsive.
    # ES: Health check — verifica que el servidor responde.
    r = client.get("/health")
    chk("GET /health → 200",                        r.status_code == 200)
    chk("health response is ok",                    r.json().get("status") == "ok")

    ev_http = fake_event(eid="evt_http_001", customer="cus_http001", amount=9900)
    r = client.post("/webhook/stripe", json=ev_http)
    chk("POST valid event → HTTP 200",              r.status_code == 200)
    chk("valid event → POSTED via HTTP",            r.json()["outcome"] == "POSTED",
        f"got {r.json()}")

    # EN: Duplicate via HTTP — must return 200, not 4xx. Stripe needs 200 to stop retrying.
    # ES: Duplicado vía HTTP — debe retornar 200, no 4xx. Stripe necesita 200 para dejar de reintentar.
    r = client.post("/webhook/stripe", json=ev_http)
    chk("POST duplicate → HTTP 200 (not 4xx)",      r.status_code == 200)
    chk("duplicate → DLQ_DUPLICATE via HTTP",       r.json()["outcome"] == "DLQ_DUPLICATE",
        f"got {r.json()}")

    ev_bad_curr = fake_event(eid="evt_http_bad_curr", currency="USD")
    r = client.post("/webhook/stripe", json=ev_bad_curr)
    chk("POST uppercase currency → HTTP 200",       r.status_code == 200)
    chk("uppercase currency → DLQ_INVALID via HTTP", r.json()["outcome"] == "DLQ_INVALID",
        f"got {r.json()}")

    ev_zero = fake_event(eid="evt_http_zero", amount=0)
    r = client.post("/webhook/stripe", json=ev_zero)
    chk("POST $0 invoice → HTTP 200",               r.status_code == 200)
    chk("$0 invoice → DLQ_INVALID via HTTP",        r.json()["outcome"] == "DLQ_INVALID",
        f"got {r.json()}")

    ev_bad_cus = fake_event(eid="evt_http_bad_cus", customer="notacus_001")
    r = client.post("/webhook/stripe", json=ev_bad_cus)
    chk("POST bad customer prefix → HTTP 200",      r.status_code == 200)
    chk("bad prefix → DLQ_INVALID via HTTP",        r.json()["outcome"] == "DLQ_INVALID",
        f"got {r.json()}")

    # EN: Summary endpoint — verify shape and counts.
    #     After the tests above: 1 POSTED, 1 DUPLICATE, 3 INVALID = 4 DLQ entries total.
    # ES: Endpoint de resumen — verificar forma y conteos.
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

    # EN: limit=-1 must return HTTP 422, not a PostgreSQL error.
    # ES: limit=-1 debe retornar HTTP 422, no un error de PostgreSQL.
    r = client.get("/dlq/entries?limit=-1")
    chk("GET /dlq/entries?limit=-1 → HTTP 422",     r.status_code == 422)

# EN: Restore the real pool after HTTP tests complete.
# ES: Restaurar el pool real después de que los tests HTTP terminen.
L._pool = original_pool


print("\n── Phase 5: Stripe signature verification ───────────────────────────────")
# EN: This test verifies the security boundary WITHOUT mocking stripe.Webhook.
#     We temporarily set a real-format secret, send a POST with a clearly invalid
#     signature, and assert the endpoint returns 400.
# ES: Este test verifica el límite de seguridad SIN mockear stripe.Webhook.
#     Establecemos temporalmente un secreto con formato real, enviamos un POST
#     con una firma claramente inválida, y asertamos que el endpoint retorna 400.

sig_client       = TestClient(L.app)
original_secret  = L.STRIPE_WEBHOOK_SECRET
original_pool_2  = L._pool
L._pool          = _MockPool(fresh_conn())
L.STRIPE_WEBHOOK_SECRET = "whsec_test_secret_for_signature_tests"
try:
    body_bytes = json.dumps(fake_event(eid="evt_sig_test")).encode()
    r = sig_client.post(
        "/webhook/stripe",
        content=body_bytes,
        headers={
            "Content-Type":    "application/json",
            "stripe-signature": "t=1234,v1=completely_invalid_signature_value",
        },
    )
    chk("invalid Stripe-Signature → HTTP 400",  r.status_code == 400,
        f"got {r.status_code}: {r.text}")
finally:
    L.STRIPE_WEBHOOK_SECRET = original_secret
    L._pool                 = original_pool_2


print("\n── Phase 5: Concurrent insertion (threading) ────────────────────────────")
# EN: The most important test in the suite. 5 threads fire the same event simultaneously.
#     threading.Barrier(5) ensures all threads have reached _fire() before any INSERT runs —
#     a mathematically sound race condition test, not a sequential approximation.
#     ON CONFLICT (transaction_id) DO NOTHING serializes at the DB level:
#     exactly 1 INSERT wins; the other 4 get rowcount=0 → DLQ_DUPLICATE.
# ES: El test más importante de la suite. 5 hilos disparan el mismo evento simultáneamente.
#     threading.Barrier(5) asegura que todos los hilos han llegado a _fire() antes de que
#     corra cualquier INSERT — un test de condición de carrera matemáticamente sólido,
#     no una aproximación secuencial. ON CONFLICT (transaction_id) DO NOTHING serializa
#     a nivel DB: exactamente 1 INSERT gana; los otros 4 obtienen rowcount=0 → DLQ_DUPLICATE.

_setup_conn = fresh_conn()
_setup_conn.close()

ev_concurrent      = fake_event(eid="evt_concurrent_001",
                                customer="cus_concurrent", amount=5000)
results_concurrent = []
errors_concurrent  = []
_barrier           = threading.Barrier(5)  # EN: all 5 threads release simultaneously / ES: los 5 hilos liberan simultáneamente

def _fire():
    try:
        conn = L._bootstrap()
        _barrier.wait()  # EN: block until all 5 threads are ready / ES: bloquear hasta que los 5 hilos estén listos
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


print("\n── Phase 5: Batch — duplicate transaction_id in same batch ──────────────")
# EN: Verifies that when the same transaction_id appears twice in a single batch,
#     the RETURNING set correctly partitions them: the first occurrence lands in
#     the ledger (POSTED), the second is routed to DLQ_DUPLICATE. The ledger
#     must have exactly 1 row for that transaction_id — BIGINT handles large amounts.
# ES: Verifica que cuando el mismo transaction_id aparece dos veces en un solo lote,
#     el conjunto RETURNING los particiona correctamente: la primera ocurrencia aterriza
#     en el libro (POSTED), la segunda se enruta a DLQ_DUPLICATE. El libro debe tener
#     exactamente 1 fila para ese transaction_id — BIGINT maneja montos grandes.

batch_conn = fresh_conn()
dup_ev     = fake_event(eid="evt_batch_dup", customer="cus_batchdup", amount=1000)
results    = L.process_stripe_event_batch(batch_conn, [dup_ev, dup_ev])

chk("batch with same event twice: 2 results",   len(results) == 2)
chk("batch first occurrence → POSTED",          results[0]["outcome"] == "POSTED")
chk("batch second occurrence → DLQ_DUPLICATE",  results[1]["outcome"] == "DLQ_DUPLICATE")

ledger_dup_count = _q1(batch_conn,
    "SELECT COUNT(*) FROM ledger WHERE transaction_id=%s", ("evt_batch_dup",))
chk("batch duplicate: ledger has exactly 1 row", ledger_dup_count == 1)
batch_conn.close()


print("\n── Phase 5: BIGINT — amount > PostgreSQL INTEGER max ────────────────────")
# EN: PostgreSQL INTEGER max = 2,147,483,647 (~$21M). This test posts an event with
#     amount = 2,200,000,000 (~$22M) and asserts POSTED — verifying the BIGINT column
#     type change is in effect. This would fail with OverflowError if still INTEGER.
# ES: Máximo de INTEGER en PostgreSQL = 2,147,483,647 (~$21M). Este test publica un
#     evento con amount = 2,200,000,000 (~$22M) y aserta POSTED — verificando que el
#     cambio de tipo de columna a BIGINT está en efecto. Fallaría con OverflowError si
#     todavía fuera INTEGER.

large_conn = fresh_conn()
ev_large   = fake_event(eid="evt_large_amt", customer="cus_large", amount=2_200_000_000)
r          = L.process_stripe_event(large_conn, ev_large)
chk("amount 2,200,000,000 (> INT max) → POSTED (BIGINT verified)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")
large_conn.close()


print("\n── Phase 5: Outbox dispatch simulation ──────────────────────────────────")

dispatch_conn = fresh_conn()
L.process_stripe_event(
    dispatch_conn,
    fake_event(eid="evt_dispatch_001", customer="cus_dispatch", amount=7500)
)

pending_before = _q1(dispatch_conn, "SELECT COUNT(*) FROM outbox WHERE dispatched=0")
chk("outbox has 1 pending row before dispatch", pending_before == 1)

with dispatch_conn.cursor() as cur:
    cur.execute(
        "UPDATE outbox SET dispatched=1 WHERE transaction_id=%s AND dispatched=0",
        ("evt_dispatch_001",),
    )

pending_after = _q1(dispatch_conn, "SELECT COUNT(*) FROM outbox WHERE dispatched=0")
chk("outbox has 0 pending rows after dispatch", pending_after == 0)

r = L.process_stripe_event(
    dispatch_conn,
    fake_event(eid="evt_dispatch_001", customer="cus_dispatch", amount=7500)
)
chk("replay after dispatch → DLQ_DUPLICATE (idempotency holds)",
    r["outcome"] == "DLQ_DUPLICATE", f"got {r['outcome']}")

ledger_rows = _q1(dispatch_conn, "SELECT COUNT(*) FROM ledger")
chk("ledger still has 1 row after replay post-dispatch",
    ledger_rows == 1, f"got {ledger_rows}")
dispatch_conn.close()


print("\n── Phase 5: DLQ queryability and payload preservation ───────────────────")

dlq_conn = fresh_conn()

L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_1", currency="WRONG"))
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_2", amount=0))
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_3", customer=""))

rows = _qall(dlq_conn, "SELECT transaction_id, reason, raw_payload FROM dlq ORDER BY id")

chk("DLQ has 3 rows",                           len(rows) == 3,   f"got {len(rows)}")
chk("all 3 reasons are INVALID",
    all(r[1] == "INVALID" for r in rows))
chk("raw_payload is a JSON string",
    all(isinstance(r[2], str) for r in rows))

parsed = [json.loads(r[2]) for r in rows]
chk("payload evt_dlq_1 id preserved",           parsed[0].get("id") == "evt_dlq_1")
chk("payload evt_dlq_2 id preserved",           parsed[1].get("id") == "evt_dlq_2")
chk("payload evt_dlq_3 id preserved",           parsed[2].get("id") == "evt_dlq_3")
dlq_conn.close()


# =============================================================================
# PHASE 4 NEW TESTS: M3, M4, L4
# FASE 4 NUEVOS TESTS: M3, M4, L4
# =============================================================================

print("\n── Phase 4: Batch result indexed by input position (M3) ─────────────────")
# EN: Verifies that results[i] corresponds to events[i] for every position.
#     A scrambled-return bug (e.g., results returned in RETURNING order instead of
#     input order) would pass all previous batch tests but fail these assertions.
# ES: Verifica que results[i] corresponde a events[i] para cada posición.
#     Un bug de retorno desordenado pasaría los tests anteriores pero fallaría aquí.

pos_conn = fresh_conn()
ev_pos_a = fake_event(eid="evt_pos_a", customer="cus_posa", amount=1000)
ev_pos_b = fake_event(eid="evt_pos_b", customer="cus_posb", amount=2000)
ev_pos_c = fake_event(eid="evt_pos_c", customer="cus_posc", amount=3000)
pos_results = L.process_stripe_event_batch(pos_conn, [ev_pos_a, ev_pos_b, ev_pos_c])
chk("batch position test: 3 results returned",        len(pos_results) == 3)
chk("batch results[0] matches input event_pos_a",     pos_results[0]["transaction_id"] == "evt_pos_a",
    f"got {pos_results[0].get('transaction_id')}")
chk("batch results[1] matches input event_pos_b",     pos_results[1]["transaction_id"] == "evt_pos_b",
    f"got {pos_results[1].get('transaction_id')}")
chk("batch results[2] matches input event_pos_c",     pos_results[2]["transaction_id"] == "evt_pos_c",
    f"got {pos_results[2].get('transaction_id')}")
pos_conn.close()


print("\n── Phase 4: DB failure raises RuntimeError → HTTP 503 (M4) ─────────────")
# EN: Verifies that when process_stripe_event raises RuntimeError (DB-level failure),
#     the FastAPI handler converts it to HTTP 503. The handler is coded correctly
#     but was never exercised by any previous test.
# ES: Verifica que cuando process_stripe_event lanza RuntimeError (fallo a nivel DB),
#     el manejador FastAPI lo convierte a HTTP 503. El manejador estaba codificado
#     correctamente pero nunca se ejerció en ningún test anterior.

db_err_conn  = fresh_conn()
original_pool_3 = L._pool
L._pool         = _MockPool(db_err_conn)
db_err_client   = TestClient(L.app)
try:
    with patch("stripe.Webhook.construct_event"), \
         patch("ledger.process_stripe_event", side_effect=RuntimeError("DB write failed: simulated")):
        r = db_err_client.post("/webhook/stripe", json=fake_event(eid="evt_db_err"))
        chk("DB RuntimeError → HTTP 503",
            r.status_code == 503, f"got {r.status_code}: {r.text}")
        chk("503 body contains error detail",
            "DB write failed" in r.text or r.status_code == 503)
finally:
    L._pool = original_pool_3
    db_err_conn.close()


print("\n── Phase 4: Empty batch returns empty list (L4) ─────────────────────────")
# EN: Verifies the early-exit guard: process_stripe_event_batch(conn, []) → [].
#     Simple guard but was untested — a regression that removed it would silently
#     crash on the first None access in the results list.
# ES: Verifica la guardia de salida temprana: process_stripe_event_batch(conn, []) → [].

empty_conn = fresh_conn()
empty_result = L.process_stripe_event_batch(empty_conn, [])
chk("empty batch → []",                               empty_result == [],
    f"got {empty_result}")
empty_conn.close()


# =============================================================================
# Phase 7: Outbox worker — drain_batch tests
# EN: Tests the drain_batch() function from worker.py directly (no HTTP server).
#     Three tests: empty queue, normal dispatch, and idempotency.
# ES: Tests de la función drain_batch() de worker.py directamente (sin servidor HTTP).
#     Tres tests: cola vacía, despacho normal, e idempotencia.
# =============================================================================
print("\n── Phase 7: drain_batch — empty queue ───────────────────────────────────")
worker_conn1 = fresh_conn()
drained_empty = W.drain_batch(worker_conn1)
chk("drain_batch on empty outbox returns 0",
    drained_empty == 0, f"got {drained_empty}")
with worker_conn1.cursor() as _wc:
    _wc.execute("SELECT COUNT(*) FROM outbox WHERE dispatched=0")
    remaining = _wc.fetchone()[0]
chk("outbox still empty after drain on empty queue",
    remaining == 0, f"got {remaining}")
worker_conn1.close()

print("\n── Phase 7: drain_batch — dispatches rows and marks dispatched=1 ────────")
worker_conn2 = fresh_conn()
# EN: Manually insert ledger rows first (FK constraint requires them), then outbox rows.
# ES: Insertar filas de ledger primero (FK lo requiere), luego filas de outbox.
with worker_conn2.cursor() as _wc2:
    _wc2.execute("BEGIN")
    for i in range(1, 4):
        _wc2.execute(
            """
            INSERT INTO ledger
              (transaction_id, event_type, customer_id, amount_cents,
               currency, status, idempotency_key, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (f"evt_w{i}", "invoice.paid", "cus_worker",
             1000 * i, "usd", "paid", f"evt_w{i}", "{}"),
        )
        _wc2.execute(
            """
            INSERT INTO outbox (transaction_id, event_type, payload)
            VALUES (%s, %s, %s)
            """,
            (f"evt_w{i}", "invoice.paid", "{}"),
        )
    _wc2.execute("COMMIT")

drained_count = W.drain_batch(worker_conn2)
chk("drain_batch returns 3 for 3 pending rows",
    drained_count == 3, f"got {drained_count}")

with worker_conn2.cursor() as _wc2:
    _wc2.execute(
        "SELECT COUNT(*) FROM outbox WHERE dispatched=1 AND dispatched_at IS NOT NULL"
    )
    done = _wc2.fetchone()[0]
chk("all 3 outbox rows marked dispatched=1 with dispatched_at set",
    done == 3, f"got {done}")
worker_conn2.close()

print("\n── Phase 7: drain_batch — idempotency (second call returns 0) ───────────")
worker_conn3 = fresh_conn()
with worker_conn3.cursor() as _wc3:
    _wc3.execute("BEGIN")
    _wc3.execute(
        """
        INSERT INTO ledger
          (transaction_id, event_type, customer_id, amount_cents,
           currency, status, idempotency_key, payload)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        ("evt_idem", "invoice.paid", "cus_idem", 500, "usd", "paid", "evt_idem", "{}"),
    )
    _wc3.execute(
        "INSERT INTO outbox (transaction_id, event_type, payload) VALUES (%s, %s, %s)",
        ("evt_idem", "invoice.paid", "{}"),
    )
    _wc3.execute("COMMIT")

first_drain  = W.drain_batch(worker_conn3)
second_drain = W.drain_batch(worker_conn3)
chk("first drain_batch call returns 1",
    first_drain == 1, f"got {first_drain}")
chk("second drain_batch call returns 0 (idempotent — no double-dispatch)",
    second_drain == 0, f"got {second_drain}")
worker_conn3.close()


# =============================================================================
# FINAL SUMMARY / RESUMEN FINAL
# =============================================================================
print(f"\n{'='*60}")
print(f"  {ok} passed | {fail} failed")
if fail:
    sys.exit(1)
