"""
test_ledger.py — Full Integration Test Suite
======================================================================================
Tests across multiple phases. Run with: python test_ledger.py
Requires: PostgreSQL running and DATABASE_URL set (see docker-compose.yml).
Covers: entry validation, output models, cross-field rules, HTTP layer,
concurrent insertion, outbox dispatch simulation, DLQ queryability.
No mocks for the database. No stubs for the ON CONFLICT logic.
Every assertion hits a real PostgreSQL database.
The test runner is deliberately simple — no pytest dependency — so anyone
can run it with a plain Python install.
"""

# ---------------------------------------------------------------------------
# Standard library imports
# ---------------------------------------------------------------------------
import io          # Used to wrap stdout for UTF-8 output on Windows
import json        # Used to verify raw_payload round-trips correctly
import logging     # Phase 12: build LogRecords to assert the JSON formatter's output
import sys         # stdout override for Windows UTF-8 encoding
import time        # Used in benchmark timing
import threading   # Used for the concurrent idempotency test
from datetime import datetime
from unittest.mock import patch

# Force UTF-8 output on Windows. Without this, Unicode characters in print()
#     cause UnicodeEncodeError because Windows stdout defaults to CP-1252.
#     This must happen before any print() call — so it's at the top of the file.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Import the ledger module directly — all tests call its functions and use
#     its models. This is the module under test.
import ledger as L
import worker as W  # Phase 7: outbox drain worker

# Integration tests verify application behavior (idempotency, validation, DLQ routing),
# not authentication. Clear BILLING_API_KEY so _require_api_key skips the check on all
# protected endpoints — no X-Api-Key header needed in any test call.
L.BILLING_API_KEY = ""

# ---------------------------------------------------------------------------
# Test infrastructure
# Minimal test runner — two globals (ok, fail) and one helper function.
#     chk() is the assertion function: prints PASS or FAIL with the test name.
#     No pytest, no unittest — intentional. Any recruiter can read and run this.
# ---------------------------------------------------------------------------
ok = fail = 0

def chk(name: str, cond: bool, detail: str = "") -> None:
    """
    Assertion helper. Increments ok or fail and prints result.
    detail is shown only on failure — useful for showing actual vs expected values.
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
    Execute a query and return the first column of the first row.
    Used for COUNT(*) and similar single-value reads.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchone()[0]


def _qall(conn, sql: str, params: tuple = ()) -> list:
    """
    Execute a query and return all rows as a list of tuples.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall()


_open_conns: list = []  # M7: every fresh_conn() is tracked here and closed at end of run


def fresh_conn():
    """
    Returns a PostgreSQL connection with all tables truncated.
    Each test section that needs isolation calls fresh_conn() to start clean.
    TRUNCATE ... RESTART IDENTITY resets all table data and BIGSERIAL sequences.
    No mocking — this hits the real PostgreSQL database defined by DATABASE_URL.
    Requires: docker-compose up -d postgres (or a running PostgreSQL instance).
    """
    conn = L._bootstrap()
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
    _open_conns.append(conn)  # M7: tracked for guaranteed close at end of run
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
    Factory function for fake Stripe events. Produces structurally valid payloads
    that pass all Pydantic validators by default. Override individual fields to
    test specific failure modes (e.g., currency="USD" to test uppercase rejection).
    customer default "cus_123" passes min_length=4 AND the cus_ prefix check.
    amount default 4900 passes ge=0 AND the nonzero invoice check.
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
#
# Tests that the Pydantic models (StripeEvent, StripeObject) correctly reject
#     invalid payloads at the entry boundary. Every invalid payload should route
#     to DLQ with reason=INVALID, never touch the ledger table.
# =============================================================================

print("\n── Phase 1: Pydantic Validation (Entry Boundary) ────────────────────────")

conn = fresh_conn()
ev   = fake_event()

# Happy path — valid event should be posted to ledger, not DLQ.
r1 = L.process_stripe_event(conn, ev)
chk("valid event → POSTED",        r1["outcome"] == "POSTED")
chk("transaction_id returned",     r1["transaction_id"] == "evt_test_001")


print("\n── Phase 1: Currency Validation (Regex Pattern) ────────────────────────")
# Tests the `pattern=r"^[a-z]{3}$"` constraint on StripeObject.currency.
#     Only exactly 3 lowercase ASCII letters are accepted. ISO 4217 enforced at entry.

conn = fresh_conn()

ev_valid = fake_event(currency="usd")
r = L.process_stripe_event(conn, ev_valid)
chk("currency 'usd' → POSTED",                  r["outcome"] == "POSTED")

# Uppercase is normalized, not rejected. ISO 4217 codes are case-insensitive and
#     upstream systems disagree about case; storing both "USD" and "usd" would split
#     every GROUP BY currency in a finance report into two rows that look like two
#     currencies. What matters is that only one spelling ever reaches the database.
ev_uppercase = fake_event(eid="evt_upper_curr_1", currency="USD")
r = L.process_stripe_event(conn, ev_uppercase)
chk("currency 'USD' (uppercase) → POSTED, normalized",
    r["outcome"] == "POSTED", f"got {r['outcome']}")
chk("uppercase currency stored lowercase",
    _q1(conn, "SELECT currency FROM ledger WHERE transaction_id='evt_upper_curr_1'") == "usd",
    str(_q1(conn, "SELECT currency FROM ledger WHERE transaction_id='evt_upper_curr_1'")))

ev_mixed = fake_event(eid="evt_upper_curr_2", currency="eUr")
L.process_stripe_event(conn, ev_mixed)
chk("mixed-case currency stored lowercase",
    _q1(conn, "SELECT currency FROM ledger WHERE transaction_id='evt_upper_curr_2'") == "eur")

chk("GROUP BY currency does not split on case",
    _q1(conn, "SELECT COUNT(DISTINCT currency) FROM ledger WHERE currency IN ('usd','USD')") == 1)

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
# Throughput measurement — 5,000 unique events, no HTTP overhead.
#     PostgreSQL floor is 500 TPS — conservative for localhost with explicit
#     per-event transactions (BEGIN + 2 INSERTs + COMMIT per event).

N     = 5_000
conn4 = fresh_conn()

import random
import string

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

# M5 — data integrity: verify actual row count in DB matches batch input size.
#     A broken batch function that returns dummy results without touching the DB
#     would pass the TPS check above but fail this assertion.
bench_row_count = _q1(conn4, "SELECT COUNT(*) FROM ledger")
chk(f"batch data integrity: exactly {N} rows in ledger",
    bench_row_count == N, f"got {bench_row_count}")
conn4.close()  # L3 — explicit close to avoid leaking connection


# =============================================================================
# PHASE 2: OUTPUT MODEL VALIDATION (DLQEntry + LedgerEntry)
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
chk("DLQEntry.to_db() is 8-tuple",              len(db_row) == 8)
chk("to_db() reason is string not enum",        db_row[1] == "DUPLICATE")
chk("to_db() payload is JSON string",           db_row[2] == '{"id": "evt_test_001"}')
# received_at is now a timezone-aware datetime (TIMESTAMPTZ), not a float.
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

# Pydantic v2 coerces float → datetime for datetime fields.
#     1234567890.0 (Unix timestamp) is accepted and stored as a tz-aware datetime.
le = L.LedgerEntry(
    transaction_id="evt_le_001",
    event_type=L.EventType.INVOICE_PAID,
    customer_id="cus_abc123",
    amount_cents=4900,
    currency="usd",
    status=L.LedgerStatus.POSTED,
    idempotency_key="idem_evt_le_001",
    payload='{"id": "evt_le_001"}',
    created_at=1234567890.0,  # coerced to datetime by Pydantic v2
)
chk("LedgerEntry builds with valid data",       le.transaction_id == "evt_le_001")
chk("LedgerEntry status is LedgerStatus",       le.status == L.LedgerStatus.POSTED)

db_row = le.to_db()
chk("LedgerEntry.to_db() is 9-tuple",           len(db_row) == 9)
chk("to_db() event_type is string not enum",    db_row[1] == "invoice.paid")
chk("to_db() status is string not enum",        db_row[5] == "POSTED")
chk("to_db() amount_cents is int",              db_row[3] == 4900)
# created_at is now a timezone-aware datetime (TIMESTAMPTZ), not a float.
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
# =============================================================================

print("\n── Phase 5: Integration — HTTP layer (TestClient) ──────────────────────")

from starlette.testclient import TestClient

# _MockPool wraps a single psycopg2 connection to satisfy the _pool.getconn() /
#     _pool.putconn() interface expected by the FastAPI route handlers. This lets tests
#     inject a controlled, truncated connection without standing up a real pool.
#     This does NOT mock the database — every INSERT and SELECT hits real PostgreSQL.
class _MockPool:
    def __init__(self, conn):
        self._conn = conn
    def getconn(self):
        return self._conn
    def putconn(self, conn):
        pass  # test connection stays open until the section ends

http_conn        = fresh_conn()
original_pool    = L._pool
L._pool          = _MockPool(http_conn)
client           = TestClient(L.app)

# Stripe signature verification is patched to a no-op for happy-path tests.
#     We are testing the application layer (Pydantic validation, idempotency, DLQ routing),
#     not Stripe's HMAC library. The signature test below verifies the security boundary
#     separately without patching.
with patch("stripe.Webhook.construct_event"):

    # Health check — verifies the server is responsive.
    r = client.get("/health")
    chk("GET /health → 200",                        r.status_code == 200)
    chk("health response is ok",                    r.json().get("status") == "ok")

    ev_http = fake_event(eid="evt_http_001", customer="cus_http001", amount=9900)
    r = client.post("/webhook/stripe", json=ev_http)
    chk("POST valid event → HTTP 200",              r.status_code == 200)
    chk("valid event → POSTED via HTTP",            r.json()["outcome"] == "POSTED",
        f"got {r.json()}")

    # Duplicate via HTTP — must return 200, not 4xx. Stripe needs 200 to stop retrying.
    r = client.post("/webhook/stripe", json=ev_http)
    chk("POST duplicate → HTTP 200 (not 4xx)",      r.status_code == 200)
    chk("duplicate → DLQ_DUPLICATE via HTTP",       r.json()["outcome"] == "DLQ_DUPLICATE",
        f"got {r.json()}")

    ev_up_curr = fake_event(eid="evt_http_up_curr", currency="USD")
    r = client.post("/webhook/stripe", json=ev_up_curr)
    chk("POST uppercase currency → HTTP 200",       r.status_code == 200)
    chk("uppercase currency normalized, not rejected", r.json()["outcome"] == "POSTED",
        f"got {r.json()}")

    # A currency that is not simply the wrong case is still rejected — normalization
    #     lowercases, it does not repair. "us" has no lowercase spelling that is valid.
    ev_bad_curr = fake_event(eid="evt_http_bad_curr", currency="us")
    r = client.post("/webhook/stripe", json=ev_bad_curr)
    chk("POST malformed currency → HTTP 200",       r.status_code == 200)
    chk("malformed currency → DLQ_INVALID via HTTP", r.json()["outcome"] == "DLQ_INVALID",
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

    # Summary endpoint — verify shape and counts.
    #     Posted above: evt_http_001 and evt_http_up_curr (uppercase currency is
    #     normalized and accepted, so it posts) = 2 ledger rows, 2 outbox rows.
    #     DLQ: 1 DUPLICATE + 3 INVALID (malformed currency, $0 invoice, bad prefix) = 4.
    r = client.get("/ledger/summary")
    chk("GET /ledger/summary → 200",                r.status_code == 200)
    summary = r.json()
    chk("summary has 'ledger' key",                 "ledger" in summary)
    chk("summary has 'dlq_depth' key",              "dlq_depth" in summary)
    chk("summary has 'outbox_pending' key",         "outbox_pending" in summary)
    chk("summary dlq_depth is 4",                   summary["dlq_depth"] == 4,
        f"got {summary['dlq_depth']}")
    chk("summary outbox_pending is 2",              summary["outbox_pending"] == 2,
        f"got {summary['outbox_pending']}")

    # limit=-1 must return HTTP 422, not a PostgreSQL error.
    r = client.get("/dlq/entries?limit=-1")
    chk("GET /dlq/entries?limit=-1 → HTTP 422",     r.status_code == 422)

# Restore the real pool after HTTP tests complete.
L._pool = original_pool


print("\n── Phase 5: Stripe signature verification ───────────────────────────────")
# This test verifies the security boundary WITHOUT mocking stripe.Webhook.
#     We temporarily set a real-format secret, send a POST with a clearly invalid
#     signature, and assert the endpoint returns 400.

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
# The most important test in the suite. 5 threads fire the same event simultaneously.
#     threading.Barrier(5) ensures all threads have reached _fire() before any INSERT runs —
#     a mathematically sound race condition test, not a sequential approximation.
#     ON CONFLICT (transaction_id) DO NOTHING serializes at the DB level:
#     exactly 1 INSERT wins; the other 4 get rowcount=0 → DLQ_DUPLICATE.

_setup_conn = fresh_conn()
_setup_conn.close()

ev_concurrent      = fake_event(eid="evt_concurrent_001",
                                customer="cus_concurrent", amount=5000)
results_concurrent = []
errors_concurrent  = []
_barrier           = threading.Barrier(5)  # all 5 threads release simultaneously

def _fire():
    try:
        conn = L._bootstrap()
        _barrier.wait()  # block until all 5 threads are ready
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
# Verifies that when the same transaction_id appears twice in a single batch,
#     the RETURNING set correctly partitions them: the first occurrence lands in
#     the ledger (POSTED), the second is routed to DLQ_DUPLICATE. The ledger
#     must have exactly 1 row for that transaction_id — BIGINT handles large amounts.

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
# PostgreSQL INTEGER max = 2,147,483,647 (~$21M). This test posts an event with
#     amount = 2,200,000,000 (~$22M) and asserts POSTED — verifying the BIGINT column
#     type change is in effect. This would fail with OverflowError if still INTEGER.

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
# =============================================================================

print("\n── Phase 4: Batch result indexed by input position (M3) ─────────────────")
# Verifies that results[i] corresponds to events[i] for every position.
#     A scrambled-return bug (e.g., results returned in RETURNING order instead of
#     input order) would pass all previous batch tests but fail these assertions.

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
# Verifies that when process_stripe_event raises RuntimeError (DB-level failure),
#     the FastAPI handler converts it to HTTP 503. The handler is coded correctly
#     but was never exercised by any previous test.

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
        # The previous assertion already pins status_code == 503; this one must verify
        # the BODY independently. (An earlier version had "or r.status_code == 503"
        # appended — a tautology that could never fail once the status check passed.)
        chk("503 body contains error detail",
            "DB write failed" in r.text, f"got body: {r.text}")
finally:
    L._pool = original_pool_3
    db_err_conn.close()


print("\n── Phase 4: Empty batch returns empty list (L4) ─────────────────────────")
# Verifies the early-exit guard: process_stripe_event_batch(conn, []) → [].
#     Simple guard but was untested — a regression that removed it would silently
#     crash on the first None access in the results list.

empty_conn = fresh_conn()
empty_result = L.process_stripe_event_batch(empty_conn, [])
chk("empty batch → []",                               empty_result == [],
    f"got {empty_result}")
empty_conn.close()


# =============================================================================
# Phase 7: Outbox worker — drain_batch tests
# Tests the drain_batch() function from worker.py directly (no HTTP server).
#     Three tests: empty queue, normal dispatch, and idempotency.
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
# Manually insert ledger rows first (FK constraint requires them), then outbox rows.
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
# PHASE 10: ALEMBIC MIGRATION TESTS
# =============================================================================
print("\n── Phase 10: Alembic migrations ─────────────────────────────────────────")

import subprocess

conn_mig = fresh_conn()

# Invoke alembic through the SAME interpreter running this suite, not through a bare
#     "alembic" on PATH. A bare name resolves to whichever installation happens to come
#     first, which on a dev box is easily a different Python with a different alembic —
#     so these tests could pass against a toolchain the project never pins, while the
#     venv under test had no alembic at all. sys.executable -m makes the environment
#     being tested and the environment doing the testing the same one.
_ALEMBIC = [sys.executable, "-m", "alembic"]

# Test 1 — alembic upgrade head runs without error and reaches the latest revision.
result = subprocess.run(
    [*_ALEMBIC, "upgrade", "head"],
    capture_output=True, text=True,
    env={**__import__("os").environ, "DATABASE_URL": L.DATABASE_URL},
)
chk("alembic upgrade head exits 0", result.returncode == 0, result.stderr.strip())

result2 = subprocess.run(
    [*_ALEMBIC, "current"],
    capture_output=True, text=True,
    env={**__import__("os").environ, "DATABASE_URL": L.DATABASE_URL},
)
# Compare against whatever alembic itself calls head rather than a literal revision.
#     Hardcoding "0002" made this test fail on the next migration for no real reason,
#     which trains people to edit the assertion instead of reading it. What actually
#     matters is that `upgrade head` landed on the latest revision, not its number.
result_heads = subprocess.run(
    [*_ALEMBIC, "heads"],
    capture_output=True, text=True,
    env={**__import__("os").environ, "DATABASE_URL": L.DATABASE_URL},
)
_head_rev = result_heads.stdout.split()[0] if result_heads.stdout.split() else ""
chk("alembic current is at head",
    bool(_head_rev) and _head_rev in result2.stdout,
    f"head={_head_rev!r} current={result2.stdout.strip()!r}")

# dlq.claimed_at arrives in migration 0003 — assert it through the Alembic path too,
#     not only through _bootstrap(), since production schema comes from Alembic.
with conn_mig.cursor() as cur:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'dlq' AND column_name = 'claimed_at'
    """)
    chk("dlq.claimed_at exists after alembic upgrade head", cur.fetchone() is not None)

# Test 2 — all three tables exist after migration.
with conn_mig.cursor() as cur:
    cur.execute("""
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        AND table_name IN ('ledger', 'outbox', 'dlq')
        ORDER BY table_name
    """)
    tables = {r[0] for r in cur.fetchall()}
chk("ledger table exists after migration",  "ledger"  in tables, str(tables))
chk("outbox table exists after migration",  "outbox"  in tables, str(tables))
chk("dlq table exists after migration",     "dlq"     in tables, str(tables))

# Test 3 — outbox has dispatched_at column (added in Phase 7, present in migration 0001).
with conn_mig.cursor() as cur:
    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'outbox' AND column_name = 'dispatched_at'
    """)
    col = cur.fetchone()
chk("outbox.dispatched_at column exists", col is not None, "column missing")

# Test 4 — downgrade to base then upgrade back to head (round-trip).
result_down = subprocess.run(
    [*_ALEMBIC, "downgrade", "base"],
    capture_output=True, text=True,
    env={**__import__("os").environ, "DATABASE_URL": L.DATABASE_URL},
)
chk("alembic downgrade base exits 0", result_down.returncode == 0, result_down.stderr.strip())

result_up = subprocess.run(
    [*_ALEMBIC, "upgrade", "head"],
    capture_output=True, text=True,
    env={**__import__("os").environ, "DATABASE_URL": L.DATABASE_URL},
)
chk("alembic upgrade head after downgrade exits 0", result_up.returncode == 0, result_up.stderr.strip())

conn_mig.close()


# =============================================================================
# PHASE 11: DLQ RETRY TESTS
# =============================================================================
print("\n── Phase 11: DLQ retry ──────────────────────────────────────────────────")


conn_r = fresh_conn()

# Test 1 — TRANSIENT entry gets max_retries=3 and next_retry_at set automatically.
transient_entry = L.DLQEntry(
    transaction_id="test_transient_001",
    reason=L.DLQReason.TRANSIENT,
    raw_payload={"id": "test_transient_001", "type": "invoice.paid"},
)
chk("TRANSIENT DLQEntry gets max_retries=3", transient_entry.max_retries == 3,
    f"got {transient_entry.max_retries}")
chk("TRANSIENT DLQEntry next_retry_at starts NULL (retry-immediately semantics)",
    transient_entry.next_retry_at is None, f"got {transient_entry.next_retry_at}")

# Test 2 — INVALID entry has max_retries=0 (no auto-retry).
invalid_entry = L.DLQEntry(
    transaction_id="test_invalid_001",
    reason=L.DLQReason.INVALID,
    raw_payload={"bad": "payload"},
)
chk("INVALID DLQEntry has max_retries=0", invalid_entry.max_retries == 0,
    f"got {invalid_entry.max_retries}")

# Test 3 — retry_dlq_batch returns 0 when no retryable entries exist.
# (fresh_conn already truncated the table — only non-retryable entries would be here)
count_empty = L.retry_dlq_batch(conn_r)
chk("retry_dlq_batch returns 0 on empty retryable queue", count_empty == 0,
    f"got {count_empty}")

# Test 4 — write a TRANSIENT entry, retry_dlq_batch picks it up and resolves it.
# Use a valid event payload that process_stripe_event will accept.
valid_payload = {
    "id": "evt_retry_test_001",
    "type": "invoice.paid",
    "data": {"object": {"customer": "cus_retry001", "amount_paid": 5000, "currency": "usd"}},
    "request": {"idempotency_key": "idem_retry_001"},
}
transient_db = L.DLQEntry(
    transaction_id="evt_retry_test_001",
    reason=L.DLQReason.TRANSIENT,
    raw_payload=valid_payload,
)
L._write_dlq(conn_r, transient_db)

retried = L.retry_dlq_batch(conn_r)
chk("retry_dlq_batch attempts 1 TRANSIENT entry", retried == 1, f"got {retried}")

with conn_r.cursor() as cur:
    cur.execute("SELECT status FROM dlq WHERE transaction_id='evt_retry_test_001'")
    row = cur.fetchone()
chk("TRANSIENT entry resolved after successful retry",
    row is not None and row[0] == "resolved", f"got {row}")

# Test 5 — write a TRANSIENT entry with bad payload and max_retries=1; exhausts after 1 attempt.
# process_stripe_event also writes an INVALID row for the same transaction_id, so we
# capture the TRANSIENT entry's id and query by id (not transaction_id) to avoid ambiguity.
bad_payload = {"id": "evt_exhaust_001", "type": "invoice.paid",
               "data": {"object": {"customer": "x", "amount_paid": -1, "currency": "usd"}},
               "request": None}
exhaust_entry = L.DLQEntry(
    transaction_id="evt_exhaust_001",
    reason=L.DLQReason.TRANSIENT,
    raw_payload=bad_payload,
    max_retries=1,
)
L._write_dlq(conn_r, exhaust_entry)

# Capture the id of the just-written row (largest id in DLQ is the one we just inserted).
with conn_r.cursor() as cur:
    cur.execute("SELECT id FROM dlq WHERE transaction_id='evt_exhaust_001' AND reason='TRANSIENT'")
    exhaust_id = cur.fetchone()[0]

L.retry_dlq_batch(conn_r)

with conn_r.cursor() as cur:
    cur.execute("SELECT status, retry_count FROM dlq WHERE id=%s", (exhaust_id,))
    exhaust_row = cur.fetchone()
chk("exhausted entry has status=exhausted",
    exhaust_row is not None and exhaust_row[0] == "exhausted", f"got {exhaust_row}")
chk("exhausted entry retry_count >= max_retries",
    exhaust_row is not None and exhaust_row[1] >= 1, f"got {exhaust_row}")

conn_r.close()


# =============================================================================
# PHASE 11b: HIGH-1 FIX — POST /dlq/{id}/retry resolves on success
# Verifies that the manual retry endpoint correctly recognises "POSTED" as a
# success outcome and marks the DLQ entry 'resolved', not 'exhausted'.
# (The old code checked for "SUCCESS" which process_stripe_event never returns.)
# =============================================================================
print("\n── Phase 11b: manual retry endpoint resolves on POSTED (HIGH-1 fix) ────")

manual_conn = fresh_conn()
original_pool_manual = L._pool
L._pool = _MockPool(manual_conn)
manual_client = TestClient(L.app)

valid_manual_payload = {
    "id": "evt_manual_retry_001",
    "type": "invoice.paid",
    "data": {"object": {"customer": "cus_manual001", "amount_paid": 5000, "currency": "usd"}},
    "request": {"idempotency_key": "idem_manual_001"},
}
t_manual = L.DLQEntry(
    transaction_id="evt_manual_retry_001",
    reason=L.DLQReason.TRANSIENT,
    raw_payload=valid_manual_payload,
)
L._write_dlq(manual_conn, t_manual)

with manual_conn.cursor() as c:
    c.execute("SELECT id FROM dlq WHERE transaction_id='evt_manual_retry_001' ORDER BY id DESC LIMIT 1")
    manual_dlq_id = c.fetchone()[0]

with patch("stripe.Webhook.construct_event"):
    r = manual_client.post(f"/dlq/{manual_dlq_id}/retry")

chk("POST /dlq/{id}/retry → 200", r.status_code == 200, f"got {r.status_code}: {r.text}")
chk("manual retry outcome is POSTED", r.json().get("outcome") == "POSTED", f"got {r.json()}")

with manual_conn.cursor() as c:
    c.execute("SELECT status FROM dlq WHERE id=%s", (manual_dlq_id,))
    st = c.fetchone()[0]
chk("manual retry DLQ status is resolved", st == "resolved", f"got {st!r}")

L._pool = original_pool_manual
manual_conn.close()


# =============================================================================
# M1: API KEY AUTH BOUNDARY
# Verifies _require_api_key actually enforces the key. The rest of the suite runs
#     with BILLING_API_KEY="" (auth disabled); this section sets a key and asserts
#     401 on missing/wrong and 200 on correct, then restores the empty key.
# =============================================================================
print("\n── M1: API key auth boundary ────────────────────────────────────────────")

auth_conn          = fresh_conn()
original_pool_auth = L._pool
original_key_auth  = L.BILLING_API_KEY
L._pool            = _MockPool(auth_conn)
L.BILLING_API_KEY  = "test_secret_key"
auth_client        = TestClient(L.app)
try:
    r = auth_client.get("/ledger/summary")
    chk("ops endpoint without X-Api-Key → 401", r.status_code == 401, f"got {r.status_code}")

    r = auth_client.get("/ledger/summary", headers={"X-Api-Key": "wrong_key"})
    chk("ops endpoint with wrong key → 401", r.status_code == 401, f"got {r.status_code}")

    r = auth_client.get("/ledger/summary", headers={"X-Api-Key": "test_secret_key"})
    chk("ops endpoint with correct key → 200", r.status_code == 200, f"got {r.status_code}")
finally:
    L.BILLING_API_KEY = original_key_auth
    L._pool           = original_pool_auth
    auth_conn.close()


# =============================================================================
# M5: DLQ RETENTION (prune_dlq)
# Verifies prune_dlq deletes old DUPLICATE rows only — recent DUPLICATE rows and
#     INVALID rows of any age are kept (they need human review).
# =============================================================================
print("\n── M5: prune_dlq retention ──────────────────────────────────────────────")

prune_conn = fresh_conn()
with prune_conn.cursor() as cur:
    cur.execute("BEGIN")
    cur.execute(
        "INSERT INTO dlq (transaction_id, reason, raw_payload, received_at) "
        "VALUES (%s, %s, %s, NOW() - INTERVAL '100 days')",
        ("evt_old_dup", "DUPLICATE", "{}"),
    )
    cur.execute(
        "INSERT INTO dlq (transaction_id, reason, raw_payload, received_at) "
        "VALUES (%s, %s, %s, NOW() - INTERVAL '10 days')",
        ("evt_new_dup", "DUPLICATE", "{}"),
    )
    cur.execute(
        "INSERT INTO dlq (transaction_id, reason, raw_payload, received_at) "
        "VALUES (%s, %s, %s, NOW() - INTERVAL '100 days')",
        ("evt_old_invalid", "INVALID", "{}"),
    )
    cur.execute("COMMIT")

deleted = L.prune_dlq(prune_conn, retention_days=90)
chk("prune_dlq deletes exactly 1 old DUPLICATE row", deleted == 1, f"got {deleted}")

remaining_ids = [r[0] for r in _qall(prune_conn, "SELECT transaction_id FROM dlq ORDER BY transaction_id")]
chk("old DUPLICATE (>90d) removed",        "evt_old_dup" not in remaining_ids, str(remaining_ids))
chk("recent DUPLICATE (10d) kept",         "evt_new_dup" in remaining_ids, str(remaining_ids))
chk("old INVALID kept (only DUPLICATE pruned)", "evt_old_invalid" in remaining_ids, str(remaining_ids))
prune_conn.close()


# =============================================================================
# A0: PER-EVENT-TYPE AMOUNT EXTRACTION
# Stripe does not put the billable figure in one field for every event type. A
#     failed invoice reports amount_paid=0 and the real figure in amount_due, so
#     reading amount_paid for everything made every invoice.payment_failed look like
#     a $0 invoice — rejected by check_invoice_amount_nonzero and dead-lettered.
#     Dunning depends on exactly those events.
# =============================================================================
print("\n── A0: amount is read from the field each event type populates ─────────")

amt_conn = fresh_conn()

ev_failed_due = {
    "id": "evt_amt_failed",
    "type": "invoice.payment_failed",
    "data": {"object": {"customer": "cus_amt01", "amount_paid": 0,
                        "amount_due": 7500, "currency": "usd"}},
}
r_amt = L.process_stripe_event(amt_conn, ev_failed_due)
chk("failed invoice with amount_due is accepted (was DLQ_INVALID)",
    r_amt["outcome"] == "VOID", f"got {r_amt}")
chk("failed invoice recorded at amount_due, not amount_paid=0",
    _q1(amt_conn, "SELECT amount_cents FROM ledger WHERE transaction_id='evt_amt_failed'") == 7500,
    str(_q1(amt_conn, "SELECT amount_cents FROM ledger WHERE transaction_id='evt_amt_failed'")))

# A paid invoice still prefers amount_paid — that is what was actually collected.
ev_paid_both = {
    "id": "evt_amt_paid",
    "type": "invoice.paid",
    "data": {"object": {"customer": "cus_amt02", "amount_paid": 3000,
                        "amount_due": 9999, "currency": "usd"}},
}
L.process_stripe_event(amt_conn, ev_paid_both)
chk("paid invoice recorded at amount_paid, not amount_due",
    _q1(amt_conn, "SELECT amount_cents FROM ledger WHERE transaction_id='evt_amt_paid'") == 3000)

# A genuinely unknown amount must dead-letter, never post as $0. Recording an
#     unknown amount as zero reconciles cleanly and is therefore invisible.
ev_no_amount = {
    "id": "evt_amt_none",
    "type": "invoice.paid",
    "data": {"object": {"customer": "cus_amt03", "currency": "usd"}},
}
chk("event with no amount field at all → DLQ_INVALID, never $0",
    L.process_stripe_event(amt_conn, ev_no_amount)["outcome"] == "DLQ_INVALID")
chk("no phantom $0 row was written",
    _q1(amt_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_amt_none'") == 0)

# Still zero after resolution → still a data-quality failure for invoice types.
ev_zero_due = {
    "id": "evt_amt_zero",
    "type": "invoice.payment_failed",
    "data": {"object": {"customer": "cus_amt04", "amount_paid": 0,
                        "amount_due": 0, "currency": "usd"}},
}
chk("failed invoice with amount_due=0 → DLQ_INVALID",
    L.process_stripe_event(amt_conn, ev_zero_due)["outcome"] == "DLQ_INVALID")

# Subscription lifecycle events carry no money and may legitimately be zero.
ev_sub_life = {
    "id": "evt_amt_sub",
    "type": "customer.subscription.updated",
    "data": {"object": {"customer": "cus_amt05", "amount": 0, "currency": "usd"}},
}
chk("subscription lifecycle event with amount=0 still accepted",
    L.process_stripe_event(amt_conn, ev_sub_life)["outcome"] == "PENDING")
amt_conn.close()


# =============================================================================
# A1: DLQ SELF-AMPLIFICATION (audit 2026-08-09)
# A retry must never create DLQ rows. The entry being replayed IS the DLQ record
#     for that payload; its status/retry_count columns carry each attempt's result.
#     Before the fix, retrying an entry whose event had already reached the ledger
#     appended a fresh DUPLICATE row and marked the original resolved — one more
#     pending row than it consumed, each itself retry-eligible. The queue amplified
#     itself and inflated the dlq_depth operators read to judge ingestion health.
# =============================================================================
print("\n── A1: retry does not amplify the DLQ (audit 2026-08-09) ───────────────")

amp_conn = fresh_conn()
ev_amp = fake_event(eid="evt_amp_001")

chk("1st delivery → POSTED",
    L.process_stripe_event(amp_conn, ev_amp)["outcome"] == "POSTED")
chk("Stripe redelivery → DLQ_DUPLICATE",
    L.process_stripe_event(amp_conn, ev_amp)["outcome"] == "DLQ_DUPLICATE")

_amp_count = lambda: _q1(amp_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_amp_001'")
chk("redelivery leaves exactly 1 DLQ row", _amp_count() == 1, f"got {_amp_count()}")

r_amp = L.process_stripe_event(amp_conn, ev_amp, from_retry=True)
chk("retry still reports DLQ_DUPLICATE", r_amp["outcome"] == "DLQ_DUPLICATE", f"got {r_amp}")
chk("retry appends no DLQ row (was 2 before fix)", _amp_count() == 1, f"got {_amp_count()}")

for _ in range(3):
    L.process_stripe_event(amp_conn, ev_amp, from_retry=True)
chk("4 retries still leave 1 DLQ row (was 5 before fix)", _amp_count() == 1, f"got {_amp_count()}")
chk("money still counted exactly once",
    _q1(amp_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_amp_001'") == 1)

# The same rule covers INVALID: replaying a malformed payload must not append rows either.
ev_amp_bad = fake_event(eid="evt_amp_bad", currency="WRONG")
L.process_stripe_event(amp_conn, ev_amp_bad)
_bad_count = lambda: _q1(amp_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_amp_bad'")
chk("invalid payload writes 1 DLQ row on ingest", _bad_count() == 1, f"got {_bad_count()}")
L.process_stripe_event(amp_conn, ev_amp_bad, from_retry=True)
L.process_stripe_event(amp_conn, ev_amp_bad, from_retry=True)
chk("invalid payload retries append no rows", _bad_count() == 1, f"got {_bad_count()}")
amp_conn.close()


# =============================================================================
# A2: 'resolved' IS TERMINAL (audit 2026-08-09)
# A resolved entry means the ledger already holds its event, so replaying it can
#     only produce DLQ_DUPLICATE. The endpoint used to accept it anyway, re-resolve
#     the row and append another DLQ row, letting one id be reopened without limit.
# =============================================================================
print("\n── A2: POST /dlq/{id}/retry rejects a resolved entry ───────────────────")

term_conn = fresh_conn()
original_pool_term = L._pool
L._pool = _MockPool(term_conn)
term_client = TestClient(L.app)

ev_term = fake_event(eid="evt_term_001")
L.process_stripe_event(term_conn, ev_term)
L.process_stripe_event(term_conn, ev_term)
term_dlq_id = _q1(term_conn, "SELECT id FROM dlq WHERE transaction_id='evt_term_001' ORDER BY id LIMIT 1")

r_t1 = term_client.post(f"/dlq/{term_dlq_id}/retry")
chk("1st manual retry → 200", r_t1.status_code == 200, f"got {r_t1.status_code}: {r_t1.text}")
chk("entry marked resolved",
    _q1(term_conn, "SELECT status FROM dlq WHERE id=%s", (term_dlq_id,)) == "resolved")

_term_count = lambda: _q1(term_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_term_001'")
chk("manual retry appends no DLQ row", _term_count() == 1, f"got {_term_count()}")

r_t2 = term_client.post(f"/dlq/{term_dlq_id}/retry")
chk("2nd manual retry on resolved → 409 (was 200)", r_t2.status_code == 409, f"got {r_t2.status_code}")
chk("409 body names the entry", str(term_dlq_id) in r_t2.text, f"got {r_t2.text}")
chk("rejected retry appends no DLQ row (was 3)", _term_count() == 1, f"got {_term_count()}")

# 'exhausted' must stay replayable — it is the operator's escape hatch.
L._write_dlq(term_conn, L.DLQEntry(transaction_id="evt_term_exh",
                                   reason=L.DLQReason.TRANSIENT,
                                   raw_payload=fake_event(eid="evt_term_exh")))
exh_id = _q1(term_conn, "SELECT id FROM dlq WHERE transaction_id='evt_term_exh' ORDER BY id DESC LIMIT 1")
with term_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("UPDATE dlq SET status='exhausted' WHERE id=%s", (exh_id,))
    c.execute("COMMIT")
chk("exhausted entry is still replayable (not 409)",
    term_client.post(f"/dlq/{exh_id}/retry").status_code == 200)

L._pool = original_pool_term
term_conn.close()


# =============================================================================
# A3: STALE RETRY CLAIMS ARE RECLAIMED (audit 2026-08-09)
# retry_dlq_batch commits the claim (status='retrying') before doing the work, so a
#     crash — or a failure inside the recovery UPDATE, likely because the outage that
#     caused the retry is still going — left rows claimed forever. The claim query only
#     selected status='pending', so nothing ever looked at them again: not pending, not
#     resolved, not exhausted, and invisible to the operator's own retry mechanism.
# =============================================================================
print("\n── A3: stranded 'retrying' rows recover automatically ──────────────────")

claim_conn = fresh_conn()
chk("dlq.claimed_at exists (migration 0003 / _bootstrap parity)",
    _q1(claim_conn,
        "SELECT COUNT(*) FROM information_schema.columns "
        "WHERE table_name='dlq' AND column_name='claimed_at'") == 1)

with claim_conn.cursor() as c:
    c.execute("BEGIN")
    for i in range(3):
        c.execute(
            "INSERT INTO dlq (transaction_id, reason, raw_payload, status, retry_count, max_retries) "
            "VALUES (%s, %s, %s, 'pending', 0, 3)",
            (f"evt_claim_{i}", "TRANSIENT", json.dumps({"id": f"evt_claim_{i}", "type": "bogus.type"})),
        )
    c.execute("COMMIT")

# Row 1's recovery fails on BOTH the normal call and the guarded retry inside the
#     except handler — the realistic case, and the only one that actually strands a
#     row. A single-failure stub recovers on the second call and would make the
#     reclaim assertions below pass without ever testing them.
_real_increment = L._increment_retry
_STRANDED = "evt_claim_1"

def _flaky_increment(c, dlq_id_, txid):
    if txid == _STRANDED:
        raise RuntimeError("simulated connection drop mid-recovery")
    return _real_increment(c, dlq_id_, txid)

L._increment_retry = _flaky_increment
try:
    attempted = L.retry_dlq_batch(claim_conn)
    batch_raised = None
except Exception as _exc:            # noqa: BLE001
    attempted, batch_raised = -1, _exc
finally:
    L._increment_retry = _real_increment

chk("batch does not propagate a recovery failure", batch_raised is None, f"raised {batch_raised!r}")
chk("batch attempts all 3 rows despite one failing", attempted == 3, f"got {attempted}")

stranded = [r[0] for r in _qall(claim_conn,
            "SELECT transaction_id FROM dlq WHERE status='retrying' ORDER BY id")]
chk("exactly the doubly-failed row is left claimed", stranded == [_STRANDED], f"got {stranded}")
chk("other rows still processed — one bad row does not abort the batch",
    _q1(claim_conn, "SELECT COUNT(*) FROM dlq WHERE status='pending' AND retry_count=1") == 2)

chk("a live claim is not stolen inside its timeout",
    L.retry_dlq_batch(claim_conn, claim_timeout_seconds=900) == 0)

chk("stale claim is reclaimed (was invisible forever)",
    L.retry_dlq_batch(claim_conn, claim_timeout_seconds=0) == 1)
chk("reclaimed row reaches a terminal state",
    _q1(claim_conn, "SELECT status FROM dlq WHERE transaction_id=%s", (_STRANDED,))
    in ("pending", "exhausted"))
chk("no row outside 'retrying' keeps a claim timestamp",
    _q1(claim_conn, "SELECT COUNT(*) FROM dlq WHERE status <> 'retrying' AND claimed_at IS NOT NULL") == 0)

# A row stranded before migration 0003 has claimed_at NULL — treat it as infinitely stale.
with claim_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("UPDATE dlq SET status='retrying', claimed_at=NULL WHERE transaction_id=%s", (_STRANDED,))
    c.execute("COMMIT")
chk("pre-migration claim (claimed_at NULL) is reclaimed",
    L.retry_dlq_batch(claim_conn, claim_timeout_seconds=900) == 1)
claim_conn.close()


# =============================================================================
# PHASE 12a: STRUCTURED JSON LOGGING
# Plain text is unparseable exactly when it matters. "Which transaction_id failed,
#     how often, how slow" becomes a regex over a human sentence that changes the
#     next time someone rewords a log line. One JSON object per line makes those
#     fields queryable, and consumers read keys rather than prose.
# =============================================================================
print("\n── Phase 12a: structured JSON logging ──────────────────────────────────")

_json_fmt = L.JsonLogFormatter()

_rec = logging.LogRecord(
    name="ledger", level=logging.INFO, pathname=__file__, lineno=1,
    msg="webhook processed %s", args=("POSTED",), exc_info=None,
)
_rec.transaction_id = "evt_json_001"
_rec.outcome = "POSTED"
_rec.duration_ms = 12.5
_parsed = json.loads(_json_fmt.format(_rec))

chk("log line is valid JSON",                    isinstance(_parsed, dict))
chk("message is interpolated, not a format string",
    _parsed["message"] == "webhook processed POSTED", f"got {_parsed['message']!r}")
chk("level is present",                          _parsed["level"] == "INFO")
chk("logger name is present",                    _parsed["logger"] == "ledger")
chk("timestamp is ISO-8601 with timezone",
    "T" in _parsed["ts"] and ("+" in _parsed["ts"] or _parsed["ts"].endswith("Z")),
    f"got {_parsed['ts']!r}")
chk("extra= transaction_id promoted to top level", _parsed["transaction_id"] == "evt_json_001")
chk("extra= outcome promoted to top level",        _parsed["outcome"] == "POSTED")
chk("extra= duration_ms promoted to top level",    _parsed["duration_ms"] == 12.5)

# One record must never span multiple lines: shippers split on newlines, so a
#     multi-line record arrives as several unparseable fragments.
_rec_multi = logging.LogRecord(
    name="ledger", level=logging.ERROR, pathname=__file__, lineno=1,
    msg="broke on\ntwo lines", args=(), exc_info=None,
)
chk("embedded newline does not split the record",
    "\n" not in L.JsonLogFormatter().format(_rec_multi))

try:
    raise ValueError("simulated failure")
except ValueError:
    _rec_exc = logging.LogRecord(
        name="ledger", level=logging.ERROR, pathname=__file__, lineno=1,
        msg="failed", args=(), exc_info=sys.exc_info(),
    )
_parsed_exc = json.loads(_json_fmt.format(_rec_exc))
chk("traceback is folded into one JSON string field",
    "ValueError" in _parsed_exc.get("exception", ""))
chk("record with traceback is still one line",
    "\n" not in _json_fmt.format(_rec_exc))

# A value json cannot serialize must degrade, not raise — losing the record to a
#     serialization error is strictly worse than logging its string form.
_rec_odd = logging.LogRecord(
    name="ledger", level=logging.INFO, pathname=__file__, lineno=1,
    msg="odd", args=(), exc_info=None,
)
_rec_odd.when = datetime(2026, 8, 9, 12, 0, 0)
chk("non-serializable extra degrades to string instead of raising",
    "2026-08-09" in json.loads(_json_fmt.format(_rec_odd))["when"])


# =============================================================================
# PHASE 12b: PROMETHEUS /metrics
# =============================================================================
print("\n── Phase 12b: Prometheus /metrics endpoint ─────────────────────────────")

metrics_conn = fresh_conn()
original_pool_metrics = L._pool
L._pool = _MockPool(metrics_conn)
metrics_client = TestClient(L.app)

with patch("stripe.Webhook.construct_event"):
    metrics_client.post("/webhook/stripe", json=fake_event(eid="evt_metrics_001"))
    metrics_client.post("/webhook/stripe", json=fake_event(eid="evt_metrics_001"))  # duplicate

r_metrics = metrics_client.get("/metrics")
chk("GET /metrics → 200", r_metrics.status_code == 200, f"got {r_metrics.status_code}")
chk("content type is Prometheus exposition",
    "text/plain" in r_metrics.headers.get("content-type", ""),
    r_metrics.headers.get("content-type", ""))

_body = r_metrics.text
for _metric in ("webhook_events_total", "http_requests_total",
                "http_request_duration_seconds", "dlq_depth", "outbox_pending"):
    chk(f"exposes {_metric}", _metric in _body)

chk("webhook outcomes are labelled, not collapsed",
    'webhook_events_total{outcome="POSTED"}' in _body, "POSTED label missing")
chk("duplicate outcome counted separately",
    'webhook_events_total{outcome="DLQ_DUPLICATE"}' in _body, "DLQ_DUPLICATE label missing")

# Gauges are read from the DB at scrape time, so they must match what the tables say.
_dlq_now = _q1(metrics_conn, "SELECT COUNT(*) FROM dlq")
chk("dlq_depth gauge matches the table at scrape time",
    f"dlq_depth {float(_dlq_now)}" in _body, f"expected {_dlq_now}")

# Route templates, never resolved paths: labelling by resolved path gives every
#     dlq_id its own time series and turns instrumentation into a cardinality bomb.
metrics_client.post("/dlq/999999/retry")
_body2 = metrics_client.get("/metrics").text
chk("histogram labels use the route template",
    '/dlq/{dlq_id}/retry' in _body2, "route template label missing")
chk("resolved id never becomes a label value",
    '/dlq/999999/retry' not in _body2, "resolved path leaked into labels")

L._pool = original_pool_metrics
metrics_conn.close()

# /metrics publishes dlq_depth and outbox_pending — the same operational figures as
#     /ledger/summary, which is key-protected. Leaving one door open is not a boundary.
auth_metrics_conn = fresh_conn()
original_pool_am = L._pool
L._pool = _MockPool(auth_metrics_conn)
L.BILLING_API_KEY = "test_secret_key"
try:
    am_client = TestClient(L.app)
    chk("/metrics without X-Api-Key → 401",
        am_client.get("/metrics").status_code == 401)
    chk("/metrics with correct X-Api-Key → 200",
        am_client.get("/metrics", headers={"X-Api-Key": "test_secret_key"}).status_code == 200)
finally:
    L.BILLING_API_KEY = ""
    L._pool = original_pool_am
    auth_metrics_conn.close()


# =============================================================================
# M7: close every connection opened via fresh_conn(). Closing an already-closed
#     connection is a no-op, so sections that closed their own connection are fine.
# =============================================================================
for _c in _open_conns:
    try:
        _c.close()
    except Exception:
        pass


# =============================================================================
# FINAL SUMMARY
# =============================================================================
print(f"\n{'='*60}")
print(f"  {ok} passed | {fail} failed")
if fail:
    sys.exit(1)
