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
import hashlib     # A10: hash API keys the way the org registry stores them
import logging     # Phase 12: build LogRecords to assert the JSON formatter's output
import os          # A6: manipulate env vars to exercise the *_FILE secret sources
import sys         # stdout override for Windows UTF-8 encoding
import time        # Used in benchmark timing
import threading   # Used for the concurrent idempotency test
from datetime import datetime
from unittest.mock import patch

# Force UTF-8 output on Windows. Without this, Unicode characters in print()
#     cause UnicodeEncodeError because Windows stdout defaults to CP-1252.
#     This must happen before any print() call — so it's at the top of the file.
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

# Refuse any database that is not disposable — before the TRUNCATE in
#     _reset_org_registry below, and before `import ledger` opens its pool. The default
#     used to be `billing`, the database `docker compose up` creates, so the documented
#     way to run this file erased the local ledger. See disposable_db.py.
import disposable_db  # noqa: E402
_TEST_DSN = disposable_db.require_disposable_database(
    "postgresql://postgres:postgres@localhost:5432/billing_test")

# Import the ledger module directly — all tests call its functions and use
#     its models. This is the module under test.
# Reset the org registry BEFORE importing ledger. ledger computes _ANY_ORG_KEYS once at
#     import time, and that single boolean decides between dev mode and enforced auth for
#     the whole process. A previous run that left a tenant key behind would flip this run
#     into enforced auth and 401 every unauthenticated call — the suite passed on a clean
#     database and failed on re-run. A test suite whose result depends on the last run is
#     not a test suite.
def _reset_org_registry() -> None:
    import psycopg2 as _pg
    try:
        _c = _pg.connect(_TEST_DSN)
        _c.autocommit = True
        with _c.cursor() as _cur:
            _cur.execute("SELECT to_regclass('org')")
            if _cur.fetchone()[0] is not None:
                _cur.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
                _cur.execute("DELETE FROM org WHERE org_id <> 'default'")
                _cur.execute("UPDATE org SET api_key_sha256 = NULL WHERE org_id = 'default'")
        _c.close()
    except Exception:  # noqa: BLE001 — a database without the org table predates 0004
        pass


_reset_org_registry()

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
#     No pytest, no unittest — intentional: the suite is one script that runs top to
#     bottom against a disposable database, with no framework to install or learn.
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
    Execute a query and return the first column of the first row, or None if there is
    no row. Used for COUNT(*) and similar single-value reads.

    None rather than an exception: a check whose row is missing must FAIL, not abort the
    run. `fetchone()[0]` raised TypeError there, so one regression (mutating C6-2's
    `request: Optional` made its row absent) killed the process mid-suite and every
    section after it reported nothing.
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        row = cur.fetchone()
        return row[0] if row else None


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
        # Tenants too: org rows are not truncated by the line above (ledger references
        #     them), so without this a section inherits the tenants another one created.
        cur.execute("DELETE FROM org WHERE org_id <> 'default'")
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
# Since C6-28 a well-formed event of a type the ledger does not record is UNSUPPORTED
#     (a resolved audit row), not INVALID; either way it never reaches the ledger.
chk("event type 'payment.created' → UNSUPPORTED, not ledgered",
    r["outcome"] == "UNSUPPORTED"
    and _q1(conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_bad_type'") == 0,
    f"got {r['outcome']}")


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
chk("unknown type → UNSUPPORTED (was DLQ_INVALID before C6-28)", r["outcome"] == "UNSUPPORTED",
    f"got {r['outcome']} instead of UNSUPPORTED")

dlq_count = _q1(conn2, "SELECT COUNT(*) FROM dlq")
chk("DLQ has 2 rows",                           dlq_count == 2)


print("\n── All supported event types route correctly ─────────────────────────")

conn3 = fresh_conn()
expected = {
    "invoice.paid":                   "POSTED",
    "invoice.payment_failed":         "VOID",
    "customer.subscription.created":  "LIFECYCLE",
    "customer.subscription.deleted":  "LIFECYCLE",
    "customer.subscription.updated":  "LIFECYCLE",
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
# org_id leads the tuple as of Phase 13 — column order must match the dlq INSERT.
chk("DLQEntry.to_db() is 9-tuple (org_id first)", len(db_row) == 9)
chk("to_db() org_id leads",                     db_row[0] == L.DEFAULT_ORG_ID)
chk("to_db() transaction_id follows org_id",    db_row[1] == "evt_test_001")
chk("to_db() reason is string not enum",        db_row[2] == "DUPLICATE")
chk("to_db() payload is JSON string",           db_row[3] == '{"id": "evt_test_001"}')
# received_at is now a timezone-aware datetime (TIMESTAMPTZ), not a float.
chk("to_db() received_at is datetime",          isinstance(db_row[4], datetime))

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
chk("LedgerEntry.to_db() is 10-tuple (org_id first)", len(db_row) == 10)
chk("to_db() org_id leads",                     db_row[0] == L.DEFAULT_ORG_ID)
chk("to_db() event_type is string not enum",    db_row[2] == "invoice.paid")
chk("to_db() status is string not enum",        db_row[6] == "POSTED")
chk("to_db() amount_cents is int",              db_row[4] == 4900)
# created_at is now a timezone-aware datetime (TIMESTAMPTZ), not a float.
chk("to_db() created_at is datetime",           isinstance(db_row[9], datetime))

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
chk("LedgerStatus.LIFECYCLE value",             L.LedgerStatus.LIFECYCLE.value == "LIFECYCLE")

conn6 = fresh_conn()
ev_failed = fake_event(eid="evt_failed_001", etype="invoice.payment_failed")
r = L.process_stripe_event(conn6, ev_failed)
chk("invoice.payment_failed → VOID",
    r["outcome"] == "VOID", f"got {r['outcome']}")

ev_sub_updated = fake_event(eid="evt_sub_upd", etype="customer.subscription.updated")
r = L.process_stripe_event(conn6, ev_sub_updated)
chk("subscription.updated → LIFECYCLE",
    r["outcome"] == "LIFECYCLE", f"got {r['outcome']}")


# =============================================================================
# PHASE 3: CROSS-FIELD VALIDATORS
# (Placed before Phase 5 so unit-level cross-field failures are verified
#  before the full HTTP integration stack is exercised.)
# =============================================================================

print("\n── Phase 3: Cross-field validator — a failed invoice needs an amount > 0 ")

conn7 = fresh_conn()

ev_zero_invoice = fake_event(eid="evt_zero_paid", etype="invoice.paid", amount=0)
r = L.process_stripe_event(conn7, ev_zero_invoice)
chk("invoice.paid with amount=0 → POSTED (a known zero: coupon, free item — 5f)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")
chk("…recorded at 0, not refused",
    _q1(conn7, "SELECT amount_cents FROM ledger WHERE transaction_id='evt_zero_paid'") == 0)

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
chk("subscription.created carrying amount_paid=0 → LIFECYCLE (no amount is read)",
    r["outcome"] == "LIFECYCLE", f"got {r['outcome']}")

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
    chk("$0 invoice.paid → POSTED via HTTP (5f)",   r.json()["outcome"] == "POSTED",
        f"got {r.json()}")

    ev_bad_cus = fake_event(eid="evt_http_bad_cus", customer="notacus_001")
    r = client.post("/webhook/stripe", json=ev_bad_cus)
    chk("POST bad customer prefix → HTTP 200",      r.status_code == 200)
    chk("bad prefix → DLQ_INVALID via HTTP",        r.json()["outcome"] == "DLQ_INVALID",
        f"got {r.json()}")

    # Summary endpoint — verify shape and counts.
    #     Posted above: evt_http_001, evt_http_up_curr (uppercase currency is
    #     normalized and accepted) and evt_http_zero ($0 invoice.paid, 5f) = 3 ledger
    #     rows, 3 outbox rows. DLQ: 1 DUPLICATE + 2 INVALID (malformed currency, bad
    #     prefix) = 3.
    r = client.get("/ledger/summary")
    chk("GET /ledger/summary → 200",                r.status_code == 200)
    summary = r.json()
    chk("summary has 'ledger' key",                 "ledger" in summary)
    chk("summary has 'dlq_depth' key",              "dlq_depth" in summary)
    chk("summary has 'outbox_pending' key",         "outbox_pending" in summary)
    chk("summary dlq_depth is 3",                   summary["dlq_depth"] == 3,
        f"got {summary['dlq_depth']}")
    chk("summary outbox_pending is 3",              summary["outbox_pending"] == 3,
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
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_2", etype="charge.refunded"))
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_3", customer=""))

rows = _qall(dlq_conn, "SELECT transaction_id, reason, raw_payload FROM dlq ORDER BY id")

chk("DLQ has 3 rows",                           len(rows) == 3,   f"got {len(rows)}")
chk("reasons are INVALID, UNSUPPORTED (charge.refunded, since C6-28), INVALID",
    [r[1] for r in rows] == ["INVALID", "UNSUPPORTED", "INVALID"], str([r[1] for r in rows]))
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

# Subscription lifecycle events carry no money. A stray amount field is not read.
ev_sub_life = {
    "id": "evt_amt_sub",
    "type": "customer.subscription.updated",
    "data": {"object": {"customer": "cus_amt05", "amount": 0, "currency": "usd"}},
}
chk("subscription lifecycle event with a stray amount=0 → LIFECYCLE",
    L.process_stripe_event(amt_conn, ev_sub_life)["outcome"] == "LIFECYCLE")
chk("…and is stored with no amount, not $0",
    _q1(amt_conn, "SELECT amount_cents IS NULL FROM ledger "
                  "WHERE transaction_id='evt_amt_sub'") is True)
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

def _amp_count():
    return _q1(amp_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_amp_001'")
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
def _bad_count():
    return _q1(amp_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_amp_bad'")
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

def _term_count():
    return _q1(term_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_term_001'")
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

# The payloads are an invoice.paid with no data: INVALID on every replay. A type the
#     ledger does not record would replay to UNSUPPORTED (C6-28), which settles the row.
with claim_conn.cursor() as c:
    c.execute("BEGIN")
    for i in range(3):
        c.execute(
            "INSERT INTO dlq (transaction_id, reason, raw_payload, status, retry_count, max_retries) "
            "VALUES (%s, %s, %s, 'pending', 0, 3)",
            (f"evt_claim_{i}", "TRANSIENT", json.dumps({"id": f"evt_claim_{i}", "type": "invoice.paid"})),
        )
    c.execute("COMMIT")

# Row 1's recovery fails on BOTH the normal call and the guarded retry inside the
#     except handler — the realistic case, and the only one that actually strands a
#     row. A single-failure stub recovers on the second call and would make the
#     reclaim assertions below pass without ever testing them.
_real_increment = L._increment_retry
_STRANDED = "evt_claim_1"

def _flaky_increment(c, dlq_id_, txid, org_scope):
    if txid == _STRANDED:
        raise RuntimeError("simulated connection drop mid-recovery")
    return _real_increment(c, dlq_id_, txid, org_scope)

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
# A4: 'resolved' IS TERMINAL AT EVERY WRITE SITE, NOT JUST THE ENDPOINT
# The 409 guards the manual endpoint. _increment_retry was the other door: it wrote
#     dlq.status with a bare WHERE id=%s and no check of the current value, so a
#     resolved entry could be pushed back to pending or exhausted. The trigger is
#     narrow but real — in retry_dlq_batch, process_stripe_event can succeed and the
#     following UPDATE ... status='resolved' can still fail transiently, after which
#     the except handler calls _increment_retry unconditionally.
# =============================================================================
print("\n── A4: _increment_retry cannot reopen a resolved entry ─────────────────")

term2_conn = fresh_conn()
with term2_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute(
        "INSERT INTO dlq (transaction_id, reason, raw_payload, status, retry_count, max_retries) "
        "VALUES ('evt_terminal_guard', 'TRANSIENT', '{}', 'resolved', 1, 3) RETURNING id"
    )
    _tid = c.fetchone()[0]
    c.execute("COMMIT")

L._increment_retry(term2_conn, _tid, "evt_terminal_guard", L.DEFAULT_ORG_ID)
_st, _rc = _qall(term2_conn, "SELECT status, retry_count FROM dlq WHERE id=%s", (_tid,))[0]
chk("resolved entry survives _increment_retry", _st == "resolved", f"got {_st!r}")
chk("resolved entry's retry_count is untouched", _rc == 1, f"got {_rc}")

# A non-resolved entry must still be incremented — the guard must not disable the mechanism.
with term2_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute(
        "INSERT INTO dlq (transaction_id, reason, raw_payload, status, retry_count, max_retries) "
        "VALUES ('evt_terminal_open', 'TRANSIENT', '{}', 'retrying', 0, 3) RETURNING id"
    )
    _oid = c.fetchone()[0]
    c.execute("COMMIT")
L._increment_retry(term2_conn, _oid, "evt_terminal_open", L.DEFAULT_ORG_ID)
_st2, _rc2 = _qall(term2_conn, "SELECT status, retry_count FROM dlq WHERE id=%s", (_oid,))[0]
chk("non-resolved entry still increments", _rc2 == 1 and _st2 == "pending", f"got {_st2!r}/{_rc2}")
term2_conn.close()


# =============================================================================
# A5: dlq_depth MEASURES BACKLOG, NOT HISTORY
# dlq_depth is what an operator reads to decide whether ingestion is failing. It
#     counted every row ever written, so a resolved INVALID or TRANSIENT entry
#     inflated it permanently — prune_dlq only ages out DUPLICATE rows. The number
#     grew with historical traffic and never came back down.
# =============================================================================
print("\n── A5: dlq_depth counts backlog; dlq_total keeps the audit count ───────")

depth_conn = fresh_conn()
original_pool_depth = L._pool
L._pool = _MockPool(depth_conn)
depth_client = TestClient(L.app)

with depth_conn.cursor() as c:
    c.execute("BEGIN")
    for _tid_s, _status in [("d1", "pending"), ("d2", "retrying"), ("d3", "exhausted"),
                            ("d4", "resolved"), ("d5", "resolved")]:
        c.execute(
            "INSERT INTO dlq (transaction_id, reason, raw_payload, status) VALUES (%s,'INVALID','{}',%s)",
            (_tid_s, _status),
        )
    c.execute("COMMIT")

_sum = depth_client.get("/ledger/summary").json()
chk("dlq_depth excludes resolved rows", _sum["dlq_depth"] == 3, f"got {_sum.get('dlq_depth')}")
chk("dlq_total still reports every row", _sum["dlq_total"] == 5, f"got {_sum.get('dlq_total')}")

_m = depth_client.get("/metrics").text
chk("dlq_depth gauge matches the backlog", "dlq_depth 3.0" in _m, "gauge disagrees with summary")
chk("dlq_total gauge exposed for audit", "dlq_total 5.0" in _m, "dlq_total missing")

L._pool = original_pool_depth
depth_conn.close()


# =============================================================================
# A6: SECRET PROVENANCE
# A container's environment is immutable for its lifetime, so a secret delivered by
#     env var cannot be rotated without replacing the container — and `docker compose
#     restart` does NOT re-read .env, it restarts the existing container with its
#     original environment. An operator who edits .env and restarts believes the new
#     secret is live when it is not, with no error, no log and no metric to say so.
#     Two answers: read secrets from a mounted file (which CAN change), and fingerprint
#     whatever is actually live so the mistake is visible.
# =============================================================================
print("\n── A6: secret provenance — *_FILE sources and fingerprints ─────────────")

import tempfile  # noqa: E402

chk("empty secret fingerprints as 'unset'", L._secret_fingerprint("") == "unset")
_fp = L._secret_fingerprint("whsec_abc123")
chk("fingerprint is 12 hex chars", len(_fp) == 12 and all(c in "0123456789abcdef" for c in _fp), _fp)
chk("fingerprint is deterministic", L._secret_fingerprint("whsec_abc123") == _fp)
chk("different secrets fingerprint differently", L._secret_fingerprint("whsec_xyz789") != _fp)
chk("fingerprint does not leak the secret",
    "whsec_abc123" not in _fp and "abc123" not in _fp, _fp)

_sec_dir = tempfile.mkdtemp()
_sec_path = os.path.join(_sec_dir, "api_key")
with io.open(_sec_path, "w", encoding="utf-8") as _fh:
    _fh.write("secret_from_file_value\n")   # trailing newline: how files are usually written

_saved = {k: os.environ.get(k) for k in ("A6_TEST_SECRET", "A6_TEST_SECRET_FILE")}
try:
    os.environ.pop("A6_TEST_SECRET", None)
    os.environ.pop("A6_TEST_SECRET_FILE", None)
    chk("falls back to the default when nothing is set",
        L._read_secret("A6_TEST_SECRET", "fallback") == "fallback")

    os.environ["A6_TEST_SECRET"] = "value_from_env"
    chk("reads the env var when no _FILE is set",
        L._read_secret("A6_TEST_SECRET", "fallback") == "value_from_env")

    os.environ["A6_TEST_SECRET_FILE"] = _sec_path
    chk("_FILE takes precedence over the env var",
        L._read_secret("A6_TEST_SECRET", "fallback") == "secret_from_file_value")
    chk("trailing newline is stripped from the file",
        "\n" not in L._read_secret("A6_TEST_SECRET", "fallback"))

    # The important one: a configured but unreadable _FILE must NOT fall back to the
    # env var. Falling back starts the service on the wrong credential and logs nothing.
    os.environ["A6_TEST_SECRET_FILE"] = os.path.join(_sec_dir, "does_not_exist")
    _raised = None
    try:
        L._read_secret("A6_TEST_SECRET", "fallback")
    except RuntimeError as _exc:
        _raised = _exc
    chk("unreadable _FILE raises instead of falling back", _raised is not None,
        "silently fell back to the env var")
    chk("the error names the variable and the path",
        _raised is not None and "A6_TEST_SECRET_FILE" in str(_raised),
        str(_raised))
finally:
    for _k, _v in _saved.items():
        if _v is None:
            os.environ.pop(_k, None)
        else:
            os.environ[_k] = _v

# Fingerprints must reach /metrics so a scraper can alert on an unexpected change —
#     or on the absence of a change after a rotation that was supposed to happen.
sec_conn = fresh_conn()
original_pool_sec = L._pool
L._pool = _MockPool(sec_conn)
sec_client = TestClient(L.app)
_sec_body = sec_client.get("/metrics").text
chk("billing_secrets_info is exposed", "billing_secrets_info" in _sec_body)
chk("the live stripe fingerprint appears",
    L._SECRET_FINGERPRINTS["stripe_webhook_secret"] in _sec_body)
chk("the secret source (env|file) is labelled",
    'stripe_secret_source="env"' in _sec_body or 'stripe_secret_source="file"' in _sec_body)
chk("no raw secret value is exposed in metrics",
    L.STRIPE_WEBHOOK_SECRET not in _sec_body, "raw secret leaked into /metrics")
L._pool = original_pool_sec
sec_conn.close()


# =============================================================================
# A9: TENANT ISOLATION AT THE IDEMPOTENCY GUARD (Phase 13)
# transaction_id IS the idempotency guard. Stripe event ids are unique within an
#     account, not across accounts, so two tenants can legitimately present the same
#     id. With transaction_id as the sole primary key the second tenant's event would
#     hit ON CONFLICT, be discarded as a duplicate and dead-lettered — silent revenue
#     loss, indistinguishable from the guard working correctly. The key is
#     (org_id, transaction_id) so the guard is per-tenant.
# =============================================================================
print("\n── A9: the idempotency guard is per-tenant, not global ─────────────────")

tenant_conn = fresh_conn()
with tenant_conn.cursor() as c:
    c.execute("BEGIN")
    for _o in ("org_alpha", "org_beta"):
        c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s) "
                  "ON CONFLICT (org_id) DO NOTHING", (_o, _o, "whsec_test"))
    c.execute("COMMIT")

chk("ledger primary key is (org_id, transaction_id)",
    _q1(tenant_conn,
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
        "WHERE conrelid='ledger'::regclass AND contype='p'") == "PRIMARY KEY (org_id, transaction_id)",
    str(_q1(tenant_conn, "SELECT pg_get_constraintdef(oid) FROM pg_constraint "
                         "WHERE conrelid='ledger'::regclass AND contype='p'")))

# The same event id, from two different tenants. Both are real money.
_shared_id = "evt_collision_001"
_ev_a = fake_event(eid=_shared_id, customer="cus_alpha01", amount=1100)
_ev_b = fake_event(eid=_shared_id, customer="cus_beta01",  amount=2200)

_r_a = L.process_stripe_event(tenant_conn, _ev_a, org_id="org_alpha")
_r_b = L.process_stripe_event(tenant_conn, _ev_b, org_id="org_beta")
chk("tenant A's event posts", _r_a["outcome"] == "POSTED", f"got {_r_a}")
chk("tenant B's identical id ALSO posts (was DLQ_DUPLICATE)",
    _r_b["outcome"] == "POSTED", f"got {_r_b}")
chk("both rows exist, one per tenant",
    _q1(tenant_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id=%s", (_shared_id,)) == 2)
chk("tenant A's amount is not tenant B's",
    _q1(tenant_conn, "SELECT amount_cents FROM ledger WHERE org_id='org_alpha' AND transaction_id=%s",
        (_shared_id,)) == 1100)
chk("tenant B kept its own amount",
    _q1(tenant_conn, "SELECT amount_cents FROM ledger WHERE org_id='org_beta' AND transaction_id=%s",
        (_shared_id,)) == 2200)

# Within one tenant the guard must still fire — isolation must not disable idempotency.
_r_a2 = L.process_stripe_event(tenant_conn, _ev_a, org_id="org_alpha")
chk("replay within the same tenant is still a duplicate",
    _r_a2["outcome"] == "DLQ_DUPLICATE", f"got {_r_a2}")
chk("no third ledger row appeared",
    _q1(tenant_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id=%s", (_shared_id,)) == 2)

# Outbox rows must land under the owning tenant, or downstream delivery crosses tenants.
chk("outbox rows carry the owning tenant",
    _qall(tenant_conn, "SELECT DISTINCT org_id FROM outbox WHERE transaction_id=%s ORDER BY org_id",
          (_shared_id,)) == [("org_alpha",), ("org_beta",)])
chk("the DLQ row from the replay belongs to tenant A only",
    _q1(tenant_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id=%s AND org_id='org_alpha'",
        (_shared_id,)) == 1)
chk("tenant B has no DLQ row",
    _q1(tenant_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id=%s AND org_id='org_beta'",
        (_shared_id,)) == 0)

# Batch path shares the guard and must isolate identically.
_batch_id = "evt_collision_batch"
L.process_stripe_event_batch(tenant_conn, [fake_event(eid=_batch_id, amount=500)], org_id="org_alpha")
_rb = L.process_stripe_event_batch(tenant_conn, [fake_event(eid=_batch_id, amount=900)], org_id="org_beta")
chk("batch path isolates tenants too", _rb[0]["outcome"] == "POSTED", f"got {_rb[0]}")
chk("batch wrote one row per tenant",
    _q1(tenant_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id=%s", (_batch_id,)) == 2)
tenant_conn.close()


# =============================================================================
# A10: THE ORG IS DERIVED FROM THE CREDENTIAL, AND SCOPES EVERY OPS QUERY
# The roadmap called for an X-Org-Id header validated against a shared key. That is
#     not an isolation boundary: anyone holding the key reads another tenant's data by
#     editing a header. Whoever presents the key IS the org. These tests assert a
#     tenant cannot see, or act on, another tenant's rows — including the retry
#     endpoint, which is a WRITE.
# =============================================================================
print("\n── A10: tenant isolation across every ops endpoint ─────────────────────")

iso_conn = fresh_conn()
_KEY_A, _KEY_B = "key_alpha_secret_value", "key_beta_secret_value"
with iso_conn.cursor() as c:
    c.execute("BEGIN")
    for _o, _k in (("org_a", _KEY_A), ("org_b", _KEY_B)):
        c.execute(
            "INSERT INTO org (org_id, name, api_key_sha256, stripe_webhook_secret) "
            "VALUES (%s,%s,%s,%s) ON CONFLICT (org_id) DO UPDATE SET api_key_sha256=EXCLUDED.api_key_sha256",
            (_o, _o, hashlib.sha256(_k.encode()).hexdigest(), "whsec_test"),
        )
    c.execute("COMMIT")

# Each tenant gets distinct data: A posts two events, B posts one.
L.process_stripe_event(iso_conn, fake_event(eid="evt_a1", amount=1000), org_id="org_a")
L.process_stripe_event(iso_conn, fake_event(eid="evt_a2", amount=2000), org_id="org_a")
L.process_stripe_event(iso_conn, fake_event(eid="evt_b1", amount=9000), org_id="org_b")
L.process_stripe_event(iso_conn, fake_event(eid="evt_a1", amount=1000), org_id="org_a")  # -> A's DLQ

original_pool_iso, original_key_iso, original_any_iso = L._pool, L.BILLING_API_KEY, L._ANY_ORG_KEYS
L._pool = _MockPool(iso_conn)
L.BILLING_API_KEY = ""
L._ANY_ORG_KEYS = True          # orgs now hold keys, so auth is enforced
iso_client = TestClient(L.app)
try:
    _hA, _hB = {"X-Api-Key": _KEY_A}, {"X-Api-Key": _KEY_B}

    chk("an unknown key is rejected",
        iso_client.get("/ledger/summary", headers={"X-Api-Key": "not_a_real_key"}).status_code == 401)
    chk("a missing key is rejected",
        iso_client.get("/ledger/summary").status_code == 401)

    _sa = iso_client.get("/ledger/summary", headers=_hA).json()
    _sb = iso_client.get("/ledger/summary", headers=_hB).json()
    chk("the response names the derived org, not a supplied one", _sa.get("org_id") == "org_a",
        str(_sa.get("org_id")))
    chk("tenant A sees only its 2 ledger rows", sum(_sa["ledger"].values()) == 2, str(_sa["ledger"]))
    chk("tenant B sees only its 1 ledger row",  sum(_sb["ledger"].values()) == 1, str(_sb["ledger"]))
    chk("tenant A sees its own DLQ row",  _sa["dlq_total"] == 1, str(_sa))
    chk("tenant B sees no DLQ rows",      _sb["dlq_total"] == 0, str(_sb))

    # A header cannot override the credential — that was the roadmap's flaw.
    _spoof = iso_client.get("/ledger/summary", headers={**_hB, "X-Org-Id": "org_a"}).json()
    chk("X-Org-Id cannot override the credential", _spoof.get("org_id") == "org_b",
        str(_spoof.get("org_id")))
    chk("the spoof attempt still sees only B's data", sum(_spoof["ledger"].values()) == 1)

    _ea = iso_client.get("/dlq/entries", headers=_hA).json()["entries"]
    _eb = iso_client.get("/dlq/entries", headers=_hB).json()["entries"]
    chk("/dlq/entries is scoped to the caller", len(_ea) == 1 and len(_eb) == 0,
        f"A={len(_ea)} B={len(_eb)}")

    # The retry endpoint is a WRITE. Another tenant's id must be indistinguishable
    #     from a nonexistent one, or the response becomes an existence oracle.
    _a_dlq_id = _ea[0]["id"]
    chk("tenant B cannot retry tenant A's DLQ entry (404, not 200)",
        iso_client.post(f"/dlq/{_a_dlq_id}/retry", headers=_hB).status_code == 404)
    chk("tenant A can retry its own entry",
        iso_client.post(f"/dlq/{_a_dlq_id}/retry", headers=_hA).status_code == 200)

    # /metrics is the operator's (item 7): a tenant key is authenticated but not
    #     authorised, so the answer is 403, and nothing of the exposition comes back.
    _ma = iso_client.get("/metrics", headers=_hA)
    chk("a tenant key cannot read /metrics (operator endpoint) → 403",
        _ma.status_code == 403 and "dlq_total" not in _ma.text, f"{_ma.status_code}")
finally:
    L._pool, L.BILLING_API_KEY, L._ANY_ORG_KEYS = original_pool_iso, original_key_iso, original_any_iso
    # Leave no credentials behind: they would flip the NEXT process into enforced auth.
    with iso_conn.cursor() as c:
        c.execute("BEGIN")
        c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
        c.execute("DELETE FROM org WHERE org_id <> 'default'")
        c.execute("COMMIT")
    iso_conn.close()


# =============================================================================
# A11: PER-TENANT INGEST (Phase 13.3)
# POST /webhook/stripe/{org_id} verifies the signature with THAT tenant's registered
#     secret. This is self-validating: a signature only checks out against the account
#     that produced the event, so the path segment is a routing hint rather than a
#     trusted claim — a caller cannot post into a tenant whose secret it does not hold.
#     An unknown org must be indistinguishable from a bad signature, or the endpoint
#     becomes an org enumerator for anyone on the internet.
# =============================================================================
print("\n── A11: per-tenant ingest, signature proves the tenant ─────────────────")

ing_conn = fresh_conn()
_SEC_A, _SEC_B = "whsec_alpha_signing", "whsec_beta_signing"
with ing_conn.cursor() as c:
    c.execute("BEGIN")
    for _o, _sec in (("ing_a", _SEC_A), ("ing_b", _SEC_B)):
        c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s) "
                  "ON CONFLICT (org_id) DO UPDATE SET stripe_webhook_secret=EXCLUDED.stripe_webhook_secret",
                  (_o, _o, _sec))
    c.execute("COMMIT")

original_pool_ing = L._pool
L._pool = _MockPool(ing_conn)
ing_client = TestClient(L.app)


def _signed(body_obj, secret):
    """Build the Stripe-Signature header the way Stripe does: HMAC over '<ts>.<body>'."""
    body = json.dumps(body_obj)
    ts = int(time.time())
    mac = hashlib.sha256  # named for clarity below
    digest = __import__("hmac").new(secret.encode(), f"{ts}.{body}".encode(), mac).hexdigest()
    return body, {"Content-Type": "application/json", "Stripe-Signature": f"t={ts},v1={digest}"}


try:
    _ev = fake_event(eid="evt_ing_001", amount=3300)

    # Correct tenant, correct secret.
    _b, _h = _signed(_ev, _SEC_A)
    _r = ing_client.post("/webhook/stripe/ing_a", content=_b, headers=_h)
    chk("signed for its own tenant → 200", _r.status_code == 200, f"got {_r.status_code}: {_r.text}")
    chk("outcome is POSTED", _r.json().get("outcome") == "POSTED", str(_r.json()))
    chk("the row landed under that tenant",
        _q1(ing_conn, "SELECT org_id FROM ledger WHERE transaction_id='evt_ing_001'") == "ing_a")

    # Tenant A's secret aimed at tenant B's path. The signature is the identity proof,
    #     so this must fail — otherwise the URL alone would let anyone write anywhere.
    _b2, _h2 = _signed(fake_event(eid="evt_ing_002"), _SEC_A)
    _r2 = ing_client.post("/webhook/stripe/ing_b", content=_b2, headers=_h2)
    chk("A's secret cannot post into B's tenant", _r2.status_code == 400, f"got {_r2.status_code}")
    chk("nothing was written for the rejected event",
        _q1(ing_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_ing_002'") == 0)

    # An unknown org must look exactly like a bad signature — no enumeration.
    _b3, _h3 = _signed(fake_event(eid="evt_ing_003"), _SEC_A)
    _r3 = ing_client.post("/webhook/stripe/does_not_exist", content=_b3, headers=_h3)
    _r4 = ing_client.post("/webhook/stripe/ing_a", content=_b3,
                          headers={**_h3, "Stripe-Signature": "t=1,v1=deadbeef"})
    chk("unknown org returns 400, not 404", _r3.status_code == 400, f"got {_r3.status_code}")
    chk("unknown org is indistinguishable from a bad signature",
        (_r3.status_code, _r3.json()) == (_r4.status_code, _r4.json()),
        f"unknown={_r3.json()} badsig={_r4.json()}")

    # Same event id from both tenants — the composite key must let both through.
    _shared = fake_event(eid="evt_ing_shared", amount=4400)
    _ba, _ha = _signed(_shared, _SEC_A)
    _bb, _hb = _signed(_shared, _SEC_B)
    chk("tenant A posts the shared id",
        ing_client.post("/webhook/stripe/ing_a", content=_ba, headers=_ha).json()["outcome"] == "POSTED")
    chk("tenant B posts the same id over HTTP too",
        ing_client.post("/webhook/stripe/ing_b", content=_bb, headers=_hb).json()["outcome"] == "POSTED")
    chk("both rows exist, one per tenant",
        _q1(ing_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_ing_shared'") == 2)

    # The legacy path still works and still routes to the default org.
    with patch("stripe.Webhook.construct_event"):
        _rl = ing_client.post("/webhook/stripe", json=fake_event(eid="evt_ing_legacy"))
    chk("legacy /webhook/stripe still returns 200", _rl.status_code == 200, f"got {_rl.status_code}")
    chk("legacy route writes to the default org",
        _q1(ing_conn, "SELECT org_id FROM ledger WHERE transaction_id='evt_ing_legacy'") == L.DEFAULT_ORG_ID)
finally:
    L._pool = original_pool_ing
    with ing_conn.cursor() as c:
        c.execute("BEGIN")
        c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
        c.execute("DELETE FROM org WHERE org_id <> 'default'")
        c.execute("COMMIT")
    ing_conn.close()


# =============================================================================
# A12: A REPLAY LANDS IN THE TENANT THAT OWNS THE ENTRY
# Both retry paths reprocess a stored raw_payload. If the replay does not carry the
#     DLQ row's own org_id it falls back to the default, so the ledger row is written
#     under the wrong tenant while the entry is marked 'resolved' — an entry asserting
#     that the event was reprocessed for an owner whose ledger is still empty. Revenue
#     credited to the wrong tenant, and a terminal state that is not true.
# =============================================================================
print("\n── A12: retry replays into the owning tenant, not the default ──────────")

rep_conn = fresh_conn()
with rep_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES ('rep_t','rep_t','whsec_x') "
              "ON CONFLICT (org_id) DO NOTHING")
    c.execute("COMMIT")

# A TRANSIENT entry owned by rep_t, holding a replayable payload.
_rep_ev = fake_event(eid="evt_replay_owner", amount=6600)
L._write_dlq(rep_conn, L.DLQEntry(org_id="rep_t", transaction_id="evt_replay_owner",
                                  reason=L.DLQReason.TRANSIENT, raw_payload=_rep_ev))

_attempted = L.retry_dlq_batch(rep_conn)
chk("the batch picked up the tenant's entry", _attempted == 1, f"got {_attempted}")
chk("the replayed row landed in the owning tenant, not 'default'",
    _q1(rep_conn, "SELECT org_id FROM ledger WHERE transaction_id='evt_replay_owner'") == "rep_t",
    str(_q1(rep_conn, "SELECT org_id FROM ledger WHERE transaction_id='evt_replay_owner'")))
chk("no row was written under the default org",
    _q1(rep_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_replay_owner' "
                  "AND org_id=%s", (L.DEFAULT_ORG_ID,)) == 0)
# 'resolved' promises the ledger row exists FOR THAT TENANT. Assert both together —
#     the bug produced a resolved entry whose owner had nothing.
chk("entry resolved AND the owner's ledger actually has the row",
    _q1(rep_conn, "SELECT status FROM dlq WHERE transaction_id='evt_replay_owner'") == "resolved"
    and _q1(rep_conn, "SELECT COUNT(*) FROM ledger WHERE org_id='rep_t' "
                      "AND transaction_id='evt_replay_owner'") == 1)

# Same through the manual endpoint.
original_pool_rep = L._pool
L._pool = _MockPool(rep_conn)
rep_client = TestClient(L.app)
try:
    _ev2 = fake_event(eid="evt_replay_manual", amount=7700)
    L._write_dlq(rep_conn, L.DLQEntry(org_id=L.DEFAULT_ORG_ID, transaction_id="evt_replay_manual",
                                      reason=L.DLQReason.TRANSIENT, raw_payload=_ev2))
    _mid = _q1(rep_conn, "SELECT id FROM dlq WHERE transaction_id='evt_replay_manual'")
    chk("manual retry → 200", rep_client.post(f"/dlq/{_mid}/retry").status_code == 200)
    chk("manual replay lands in the caller's tenant",
        _q1(rep_conn, "SELECT org_id FROM ledger WHERE transaction_id='evt_replay_manual'")
        == L.DEFAULT_ORG_ID)
finally:
    L._pool = original_pool_rep
    with rep_conn.cursor() as c:
        c.execute("BEGIN")
        c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
        c.execute("DELETE FROM org WHERE org_id <> 'default'")
        c.execute("COMMIT")
    rep_conn.close()


# =============================================================================
# A13: THE WORKER DELIVERS TO EACH TENANT'S OWN DESTINATION, FAIRLY (Phase 13.4)
# Isolating the database and then forwarding every tenant's events to one shared URL
#     is not isolation — it is the largest leak in the system, since the payload
#     carries customer ids and amounts. And a shared queue drained by `ORDER BY id`
#     lets one tenant with a large backlog own every batch, which turns a noisy
#     neighbour into an outage for the quiet ones.
# =============================================================================
print("\n── A13: per-tenant delivery and fair draining ──────────────────────────")

wrk_conn = fresh_conn()
with wrk_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret, downstream_url) "
              "VALUES ('wrk_a','wrk_a','whsec_x','https://a.example/hook') "
              "ON CONFLICT (org_id) DO UPDATE SET downstream_url=EXCLUDED.downstream_url")
    c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret, downstream_url) "
              "VALUES ('wrk_b','wrk_b','whsec_x','https://b.example/hook') "
              "ON CONFLICT (org_id) DO UPDATE SET downstream_url=EXCLUDED.downstream_url")
    c.execute("COMMIT")

# Tenant A floods the queue; tenant B enqueues two events afterwards. Under a global
#     ORDER BY id, B would wait behind all of A's rows.
for _i in range(30):
    L.process_stripe_event(wrk_conn, fake_event(eid=f"evt_wrk_a{_i}"), org_id="wrk_a")
for _i in range(2):
    L.process_stripe_event(wrk_conn, fake_event(eid=f"evt_wrk_b{_i}"), org_id="wrk_b")

_seen: list = []
_orig_dispatch = W._dispatch_row
W._dispatch_row = lambda row, destination=None: _seen.append((row["org_id"], destination))
_orig_batch = W.WORKER_BATCH_SIZE
W.WORKER_BATCH_SIZE = 10          # smaller than A's backlog, so fairness is observable
try:
    _n = W.drain_batch(wrk_conn)
    chk("the batch drained rows", _n == 10, f"got {_n}")

    _a_dest = {d for o, d in _seen if o == "wrk_a"}
    _b_dest = {d for o, d in _seen if o == "wrk_b"}
    chk("tenant A's events go to A's endpoint", _a_dest == {"https://a.example/hook"}, str(_a_dest))
    chk("tenant B's events go to B's endpoint", _b_dest == {"https://b.example/hook"}, str(_b_dest))
    chk("no event was sent to another tenant's endpoint",
        not (_a_dest & _b_dest), f"A={_a_dest} B={_b_dest}")

    # The fairness claim: B is not starved behind A's 30 rows.
    _b_count = sum(1 for o, _ in _seen if o == "wrk_b")
    chk("the quiet tenant is not starved by the noisy one", _b_count == 2,
        f"tenant B got {_b_count} of {len(_seen)} slots — a global ORDER BY id gives it 0")
finally:
    W._dispatch_row = _orig_dispatch
    W.WORKER_BATCH_SIZE = _orig_batch

# A tenant with no downstream_url falls back to the process-wide DOWNSTREAM_URL,
#     so single-operator deployments are unaffected by any of this.
_seen2: list = []
W._dispatch_row = lambda row, destination=None: _seen2.append(destination)
try:
    L.process_stripe_event(wrk_conn, fake_event(eid="evt_wrk_default"), org_id=L.DEFAULT_ORG_ID)
    W.drain_batch(wrk_conn)
    chk("a tenant without its own URL yields None, so the worker falls back",
        None in _seen2, str(_seen2))
finally:
    W._dispatch_row = _orig_dispatch
    with wrk_conn.cursor() as c:
        c.execute("BEGIN")
        c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
        c.execute("DELETE FROM org WHERE org_id <> 'default'")
        c.execute("COMMIT")
    wrk_conn.close()


# =============================================================================
# A14: BATCH-BORN DLQ ROWS CARRY THEIR OWN TENANT
# The batch path built DLQEntry without org_id, so every DLQ row it produced landed
#     under 'default' regardless of the tenant that sent the event. That is not a
#     labelling error: the retry paths trust dlq.org_id to decide where a replay
#     lands, so the default tenant could see another tenant's raw payload AND replay
#     it into its own ledger — the same Stripe event POSTED under two tenants, which
#     is double-counted revenue in the source of truth.
#     A9 covered ledger isolation on the batch path but never asserted dlq.org_id,
#     which is exactly how this survived.
# =============================================================================
print("\n── A14: batch DLQ rows belong to the sending tenant ────────────────────")

bt_conn = fresh_conn()
with bt_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES ('bt_t','bt_t','whsec_x') "
              "ON CONFLICT (org_id) DO NOTHING")
    c.execute("COMMIT")

# DUPLICATE via the batch path.
_bt_ev = fake_event(eid="evt_batch_dup_tenant", amount=500)
L.process_stripe_event_batch(bt_conn, [_bt_ev], org_id="bt_t")
_r_dup = L.process_stripe_event_batch(bt_conn, [_bt_ev], org_id="bt_t")
chk("second batch ingest is a duplicate", _r_dup[0]["outcome"] == "DLQ_DUPLICATE", str(_r_dup[0]))
chk("the DUPLICATE row belongs to the sending tenant, not 'default'",
    _q1(bt_conn, "SELECT org_id FROM dlq WHERE transaction_id='evt_batch_dup_tenant'") == "bt_t",
    str(_q1(bt_conn, "SELECT org_id FROM dlq WHERE transaction_id='evt_batch_dup_tenant'")))
chk("no batch DLQ row leaked into the default tenant",
    _q1(bt_conn, "SELECT COUNT(*) FROM dlq WHERE org_id=%s", (L.DEFAULT_ORG_ID,)) == 0)

# INVALID via the batch path.
_bt_bad = fake_event(eid="evt_batch_bad_tenant", currency="xx")
L.process_stripe_event_batch(bt_conn, [_bt_bad], org_id="bt_t")
chk("the INVALID row belongs to the sending tenant too",
    _q1(bt_conn, "SELECT org_id FROM dlq WHERE transaction_id='evt_batch_bad_tenant'") == "bt_t")

# The consequence the bug produced: replaying that row must not mint a second ledger
#     row under a different tenant for the same Stripe event.
_dup_id = _q1(bt_conn, "SELECT id FROM dlq WHERE transaction_id='evt_batch_dup_tenant'")
_row_org = _q1(bt_conn, "SELECT org_id FROM dlq WHERE id=%s", (_dup_id,))
L.process_stripe_event(bt_conn, _bt_ev, from_retry=True, org_id=_row_org)
chk("the event exists under exactly one tenant after a replay",
    _q1(bt_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_batch_dup_tenant'") == 1,
    "the same Stripe event is POSTED under more than one tenant")
chk("and that tenant is the sender",
    _q1(bt_conn, "SELECT org_id FROM ledger WHERE transaction_id='evt_batch_dup_tenant'") == "bt_t")

with bt_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
    c.execute("DELETE FROM org WHERE org_id <> 'default'")
    c.execute("COMMIT")
bt_conn.close()


# =============================================================================
# A15: ROW-LEVEL SECURITY — ISOLATION THE DATABASE ENFORCES (Phase 13.6)
# Every other tenancy test proves the application remembers its org_id predicate.
#     This one proves the guarantee survives forgetting it. A first attempt at this
#     shipped policies that were completely inert — the app connects as a SUPERUSER,
#     which bypasses RLS regardless of ENABLE or FORCE, and the whole suite passed.
#     So these tests connect as the RESTRICTED role on purpose; running them as the
#     owner would prove nothing.
# =============================================================================
print("\n── A15: row-level security enforced by PostgreSQL ──────────────────────")

import psycopg2 as _pg  # noqa: E402

# Default: the restricted role on the SAME disposable database the owner connection was
#     checked against. This used to hardcode .../billing, so a local run with only
#     DATABASE_URL set wrote A15's rows into the real database as billing_app.
from urllib.parse import urlsplit as _urlsplit  # noqa: E402
_owner = _urlsplit(_TEST_DSN)
_APP_DSN = os.environ.get("APP_DATABASE_URL", "").strip() or (
    f"postgresql://billing_app:billing_app@{_owner.hostname or 'localhost'}:"
    f"{_owner.port or 5432}{_owner.path}")

rls_owner = fresh_conn()
_rls_ready = _q1(rls_owner,
                 "SELECT COUNT(*) FROM pg_policies WHERE tablename='ledger'") == 1
chk("migration 0006 created the tenant_isolation policy", _rls_ready,
    "policy missing — did alembic upgrade head run?")

if _rls_ready:
    with rls_owner.cursor() as c:
        c.execute("BEGIN")
        for _t in ("rls_a", "rls_b"):
            c.execute("INSERT INTO org (org_id,name,stripe_webhook_secret) VALUES (%s,%s,'w') "
                      "ON CONFLICT (org_id) DO NOTHING", (_t, _t))
            c.execute("INSERT INTO ledger (org_id,transaction_id,event_type,customer_id,"
                      "amount_cents,currency,status,idempotency_key,payload) "
                      "VALUES (%s,%s,'invoice.paid','cus_x',100,'usd','POSTED','k','{}')",
                      (_t, f"evt_{_t}"))
        c.execute("COMMIT")

    _app = _pg.connect(_APP_DSN)
    try:
        with _app.cursor() as c:
            c.execute("SELECT usesuper FROM pg_user WHERE usename = current_user")
            _is_super = c.fetchone()[0]
        # If this fails, everything below is decorative — a superuser bypasses RLS.
        chk("the application role is NOT a superuser", _is_super is False,
            "a superuser bypasses RLS entirely; the policies would be inert")

        def _scoped(sql, scope=None):
            c = _app.cursor()
            c.execute("BEGIN")
            if scope is not None:
                c.execute("SET LOCAL app.org_id = %s", (scope,))
            c.execute(sql)
            rows = c.fetchall()
            c.execute("ROLLBACK")
            c.close()
            return rows

        chk("no scope set → no rows (default deny)",
            _scoped("SELECT org_id FROM ledger") == [], "unset must not mean unrestricted")
        chk("scoped to a tenant → only that tenant",
            _scoped("SELECT org_id FROM ledger", "rls_a") == [("rls_a",)])
        chk("ALL_ORGS sees every tenant",
            sorted(r[0] for r in _scoped("SELECT org_id FROM ledger", L.ALL_ORGS))
            == ["rls_a", "rls_b"])

        # THE test. The query does not mention org_id at all — exactly the mistake
        #     process_stripe_event_batch made — and the other tenant is still invisible.
        chk("a query that FORGETS its org_id predicate still cannot see another tenant",
            _scoped("SELECT org_id, transaction_id FROM ledger", "rls_a")
            == [("rls_a", "evt_rls_a")],
            "RLS is not enforcing — this is the whole point of Phase 13.6")

        # WITH CHECK: reading is only half of it. USING alone would still permit
        #     inserting a row that belongs to somebody else.
        c = _app.cursor()
        c.execute("BEGIN")
        c.execute("SET LOCAL app.org_id = 'rls_a'")
        _refused = False
        try:
            c.execute("INSERT INTO ledger (org_id,transaction_id,event_type,customer_id,"
                      "amount_cents,currency,status,idempotency_key,payload) "
                      "VALUES ('rls_b','evt_forged','invoice.paid','cus_x',1,'usd',"
                      "'POSTED','k','{}')")
        except _pg.Error:
            _refused = True
        finally:
            c.execute("ROLLBACK")
            c.close()
        chk("writing another tenant's row is refused by WITH CHECK", _refused,
            "USING without WITH CHECK would let this through")

        # The DLQ is where the 2026-08-11 leak actually landed, so assert it too.
        chk("dlq is covered by a policy",
            _q1(rls_owner, "SELECT COUNT(*) FROM pg_policies WHERE tablename='dlq'") == 1)
        chk("outbox is covered by a policy",
            _q1(rls_owner, "SELECT COUNT(*) FROM pg_policies WHERE tablename='outbox'") == 1)
    finally:
        _app.close()

with rls_owner.cursor() as c:
    c.execute("BEGIN")
    c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
    c.execute("DELETE FROM org WHERE org_id <> 'default'")
    c.execute("COMMIT")
rls_owner.close()


# =============================================================================
# A16: THE APPLICATION'S OWN PATHS RUN UNDER ENFORCED RLS (Phase 13.7)
# A15 proved the policies bite, using hand-written SQL. That says nothing about whether
#     the APPLICATION establishes a scope at each of its own query sites, and 13.6 shipped
#     with it establishing one at none of them: the service connected as `postgres` and
#     every policy was inert.
#
#     Under default-deny a forgotten scope is not a leak — it is a service that sees
#     nothing and cannot write. So these tests drive the real endpoints, the real core
#     functions and the real worker drain over a billing_app connection. There is no
#     partial credit here and that is the point: any one of the sites losing its scope
#     turns some assertion below into zero rows or a policy violation.
# =============================================================================
print("\n── A16: the service's own paths under enforced RLS ─────────────────────")

app_owner = fresh_conn()          # provisioning and cleanup need rights billing_app lacks
_KEY_P, _KEY_Q = "key_rls_p_secret_value", "key_rls_q_secret_value"
with app_owner.cursor() as c:
    c.execute("BEGIN")
    for _o, _k in (("rlsapp_p", _KEY_P), ("rlsapp_q", _KEY_Q)):
        c.execute(
            "INSERT INTO org (org_id, name, api_key_sha256, stripe_webhook_secret) "
            "VALUES (%s,%s,%s,'whsec_test') ON CONFLICT (org_id) DO UPDATE "
            "SET api_key_sha256 = EXCLUDED.api_key_sha256",
            (_o, _o, hashlib.sha256(_k.encode()).hexdigest()),
        )
    c.execute("COMMIT")

app_conn = None
try:
    app_conn = _pg.connect(_APP_DSN)
    app_conn.autocommit = True    # _tx issues an explicit BEGIN, as on every connection
except _pg.Error as _exc:         # noqa: BLE001
    chk("the service can connect as the restricted role", False, str(_exc))

if app_conn is not None:
    original_pool_app = L._pool
    original_any_app  = L._ANY_ORG_KEYS
    L._pool = _MockPool(app_conn)
    L._ANY_ORG_KEYS = True
    app_client = TestClient(L.app)
    try:
        # The startup self-check, run against the connection the service would actually
        #     use. This is the acceptance criterion for the whole phase: with the pool on
        #     billing_app it must report enforced, which is the difference between the
        #     rls_enforced and rls_inert log lines.
        _status = L._rls_status()
        chk("_rls_status reports enforced on an application connection",
            _status["enforced"] is True, str(_status))
        chk("_rls_status names the restricted role", _status["db_user"] == "billing_app",
            str(_status.get("db_user")))

        # ── the write path ────────────────────────────────────────────────────
        # An unscoped INSERT under default-deny raises "new row violates row-level
        #     security policy" rather than returning a bad answer, so these outcomes
        #     are only reachable if _tx carried the tenant.
        _wp = L.process_stripe_event(app_conn, fake_event(eid="evt_rls_p1", amount=1000),
                                     org_id="rlsapp_p")
        chk("process_stripe_event writes under RLS", _wp["outcome"] == "POSTED", str(_wp))
        L.process_stripe_event(app_conn, fake_event(eid="evt_rls_p2", amount=2000),
                               org_id="rlsapp_p")
        L.process_stripe_event(app_conn, fake_event(eid="evt_rls_q1", amount=9000),
                               org_id="rlsapp_q")
        # A replay of p1 — routes to P's DLQ, exercising _write_dlq's entry.org_id scope.
        _wd = L.process_stripe_event(app_conn, fake_event(eid="evt_rls_p1", amount=1000),
                                     org_id="rlsapp_p")
        chk("the DLQ append path writes under RLS", _wd["outcome"] == "DLQ_DUPLICATE", str(_wd))

        # The batch path is where the 2026-08-11 wrong-tenant write happened. Its INVALID
        #     branch builds a DLQ row directly, so it gets asserted rather than assumed.
        _batch = L.process_stripe_event_batch(
            app_conn,
            [fake_event(eid="evt_rls_q2", amount=500), {"id": "evt_rls_bad", "type": "invoice.paid"}],
            org_id="rlsapp_q",
        )
        chk("the batch path writes under RLS",
            [r["outcome"] for r in _batch] == ["POSTED", "DLQ_INVALID"],
            str([r["outcome"] for r in _batch]))

        # ── the read paths, through the real endpoints ────────────────────────
        _hP, _hQ = {"X-Api-Key": _KEY_P}, {"X-Api-Key": _KEY_Q}
        _sp = app_client.get("/ledger/summary", headers=_hP).json()
        _sq = app_client.get("/ledger/summary", headers=_hQ).json()
        chk("/ledger/summary answers under RLS and is scoped to P",
            sum(_sp["ledger"].values()) == 2, str(_sp))
        chk("/ledger/summary is scoped to Q", sum(_sq["ledger"].values()) == 2, str(_sq))
        chk("P's DLQ row is visible to P",  _sp["dlq_total"] == 1, str(_sp))
        chk("Q's DLQ row is visible to Q",  _sq["dlq_total"] == 1, str(_sq))
        chk("outbox counts survive the scope change",
            _sp["outbox_pending"] == 2 and _sq["outbox_pending"] == 2,
            f"P={_sp['outbox_pending']} Q={_sq['outbox_pending']}")

        _ep = app_client.get("/dlq/entries", headers=_hP).json()["entries"]
        _eq = app_client.get("/dlq/entries", headers=_hQ).json()["entries"]
        chk("/dlq/entries answers under RLS, one row each",
            len(_ep) == 1 and len(_eq) == 1, f"P={len(_ep)} Q={len(_eq)}")
        chk("/metrics refuses a tenant key under RLS too (403)",
            app_client.get("/metrics", headers=_hP).status_code == 403)
        # The operator read spans tenants (ALL_ORGS). Under RLS that only works if the
        #     policy honours the all-orgs scope for billing_app; a default-deny miss
        #     would report 0 here, not an error.
        _orig_key_rlsm = L.BILLING_API_KEY
        L.BILLING_API_KEY = "operator_key_for_rls_metrics_check_0123"
        try:
            _mo = app_client.get("/metrics", headers={"X-Api-Key": L.BILLING_API_KEY})
            chk("/metrics answers the operator under RLS, across both tenants",
                _mo.status_code == 200 and "dlq_total 2.0" in _mo.text,
                f"{_mo.status_code} " + " ".join(
                    _ln for _ln in _mo.text.splitlines() if _ln.startswith("dlq_total")))
        finally:
            L.BILLING_API_KEY = _orig_key_rlsm

        # ── the write endpoint, and the cross-tenant refusal ──────────────────
        chk("Q cannot retry P's DLQ entry",
            app_client.post(f"/dlq/{_ep[0]['id']}/retry", headers=_hQ).status_code == 404)
        chk("P can retry its own entry",
            app_client.post(f"/dlq/{_ep[0]['id']}/retry", headers=_hP).status_code == 200)

        # ── the cross-tenant jobs: ALL_ORGS has to work from this role too ────
        chk("prune_dlq runs under RLS as the application role",
            L.prune_dlq(app_conn, retention_days=90) == 0)
        chk("retry_dlq_batch runs under RLS as the application role",
            L.retry_dlq_batch(app_conn) == 0)

        # The worker holds its own connection and drains every tenant in one query.
        #     Scoped to a single org this returns fewer rows than were enqueued.
        _drained = W.drain_batch(app_conn)
        chk("the worker drains every tenant under RLS", _drained == 4, f"drained {_drained}")
        chk("nothing is left undispatched",
            _q1(app_owner, "SELECT COUNT(*) FROM outbox WHERE dispatched=0") == 0)
    except _pg.Error as _rls_exc:    # noqa: BLE001
        # A site that lost its scope does not return a wrong answer here — it raises
        #     "new row violates row-level security policy" or reads nothing. Turning that
        #     into a FAIL rather than letting it abort the process keeps the remaining
        #     ~100 sections running and puts the offending statement in the output, which
        #     is the whole diagnostic when 16 sites must all be right at once.
        chk("every application query site carries an RLS scope", False, repr(_rls_exc))
    finally:
        L._pool, L._ANY_ORG_KEYS = original_pool_app, original_any_app
        app_conn.close()

with app_owner.cursor() as c:
    c.execute("BEGIN")
    c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
    c.execute("DELETE FROM org WHERE org_id <> 'default'")
    c.execute("COMMIT")
app_owner.close()


# =============================================================================
# A17: THE DEFAULT TENANT'S SIGNING SECRET STILL FOLLOWS ROTATION (Phase 13.7)
# _bootstrap() did two unrelated jobs under one name: create the tables, and sync the
#     default org's stripe_webhook_secret from STRIPE_WEBHOOK_SECRET. 13.7 stopped the
#     API from calling it — billing_app cannot issue DDL and holds SELECT-only on the
#     registry — and very nearly dropped the second job with the first.
#
#     Nothing would have failed loudly. Migration 0004 seeds the literal
#     'whsec_YOUR_SECRET_HERE' under ON CONFLICT DO NOTHING and alembic re-runs nothing,
#     so a fresh deploy would verify every legacy /webhook/stripe request against a
#     placeholder and a rotation would never reach the registry — surfacing as Stripe
#     apparently sending bad signatures, which points at the sender rather than the row.
#     provision.py now does it from the migrate service, which holds the owner credential.
# =============================================================================
print("\n── A17: rotation still reaches the default tenant's registry row ───────")

import provision as P  # noqa: E402

prov_conn = fresh_conn()
with prov_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("UPDATE org SET stripe_webhook_secret='whsec_stale_value' WHERE org_id='default'")
    c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) "
              "VALUES ('prov_other','prov_other','whsec_other_tenant') "
              "ON CONFLICT (org_id) DO NOTHING")
    c.execute("COMMIT")

P.sync_default_org_secret(prov_conn, "whsec_rotated_value")
chk("a rotated secret overwrites the stale registry row",
    _q1(prov_conn, "SELECT stripe_webhook_secret FROM org WHERE org_id='default'")
    == "whsec_rotated_value",
    "DO NOTHING here would leave the legacy route verifying against the old secret")
chk("other tenants' secrets are untouched",
    _q1(prov_conn, "SELECT stripe_webhook_secret FROM org WHERE org_id='prov_other'")
    == "whsec_other_tenant")

# The reason this lives in provision.py and not in the service: the role that serves
#     traffic must not be able to rewrite the registry that decides who a tenant is.
_prov_refused = False
try:
    _prov_app = _pg.connect(_APP_DSN)
    _prov_app.autocommit = True
    try:
        with _prov_app.cursor() as c:
            c.execute("UPDATE org SET stripe_webhook_secret='whsec_seized' WHERE org_id='default'")
    except _pg.Error:
        _prov_refused = True
    finally:
        _prov_app.close()
except _pg.Error as _exc:   # noqa: BLE001
    chk("can connect as the restricted role for the registry check", False, str(_exc))
chk("the application role cannot rewrite the org registry", _prov_refused,
    "billing_app holds SELECT only — a service that can rewrite the registry can "
    "reassign a tenant's signing secret to itself")

with prov_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("DELETE FROM org WHERE org_id <> 'default'")
    c.execute("UPDATE org SET stripe_webhook_secret=%s WHERE org_id='default'",
              (L.STRIPE_WEBHOOK_SECRET,))
    c.execute("COMMIT")
prov_conn.close()


# =============================================================================
# A18: /metrics IS NOT A CROSS-TENANT READ
# History: the SQL behind /metrics was scoped by org_id, but the gauges are process-wide
#     objects and generate_latest() renders whatever they hold, so two tenants scraping at
#     once could be served each other's dlq_depth. A lock made set-then-render atomic.
#     That fixed the gauges and left the rest: webhook and HTTP counters summed over every
#     tenant, and billing_secrets_info — the operator's secret fingerprints — in every
#     tenant's response.
# Item 7 (Cycle 6) settles the model instead: /metrics is the operator's endpoint. A
#     tenant key gets 403 and none of the exposition; the operator key gets every tenant's
#     backlog. The forced-interleave check that lived here is gone with the property it
#     guarded: no tenant reads these gauges any more, so there is nothing to interleave.
#
# This section also exercises the REAL ThreadedConnectionPool. Every other TestClient in
#     this suite substitutes _MockPool, so the pool the application actually constructs at
#     import — and its connection settings — would otherwise have no coverage at all.
# =============================================================================
print("\n── A18: /metrics is not a cross-tenant read ────────────────────────────")

chk("the real pool hands out autocommit connections",
    L._pool.getconn().autocommit is True,
    "_tx issues a literal BEGIN and documents that it relies on autocommit; without it "
    "psycopg2 opens an implicit transaction first and PostgreSQL warns on every request")

met_owner = fresh_conn()
_KEY_M, _KEY_N = "key_metrics_m_value", "key_metrics_n_value"
_KEY_OP = "key_metrics_operator_value_0123456789"
_DEPTH_M, _DEPTH_N = 2, 9
with met_owner.cursor() as c:
    c.execute("BEGIN")
    for _o, _k, _n in (("met_m", _KEY_M, _DEPTH_M), ("met_n", _KEY_N, _DEPTH_N)):
        c.execute("INSERT INTO org (org_id,name,api_key_sha256,stripe_webhook_secret) "
                  "VALUES (%s,%s,%s,'whsec_test') ON CONFLICT (org_id) DO UPDATE "
                  "SET api_key_sha256 = EXCLUDED.api_key_sha256",
                  (_o, _o, hashlib.sha256(_k.encode()).hexdigest()))
        for _i in range(_n):
            c.execute("INSERT INTO dlq (org_id,transaction_id,reason,raw_payload,status,"
                      "retry_count,max_retries) VALUES (%s,%s,'INVALID','{}','pending',0,0)",
                      (_o, f"evt_met_{_o}_{_i}"))
    c.execute("COMMIT")

original_any_met, original_key_met = L._ANY_ORG_KEYS, L.BILLING_API_KEY
L._ANY_ORG_KEYS, L.BILLING_API_KEY = True, _KEY_OP
met_client = TestClient(L.app)      # NO _MockPool — this drives the real pool on purpose


def _depth_line(text: str) -> str:
    for _line in text.splitlines():
        if _line.startswith("dlq_depth "):
            return _line.split()[1]
    return "absent"


try:
    for _label, _k in (("M", _KEY_M), ("N", _KEY_N)):
        _r = met_client.get("/metrics", headers={"X-Api-Key": _k})
        chk(f"tenant {_label}'s key → 403 on /metrics",
            _r.status_code == 403, f"got {_r.status_code}")
        chk(f"tenant {_label} receives no exposition at all (no depth, no secret fingerprints)",
            "dlq_depth" not in _r.text and "billing_secrets" not in _r.text, _r.text[:120])
    chk("no key → 401", met_client.get("/metrics").status_code == 401)
    chk("an unknown key → 401, not 403 (it authenticates nobody)",
        met_client.get("/metrics", headers={"X-Api-Key": "nobody_holds_this"}).status_code == 401)

    _ro = met_client.get("/metrics", headers={"X-Api-Key": _KEY_OP})
    chk("the operator key → 200", _ro.status_code == 200, f"got {_ro.status_code}")
    chk("the operator sees every tenant's backlog",
        _depth_line(_ro.text) == f"{float(_DEPTH_M + _DEPTH_N)}",
        f"dlq_depth={_depth_line(_ro.text)}, expected {_DEPTH_M + _DEPTH_N}")
    chk("billing_secrets_info is still exposed — to the operator",
        "billing_secrets_info" in _ro.text)
    chk("/ledger/summary stays per-tenant for a tenant key",
        met_client.get("/ledger/summary", headers={"X-Api-Key": _KEY_M}).json().get("dlq_depth")
        == _DEPTH_M)
finally:
    L._ANY_ORG_KEYS, L.BILLING_API_KEY = original_any_met, original_key_met
    with met_owner.cursor() as c:
        c.execute("BEGIN")
        c.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")
        c.execute("DELETE FROM org WHERE org_id <> 'default'")
        c.execute("COMMIT")
    met_owner.close()


# =============================================================================
# A7: CONNECTION POOL RETAINS ITS CONNECTIONS
# psycopg2 returns a connection to the idle pool only while len(self._pool) < minconn;
#     every other release calls conn.close(). With minconn=1 the pool retained exactly
#     ONE connection, so under concurrency each request opened a fresh PostgreSQL
#     connection and closed it on release — and since _connect() runs inside getconn
#     while the pool lock is held, that serialized every caller.
#     Measured before the fix: 1 TPS at 4 concurrent clients, p50 4.2 SECONDS.
#     After: 61 TPS at 4, 143 TPS at 16, p50 under 100ms throughout.
# =============================================================================
print("\n── A7: the connection pool actually pools ──────────────────────────────")

chk("pool retains connections (minconn == maxconn)",
    L.DB_POOL_SIZE >= 1 and L._pool.minconn == L._pool.maxconn,
    f"minconn={L._pool.minconn} maxconn={L._pool.maxconn} — with minconn < maxconn, "
    f"psycopg2 closes every connection beyond the first on release")
chk("pool size is configurable", isinstance(L.DB_POOL_SIZE, int) and L.DB_POOL_SIZE > 0)

# A connection released and re-acquired must be the SAME object. If it is not, the pool
#     closed it and opened another — which is the defect above, not pooling.
_c1 = L._pool.getconn()
_id1 = id(_c1)
L._pool.putconn(_c1)
_c2 = L._pool.getconn()
chk("a released connection comes back, it is not reopened", id(_c2) == _id1,
    "pool discarded the connection instead of retaining it")
chk("the reused connection is still open", not _c2.closed)
L._pool.putconn(_c2)


# =============================================================================
# A8: RATE LIMITS ARE CONFIGURABLE, DEFAULTS UNCHANGED
# A load test cannot measure the ingest path while the limiter caps it at 100/minute.
#     Making the limits configurable is what allowed the pool defect above to be found
#     at all — but the defaults must stay exactly what they were.
# =============================================================================
print("\n── A8: rate limits configurable, defaults preserved ────────────────────")

chk("webhook default is still 100/minute", L.RATE_LIMIT_WEBHOOK == "100/minute",
    f"got {L.RATE_LIMIT_WEBHOOK!r}")
chk("ops default is still 20/minute", L.RATE_LIMIT_OPS == "20/minute", f"got {L.RATE_LIMIT_OPS!r}")
chk("metrics default is still 60/minute", L.RATE_LIMIT_METRICS == "60/minute",
    f"got {L.RATE_LIMIT_METRICS!r}")


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

# .get() throughout: a missing key must report FAIL, not raise a KeyError that
#     aborts every remaining section of the suite.
chk("log line is valid JSON",                    isinstance(_parsed, dict))
chk("message is interpolated, not a format string",
    _parsed.get("message") == "webhook processed POSTED", f"got {_parsed.get('message')!r}")
chk("level is present",                          _parsed.get("level") == "INFO")
chk("logger name is present",                    _parsed.get("logger") == "ledger")
chk("timestamp is ISO-8601 with timezone",
    "T" in _parsed.get("ts", "") and ("+" in _parsed.get("ts", "") or _parsed.get("ts", "").endswith("Z")),
    f"got {_parsed.get('ts')!r}")
chk("extra= transaction_id promoted to top level",
    _parsed.get("transaction_id") == "evt_json_001", f"got {_parsed.get('transaction_id')!r}")
chk("extra= outcome promoted to top level",
    _parsed.get("outcome") == "POSTED", f"got {_parsed.get('outcome')!r}")
chk("extra= duration_ms promoted to top level",
    _parsed.get("duration_ms") == 12.5, f"got {_parsed.get('duration_ms')!r}")

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
    "2026-08-09" in str(json.loads(_json_fmt.format(_rec_odd)).get("when", "")))


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
#     Compare against the ACTIONABLE count, not COUNT(*). The two coincide here because
#     nothing in this section is resolved yet, and an assertion that passes only by that
#     coincidence stops testing anything the moment a resolved row appears.
_dlq_now = _q1(metrics_conn,
               "SELECT COUNT(*) FROM dlq WHERE status IN ('pending','retrying','exhausted')")
_dlq_all = _q1(metrics_conn, "SELECT COUNT(*) FROM dlq")
chk("dlq_depth gauge matches the backlog at scrape time",
    f"dlq_depth {float(_dlq_now)}" in _body, f"expected {_dlq_now}")
chk("dlq_total gauge matches every row at scrape time",
    f"dlq_total {float(_dlq_all)}" in _body, f"expected {_dlq_all}")

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
# C6-1: the suite and the benchmark refuse a database that is not disposable
#     Both TRUNCATE ledger, outbox and dlq, and both used to default to `billing` — the
#     database `docker compose up` creates — so following the README erased it. The
#     scripts are run for real here, in a subprocess, against an address nothing listens
#     on. A refusal exits 2. If the guard ever stops firing, the child instead crashes on
#     connect and exits 1, so a missing guard cannot pass these checks by accident.
# =============================================================================
print("\n── C6-1: destructive tooling refuses a database that is not disposable ──")

import subprocess  # noqa: E402

chk("plain `billing` is not disposable",
    disposable_db.refusal("postgresql://u:p@localhost:5432/billing") != "")
chk("billing_test is disposable",
    disposable_db.refusal("postgresql://u:p@localhost:5432/billing_test") == "")
chk("billing_bench is disposable",
    disposable_db.refusal("postgres://u:p@db:5432/billing_bench") == "")
chk("a dbname= parameter cannot route a disposable-looking URI to a real database",
    disposable_db.refusal("postgresql://u:p@localhost/billing_test?dbname=billing") != "")
chk("a URI that names no database is refused",
    disposable_db.refusal("postgresql://u:p@localhost:5432") != "")
chk("a key=value DSN is refused rather than half-parsed",
    disposable_db.refusal("host=localhost dbname=billing_test") != "")
chk("the refusal never echoes the password",
    "s3cret" not in disposable_db.refusal("postgresql://u:s3cret@localhost/billing"))

_here = os.path.dirname(os.path.abspath(__file__))
_live_dsn  = "postgresql://nobody:s3cret@127.0.0.1:1/billing"        # port 1: nothing listens
_throwaway = "postgresql://nobody:s3cret@127.0.0.1:1/billing_test"


def _run_script(script: str, **env: str) -> "subprocess.CompletedProcess":
    return subprocess.run(
        [sys.executable, os.path.join(_here, script)],
        env={**os.environ, "PYTHONIOENCODING": "utf-8", **env},
        capture_output=True, text=True, encoding="utf-8", timeout=120, cwd=_here)


for _script in ("test_ledger.py", "benchmark.py"):
    _p = _run_script(_script, DATABASE_URL=_live_dsn, APP_DATABASE_URL="")
    chk(f"{_script} refuses DATABASE_URL=.../billing with exit 2",
        _p.returncode == disposable_db.GUARD_EXIT_CODE,
        f"exit {_p.returncode}: {_p.stderr[-300:]}")
    chk(f"{_script} names the database it refused",
        "'billing'" in _p.stderr, _p.stderr[-300:])
    chk(f"{_script} never prints the DSN's password",
        "s3cret" not in _p.stderr + _p.stdout)

# The restricted-role connection writes rows too, so it is checked on its own: an
#     environment with the owner DSN fixed and APP_DATABASE_URL forgotten must still stop.
_p = _run_script("test_ledger.py", DATABASE_URL=_throwaway, APP_DATABASE_URL=_live_dsn)
chk("APP_DATABASE_URL on a real database is refused even when DATABASE_URL is disposable",
    _p.returncode == disposable_db.GUARD_EXIT_CODE and "APP_DATABASE_URL" in _p.stderr,
    f"exit {_p.returncode}: {_p.stderr[-300:]}")


# =============================================================================
# C6-2: the request shapes Stripe sends for events no API call caused
#     Stripe documents Event.request as nullable, and an automatic event — a renewal,
#     a scheduled payment retry, Stripe's own subscription handling — arrives as
#     "request": {"id": null, "idempotency_key": null}. The first shape was a 422 before
#     the signature check; the second a 500 after it. Neither reached the ledger or the
#     DLQ, and Stripe retried each one for three days. Signed for real, not patched: the
#     500 came from a code path a stubbed construct_event would still reach, but the 422
#     came from body parsing, and only a genuine request shows both.
# =============================================================================
print("\n── C6-2: automatic Stripe events (null request, null idempotency key) ──")

c62_conn = fresh_conn()
_SEC_C62 = "whsec_c62_signing_secret"
with c62_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
              ("c62", "c62", _SEC_C62))
    c.execute("COMMIT")

original_pool_c62 = L._pool
L._pool = _MockPool(c62_conn)
# raise_server_exceptions=False: the failure this section guards against was an unhandled
#     exception. The default client re-raises it into the test instead of returning the 500
#     Stripe actually received, and the check would report a crash rather than a status.
c62_client = TestClient(L.app, raise_server_exceptions=False)


def _post_c62(event: dict):
    _body, _hdrs = _signed(event, _SEC_C62)
    return c62_client.post("/webhook/stripe/c62", content=_body, headers=_hdrs)


def _stored_key(eid: str):
    return _q1(c62_conn, "SELECT idempotency_key FROM ledger "
                         "WHERE org_id = 'c62' AND transaction_id = %s", (eid,))


try:
    _ev_null_req = fake_event(eid="evt_c62_null_request", include_request=False)
    _ev_null_req["request"] = None
    _r = _post_c62(_ev_null_req)
    chk("request: null → 200, not 422", _r.status_code == 200,
        f"got {_r.status_code}: {_r.text[:200]}")
    chk("request: null → POSTED",
        _r.status_code == 200 and _r.json().get("outcome") == "POSTED", _r.text[:200])
    chk("request: null → keyed by the event id",
        _stored_key("evt_c62_null_request") == "evt_c62_null_request")

    _ev_auto = fake_event(eid="evt_c62_automatic", include_request=False)
    _ev_auto["request"] = {"id": None, "idempotency_key": None}
    _r = _post_c62(_ev_auto)
    chk("request.idempotency_key: null → 200, not 500", _r.status_code == 200,
        f"got {_r.status_code}: {_r.text[:200]}")
    chk("request.idempotency_key: null → POSTED",
        _r.status_code == 200 and _r.json().get("outcome") == "POSTED", _r.text[:200])
    chk("request.idempotency_key: null → keyed by the event id",
        _stored_key("evt_c62_automatic") == "evt_c62_automatic")

    # The fallback is for a MISSING key only. One Stripe did send is still the one stored.
    _r = _post_c62(fake_event(eid="evt_c62_keyed"))
    chk("a non-null idempotency key is still the one stored",
        _r.status_code == 200 and _stored_key("evt_c62_keyed") == "idem_evt_c62_keyed",
        f"got {_r.status_code}, stored {_stored_key('evt_c62_keyed')!r}")

    # Redelivery of an automatic event is still a duplicate, not a second row. The
    #     ledger key is the event id either way; this proves the new path did not loosen it.
    _r = _post_c62(_ev_auto)
    chk("redelivered automatic event → DLQ_DUPLICATE",
        _r.status_code == 200 and _r.json().get("outcome") == "DLQ_DUPLICATE", _r.text[:200])
    chk("redelivery left exactly one ledger row",
        _q1(c62_conn, "SELECT COUNT(*) FROM ledger "
                      "WHERE org_id = 'c62' AND transaction_id = 'evt_c62_automatic'") == 1)

    # Function level, the path /dlq/{id}/retry replays through: same rule, no HTTP model.
    _fn = L.process_stripe_event(
        c62_conn, {**fake_event(eid="evt_c62_fn", include_request=False),
                   "request": {"id": None, "idempotency_key": None}}, org_id="c62")
    chk("process_stripe_event accepts a null idempotency key",
        _fn.get("outcome") == "POSTED", str(_fn))
finally:
    L._pool = original_pool_c62
    c62_conn.close()


# =============================================================================
# C6-3: demo credentials cannot serve traffic outside a declared, loopback-only dev run
#     Every default in docker-compose.yml was a working credential — the placeholder
#     signing secret VERIFIED signatures (the code comment claimed it rejected them), the
#     default API key opened the ops endpoints — and the same file published Caddy on
#     0.0.0.0. The rules are tested one at a time through the pure function, then the
#     whole decision end to end through the application lifespan, which is where uvicorn
#     runs it before accepting a connection.
# =============================================================================
print("\n── C6-3: demo credentials refused outside a loopback dev run ───────────")

_STRONG_SECRET = "whsec_" + "a" * 32
_STRONG_KEY    = "k" * 40
_STRONG_DSN    = "postgresql://billing_app:" + "p" * 32 + "@db:5432/billing"


def _problems(**over):
    args = dict(webhook_secret=_STRONG_SECRET, api_key=_STRONG_KEY, app_dsn=_STRONG_DSN,
                any_org_keys=False, placeholder_orgs=[])
    args.update(over)
    return L._demo_credential_problems(**args)


chk("loopback: 127.0.0.1", L._is_loopback("127.0.0.1"))
chk("loopback: ::1 and [::1]", L._is_loopback("::1") and L._is_loopback("[::1]"))
chk("loopback: localhost", L._is_loopback("localhost"))
chk("not loopback: 0.0.0.0", not L._is_loopback("0.0.0.0"))
chk("not loopback: a LAN address", not L._is_loopback("192.168.1.20"))
chk("not loopback: empty or a hostname", not L._is_loopback("") and not L._is_loopback("example.com"))

chk("dev + loopback allows demo credentials", L._demo_credentials_allowed("dev", "127.0.0.1"))
chk("unset BILLING_ENV does not", not L._demo_credentials_allowed("", "127.0.0.1"))
chk("production does not", not L._demo_credentials_allowed("production", "127.0.0.1"))
chk("dev with a published ingress does not", not L._demo_credentials_allowed("dev", "0.0.0.0"))

chk("strong credentials raise nothing", _problems() == [], str(_problems()))
chk("placeholder signing secret is a problem",
    any("STRIPE_WEBHOOK_SECRET" in p for p in _problems(webhook_secret=L._PLACEHOLDER_SECRET)))
chk("empty signing secret is a problem", _problems(webhook_secret="") != [])
chk("a non-whsec_ signing secret is a problem", _problems(webhook_secret="x" * 40) != [])
chk("placeholder left in the org registry is a problem, naming the org",
    any("acme" in p for p in _problems(placeholder_orgs=["acme"])))
chk("no API key anywhere is a problem", _problems(api_key="") != [])
chk("no BILLING_API_KEY is fine once tenants hold keys",
    _problems(api_key="", any_org_keys=True) == [])
chk("the published default API key is a problem",
    _problems(api_key="dev_key_change_in_production") != [])
chk("a short API key is a problem", _problems(api_key="short") != [])
chk("billing_app's bootstrap password is a problem",
    _problems(app_dsn="postgresql://billing_app:billing_app@db/billing") != [])
# The short key is a canary, not a word: a key of "short" matched "shorter than 32
#     characters" in the reason itself, and the check failed on English, not on a leak.
_CANARY_KEY = "Kq7_canary_9Zx"
chk("no reason ever echoes a credential",
    all(v not in " ".join(_problems(webhook_secret=L._PLACEHOLDER_SECRET, api_key=_CANARY_KEY))
        for v in (L._PLACEHOLDER_SECRET, _CANARY_KEY)))

# The registry read, against the real table. Inactive orgs are excluded: their secret
#     cannot verify anything, because _org_webhook_secret refuses them first.
c63_conn = fresh_conn()
with c63_conn.cursor() as c:
    c.execute("BEGIN")
    c.execute("UPDATE org SET stripe_webhook_secret = %s WHERE org_id = 'default'",
              (_STRONG_SECRET,))
    c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret, active) VALUES "
              "('c63_live', 'c63', %s, TRUE), ('c63_gone', 'c63', %s, FALSE), "
              "('c63_ok', 'c63', %s, TRUE)",
              (L._PLACEHOLDER_SECRET, L._PLACEHOLDER_SECRET, _STRONG_SECRET))
    c.execute("COMMIT")
chk("registry scan finds the active org on the placeholder, and only it",
    L._orgs_with_placeholder_secret(c63_conn) == ["c63_live"],
    str(L._orgs_with_placeholder_secret(c63_conn)))

# End to end through the lifespan. `with TestClient(app)` runs startup exactly as
#     uvicorn does; a raise there is uvicorn's "Application startup failed. Exiting."
_saved_c63 = {k: getattr(L, k) for k in ("BILLING_ENV", "INGRESS_BIND_ADDR", "STRIPE_WEBHOOK_SECRET",
                                         "BILLING_API_KEY", "APP_DATABASE_URL", "_ANY_ORG_KEYS", "_pool")}
L._pool = _MockPool(c63_conn)


def _starts() -> str:
    """'started' if the app came up, else the refusal text."""
    try:
        with TestClient(L.app) as _c:
            return "started" if _c.get("/health").status_code == 200 else "unhealthy"
    except RuntimeError as exc:
        return str(exc)


try:
    L.STRIPE_WEBHOOK_SECRET = L._PLACEHOLDER_SECRET
    L.BILLING_API_KEY = "dev_key_change_in_production"
    L.APP_DATABASE_URL = _STRONG_DSN
    L._ANY_ORG_KEYS = False

    L.BILLING_ENV, L.INGRESS_BIND_ADDR = "", "127.0.0.1"
    _out = _starts()
    chk("demo credentials with BILLING_ENV unset → refuses to start",
        _out.startswith("refusing to start"), _out[:200])
    chk("the refusal names what to fix", "STRIPE_WEBHOOK_SECRET" in _out and "c63_live" in _out,
        _out[:300])

    L.BILLING_ENV, L.INGRESS_BIND_ADDR = "dev", "0.0.0.0"
    _out = _starts()
    chk("demo credentials, dev, ingress published on 0.0.0.0 → refuses to start",
        _out.startswith("refusing to start"), _out[:200])

    L.BILLING_ENV, L.INGRESS_BIND_ADDR = "dev", "127.0.0.1"
    chk("demo credentials, dev, loopback ingress → starts", _starts() == "started")

    with c63_conn.cursor() as c:
        c.execute("UPDATE org SET stripe_webhook_secret = %s WHERE org_id = 'c63_live'",
                  (_STRONG_SECRET,))
    L.STRIPE_WEBHOOK_SECRET, L.BILLING_API_KEY = _STRONG_SECRET, _STRONG_KEY
    L.BILLING_ENV, L.INGRESS_BIND_ADDR = "", "0.0.0.0"
    _out = _starts()
    chk("real credentials, strict, published ingress → starts", _out == "started", _out[:200])
finally:
    for _k, _v in _saved_c63.items():
        setattr(L, _k, _v)
    c63_conn.close()


# =============================================================================
# C6-4: real Stripe subscription events are ledgered with no amount (item 5d)
#     A Stripe Subscription object has no top-level amount — prices live in
#     items.data[].price. The fixtures above invented one, so the suite stayed green
#     while every real customer.subscription.* event failed validation and landed in the
#     DLQ. The events below are Stripe's documented objects (fixtures/stripe/), sent as
#     raw bytes, signed, the way Stripe delivers them.
# =============================================================================
print("\n── C6-4: subscription events are lifecycle rows, not money ─────────────")

import psycopg2.errors  # noqa: E402

_FIXTURE_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "fixtures", "stripe")
_SUB_TYPES = ("customer.subscription.created", "customer.subscription.updated",
              "customer.subscription.deleted")


def _fixture(event_type: str, new_id: str = "") -> dict:
    """A Stripe fixture as a dict; new_id gives it a fresh event id for a second use."""
    with open(os.path.join(_FIXTURE_DIR, event_type + ".json"), "rb") as _f:
        _ev = json.loads(_f.read())
    if new_id:
        _ev["id"] = new_id
    return _ev


def _amount_status(conn, eid: str, org: str = L.DEFAULT_ORG_ID):
    with conn.cursor() as _c:
        _c.execute("SELECT amount_cents, status FROM ledger WHERE org_id=%s AND transaction_id=%s",
                   (org, eid))
        return _c.fetchone()


c64_conn = fresh_conn()

try:
    # The premise, checked rather than assumed: the fixtures carry no top-level amount.
    for _t in _SUB_TYPES:
        _keys = set(_fixture(_t)["data"]["object"])
        chk(f"{_t} fixture has no top-level amount field, as Stripe sends it",
            not ({"amount", "amount_paid", "amount_due", "total"} & _keys),
            str(sorted({"amount", "amount_paid", "amount_due", "total"} & _keys)))

    _dlq_before = _q1(c64_conn, "SELECT COUNT(*) FROM dlq")
    for _t in _SUB_TYPES:
        _ev = _fixture(_t)
        _r = L.process_stripe_event(c64_conn, _ev)
        chk(f"{_t} → LIFECYCLE (every real one was DLQ_INVALID)",
            _r["outcome"] == "LIFECYCLE", str(_r))
        chk(f"{_t} is stored with amount_cents NULL, never 0",
            _amount_status(c64_conn, _ev["id"]) == (None, "LIFECYCLE"),
            str(_amount_status(c64_conn, _ev["id"])))
        chk(f"{_t} still reaches the outbox (downstream acts on plan changes)",
            _q1(c64_conn, "SELECT COUNT(*) FROM outbox WHERE transaction_id=%s",
                (_ev["id"],)) == 1)
    chk("no DLQ row for any of them — dlq_depth is no longer inflated",
        _q1(c64_conn, "SELECT COUNT(*) FROM dlq") == _dlq_before)

    # Invoices, from the same documented shapes, keep their money.
    _paid = _fixture("invoice.paid")
    chk("real invoice.paid → POSTED", L.process_stripe_event(c64_conn, _paid)["outcome"] == "POSTED")
    chk("…at amount_paid", _amount_status(c64_conn, _paid["id"]) == (1000, "POSTED"),
        str(_amount_status(c64_conn, _paid["id"])))
    _failed = _fixture("invoice.payment_failed")
    chk("real invoice.payment_failed → VOID",
        L.process_stripe_event(c64_conn, _failed)["outcome"] == "VOID")
    chk("…at amount_due, not amount_paid=0",
        _amount_status(c64_conn, _failed["id"]) == (1000, "VOID"),
        str(_amount_status(c64_conn, _failed["id"])))

    # An amount field on a subscription object is never read. Posting it would put a
    #     figure in the ledger that no invoice collected.
    _stray = _fixture("customer.subscription.updated", "evt_c64_stray_amount")
    _stray["data"]["object"]["amount"] = 4900
    chk("subscription event with a stray amount → LIFECYCLE",
        L.process_stripe_event(c64_conn, _stray)["outcome"] == "LIFECYCLE")
    chk("…and the stray 4900 is not stored",
        _amount_status(c64_conn, "evt_c64_stray_amount") == (None, "LIFECYCLE"),
        str(_amount_status(c64_conn, "evt_c64_stray_amount")))

    # An invoice with no amount is still dead-lettered: the exemption is for
    #     subscriptions only, not a general relaxation.
    _no_amt = _fixture("invoice.paid", "evt_c64_invoice_no_amount")
    for _k in ("amount_paid", "amount_due", "amount"):
        _no_amt["data"]["object"].pop(_k, None)
    chk("invoice with no amount field → DLQ_INVALID, still",
        L.process_stripe_event(c64_conn, _no_amt)["outcome"] == "DLQ_INVALID")

    # Batch path: same rule, one transaction.
    _batch = [_fixture(_t, f"evt_c64_batch_{_i}") for _i, _t in enumerate(_SUB_TYPES)]
    _batch.append(_fixture("invoice.paid", "evt_c64_batch_inv"))
    _res = L.process_stripe_event_batch(c64_conn, _batch)
    chk("batch: three subscriptions + one invoice → LIFECYCLE ×3, POSTED",
        [_x["outcome"] for _x in _res] == ["LIFECYCLE", "LIFECYCLE", "LIFECYCLE", "POSTED"],
        str([_x["outcome"] for _x in _res]))
    chk("batch: subscription rows carry no amount",
        _q1(c64_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id LIKE 'evt_c64_batch_%%' "
                      "AND amount_cents IS NULL") == 3)

    # The database holds the same line on its own (migration 0007), so a future code
    #     path that forgets the rule fails loudly instead of writing a phantom amount.
    chk("constraint ledger_amount_matches_kind exists",
        _q1(c64_conn, "SELECT COUNT(*) FROM pg_constraint "
                      "WHERE conname = 'ledger_amount_matches_kind'") == 1)
    for _label, _etype, _amt in (
        ("a subscription row WITH an amount", "customer.subscription.created", 500),
        ("an invoice row WITHOUT an amount",  "invoice.paid", None),
    ):
        _rejected = False
        try:
            with c64_conn.cursor() as _c:
                _c.execute(
                    "INSERT INTO ledger (org_id, transaction_id, event_type, customer_id, "
                    "amount_cents, currency, status, idempotency_key, payload) "
                    "VALUES ('default', %s, %s, 'cus_c64', %s, 'usd', 'POSTED', 'k', '{}')",
                    (f"evt_c64_chk_{_etype}", _etype, _amt))
        except psycopg2.errors.CheckViolation:
            _rejected = True
        chk(f"database rejects {_label}", _rejected)

    # The backlog. Before this fix every real subscription event was dead-lettered, so a
    #     deployment that takes it holds DLQ entries an operator will replay. A replay
    #     that writes the ledger row must also settle the entry; the two retry paths kept
    #     their own literal list of settling outcomes, and LIFECYCLE was in neither.
    def _seed_dlq(eid: str, max_retries: int) -> int:
        with c64_conn.cursor() as _c:
            _c.execute(
                "INSERT INTO dlq (org_id, transaction_id, reason, raw_payload, received_at, "
                "status, retry_count, max_retries) "
                "VALUES ('default', %s, 'INVALID', %s, NOW(), 'pending', 0, %s) RETURNING id",
                (eid, json.dumps(_fixture("customer.subscription.created", eid)), max_retries))
            return _c.fetchone()[0]

    _bid = _seed_dlq("evt_c64_backlog_batch", 3)
    L.retry_dlq_batch(c64_conn)
    chk("retry_dlq_batch: a pre-fix subscription entry is resolved on the first replay",
        _q1(c64_conn, "SELECT status FROM dlq WHERE id=%s", (_bid,)) == "resolved",
        str(_q1(c64_conn, "SELECT status, retry_count FROM dlq WHERE id=%s", (_bid,))))
    chk("retry_dlq_batch: …and the event is in the ledger with no amount",
        _amount_status(c64_conn, "evt_c64_backlog_batch") == (None, "LIFECYCLE"))

    _mid = _seed_dlq("evt_c64_backlog_manual", 0)
    original_pool_c64 = L._pool
    L._pool = _MockPool(c64_conn)
    try:
        _c64_client = TestClient(L.app, raise_server_exceptions=False)
        _r = _c64_client.post(f"/dlq/{_mid}/retry")
        chk("POST /dlq/{id}/retry on a pre-fix subscription entry → LIFECYCLE",
            _r.status_code == 200 and _r.json().get("outcome") == "LIFECYCLE",
            f"{_r.status_code} {_r.text[:200]}")
        chk("…and the entry is resolved, not left retrying against a written row",
            _q1(c64_conn, "SELECT status FROM dlq WHERE id=%s", (_mid,)) == "resolved",
            str(_q1(c64_conn, "SELECT status FROM dlq WHERE id=%s", (_mid,))))

        # Through the signed HTTP path, raw bytes as Stripe sends them.
        _SEC_C64 = "whsec_c64_signing_secret"
        with c64_conn.cursor() as _c:
            _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
                       ("c64", "c64", _SEC_C64))
        for _t in _SUB_TYPES:
            _body = json.dumps(_fixture(_t, f"evt_c64_http_{_t.rsplit('.', 1)[1]}"))
            _ts = int(time.time())
            _sig = __import__("hmac").new(_SEC_C64.encode(), f"{_ts}.{_body}".encode(),
                                          hashlib.sha256).hexdigest()
            _r = _c64_client.post("/webhook/stripe/c64", content=_body,
                                  headers={"Content-Type": "application/json",
                                           "Stripe-Signature": f"t={_ts},v1={_sig}"})
            chk(f"signed {_t} over HTTP → 200 LIFECYCLE",
                _r.status_code == 200 and _r.json().get("outcome") == "LIFECYCLE",
                f"{_r.status_code} {_r.text[:200]}")

        _sum = _c64_client.get("/ledger/summary").json()
        chk("/ledger/summary counts LIFECYCLE rows as their own status",
            _sum.get("ledger", {}).get("LIFECYCLE", 0) >= 3, str(_sum.get("ledger")))
    finally:
        L._pool = original_pool_c64
finally:
    c64_conn.close()


# =============================================================================
# C6-5: the stored payload is the event Stripe signed, every field of it (item 5c)
#     The webhook handler passed payload.model_dump() to the ledger: a four-field model
#     that drops everything else, so created, livemode, api_version, object and
#     pending_webhooks never reached the ledger, the outbox or the DLQ — while the docs
#     said the raw payload was preserved byte-perfect.
# =============================================================================
print("\n── C6-5: every field Stripe sent reaches the ledger, outbox and DLQ ────")

import hmac  # noqa: E402


def _post_raw(client, path: str, body: str, secret: str):
    """POST `body` exactly as given, signed the way Stripe signs: HMAC over '<ts>.<body>'."""
    _ts = int(time.time())
    _sig = hmac.new(secret.encode(), f"{_ts}.{body}".encode(), hashlib.sha256).hexdigest()
    return client.post(path, content=body, headers={"Content-Type": "application/json",
                                                    "Stripe-Signature": f"t={_ts},v1={_sig}"})


def _stored_json(conn, sql: str, params: tuple) -> dict:
    _v = _q1(conn, sql, params)
    return json.loads(_v) if _v else {}


c65_conn = fresh_conn()
_SEC_C65 = "whsec_c65_signing_secret"
with c65_conn.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
               ("c65", "c65", _SEC_C65))
original_pool_c65 = L._pool
L._pool = _MockPool(c65_conn)
try:
    _c65_client = TestClient(L.app, raise_server_exceptions=False)
    _ev = _fixture("invoice.paid", "evt_c65_full")
    _r = _post_raw(_c65_client, "/webhook/stripe/c65", json.dumps(_ev), _SEC_C65)
    chk("signed real invoice.paid → 200 POSTED",
        _r.status_code == 200 and _r.json().get("outcome") == "POSTED", f"{_r.status_code} {_r.text[:200]}")

    _led = _stored_json(c65_conn, "SELECT payload FROM ledger WHERE org_id='c65' "
                                  "AND transaction_id=%s", ("evt_c65_full",))
    _env = ("created", "livemode", "api_version", "object", "pending_webhooks")
    chk("ledger payload keeps the envelope fields the HTTP model never declared",
        all(_k in _led for _k in _env), str([_k for _k in _env if _k not in _led]))
    chk("ledger payload equals the event as sent", _led == _ev)
    chk("outbox payload — what downstream receives — equals the event as sent",
        _stored_json(c65_conn, "SELECT payload FROM outbox WHERE org_id='c65' "
                               "AND transaction_id=%s", ("evt_c65_full",)) == _ev)

    _bad = _fixture("invoice.paid", "evt_c65_invalid")
    _bad["data"]["object"]["customer"] = "not_a_customer"
    _r = _post_raw(_c65_client, "/webhook/stripe/c65", json.dumps(_bad), _SEC_C65)
    chk("invalid event over HTTP → 200 DLQ_INVALID",
        _r.status_code == 200 and _r.json().get("outcome") == "DLQ_INVALID", f"{_r.status_code} {_r.text[:200]}")
    chk("DLQ raw_payload equals the event as sent, so a replay sees what Stripe sent",
        _stored_json(c65_conn, "SELECT raw_payload FROM dlq WHERE org_id='c65' "
                               "AND transaction_id=%s", ("evt_c65_invalid",)) == _bad)
finally:
    L._pool = original_pool_c65
    c65_conn.close()


# =============================================================================
# C6-6: an invalid event the DLQ could not record is not acknowledged (item 3)
#     _write_dlq swallowed every error and process_stripe_event returned DLQ_INVALID
#     anyway, so the webhook answered 200 and Stripe stopped delivering an event that
#     existed only in a log file the container does not persist. The failure below is
#     real, not mocked: a second session holds an exclusive lock on dlq, and the
#     request's connection gives up after one second (statement_timeout).
# =============================================================================
print("\n── C6-6: a failed DLQ write turns 200 into 503 for an invalid event ───")

c66_conn = fresh_conn()
_SEC_C66 = "whsec_c66_signing_secret"
with c66_conn.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
               ("c66", "c66", _SEC_C66))
original_pool_c66 = L._pool
L._pool = _MockPool(c66_conn)
_locker = psycopg2.connect(_TEST_DSN)
try:
    _c66_client = TestClient(L.app, raise_server_exceptions=False)
    _paid = _fixture("invoice.paid", "evt_c66_paid")
    _bad = _fixture("invoice.paid", "evt_c66_invalid")
    _bad["data"]["object"]["customer"] = "not_a_customer"

    _r = _post_raw(_c66_client, "/webhook/stripe/c66", json.dumps(_paid), _SEC_C66)
    chk("setup: first delivery of a valid event → POSTED",
        _r.status_code == 200 and _r.json().get("outcome") == "POSTED", _r.text[:200])

    with c66_conn.cursor() as _c:
        _c.execute("SET statement_timeout = '1s'")
    with _locker.cursor() as _lc:
        _lc.execute("LOCK TABLE dlq IN ACCESS EXCLUSIVE MODE")   # held until rollback
    try:
        _r = _post_raw(_c66_client, "/webhook/stripe/c66", json.dumps(_bad), _SEC_C66)
        chk("invalid event whose DLQ write failed → 503, not 200",
            _r.status_code == 503, f"{_r.status_code} {_r.text[:200]}")
        _r = _post_raw(_c66_client, "/webhook/stripe/c66", json.dumps(_paid), _SEC_C66)
        chk("duplicate whose DLQ audit row failed → still 200 (the ledger holds the event)",
            _r.status_code == 200 and _r.json().get("outcome") == "DLQ_DUPLICATE",
            f"{_r.status_code} {_r.text[:200]}")
    finally:
        _locker.rollback()
        with c66_conn.cursor() as _c:
            _c.execute("SET statement_timeout = 0")

    chk("nothing was recorded for the invalid event while the DLQ was down",
        _q1(c66_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_c66_invalid'") == 0)
    # Stripe redelivers after a 503. With the database back, the same delivery lands.
    _r = _post_raw(_c66_client, "/webhook/stripe/c66", json.dumps(_bad), _SEC_C66)
    chk("redelivery after recovery → 200 DLQ_INVALID",
        _r.status_code == 200 and _r.json().get("outcome") == "DLQ_INVALID", _r.text[:200])
    chk("…and the DLQ now holds the full event",
        _stored_json(c66_conn, "SELECT raw_payload FROM dlq WHERE org_id='c66' "
                               "AND transaction_id=%s", ("evt_c66_invalid",)) == _bad)
finally:
    L._pool = original_pool_c66
    _locker.close()
    c66_conn.close()


# =============================================================================
# C6-7: a second worker replica drains in parallel (item 6c)
#     The candidate CTEs took no lock; only the outer SELECT had SKIP LOCKED. A second
#     worker therefore computed the same window and the same picked ids as the first,
#     skipped all of them, and returned 0. Two real connections here: worker A is held
#     mid-batch inside its transaction while worker B drains.
# =============================================================================
print("\n── C6-7: a second worker drains while the first holds its batch ────────")

c67_a = fresh_conn()
c67_b = psycopg2.connect(_TEST_DSN)
c67_b.autocommit = True                 # _tx issues its own BEGIN
L.process_stripe_event_batch(c67_a, [fake_event(eid=f"evt_c67_{_i:03d}") for _i in range(120)])
_orig_dispatch_67, _orig_batch_67 = W._dispatch_row, W.WORKER_BATCH_SIZE
W.WORKER_BATCH_SIZE = 5                 # window = 50 rows; the backlog spans more than two
_seen_a, _seen_b = [], []
_hold_a, _a_parked = threading.Event(), threading.Event()


def _dispatch_67(row, destination=None):
    if threading.current_thread().name == "c67-worker-a":
        _seen_a.append(row["id"])
        if len(_seen_a) == 1:           # park A inside its transaction, rows locked
            _a_parked.set()
            _hold_a.wait(30)
    else:
        _seen_b.append(row["id"])


W._dispatch_row = _dispatch_67
try:
    _ta = threading.Thread(target=lambda: W.drain_batch(c67_a), name="c67-worker-a")
    _ta.start()
    chk("worker A is mid-batch, holding its rows", _a_parked.wait(30))
    _nb = W.drain_batch(c67_b)
    _hold_a.set()
    _ta.join(30)
    chk("a second worker dispatches while the first holds its batch (was 0)",
        _nb == 5, f"got {_nb}")
    chk("the two workers never dispatch the same row",
        not (set(_seen_a) & set(_seen_b)), f"overlap {sorted(set(_seen_a) & set(_seen_b))}")
    chk("both batches are marked dispatched, each once",
        _q1(c67_a, "SELECT COUNT(*) FROM outbox WHERE dispatched = 1") == 10,
        str(_q1(c67_a, "SELECT COUNT(*) FROM outbox WHERE dispatched = 1")))
finally:
    _hold_a.set()
    W._dispatch_row, W.WORKER_BATCH_SIZE = _orig_dispatch_67, _orig_batch_67
    c67_b.close()
    c67_a.close()


# =============================================================================
# C6-8: an event the database refuses is dead-lettered, not answered with 500
#     A validated event that a ledger CHECK refused raised IntegrityError, which nothing caught: the webhook answered 500,
#     Stripe retried a delivery that fails identically every time, and after three days
#     the event was gone with no ledger row and no DLQ row. The payload limit that
#     used to trigger it is now 8 MiB (C6-18), out of reach through the 1 MiB body cap,
#     so this section installs its own 50,000-char CHECK to exercise the refusal path.
# =============================================================================
print("\n── C6-8: a refused row goes to the DLQ, not to a 500 ───────────────────")

c68_conn = fresh_conn()
with c68_conn.cursor() as _c:
    _c.execute("ALTER TABLE ledger ADD CONSTRAINT c68_probe_check "
               "CHECK (length(payload) < 50000) NOT VALID")
_SEC_C68 = "whsec_c68_signing_secret"
with c68_conn.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
               ("c68", "c68", _SEC_C68))


def _oversize(eid: str) -> dict:
    _ev = _fixture("invoice.paid", eid)
    _ev["data"]["object"]["metadata"] = {f"k{_i}": "x" * 450 for _i in range(110)}
    return _ev


original_pool_c68 = L._pool
L._pool = _MockPool(c68_conn)
_locker68 = psycopg2.connect(_TEST_DSN)
try:
    _c68_client = TestClient(L.app, raise_server_exceptions=False)
    _big = _oversize("evt_c68_big")
    chk("the probe really crosses the probe's 50 000-char limit", len(json.dumps(_big)) > 50000)
    _r = _post_raw(_c68_client, "/webhook/stripe/c68", json.dumps(_big), _SEC_C68)
    chk("oversize valid event → 200 DLQ_INVALID, not 500",
        _r.status_code == 200 and _r.json().get("outcome") == "DLQ_INVALID",
        f"{_r.status_code} {_r.text[:200]}")
    chk("…the reason names the constraint that refused it",
        "c68_probe_check" in (_r.json().get("reason") or ""), _r.text[:200])
    chk("…no partial write: no ledger row, no outbox row",
        _q1(c68_conn, "SELECT COUNT(*) FROM ledger WHERE transaction_id='evt_c68_big'") == 0
        and _q1(c68_conn, "SELECT COUNT(*) FROM outbox WHERE transaction_id='evt_c68_big'") == 0)
    chk("…and the DLQ holds the whole event",
        _stored_json(c68_conn, "SELECT raw_payload FROM dlq WHERE org_id='c68' "
                               "AND transaction_id=%s", ("evt_c68_big",)) == _big)

    # A replay of a refused event must not append another DLQ row per attempt.
    _dlq_n = _q1(c68_conn, "SELECT COUNT(*) FROM dlq")
    _rr = L.process_stripe_event(c68_conn, _big, from_retry=True, org_id="c68")
    chk("a replay of a refused event → DLQ_INVALID with no new DLQ row",
        _rr["outcome"] == "DLQ_INVALID" and _q1(c68_conn, "SELECT COUNT(*) FROM dlq") == _dlq_n,
        str(_rr))

    # And if the DLQ cannot take it either, Stripe must hear 503, not 200.
    with c68_conn.cursor() as _c:
        _c.execute("SET statement_timeout = '1s'")
    with _locker68.cursor() as _lc:
        _lc.execute("LOCK TABLE dlq IN ACCESS EXCLUSIVE MODE")
    try:
        _r = _post_raw(_c68_client, "/webhook/stripe/c68",
                       json.dumps(_oversize("evt_c68_big_dlq_down")), _SEC_C68)
        chk("refused event whose DLQ write also failed → 503", _r.status_code == 503,
            f"{_r.status_code} {_r.text[:200]}")
    finally:
        _locker68.rollback()
        with c68_conn.cursor() as _c:
            _c.execute("SET statement_timeout = 0")
finally:
    L._pool = original_pool_c68
    _locker68.close()
    c68_conn.close()
    _undo68 = psycopg2.connect(_TEST_DSN)
    _undo68.autocommit = True
    with _undo68.cursor() as _c:
        _c.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS c68_probe_check")
    _undo68.close()


# =============================================================================
# C6-9: the RLS self-check tells the truth, and the deploy rotates billing_app (item 1)
#     _rls_status counted policies across the three tables and checked usesuper only, so
#     a BYPASSRLS role — which skips every policy exactly as a superuser does — or a
#     table with its row security switched off still logged rls_enforced. And 0006's
#     billing_app password, printed in the repository, was rotated by nothing.
# =============================================================================
print("\n── C6-9: rls_enforced means enforced; provision rotates billing_app ────")

from urllib.parse import urlsplit as _usplit  # noqa: E402

c69_owner = fresh_conn()
_orig_pool_69 = L._pool


def _status_as(conn) -> dict:
    L._pool = _MockPool(conn)
    try:
        return L._rls_status()
    finally:
        L._pool = _orig_pool_69


try:
    _st = _status_as(c69_owner)
    chk("the owner connection (superuser) is reported inert, and why",
        _st["enforced"] is False and "superuser" in _st["inert_because"], str(_st))

    # A role that is not a superuser but holds BYPASSRLS: the case usesuper missed.
    _BYP_PW = "c69_bypass_pw_0123456789"
    with c69_owner.cursor() as _c:
        _c.execute("DROP ROLE IF EXISTS c69_bypass")
        _c.execute("CREATE ROLE c69_bypass LOGIN NOSUPERUSER BYPASSRLS PASSWORD %s", (_BYP_PW,))
    _o = _usplit(_TEST_DSN)
    _byp = psycopg2.connect(f"postgresql://c69_bypass:{_BYP_PW}@{_o.hostname}:{_o.port or 5432}{_o.path}")
    _byp.autocommit = True
    try:
        _st = _status_as(_byp)
        chk("a BYPASSRLS role is reported inert (it used to log rls_enforced)",
            _st["enforced"] is False and "BYPASSRLS" in _st["inert_because"]
            and _st["superuser"] is False, str(_st))
    finally:
        _byp.close()
        with c69_owner.cursor() as _c:
            _c.execute("DROP ROLE IF EXISTS c69_bypass")

    # billing_app is enforced — until one table loses row security.
    _app69 = psycopg2.connect(_APP_DSN)
    _app69.autocommit = True
    try:
        chk("billing_app with policies on all three tables → enforced",
            _status_as(_app69)["enforced"] is True, str(_status_as(_app69)))
        with c69_owner.cursor() as _c:
            _c.execute("ALTER TABLE outbox DISABLE ROW LEVEL SECURITY")
        try:
            _st = _status_as(_app69)
            chk("one table without row security → inert, naming the table",
                _st["enforced"] is False
                and any("outbox" in _r for _r in _st["inert_because"]), str(_st))
        finally:
            with c69_owner.cursor() as _c:
                _c.execute("ALTER TABLE outbox ENABLE ROW LEVEL SECURITY")
        chk("…and enforced again once it is restored", _status_as(_app69)["enforced"] is True)
    finally:
        _app69.close()

    # provision.py rotates billing_app from APP_DATABASE_URL — and only billing_app.
    _a = _usplit(_APP_DSN)
    _host = f"{_a.hostname}:{_a.port or 5432}{_a.path}"
    chk("bootstrap password in APP_DATABASE_URL → left unchanged",
        P.sync_app_role_password(c69_owner, f"postgresql://billing_app:billing_app@{_host}")
        .startswith("unchanged"))
    chk("a DSN for another role → skipped, nothing altered",
        P.sync_app_role_password(c69_owner, f"postgresql://someone_else:pw@{_host}")
        .startswith("skipped"))
    _NEW_PW = "c69_rotated_app_password_0123456789"
    try:
        chk("a real password → rotated",
            P.sync_app_role_password(c69_owner, f"postgresql://billing_app:{_NEW_PW}@{_host}")
            == "rotated")
        _ok_new = _ok_old = False
        try:
            psycopg2.connect(f"postgresql://billing_app:{_NEW_PW}@{_host}").close()
            _ok_new = True
        except psycopg2.Error:
            pass
        try:
            psycopg2.connect(f"postgresql://billing_app:billing_app@{_host}").close()
            _ok_old = True
        except psycopg2.Error:
            pass
        chk("billing_app now authenticates with the new password", _ok_new)
        chk("…and no longer with the published one", not _ok_old)
    finally:
        # The role is cluster-wide and the rest of this suite connects with the
        #     bootstrap password: put it back whatever happened above.
        with c69_owner.cursor() as _c:
            _c.execute("ALTER ROLE billing_app PASSWORD 'billing_app'")
finally:
    L._pool = _orig_pool_69
    c69_owner.close()


# =============================================================================
# C6-10: a $0 invoice.paid is ledgered at 0 (item 5f, decided 2026-10-06)
#     A 100%-off coupon or a free item yields a real invoice for $0 that Stripe marks
#     paid. check_invoice_amount_nonzero sent every one to the DLQ as INVALID, where it
#     counted in dlq_depth like an error. It is now POSTED at 0: a known zero. An
#     unknown amount (no amount field) and a $0 payment_failed are still refused.
# =============================================================================
print("\n── C6-10: a $0 invoice.paid is POSTED at 0, not dead-lettered ─────────")

c610_conn = fresh_conn()
_SEC_C610 = "whsec_c610_signing_secret"
with c610_conn.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
               ("c610", "c610", _SEC_C610))
original_pool_c610 = L._pool
L._pool = _MockPool(c610_conn)
try:
    _c610_client = TestClient(L.app, raise_server_exceptions=False)
    _free = _fixture("invoice.paid", "evt_c610_coupon")
    for _k in ("amount_due", "amount_paid", "amount_remaining", "subtotal", "total"):
        if _k in _free["data"]["object"]:
            _free["data"]["object"][_k] = 0
    _r = _post_raw(_c610_client, "/webhook/stripe/c610", json.dumps(_free), _SEC_C610)
    chk("signed $0 invoice.paid → 200 POSTED",
        _r.status_code == 200 and _r.json().get("outcome") == "POSTED",
        f"{_r.status_code} {_r.text[:200]}")
    chk("…stored at amount_cents 0",
        _q1(c610_conn, "SELECT amount_cents FROM ledger WHERE org_id='c610' "
                       "AND transaction_id='evt_c610_coupon'") == 0)
    chk("…and nothing in the DLQ for it",
        _q1(c610_conn, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_c610_coupon'") == 0)

    _failed0 = _fixture("invoice.payment_failed", "evt_c610_failed_zero")
    _failed0["data"]["object"]["amount_due"] = 0
    _failed0["data"]["object"]["amount_paid"] = 0
    _r = _post_raw(_c610_client, "/webhook/stripe/c610", json.dumps(_failed0), _SEC_C610)
    chk("a $0 invoice.payment_failed is still refused → DLQ_INVALID",
        _r.status_code == 200 and _r.json().get("outcome") == "DLQ_INVALID", _r.text[:200])
finally:
    L._pool = original_pool_c610
    c610_conn.close()


# =============================================================================
# C6-11: an old pending DLQ entry can be found over HTTP (item 8a)
#     /dlq/entries returned the newest `limit` rows (max 1000) with no status, no retry
#     state and no paging, so a pending entry behind a thousand newer resolved ones was
#     unreachable — and pending is what an operator comes looking for.
# =============================================================================
print("\n── C6-11: /dlq/entries filters by status and pages by cursor ──────────")

c611_conn = fresh_conn()
# This section makes ~7 ops requests right after others that also count against
#     RATE_LIMIT_OPS (20/minute per client); a 429 here would be the limiter, not 8a.
L._limiter.reset()
with c611_conn.cursor() as _c:
    _c.execute("INSERT INTO dlq (transaction_id, reason, raw_payload, status, retry_count, "
               "max_retries) VALUES ('evt_c611_old_pending','INVALID','{}','pending',1,3)")
    _c.execute("INSERT INTO dlq (transaction_id, reason, raw_payload, status) "
               "SELECT 'evt_c611_resolved_' || g, 'DUPLICATE', '{}', 'resolved' "
               "FROM generate_series(1, 1200) g")
    for _st in ("retrying", "exhausted"):
        _c.execute("INSERT INTO dlq (transaction_id, reason, raw_payload, status) "
                   "VALUES (%s,'INVALID','{}',%s)", (f"evt_c611_{_st}", _st))
original_pool_c611 = L._pool
L._pool = _MockPool(c611_conn)
try:
    _c611 = TestClient(L.app, raise_server_exceptions=False)
    _all = _c611.get("/dlq/entries?limit=1000").json()
    chk("the unfiltered newest-1000 page does not contain the old pending entry",
        all(_e["transaction_id"] != "evt_c611_old_pending" for _e in _all["entries"]))
    _p = _c611.get("/dlq/entries?status=pending").json()
    chk("status=pending finds it",
        [_e["transaction_id"] for _e in _p["entries"]] == ["evt_c611_old_pending"], str(_p)[:200])
    _e0 = _p["entries"][0] if _p["entries"] else {}
    chk("…with its retry state",
        _e0.get("status") == "pending" and _e0.get("retry_count") == 1
        and _e0.get("max_retries") == 3 and "next_retry_at" in _e0 and "claimed_at" in _e0,
        str(_e0)[:200])
    _act = _c611.get("/dlq/entries?status=actionable").json()
    chk("status=actionable = pending + retrying + exhausted",
        sorted(_e["status"] for _e in _act["entries"]) == ["exhausted", "pending", "retrying"],
        str([_e["status"] for _e in _act["entries"]]))
    chk("an unknown status → 422, not an empty 'all clear'",
        _c611.get("/dlq/entries?status=pendng").status_code == 422)

    # Page through everything with the cursor: every row exactly once, newest first.
    _seen, _cursor, _pages = [], None, 0
    while _pages < 20:
        _url = "/dlq/entries?limit=500" + (f"&before_id={_cursor}" if _cursor else "")
        _pg = _c611.get(_url).json()
        _seen += [_e["id"] for _e in _pg["entries"]]
        _pages += 1
        _cursor = _pg["next_before_id"]
        if _cursor is None:
            break
    chk("the cursor walks every entry exactly once",
        len(_seen) == 1203 and len(set(_seen)) == 1203, f"{len(_seen)} rows, {len(set(_seen))} distinct")
    chk("…newest first, in order", _seen == sorted(_seen, reverse=True))
    chk("…and the last page says so (next_before_id null)", _cursor is None)
finally:
    L._pool = original_pool_c611
    c611_conn.close()


# =============================================================================
# C6-12: the outbox lease — no locks across HTTP, no tenant starvation (items 6a, 6b)
#     drain_batch held FOR UPDATE row locks across every HTTP call in the batch, and a
#     failed dispatch left its row untouched, to be retried on the next poll. A tenant
#     whose downstream kept failing therefore stayed at the head of the queue, and with
#     more rows than one window (batch x 10) no other tenant was ever selected.
# =============================================================================
print("\n── C6-12: claim, dispatch with no transaction open, commit per row ─────")

c612 = fresh_conn()
c612_obs = psycopg2.connect(_TEST_DSN)
c612_obs.autocommit = True
with c612.cursor() as _c:
    for _o in ("c612_x", "c612_y"):
        _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,'whsec_t')",
                   (_o, _o))
L.process_stripe_event_batch(c612, [fake_event(eid=f"evt_c612_x_{_i:03d}") for _i in range(60)],
                             org_id="c612_x")
L.process_stripe_event_batch(c612, [fake_event(eid=f"evt_c612_y_{_i}") for _i in range(5)],
                             org_id="c612_y")

_orig_dispatch_612, _orig_batch_612 = W._dispatch_row, W.WORKER_BATCH_SIZE
_orig_conc_612 = W.WORKER_DISPATCH_CONCURRENCY
W.WORKER_BATCH_SIZE = 5
# Serial dispatch here: with tenants in parallel, the main thread commits one tenant's
#     outcome while another tenant's dispatch samples the session state, and that
#     sample reads "idle in transaction" for a recording transaction that is not held
#     across any HTTP call. C6-22 covers the concurrent path.
W.WORKER_DISPATCH_CONCURRENCY = 1
_calls_612, _states_612 = [], []


def _dispatch_612(row, destination=None):
    _calls_612.append((row["org_id"], row["id"]))
    with c612_obs.cursor() as _oc:          # what is the worker's own session doing?
        _oc.execute("SELECT state FROM pg_stat_activity WHERE pid = %s", (c612.get_backend_pid(),))
        _states_612.append(_oc.fetchone()[0])
    if row["org_id"] == "c612_x":
        raise RuntimeError("downstream for c612_x is down")


def _y_dispatched() -> int:
    return _q1(c612, "SELECT COUNT(*) FROM outbox WHERE org_id='c612_y' AND dispatched=1")


W._dispatch_row = _dispatch_612
try:
    W.drain_batch(c612)
    chk("6b: the quiet tenant is served on the FIRST drain, ahead of 60 older failing rows",
        sum(1 for _o, _ in _calls_612 if _o == "c612_y") >= 2,
        str([_o for _o, _ in _calls_612]))
    chk("6a: no transaction is open while a row is being dispatched",
        _states_612 and all(_st == "idle" for _st in _states_612), str(_states_612))
    _x_tried = [_i for _o, _i in _calls_612 if _o == "c612_x"]
    _row = None
    with c612.cursor() as _c:
        _c.execute("SELECT attempts, next_attempt_at > NOW(), last_error, claimed_until, "
                   "dispatched FROM outbox WHERE id = %s", (_x_tried[0],))
        _row = _c.fetchone()
    chk("6b: a failed row is backed off with its error recorded, lease released",
        _row is not None and _row[0] == 1 and _row[1] is True and "down" in (_row[2] or "")
        and _row[3] is None and _row[4] == 0, str(_row))

    for _ in range(3):
        W.drain_batch(c612)
    chk("the quiet tenant is fully delivered within four drains", _y_dispatched() == 5,
        str(_y_dispatched()))
    _ids = [_i for _o, _i in _calls_612 if _o == "c612_x"]
    chk("a backed-off row is not re-attempted before its time (no id twice)",
        len(_ids) == len(set(_ids)), f"{len(_ids)} attempts, {len(set(_ids))} distinct")

    # A crashed worker's claim: skipped while the lease holds, delivered once it expires.
    L.process_stripe_event(c612, fake_event(eid="evt_c612_y_late"), org_id="c612_y")
    with c612.cursor() as _c:
        _c.execute("UPDATE outbox SET claimed_until = NOW() + interval '1 hour' "
                   "WHERE transaction_id = 'evt_c612_y_late'")
    W.drain_batch(c612)
    chk("a row under another worker's live lease is skipped", _y_dispatched() == 5)
    with c612.cursor() as _c:
        _c.execute("UPDATE outbox SET claimed_until = NOW() - interval '1 second' "
                   "WHERE transaction_id = 'evt_c612_y_late'")
    W.drain_batch(c612)
    chk("…and delivered once the lease has expired", _y_dispatched() == 6, str(_y_dispatched()))

    # A worker that outlived its lease: B reclaimed the row while A was still dispatching.
    #     _finish_row used to update by id alone, so A's late failure cleared B's live
    #     lease and stamped attempts/last_error on whatever B had done with the row.
    L.process_stripe_event(c612, fake_event(eid="evt_c612_y_stale"), org_id="c612_y")
    _row_a = next(_r for _r in W._claim_batch(c612)
                  if _r["transaction_id"] == "evt_c612_y_stale")

    def _stale_state():
        with c612.cursor() as _c:
            _c.execute("SELECT dispatched, attempts, last_error, claimed_until > NOW() "
                       "FROM outbox WHERE transaction_id = 'evt_c612_y_stale'")
            return _c.fetchone()

    with c612.cursor() as _c:                     # A's lease expires; B claims the row
        _c.execute("UPDATE outbox SET claimed_until = NOW() + interval '1 hour' "
                   "WHERE transaction_id = 'evt_c612_y_stale'")
    _applied = W._finish_row(c612, _row_a, "A's late timeout")
    chk("a stale worker's failure does not touch a row another worker now leases",
        _applied is False and _stale_state() == (0, 0, None, True), str(_stale_state()))
    _applied = W._finish_row(c612, _row_a, None)
    chk("…but its success is recorded: the event did reach the downstream",
        _applied is True and _stale_state()[0] == 1, str(_stale_state()))
    _row_b = dict(_row_a, claimed_until=None)
    _applied = W._finish_row(c612, _row_b, "B's late timeout")
    chk("a late failure never stamps attempts or last_error on a delivered row",
        _applied is False and _stale_state()[:3] == (1, 0, None), str(_stale_state()))
finally:
    W._dispatch_row, W.WORKER_BATCH_SIZE = _orig_dispatch_612, _orig_batch_612
    W.WORKER_DISPATCH_CONCURRENCY = _orig_conc_612
    c612_obs.close()
    c612.close()


# =============================================================================
# C6-13: tenants rotate across claims, not only within one
#     The claim ranked tenants round-robin inside a batch but broke rank-1 ties by row id,
#     so with more backlogged tenants than batch slots the same oldest backlogs took every
#     rank-1 slot on every claim: a newer tenant waited until all of them had drained.
# =============================================================================
print("\n── C6-13: rank-1 slots rotate across claims ─────────────────────────────")

c613 = fresh_conn()
with c613.cursor() as _c:
    for _o in ("c613_a", "c613_b", "c613_c", "c613_d"):
        _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,'whsec_t')",
                   (_o, _o))
for _o, _n in (("c613_a", 3), ("c613_b", 3), ("c613_c", 3), ("c613_d", 1)):
    L.process_stripe_event_batch(c613, [fake_event(eid=f"evt_{_o}_{_i}") for _i in range(_n)],
                                 org_id=_o)

_orig_dispatch_613, _orig_batch_613, _orig_rr_613 = W._dispatch_row, W.WORKER_BATCH_SIZE, W._rr_after
W.WORKER_BATCH_SIZE, W._rr_after = 2, ""
_claims_613 = []
W._dispatch_row = lambda row, destination=None: _claims_613[-1].append(row["org_id"])
try:
    for _ in range(2):
        _claims_613.append([])
        W.drain_batch(c613)
    chk("four backlogged tenants, two slots: every tenant is served within two claims",
        {_o for _cl in _claims_613 for _o in _cl} == {"c613_a", "c613_b", "c613_c", "c613_d"},
        str(_claims_613))
    _claims_613.append([])
    W.drain_batch(c613)
    chk("…and the third claim starts the rotation over",
        _claims_613[2] == ["c613_a", "c613_b"], str(_claims_613))
finally:
    W._dispatch_row, W.WORKER_BATCH_SIZE, W._rr_after = (_orig_dispatch_613, _orig_batch_613,
                                                         _orig_rr_613)
    c613.close()


# =============================================================================
# C6-14: the request body is capped while it is read
#     FastAPI read the whole body into memory before the limiter, the signature check or
#     any handler ran, and nothing capped it: one unauthenticated 100 MB POST OOM-killed
#     the 256 MB billing container (reproduced against the compose stack).
# =============================================================================
print("\n── C6-14: oversized bodies → 413, read no further than the cap ──────────")

_cap_client = TestClient(L.app)
_cap = L.MAX_REQUEST_BODY_BYTES
_r = _cap_client.post("/webhook/stripe", content=b"x" * (_cap + 1),
                      headers={"Content-Type": "application/json"})
chk("a declared Content-Length over the cap → 413", _r.status_code == 413, str(_r.status_code))




def _chunked_614(total=50 * 1024 * 1024, step=64 * 1024):
    for _ in range(total // step):
        yield b"0" * step


_r = _cap_client.post("/webhook/stripe/default", content=_chunked_614(),
                      headers={"Content-Type": "application/json"})
chk("a chunked 50 MB body with no Content-Length → 413", _r.status_code == 413, str(_r.status_code))
# TestClient buffers a generator body itself before calling the app, so the read-stops-at-
#     the-cap property is checked on the middleware directly, with a receive() that counts
#     what it is asked for — the same channel uvicorn feeds from the socket.
import asyncio as _asyncio_614  # noqa: E402

_pulled_614, _sent_614, _app_called_614 = [0], [], [False]


async def _receive_614():
    _pulled_614[0] += 64 * 1024
    return {"type": "http.request", "body": b"0" * (64 * 1024),
            "more_body": _pulled_614[0] < 50 * 1024 * 1024}


async def _send_614(message):
    _sent_614.append(message)


async def _inner_app_614(scope, receive, send):
    _app_called_614[0] = True


_asyncio_614.run(L._BodySizeCap(_inner_app_614, _cap)(
    {"type": "http", "method": "POST", "path": "/webhook/stripe", "headers": [],
     "client": ("203.0.113.9", 1)}, _receive_614, _send_614))
chk("…and reading stopped at the cap, not at 50 MB, without reaching the app",
    _pulled_614[0] <= _cap + 64 * 1024 and not _app_called_614[0]
    and _sent_614 and _sent_614[0].get("status") == 413,
    f"{_pulled_614[0]} bytes pulled, app called={_app_called_614[0]}")
_r = _cap_client.post("/webhook/stripe", content=b'{"id":"evt_cap","type":"invoice.paid"}',
                      headers={"Content-Type": "application/json", "Stripe-Signature": "t=1,v1=00"})
chk("a small body still reaches the signature check (400, not 413)", _r.status_code == 400,
    str(_r.status_code))
_r = _cap_client.post("/webhook/stripe", content=b"{}",
                      headers={"Content-Type": "application/json", "Content-Length": "12abc"})
chk("a malformed Content-Length is refused before reading", _r.status_code in (400, 413),
    str(_r.status_code))


# =============================================================================
# C6-15: a redirecting downstream is a failed dispatch, not a delivered one
#     urllib followed a 302 on the POST as a body-less GET, and the target's 200 marked
#     the row dispatched with no error: a paid event "delivered" that never arrived.
# =============================================================================
print("\n── C6-15: downstream 3xx → dispatch fails, nothing follows the redirect ─")

import http.server as _hs_615  # noqa: E402
import threading as _th_615  # noqa: E402

_hits_615 = []


class _Redirector615(_hs_615.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        _hits_615.append(("POST", self.path))
        self.send_response(302)
        self.send_header("Location", "/sink")
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        _hits_615.append(("GET", self.path))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


_srv_615 = _hs_615.HTTPServer(("127.0.0.1", 0), _Redirector615)
_th_615.Thread(target=_srv_615.serve_forever, daemon=True).start()
try:
    _err_615 = None
    try:
        W._dispatch_row({"id": 1, "payload": "{}", "event_type": "invoice.paid",
                         "transaction_id": "evt_c615", "org_id": "default"},
                        f"http://127.0.0.1:{_srv_615.server_port}/hook")
    except RuntimeError as _e:
        _err_615 = str(_e)
    chk("a 302 from the downstream raises (the row backs off), naming the status",
        _err_615 is not None and "302" in _err_615, str(_err_615))
    chk("…and the worker never follows it with a body-less GET",
        _hits_615 == [("POST", "/hook")], str(_hits_615))
finally:
    _srv_615.shutdown()


# =============================================================================
# C6-16: a slow downstream is cut at a deadline and cannot hold other tenants
#     The 10 s timeout bounded each socket read, not the request, and dispatch is serial:
#     a downstream trickling one byte every few seconds held the whole worker — every
#     other tenant's deliveries — for as long as it kept trickling.
# =============================================================================
print("\n── C6-16: dispatch deadline; a failing tenant yields the batch ──────────")

import socket as _sock_616  # noqa: E402

_lsn_616 = _sock_616.socket()
_lsn_616.bind(("127.0.0.1", 0))
_lsn_616.listen(4)
_stop_616 = _th_615.Event()


def _drip_616():
    while not _stop_616.is_set():
        try:
            _lsn_616.settimeout(0.5)
            _c, _ = _lsn_616.accept()
        except OSError:
            continue
        _c.recv(65536)
        try:
            _c.sendall(b"HTTP/1.1 200 OK\r\nX-A: ")
            while not _stop_616.is_set():
                _c.sendall(b"a")
                time.sleep(0.2)
        except OSError:
            pass
        finally:
            _c.close()


_th_615.Thread(target=_drip_616, daemon=True).start()
_orig_deadline_616 = W.DISPATCH_DEADLINE_SECONDS
W.DISPATCH_DEADLINE_SECONDS = 1.5
try:
    _t0_616, _err_616 = time.monotonic(), None
    try:
        W._dispatch_row({"id": 1, "payload": "{}", "event_type": "invoice.paid",
                         "transaction_id": "evt_c616", "org_id": "default"},
                        f"http://127.0.0.1:{_lsn_616.getsockname()[1]}/hook")
    except RuntimeError as _e:
        _err_616 = str(_e)
    _took_616 = time.monotonic() - _t0_616
    chk("a trickling downstream is cut at the dispatch deadline, not after it finishes",
        _err_616 is not None and "deadline" in _err_616 and _took_616 < 4,
        f"{_took_616:.1f}s {_err_616}")
finally:
    W.DISPATCH_DEADLINE_SECONDS = _orig_deadline_616
    _stop_616.set()
    _lsn_616.close()

c616 = fresh_conn()
with c616.cursor() as _c:
    for _o in ("c616_bad", "c616_good"):
        _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,'whsec_t')",
                   (_o, _o))
L.process_stripe_event_batch(c616, [fake_event(eid=f"evt_c616_bad_{_i}") for _i in range(4)],
                             org_id="c616_bad")
L.process_stripe_event_batch(c616, [fake_event(eid=f"evt_c616_good_{_i}") for _i in range(2)],
                             org_id="c616_good")
_orig_dispatch_616, _orig_batch_616, _orig_rr_616 = W._dispatch_row, W.WORKER_BATCH_SIZE, W._rr_after
W.WORKER_BATCH_SIZE, W._rr_after = 10, ""
_calls_616 = []


def _dispatch_616(row, destination=None):
    _calls_616.append(row["org_id"])
    if row["org_id"] == "c616_bad":
        raise RuntimeError("downstream exceeded the 15 s dispatch deadline")


W._dispatch_row = _dispatch_616
try:
    W.drain_batch(c616)
    chk("a failing tenant is tried once per batch; the other tenant is fully served",
        _calls_616.count("c616_bad") == 1 and _calls_616.count("c616_good") == 2, str(_calls_616))
    with c616.cursor() as _c:
        _c.execute("SELECT COUNT(*) FILTER (WHERE attempts = 0 AND claimed_until IS NULL "
                   "AND next_attempt_at IS NULL), COUNT(*) FROM outbox "
                   "WHERE org_id = 'c616_bad' AND dispatched = 0")
        _rel_616 = _c.fetchone()
    chk("…its untried rows are released with no attempt counted, eligible next claim",
        _rel_616 == (3, 4), str(_rel_616))
finally:
    W._dispatch_row, W.WORKER_BATCH_SIZE, W._rr_after = (_orig_dispatch_616, _orig_batch_616,
                                                         _orig_rr_616)
    c616.close()


# =============================================================================
# C6-17: a worker stopped mid-batch hands its unsent rows back
#     A restart (deploy, SIGTERM) left the rest of the claimed batch under its lease:
#     against the compose stack, five paid events sat undelivered for ~17 minutes.
# =============================================================================
print("\n── C6-17: SIGTERM mid-batch releases the rows not yet sent ──────────────")

c617 = fresh_conn()
L.process_stripe_event_batch(c617, [fake_event(eid=f"evt_c617_{_i}") for _i in range(5)])
_orig_dispatch_617, _orig_batch_617 = W._dispatch_row, W.WORKER_BATCH_SIZE
W.WORKER_BATCH_SIZE = 10
_sent_617 = []


def _dispatch_617(row, destination=None):
    _sent_617.append(row["transaction_id"])
    if len(_sent_617) == 2:
        W._stop = True          # SIGTERM arrives while the second row is in flight


W._dispatch_row = _dispatch_617
try:
    W.drain_batch(c617)
    with c617.cursor() as _c:
        _c.execute("SELECT COUNT(*) FILTER (WHERE dispatched = 1), "
                   "COUNT(*) FILTER (WHERE dispatched = 0 AND claimed_until IS NULL AND attempts = 0) "
                   "FROM outbox WHERE transaction_id LIKE 'evt_c617_%'")
        _st_617 = _c.fetchone()
    chk("the row in flight finishes; the three not yet sent are released untried",
        len(_sent_617) == 2 and _st_617 == (2, 3), f"sent={len(_sent_617)} state={_st_617}")
finally:
    W._stop = False
    W._dispatch_row, W.WORKER_BATCH_SIZE = _orig_dispatch_617, _orig_batch_617
    c617.close()



# =============================================================================
# C6-18: a large real invoice is ledgered, not dead-lettered
#     CHECK (length(payload) < 50000) refused a signed invoice.paid with forty line items
#     (60 KB): 200 to Stripe, DLQ INVALID with max_retries 0, and a replay hit the same
#     CHECK — a $4,999 payment that never reached the ledger. Migration 0009: 8 MiB.
# =============================================================================
print("\n── C6-18: a 60 KB invoice.paid is POSTED; the limit covers the body cap ──")

c618 = fresh_conn()
_SEC_C618 = "whsec_c618_signing_secret"
with c618.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
               ("c618", "c618", _SEC_C618))
_orig_pool_618 = L._pool
L._pool = _MockPool(c618)
try:
    _cl618 = TestClient(L.app, raise_server_exceptions=False)
    _ev618 = _fixture("invoice.paid", "evt_c618_lines")
    _ev618["data"]["object"]["lines"] = {"object": "list", "data": [
        {"id": f"il_{_i}", "amount": 12000, "description": "Plan Pro — línea " + "x" * 1400}
        for _i in range(40)]}
    chk("the probe is a 60 KB event", len(json.dumps(_ev618)) > 60000)
    _r = _post_raw(_cl618, "/webhook/stripe/c618", json.dumps(_ev618), _SEC_C618)
    chk("a signed 60 KB invoice.paid → POSTED, with its ledger and outbox rows",
        _r.status_code == 200 and _r.json().get("outcome") == "POSTED"
        and _q1(c618, "SELECT COUNT(*) FROM outbox WHERE transaction_id='evt_c618_lines'") == 1,
        f"{_r.status_code} {_r.text[:160]}")
    # The worst case the 1 MiB body cap admits: all non-ASCII, escaped 6x by json.dumps.
    _ev618b = _fixture("invoice.paid", "evt_c618_wide")
    _ev618b["data"]["object"]["metadata"] = {"note": "é" * 450_000}
    _body618 = json.dumps(_ev618b, ensure_ascii=False)
    chk("…and the largest body the cap admits fits the ledger limit",
        len(_body618.encode()) <= L.MAX_REQUEST_BODY_BYTES
        and len(json.dumps(_ev618b)) <= 8388608, f"{len(_body618.encode())} bytes on the wire")
    _r = _post_raw(_cl618, "/webhook/stripe/c618", _body618, _SEC_C618)
    chk("…and is POSTED", _r.status_code == 200 and _r.json().get("outcome") == "POSTED",
        f"{_r.status_code} {_r.text[:160]}")
finally:
    L._pool = _orig_pool_618
    c618.close()



# =============================================================================
# C6-19: malformed input gets a 4xx — the same one for every tenant
#     A non-ASCII signature (TypeError in compare_digest) or a non-UTF-8 body escaped as
#     500 for existing orgs while unknown orgs got 400: a tenant-enumeration oracle. A
#     non-ASCII X-Api-Key was a 500. An 8 KB org id was logged twice, verbatim. A body
#     nested 1,000 deep made the 422 handler recurse into a 500. All reproduced live.
# =============================================================================
print("\n── C6-19: hostile input → the same 4xx, bounded logs ────────────────────")

c619 = fresh_conn()
with c619.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES ('c619','c619','whsec_c619')")
_orig_pool_619 = L._pool
L._pool = _MockPool(c619)
_cl619 = TestClient(L.app, raise_server_exceptions=False)
_logged_619 = []


class _Grab619(logging.Handler):
    def emit(self, record):
        _logged_619.append(record.getMessage() + " " + str(getattr(record, "org_id", "")))


_grab_619 = _Grab619()
L._log.addHandler(_grab_619)
try:
    _bad_sig = {"Content-Type": "application/json", "Stripe-Signature": b"t=1,v1=\xe9"}
    _known = _cl619.post("/webhook/stripe/c619", content=b'{"id":"a","type":"b"}', headers=_bad_sig)
    _unknown = _cl619.post("/webhook/stripe/nosuchorg", content=b'{"id":"a","type":"b"}',
                           headers=_bad_sig)
    chk("a non-ASCII signature → 400 for a real tenant, same as for an unknown one",
        _known.status_code == _unknown.status_code == 400 and _known.text == _unknown.text,
        f"{_known.status_code} vs {_unknown.status_code}")
    _u16 = '{"id":"evt_u16","type":"invoice.paid"}'.encode("utf-16")
    _r = _cl619.post("/webhook/stripe/c619", content=_u16,
                     headers={"Content-Type": "application/json", "Stripe-Signature": "t=1,v1=00"})
    chk("a UTF-16 body → 4xx, not 500", 400 <= _r.status_code < 500, str(_r.status_code))
    _orig_key_619, L.BILLING_API_KEY = L.BILLING_API_KEY, "op_key_c619"
    try:
        _r = _cl619.get("/ledger/summary", headers={"X-Api-Key": b"caf\xe9"})
        _r2 = _cl619.get("/metrics", headers={"X-Api-Key": b"caf\xe9"})
    finally:
        L.BILLING_API_KEY = _orig_key_619
    chk("a non-ASCII X-Api-Key → 401 on ops and /metrics, not 500",
        _r.status_code == 401 and _r2.status_code == 401, f"{_r.status_code} {_r2.status_code}")
    _r = _cl619.post("/webhook/stripe/" + "A" * 8000, content=b'{"id":"a","type":"b"}',
                     headers={"Content-Type": "application/json"})
    _longest = max((len(_m) for _m in _logged_619 if "AAAA" in _m), default=0)
    chk("an 8 KB org id → 400, logged truncated", _r.status_code == 400 and 0 < _longest < 300,
        f"{_r.status_code}, longest log line {_longest}")
    _r = _cl619.post("/webhook/stripe", content=b"[" * 1000 + b"]" * 1000,
                     headers={"Content-Type": "application/json", "Stripe-Signature": "t=1,v1=00"})
    chk("a body nested 1,000 deep → 422, not a 500 from the error handler",
        _r.status_code == 422 and len(_r.content) < 5000, f"{_r.status_code} {len(_r.content)} bytes")
finally:
    L._log.removeHandler(_grab_619)
    L._pool = _orig_pool_619
    c619.close()



# =============================================================================
# C6-20: a busy or dead database is a 503 with Retry-After, never a 500
#     ThreadedConnectionPool.getconn() raised PoolError the moment all connections were
#     out: 64 concurrent webhooks against the compose stack got 4,526 of 20,000 answered
#     500 or reset. With PostgreSQL killed, 28 of 35 failures were 500/502, not 503.
# =============================================================================
print("\n── C6-20: pool waits, then 503; database errors → 503 ───────────────────")

_orig_pool_620, _orig_wait_620 = L._pool, L.DB_POOL_WAIT_SECONDS
_p620 = L._AutocommitPool(2, 2, dsn=L.APP_DATABASE_URL)
try:
    L.DB_POOL_WAIT_SECONDS = 2.0
    _a, _b = _p620.getconn(), _p620.getconn()
    _th_615.Timer(0.3, lambda: _p620.putconn(_a)).start()
    _t0 = time.monotonic()
    try:
        _c3 = _p620.getconn()
    except Exception as _e:
        _c3 = None
        _p620.putconn(_b)          # keep the rest of the section runnable
        _b = _p620.getconn()
    chk("a request with every connection out waits for one instead of failing",
        _c3 is not None and 0.2 < time.monotonic() - _t0 < 1.5, f"{time.monotonic() - _t0:.2f}s")
    time.sleep(0.4)                # the timer's putconn has run either way
    L.DB_POOL_WAIT_SECONDS = 0.3
    _busy = None
    try:
        _p620.getconn()
    except L._PoolBusy as _e:
        _busy = _e
    chk("…and gives up with _PoolBusy after DB_POOL_WAIT_SECONDS", _busy is not None)
    L._pool = _p620
    _cl620 = TestClient(L.app, raise_server_exceptions=False)
    _r = _cl620.post("/webhook/stripe", content=b'{"id":"evt_c620","type":"invoice.paid"}',
                     headers={"Content-Type": "application/json", "Stripe-Signature": "t=1,v1=00"})
    chk("an ingest request that cannot get a connection → 503 + Retry-After, not 500",
        _r.status_code == 503 and _r.headers.get("retry-after") == "1", f"{_r.status_code}")
    _p620.putconn(_b)
    if _c3 is not None:
        _p620.putconn(_c3)

    class _DeadPool620:
        def getconn(self, key=None):
            raise psycopg2.OperationalError("server closed the connection unexpectedly")

        def putconn(self, *a, **k):
            pass

    L._pool = _DeadPool620()
    _orig_key_620, L.BILLING_API_KEY = L.BILLING_API_KEY, ""
    _orig_any_620, L._ANY_ORG_KEYS = L._ANY_ORG_KEYS, False
    try:
        _r1 = _cl620.post("/webhook/stripe/default", content=b'{"id":"evt_c620b","type":"invoice.paid"}',
                          headers={"Content-Type": "application/json", "Stripe-Signature": "t=1,v1=00"})
        _r2 = _cl620.get("/ledger/summary")
    finally:
        L.BILLING_API_KEY, L._ANY_ORG_KEYS = _orig_key_620, _orig_any_620
    chk("a dead database → 503 on ingest and on ops endpoints, not 500",
        _r1.status_code == 503 and _r2.status_code == 503, f"{_r1.status_code} {_r2.status_code}")
finally:
    L._pool, L.DB_POOL_WAIT_SECONDS = _orig_pool_620, _orig_wait_620
    _p620.closeall()



# =============================================================================
# C6-21: failed authentications are rate-limited, before the database lookup
#     slowapi counts requests inside the decorated endpoint, but the 401 is raised in
#     the auth dependency, which FastAPI resolves first: against the compose stack one IP
#     made 5,879 wrong-key requests in 10 s, each one an org lookup, while a valid key
#     was cut off at 20/minute.
# =============================================================================
print("\n── C6-21: wrong keys → 401 up to the budget, then 429 with no lookup ────")

_orig_621 = (L._ANY_ORG_KEYS, L.BILLING_API_KEY, L._lookup_org_by_key, L._pool)
_lookups_621 = [0]


def _count_lookup_621(conn, key):
    _lookups_621[0] += 1
    return None


L._ANY_ORG_KEYS, L.BILLING_API_KEY = True, "op_key_c621"
L._lookup_org_by_key = _count_lookup_621
c621 = fresh_conn()
L._pool = _MockPool(c621)
L._limiter.reset()
try:
    _cl621 = TestClient(L.app, raise_server_exceptions=False)
    _codes_621 = [_cl621.get("/ledger/summary", headers={"X-Api-Key": f"wrong{_i}"}).status_code
                  for _i in range(25)]
    _budget_621 = int(L.RATE_LIMIT_AUTH_FAILURES.split("/")[0])
    chk("wrong keys get 401 up to the failed-auth budget, then 429",
        _codes_621 == [401] * _budget_621 + [429] * (25 - _budget_621), str(_codes_621))
    chk("…and once refused, a wrong key costs no database lookup",
        _lookups_621[0] == _budget_621, f"{_lookups_621[0]} lookups")
    _r = _cl621.get("/metrics", headers={"X-Api-Key": "wrong-again"})
    chk("/metrics shares the same budget", _r.status_code == 429, str(_r.status_code))
finally:
    L._ANY_ORG_KEYS, L.BILLING_API_KEY, L._lookup_org_by_key, L._pool = _orig_621
    L._limiter.reset()
    c621.close()


# =============================================================================
# C6-22: tenants are dispatched concurrently; one slow tenant no longer sets the pace
#     Dispatch was serial. A tenant whose downstream trickled bytes cost every other
#     tenant up to one 15 s deadline per batch, so while it had fresh rows global
#     throughput was one batch per deadline (reproduced live). Rows of different tenants
#     now go out on a bounded thread pool; one tenant's rows stay serial, in id order, and
#     every database call stays on the thread that owns the connection.
# =============================================================================
print("\n── C6-22: tenants dispatch in parallel, a tenant's rows in id order ─────")

c622 = fresh_conn()
c622_obs = psycopg2.connect(_TEST_DSN)
c622_obs.autocommit = True
with c622.cursor() as _c:
    for _o in ("c622_slow", "c622_fast"):
        _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,'whsec_t')",
                   (_o, _o))
L.process_stripe_event_batch(c622, [fake_event(eid=f"evt_c622_slow_{_i}") for _i in range(3)],
                             org_id="c622_slow")
L.process_stripe_event_batch(c622, [fake_event(eid=f"evt_c622_fast_{_i}") for _i in range(3)],
                             org_id="c622_fast")

_orig_622 = (W._dispatch_row, W._finish_row, W._release_row, W.WORKER_BATCH_SIZE,
             W.WORKER_DISPATCH_CONCURRENCY, W._rr_after)
W.WORKER_BATCH_SIZE, W.WORKER_DISPATCH_CONCURRENCY, W._rr_after = 10, 8, ""
_log_622, _fast_done_622, _slow_end_622 = [], [], []
_fast_in_db_622 = []                 # fast rows already committed when slow's last row starts
_db_threads_622 = set()
_lock_622 = threading.Lock()


def _dispatch_622(row, destination=None):
    _org = row["org_id"]
    with _lock_622:
        _log_622.append((_org, row["id"]))
    if _org == "c622_slow":
        if len([1 for _o, _ in _log_622 if _o == "c622_slow"]) == 3:
            _fast_in_db_622.append(_q1(c622_obs, "SELECT COUNT(*) FROM outbox "
                                                 "WHERE org_id='c622_fast' AND dispatched=1"))
        time.sleep(1.0)
        _slow_end_622.append(time.monotonic())
    else:
        time.sleep(0.5)
        _fast_done_622.append(time.monotonic())


def _spy_finish_622(*a, **kw):
    _db_threads_622.add(threading.current_thread())
    return _orig_622[1](*a, **kw)


def _spy_release_622(*a, **kw):
    _db_threads_622.add(threading.current_thread())
    return _orig_622[2](*a, **kw)


W._dispatch_row, W._finish_row, W._release_row = _dispatch_622, _spy_finish_622, _spy_release_622
try:
    _t0_622 = time.monotonic()
    _n_622 = W.drain_batch(c622)
    _took_622 = time.monotonic() - _t0_622
    chk("all six rows are dispatched and drain_batch still returns the count",
        _n_622 == 6 and _q1(c622, "SELECT COUNT(*) FROM outbox WHERE dispatched=1") == 6,
        str(_n_622))
    chk("the fast tenant's rows are all dispatched before the slow tenant finishes",
        len(_fast_done_622) == 3 and len(_slow_end_622) == 3
        and max(_fast_done_622) < _slow_end_622[-1],
        f"fast done at {[round(t - _t0_622, 1) for t in _fast_done_622]}, "
        f"slow done at {[round(t - _t0_622, 1) for t in _slow_end_622]}")
    chk("total drain time is the slow tenant's (~3 s), not the sum of both (~4.5 s)",
        _took_622 < 3.75, f"{_took_622:.2f}s")
    chk("a finished tenant is committed while the slow one is still dispatching",
        _fast_in_db_622 == [3], str(_fast_in_db_622))
    chk("every database write ran on the thread that called drain_batch",
        _db_threads_622 == {threading.current_thread()}, str(_db_threads_622))

    _ids_622 = {_o: [_i for _o2, _i in _log_622 if _o2 == _o] for _o in ("c622_slow", "c622_fast")}
    chk("one tenant's rows are dispatched serially, in id order",
        all(len(_v) == 3 and _v == sorted(_v) for _v in _ids_622.values()), str(_ids_622))
finally:
    (W._dispatch_row, W._finish_row, W._release_row, W.WORKER_BATCH_SIZE,
     W.WORKER_DISPATCH_CONCURRENCY, W._rr_after) = _orig_622
    c622_obs.close()
    c622.close()

# A header value http.client refuses (a control character, a character outside latin-1)
#     raised ValueError before any byte was sent, so that outbox row retried forever.
import urllib.parse  # noqa: E402

_seen_622 = []


class _HeaderSink622(_hs_615.BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        _seen_622.append({k: self.headers.get(k) for k in
                          ("X-Transaction-Id", "X-Org-Id", "X-Event-Type", "X-Injected")})
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()


_srv_622 = _hs_615.HTTPServer(("127.0.0.1", 0), _HeaderSink622)
_th_615.Thread(target=_srv_622.serve_forever, daemon=True).start()
_url_622 = f"http://127.0.0.1:{_srv_622.server_port}/hook"
try:
    _odd_622 = "evt_c622\x01\u00e9\u4e2d 100%\r\nX-Injected: 1"
    _err_622 = None
    try:
        W._dispatch_row({"id": 1, "payload": "{}", "event_type": "invoice.paid",
                         "transaction_id": _odd_622, "org_id": "c622\u00e9"}, _url_622)
    except Exception as _e:
        _err_622 = f"{type(_e).__name__}: {_e}"
    chk("a transaction_id with control / non-latin-1 characters is delivered, not raised",
        _err_622 is None and len(_seen_622) == 1, str(_err_622))
    _h_622 = _seen_622[0] if _seen_622 else {}
    chk("…the header is percent-encoded and decodes back to the original id",
        urllib.parse.unquote(_h_622.get("X-Transaction-Id") or "") == _odd_622
        and "%01" in (_h_622.get("X-Transaction-Id") or ""), str(_h_622))
    chk("…a line break in the id cannot inject a header; org id is encoded too",
        _h_622.get("X-Injected") is None
        and urllib.parse.unquote(_h_622.get("X-Org-Id") or "") == "c622\u00e9", str(_h_622))
    W._dispatch_row({"id": 2, "payload": "{}", "event_type": "invoice.paid",
                     "transaction_id": "evt_1Abc-2_x.y:z 100%", "org_id": "acme"}, _url_622)
    chk("an ordinary id is sent byte for byte (no encoding, '%' untouched)",
        _seen_622[-1]["X-Transaction-Id"] == "evt_1Abc-2_x.y:z 100%"
        and _seen_622[-1]["X-Org-Id"] == "acme" and _seen_622[-1]["X-Event-Type"] == "invoice.paid",
        str(_seen_622[-1]))
finally:
    _srv_622.shutdown()


# =============================================================================
# C6-23: API-only startup notices come from the API process, not from the worker
#     worker.py imports ledger, and ledger printed its startup notices at import. The
#     worker therefore logged "BILLING_API_KEY is empty ... UNAUTHENTICATED" about
#     endpoints it does not serve, and a "secrets loaded" line whose fingerprint came
#     from the worker's own environment, not the API's. An operator checking that a
#     rotation landed compared the wrong hash. The notices now run from the lifespan.
# =============================================================================
print("\n── C6-23: API-only startup notices are emitted by the API lifespan only ──")

import subprocess as _sp_623  # noqa: E402

_env_623 = {k: v for k, v in os.environ.items()
            if k not in ("BILLING_API_KEY", "BILLING_API_KEY_FILE", "BILLING_ENV")}
_proc_623 = _sp_623.run(
    [sys.executable, "-c", "import ledger"], cwd=os.path.dirname(os.path.abspath(__file__)),
    env=_env_623, capture_output=True, text=True, timeout=60)
_out_623 = _proc_623.stdout + _proc_623.stderr
chk("importing ledger with no BILLING_API_KEY succeeds", _proc_623.returncode == 0,
    _out_623[-300:])
chk("…and logs no UNAUTHENTICATED warning (the importer may be the worker)",
    "UNAUTHENTICATED" not in _out_623,
    "; ".join(_l[:120] for _l in _out_623.splitlines() if "UNAUTHENTICATED" in _l))
chk("…and logs no 'secrets loaded' fingerprint line",
    "secrets loaded" not in _out_623,
    "; ".join(_l[:120] for _l in _out_623.splitlines() if "secrets loaded" in _l))

_recs_623 = []


class _Capture623(logging.Handler):
    def emit(self, record):
        _recs_623.append(record.getMessage())


_orig_623 = {k: getattr(L, k) for k in ("BILLING_ENV", "INGRESS_BIND_ADDR", "BILLING_API_KEY",
                                        "_ANY_ORG_KEYS", "_pool")}
c623 = fresh_conn()
L._pool = _MockPool(c623)
L.BILLING_ENV, L.INGRESS_BIND_ADDR, L.BILLING_API_KEY, L._ANY_ORG_KEYS = "dev", "127.0.0.1", "", False
_h623 = _Capture623()
L._log.addHandler(_h623)
try:
    with TestClient(L.app) as _cl623:
        _cl623.get("/health")
finally:
    L._log.removeHandler(_h623)
    for _k, _v in _orig_623.items():
        setattr(L, _k, _v)
    c623.close()
chk("the API lifespan still warns that the ops endpoints are UNAUTHENTICATED",
    any("UNAUTHENTICATED" in m for m in _recs_623), str(_recs_623)[:300])
chk("the API lifespan still logs the secrets fingerprint line",
    any(m.startswith("secrets loaded stripe=") for m in _recs_623), str(_recs_623)[:300])


# =============================================================================
# C6-24: the interactive API docs and the OpenAPI schema are dev-only
#     FastAPI serves /docs, /redoc and /openapi.json unauthenticated by default, ahead
#     of the API key check. The schema is a free map of every ops route and the per-tenant
#     ingest path. Only a declared local dev run (BILLING_ENV=dev on a loopback ingress)
#     serves them; anything else answers 404.
# =============================================================================
print("\n── C6-24: /docs, /redoc and /openapi.json are 404 outside dev mode ──────")

_DOC_PATHS_624 = ("/docs", "/redoc", "/openapi.json")
_cl624 = TestClient(L.app, raise_server_exceptions=False)
chk("this suite runs without BILLING_ENV=dev, so docs are off in the loaded app",
    L._DOCS_ENABLED is False, str(L._DOCS_ENABLED))
_codes_624 = {p: _cl624.get(p).status_code for p in _DOC_PATHS_624}
chk("/docs, /redoc and /openapi.json → 404 when not in dev mode",
    set(_codes_624.values()) == {404}, str(_codes_624))
chk("…and they are 404 even with a valid API key (not an auth question)",
    _cl624.get("/openapi.json", headers={"X-Api-Key": L.BILLING_API_KEY}).status_code == 404)

_probe_624 = ("import ledger as L; from starlette.testclient import TestClient; "
              "c = TestClient(L.app); "
              "print({p: c.get(p).status_code for p in ('/docs', '/redoc', '/openapi.json')})")


def _probe_docs_624(env_overrides: dict) -> str:
    env = {k: v for k, v in os.environ.items() if k not in ("BILLING_ENV", "INGRESS_BIND_ADDR")}
    env.update(env_overrides)
    proc = subprocess.run([sys.executable, "-c", _probe_624],
                          cwd=os.path.dirname(os.path.abspath(__file__)),
                          env=env, capture_output=True, text=True, timeout=60)
    return (proc.stdout.strip().splitlines() or [proc.stderr[-300:]])[-1]


_dev_624 = _probe_docs_624({"BILLING_ENV": "dev", "INGRESS_BIND_ADDR": "127.0.0.1"})
chk("BILLING_ENV=dev on loopback → all three are served (200)",
    _dev_624 == "{'/docs': 200, '/redoc': 200, '/openapi.json': 200}", _dev_624)
_exposed_624 = _probe_docs_624({"BILLING_ENV": "dev", "INGRESS_BIND_ADDR": "0.0.0.0"})
chk("BILLING_ENV=dev but ingress published on 0.0.0.0 → still 404",
    _exposed_624 == "{'/docs': 404, '/redoc': 404, '/openapi.json': 404}", _exposed_624)
_prod_624 = _probe_docs_624({"BILLING_ENV": "production"})
chk("BILLING_ENV=production → 404",
    _prod_624 == "{'/docs': 404, '/redoc': 404, '/openapi.json': 404}", _prod_624)


# =============================================================================
# C6-25: the ingress cuts slow clients (slowloris) with server timeouts
#     Caddy's defaults leave header and body reads unbounded: a client dribbling one byte
#     every few seconds holds a connection indefinitely, and the per-IP rate limiter
#     never sees a finished request to count. The Caddyfile now sets global
#     read_header / read_body / idle timeouts. The suite cannot run Caddy, so this checks
#     the declaration (the Caddyfile was separately accepted by `caddy validate`).
# =============================================================================
print("\n── C6-25: Caddyfile declares header, body and idle timeouts ─────────────")

import re as _re_625  # noqa: E402

_caddy_625 = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Caddyfile"),
                  encoding="utf-8").read()
_code_625 = "\n".join(_l.split("#", 1)[0] for _l in _caddy_625.splitlines())
_m_625 = _re_625.search(r"^\{\s*servers\s*\{\s*timeouts\s*\{(?P<body>[^}]*)\}", _code_625,
                        _re_625.M | _re_625.S)
chk("a global `servers { timeouts { ... } }` block exists",
    _m_625 is not None, _code_625[:200])


def _secs_625(name: str):
    if not _m_625:
        return None
    m = _re_625.search(rf"^\s*{name}\s+(\d+)(ms|s|m)\b", _m_625.group("body"), _re_625.M)
    if not m:
        return None
    return int(m.group(1)) * {"ms": 0.001, "s": 1, "m": 60}[m.group(2)]


_rh_625, _rb_625, _idle_625 = _secs_625("read_header"), _secs_625("read_body"), _secs_625("idle")
chk("read_header is set and at most 15 s", _rh_625 is not None and _rh_625 <= 15, str(_rh_625))
chk("read_body is set and at most 60 s", _rb_625 is not None and _rb_625 <= 60, str(_rb_625))
chk("idle is set and at most 5 min", _idle_625 is not None and _idle_625 <= 300, str(_idle_625))
chk("the timeouts sit in the global block, ahead of the site block",
    bool(_m_625) and _m_625.start() < _code_625.index("{$CADDY_DOMAIN}"))


# =============================================================================
# C6-26: an event id outside printable ASCII is INVALID at ingest
#     A signature proves who sent an event, not that its id is sane. An id with a CR/LF
#     was ledgered and then failed every delivery at header-build time (the id becomes
#     the worker's X-Transaction-Id header), retrying forever; a NUL made the INSERT
#     raise a ValueError that escaped as a 500. The id is now printable ASCII without
#     spaces, 1-255 characters; anything else takes the INVALID path. Rejecting it must
#     not turn into a 503 either, so the DLQ row stores a printable rendering of the id.
# =============================================================================
print("\n── C6-26: control characters / spaces / non-ASCII / >255 in an event id ──")

c626 = fresh_conn()
_bad_ids_626 = {
    "LF":            "evt_c626\nx",
    "CRLF header injection": "evt_c626\r\nX-Evil: 1",
    "trailing LF":   "evt_c626_end\n",
    "NUL":           "evt_c626\x00x",
    "TAB":           "evt_c626\tx",
    "space":         "evt c626",
    "DEL":           "evt_c626\x7fx",
    "non-ASCII":     "evt_c626_é",
    "256 chars":     "e" * 256,
}
for _label, _bad in _bad_ids_626.items():
    try:
        _res = L.process_stripe_event(c626, fake_event(eid=_bad))
    except Exception as _exc:  # noqa: BLE001 — the point is that nothing may escape
        _res = {"outcome": f"raised {type(_exc).__name__}: {_exc}"}
    chk(f"id with {_label} → DLQ_INVALID, nothing raised", _res.get("outcome") == "DLQ_INVALID",
        str(_res)[:160])
chk("…none of them reached the ledger or the outbox",
    _q1(c626, "SELECT COUNT(*) FROM ledger") == 0 and _q1(c626, "SELECT COUNT(*) FROM outbox") == 0)
chk("…each left one INVALID DLQ row",
    _q1(c626, "SELECT COUNT(*) FROM dlq WHERE reason='INVALID'") == len(_bad_ids_626),
    str(_q1(c626, "SELECT COUNT(*) FROM dlq")))
_stored_626 = [r[0] for r in _qall(c626, "SELECT transaction_id FROM dlq ORDER BY id")]
chk("…whose transaction_id is printable ASCII, so listing and replay stay safe",
    all(L._EVENT_ID_RE.fullmatch(t) for t in _stored_626), str(_stored_626))
chk("…and the original id is preserved in raw_payload",
    json.loads(_q1(c626, "SELECT raw_payload FROM dlq ORDER BY id LIMIT 1") or "{}").get("id")
    == "evt_c626\nx")

for _label, _good in {"evt_ + alphanumerics": "evt_1Pq3Zs2eZvKYlo2C",
                      "all printable ASCII punctuation": "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~",
                      "exactly 255 chars": "e" * 255}.items():
    _res = L.process_stripe_event(c626, fake_event(eid=_good))
    chk(f"id with {_label} → still POSTED", _res.get("outcome") == "POSTED", str(_res)[:160])

# The batch path validates with the same model and writes its own DLQ rows.
_batch_626 = L.process_stripe_event_batch(
    c626, [fake_event(eid="evt_c626_batch\r\nx"), fake_event(eid="evt_c626_batch_ok")])
chk("batch: a CRLF id → DLQ_INVALID, its neighbour → POSTED",
    [r["outcome"] for r in _batch_626] == ["DLQ_INVALID", "POSTED"], str(_batch_626))

# Over HTTP: a signed event whose id carries a NUL must be answered 200 (it is recorded
#     in the DLQ), not 500 and not 503, which Stripe would retry for three days.
_SEC_C626 = "whsec_c626_signing_secret"
with c626.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
               ("c626", "c626", _SEC_C626))
_orig_pool_626 = L._pool
L._pool = _MockPool(c626)
try:
    _cl626 = TestClient(L.app, raise_server_exceptions=False)
    _ev626 = _fixture("invoice.paid", "evt_c626_http\x00x")
    _r = _post_raw(_cl626, "/webhook/stripe/c626", json.dumps(_ev626), _SEC_C626)
    chk("signed event with a NUL in its id → 200 DLQ_INVALID (not 500/503)",
        _r.status_code == 200 and _r.json().get("outcome") == "DLQ_INVALID",
        f"{_r.status_code} {_r.text[:160]}")
    chk("…recorded under the tenant, as INVALID",
        _q1(c626, "SELECT COUNT(*) FROM dlq WHERE org_id='c626' AND reason='INVALID'") == 1)
    _ok626 = _fixture("invoice.paid", "evt_c626_http_ok")
    _r = _post_raw(_cl626, "/webhook/stripe/c626", json.dumps(_ok626), _SEC_C626)
    chk("a normal Stripe-shaped id over the same route → POSTED",
        _r.status_code == 200 and _r.json().get("outcome") == "POSTED", f"{_r.status_code} {_r.text[:160]}")
finally:
    L._pool = _orig_pool_626
    c626.close()


# =============================================================================
# C6-27: a replayed event leaves one DUPLICATE row, not one per replay
#     A signed payload stays valid for Stripe's 300 s tolerance, so replaying it as fast
#     as the webhook rate limit allowed appended a DUPLICATE row per request, each a full
#     copy of the payload. The answer is unchanged (DLQ_DUPLICATE, 200); the audit trail
#     is now one row per (org_id, transaction_id), enforced by a unique index
#     (migration 0010) with ON CONFLICT DO NOTHING on the insert paths.
# =============================================================================
print("\n── C6-27: replays keep one DUPLICATE row per (org, event) ───────────────")

c627 = fresh_conn()
_dups_627 = lambda org, tid: _q1(  # noqa: E731
    c627, "SELECT COUNT(*) FROM dlq WHERE reason='DUPLICATE' AND org_id=%s AND transaction_id=%s",
    (org, tid))

_ev627 = fake_event(eid="evt_c627_replay")
_errlog_627 = []


class _ErrGrab627(logging.Handler):
    def emit(self, record):
        if record.levelno >= logging.ERROR:
            _errlog_627.append(record.getMessage()[:120])


_eg627 = _ErrGrab627()
L._log.addHandler(_eg627)
try:
    _outcomes_627 = [L.process_stripe_event(c627, _ev627)["outcome"] for _ in range(10)]
finally:
    L._log.removeHandler(_eg627)
chk("10 submissions of one event → POSTED once, DLQ_DUPLICATE for the other 9",
    _outcomes_627 == ["POSTED"] + ["DLQ_DUPLICATE"] * 9, str(_outcomes_627))
chk("…without an ERROR log per replay (a swallowed unique violation would log each one)",
    not _errlog_627, str(_errlog_627[:2]))
chk("…but only ONE DUPLICATE row", _dups_627(L.DEFAULT_ORG_ID, "evt_c627_replay") == 1,
    str(_dups_627(L.DEFAULT_ORG_ID, "evt_c627_replay")))

_batch_627 = L.process_stripe_event_batch(
    c627, [_ev627, _ev627, fake_event(eid="evt_c627_b2"), fake_event(eid="evt_c627_b2")])
chk("batch: replays of known and same-batch events are still DLQ_DUPLICATE",
    [r["outcome"] for r in _batch_627] == ["DLQ_DUPLICATE", "DLQ_DUPLICATE", "POSTED", "DLQ_DUPLICATE"],
    str(_batch_627))
chk("…and add no second row for the known event, one for the new one",
    _dups_627(L.DEFAULT_ORG_ID, "evt_c627_replay") == 1 and _dups_627(L.DEFAULT_ORG_ID, "evt_c627_b2") == 1)

# Concurrent replays: the unique index is what holds under a race, not a SELECT first.
_errs_627 = []
_barrier_627 = threading.Barrier(6)


def _replay_627():
    try:
        _cn = L._bootstrap()
        _barrier_627.wait()
        L.process_stripe_event(_cn, _ev627)
        _cn.close()
    except Exception as _exc:  # noqa: BLE001
        _errs_627.append(repr(_exc))


_ths_627 = [threading.Thread(target=_replay_627) for _ in range(6)]
for _t in _ths_627:
    _t.start()
for _t in _ths_627:
    _t.join()
chk("6 concurrent replays raise nothing and still leave one DUPLICATE row",
    not _errs_627 and _dups_627(L.DEFAULT_ORG_ID, "evt_c627_replay") == 1,
    f"{_errs_627} rows={_dups_627(L.DEFAULT_ORG_ID, 'evt_c627_replay')}")

# Over HTTP, the actual attack: one captured signed body, sent repeatedly.
_SEC_C627 = "whsec_c627_signing_secret"
for _o in ("c627a", "c627b"):
    with c627.cursor() as _c:
        _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
                   (_o, _o, _SEC_C627))
_orig_pool_627 = L._pool
L._pool = _MockPool(c627)
L._limiter.reset()
try:
    _cl627 = TestClient(L.app, raise_server_exceptions=False)
    _body627 = json.dumps(_fixture("invoice.paid", "evt_c627_http"))
    _res627 = [_post_raw(_cl627, "/webhook/stripe/c627a", _body627, _SEC_C627) for _ in range(6)]
    chk("a captured body replayed 6 times: 200 each, POSTED then DLQ_DUPLICATE x5",
        [r.status_code for r in _res627] == [200] * 6
        and [r.json().get("outcome") for r in _res627] == ["POSTED"] + ["DLQ_DUPLICATE"] * 5,
        str([(r.status_code, r.text[:60]) for r in _res627]))
    chk("…leaving one DUPLICATE row for tenant c627a", _dups_627("c627a", "evt_c627_http") == 1,
        str(_dups_627("c627a", "evt_c627_http")))
    _rb = [_post_raw(_cl627, "/webhook/stripe/c627b", _body627, _SEC_C627) for _ in range(3)]
    chk("the same id under tenant c627b is its own event: POSTED, then one DUPLICATE row",
        [r.json().get("outcome") for r in _rb] == ["POSTED", "DLQ_DUPLICATE", "DLQ_DUPLICATE"]
        and _dups_627("c627b", "evt_c627_http") == 1 and _dups_627("c627a", "evt_c627_http") == 1,
        str([r.text[:60] for r in _rb]))
    _lst = _cl627.get("/dlq/entries").json()["entries"]
    chk("…and /dlq/entries (default tenant) shows the replayed event once",
        sum(1 for e in _lst if e["transaction_id"] == "evt_c627_replay" and e["reason"] == "DUPLICATE") == 1)
finally:
    L._pool = _orig_pool_627
    L._limiter.reset()

# Migration 0010 de-duplicates what already exists before building the index.
_own = [*_ALEMBIC]
_env627 = {**os.environ, "DATABASE_URL": L.DATABASE_URL}
_down627 = subprocess.run([*_own, "downgrade", "0009"], capture_output=True, text=True, env=_env627)
chk("alembic downgrade 0009 exits 0 and drops the unique index",
    _down627.returncode == 0
    and _q1(c627, "SELECT COUNT(*) FROM pg_indexes WHERE indexname='uq_dlq_one_duplicate_per_event'") == 0,
    _down627.stderr.strip()[-300:])
with c627.cursor() as _c:
    _c.execute("TRUNCATE dlq RESTART IDENTITY")
    for _tid, _n in (("evt_c627_m1", 3), ("evt_c627_m2", 2)):
        for _i in range(_n):
            _c.execute("INSERT INTO dlq (transaction_id, reason, raw_payload, status) "
                       "VALUES (%s,'DUPLICATE',%s,'pending')", (_tid, json.dumps({"n": _i})))
    _c.execute("INSERT INTO dlq (transaction_id, reason, raw_payload) "
               "VALUES ('evt_c627_m1','INVALID','{}'), ('evt_c627_m1','INVALID','{}')")
_up627 = subprocess.run([*_own, "upgrade", "head"], capture_output=True, text=True, env=_env627)
chk("alembic upgrade head exits 0 over existing duplicate rows", _up627.returncode == 0,
    _up627.stderr.strip()[-300:])
_left627 = _qall(c627, "SELECT transaction_id, count(*), min(raw_payload) FROM dlq "
                       "WHERE reason='DUPLICATE' GROUP BY 1 ORDER BY 1")
chk("…one DUPLICATE row left per event, the oldest",
    _left627 == [("evt_c627_m1", 1, '{"n": 0}'), ("evt_c627_m2", 1, '{"n": 0}')], str(_left627))
chk("…and INVALID rows (not subject to the index) are untouched",
    _q1(c627, "SELECT COUNT(*) FROM dlq WHERE reason='INVALID'") == 2)
chk("…and the unique index exists again",
    _q1(c627, "SELECT COUNT(*) FROM pg_indexes WHERE indexname='uq_dlq_one_duplicate_per_event'") == 1)
c627.close()


# =============================================================================
# C6-28: event types the ledger does not record are UNSUPPORTED, not INVALID
#     A real Stripe account sends many types the endpoint never asked for
#     (charge.succeeded, payment_intent.succeeded, checkout.session.completed ...). Each
#     landed in the DLQ as INVALID with status 'pending': counted in dlq_depth, the
#     "something is wrong" alarm, and never pruned. They now get their own outcome and
#     reason, UNSUPPORTED: answered 200, visible in /dlq/entries, stored as a resolved
#     row so dlq_depth and `actionable` ignore it, and aged out by prune_dlq.
# =============================================================================
print("\n── C6-28: UNSUPPORTED event types are visible, quiet and pruned ─────────")

c628 = fresh_conn()
_SEC_C628 = "whsec_c628_signing_secret"
with c628.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,%s)",
               ("c628", "c628", _SEC_C628))
_orig_pool_628 = L._pool
_orig_key_628 = (L.BILLING_API_KEY, L._ANY_ORG_KEYS)
L._pool = _MockPool(c628)
L._limiter.reset()
_TYPES_628 = ["charge.succeeded", "payment_intent.succeeded", "checkout.session.completed"]
try:
    _cl628 = TestClient(L.app, raise_server_exceptions=False)
    _res628 = []
    for _i, _t in enumerate(_TYPES_628):
        _ev = {"id": f"evt_c628_{_i}", "type": _t, "object": "event", "livemode": False,
               "data": {"object": {"id": f"obj_{_i}", "object": _t.split(".")[0], "amount": 4900}}}
        _res628.append(_post_raw(_cl628, "/webhook/stripe/c628", json.dumps(_ev), _SEC_C628))
    chk("three unsupported Stripe types → 200 with outcome UNSUPPORTED",
        all(r.status_code == 200 and r.json().get("outcome") == "UNSUPPORTED" for r in _res628),
        str([(r.status_code, r.text[:80]) for r in _res628]))
    chk("…the reason names the type",
        "charge.succeeded" in _res628[0].json().get("reason", ""), _res628[0].text)
    chk("…none reached the ledger or the outbox",
        _q1(c628, "SELECT COUNT(*) FROM ledger WHERE org_id='c628'") == 0
        and _q1(c628, "SELECT COUNT(*) FROM outbox WHERE org_id='c628'") == 0)
    _rows628 = _qall(c628, "SELECT reason, status, max_retries, raw_payload FROM dlq "
                           "WHERE org_id='c628' ORDER BY id")
    chk("…each is a DLQ row: reason UNSUPPORTED, status resolved, never retried",
        len(_rows628) == 3 and all(r[:3] == ("UNSUPPORTED", "resolved", 0) for r in _rows628),
        str([r[:3] for r in _rows628]))
    chk("…with the full payload kept",
        json.loads(_rows628[0][3]).get("livemode") is False and json.loads(_rows628[0][3])["data"]["object"]["amount"] == 4900)

    # Operator views: the whole point is that these do not look like trouble.
    L.BILLING_API_KEY, L._ANY_ORG_KEYS = "", False
    L._DLQ_DEPTH.set(-1)
    _m628 = _cl628.get("/metrics").text
    chk("dlq_depth gauge stays 0 (not an alarm)", "dlq_depth 0.0" in _m628, "dlq_depth missing or non-zero")
    chk("dlq_total counts them as audit rows", "dlq_total 3.0" in _m628)
    chk("webhook_events_total carries an UNSUPPORTED outcome label",
        'webhook_events_total{outcome="UNSUPPORTED"}' in _m628)

    # /ledger/summary and /dlq/entries are scoped to the default org: replay under it.
    for _i, _t in enumerate(_TYPES_628):
        _r = L.process_stripe_event(c628, {"id": f"evt_c628_d{_i}", "type": _t, "data": {"object": {}}})
        chk(f"default-org seed event {_i} → UNSUPPORTED", _r["outcome"] == "UNSUPPORTED", str(_r))
    _sum628 = _cl628.get("/ledger/summary").json()
    chk("/ledger/summary: dlq_depth 0, dlq_total 3 (default org)",
        _sum628["dlq_depth"] == 0 and _sum628["dlq_total"] == 3, str(_sum628))
    _all628 = _cl628.get("/dlq/entries").json()["entries"]
    chk("/dlq/entries lists them with reason UNSUPPORTED",
        len(_all628) == 3 and {e["reason"] for e in _all628} == {"UNSUPPORTED"}, str(_all628)[:200])
    chk("…but ?status=actionable does not",
        _cl628.get("/dlq/entries?status=actionable").json()["entries"] == [])

    # A genuinely broken event is still the alarm it always was.
    L.process_stripe_event(c628, {"id": "evt_c628_bad", "type": "invoice.paid", "data": {}})
    chk("a supported type with a broken body is still INVALID and counted",
        _cl628.get("/ledger/summary").json()["dlq_depth"] == 1)
    chk("…an unsupported type with an unusable id is INVALID, not UNSUPPORTED",
        L.process_stripe_event(c628, {"id": "evt\nbad", "type": "charge.succeeded", "data": {}})["outcome"]
        == "DLQ_INVALID")
    chk("…and so is an event with no string type",
        L.process_stripe_event(c628, {"id": "evt_c628_notype", "data": {}})["outcome"] == "DLQ_INVALID")

    # Replays do not multiply the rows (migration 0011).
    _before628 = _q1(c628, "SELECT COUNT(*) FROM dlq WHERE reason='UNSUPPORTED'")
    _errs628 = []
    _grab628 = type("G", (logging.Handler,), {"emit": lambda self, r: _errs628.append(r.getMessage()[:100])
                                               if r.levelno >= logging.ERROR else None})()
    L._log.addHandler(_grab628)
    try:
        for _ in range(5):
            L.process_stripe_event(c628, {"id": "evt_c628_d0", "type": _TYPES_628[0], "data": {"object": {}}})
    finally:
        L._log.removeHandler(_grab628)
    chk("replaying an unsupported event 5x adds no row and logs no error",
        _q1(c628, "SELECT COUNT(*) FROM dlq WHERE reason='UNSUPPORTED'") == _before628 and not _errs628,
        f"{_before628} -> {_q1(c628, 'SELECT COUNT(*) FROM dlq WHERE reason=%s', ('UNSUPPORTED',))} {_errs628[:1]}")

    # Nothing to replay: a resolved row is terminal.
    _uid = _q1(c628, "SELECT id FROM dlq WHERE reason='UNSUPPORTED' AND org_id=%s ORDER BY id LIMIT 1",
               (L.DEFAULT_ORG_ID,))
    chk("manual retry of an UNSUPPORTED entry → 409 (resolved is terminal)",
        _cl628.post(f"/dlq/{_uid}/retry").status_code == 409)

    # A row written as INVALID before this outcome existed settles when replayed.
    with c628.cursor() as _c:
        _c.execute("INSERT INTO dlq (transaction_id, reason, raw_payload, status, max_retries) "
                   "VALUES ('evt_c628_legacy','INVALID',%s,'pending',0) RETURNING id",
                   (json.dumps({"id": "evt_c628_legacy", "type": "charge.succeeded", "data": {"object": {}}}),))
        _legacy = _c.fetchone()[0]
    _r = _cl628.post(f"/dlq/{_legacy}/retry")
    chk("a legacy pending INVALID row of an unsupported type resolves on manual retry",
        _r.status_code == 200 and _r.json().get("outcome") == "UNSUPPORTED"
        and _q1(c628, "SELECT status FROM dlq WHERE id=%s", (_legacy,)) == "resolved", _r.text[:160])
    chk("…without appending a row for the replay",
        _q1(c628, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_c628_legacy'") == 1)

    # Retention: aged out like DUPLICATE; INVALID and young rows stay.
    with c628.cursor() as _c:
        _c.execute("UPDATE dlq SET received_at = NOW() - interval '100 days' "
                   "WHERE transaction_id IN ('evt_c628_d0','evt_c628_d1','evt_c628_bad')")
    _n_unsup = _q1(c628, "SELECT COUNT(*) FROM dlq WHERE reason='UNSUPPORTED'")
    _pruned = L.prune_dlq(c628, retention_days=90)
    chk("prune_dlq deletes the 2 aged UNSUPPORTED rows, nothing else",
        _pruned == 2 and _q1(c628, "SELECT COUNT(*) FROM dlq WHERE reason='UNSUPPORTED'") == _n_unsup - 2,
        f"pruned {_pruned}")
    chk("…the aged INVALID row stays for human review",
        _q1(c628, "SELECT COUNT(*) FROM dlq WHERE transaction_id='evt_c628_bad'") == 1)

    # Batch path: same classification, one transaction.
    _b628 = L.process_stripe_event_batch(c628, [
        fake_event(eid="evt_c628_bok"),
        {"id": "evt_c628_bu", "type": "charge.refunded", "data": {"object": {}}},
        {"id": "evt_c628_bi", "type": "invoice.paid", "data": {}},
    ], org_id="c628")
    chk("batch: POSTED, UNSUPPORTED, DLQ_INVALID",
        [r["outcome"] for r in _b628] == ["POSTED", "UNSUPPORTED", "DLQ_INVALID"],
        str([r["outcome"] for r in _b628]))
    chk("…the batch UNSUPPORTED row is resolved",
        _q1(c628, "SELECT status FROM dlq WHERE transaction_id='evt_c628_bu'") == "resolved")
finally:
    L._pool = _orig_pool_628
    L.BILLING_API_KEY, L._ANY_ORG_KEYS = _orig_key_628
    L._limiter.reset()
    c628.close()


# =============================================================================
# C6-29: a slow tenant's task in flight does not delay claiming the other tenants
#     C6-22 fixed dispatch inside one batch, but drain_batch still waited for EVERY tenant
#     task before returning, so the polling loop could not claim again until the slowest
#     tenant hit its 15 s deadline. Live: a tenant trickling bytes, then 10 rows for
#     another tenant, delivered only after 15.2 s; and since the rotation gives the slow
#     tenant a slot in every claim, every batch lasted one deadline. The dispatcher keeps
#     tasks across polls, claims only tenants with no task in flight, records finished
#     tasks without waiting for the others, and releases unsent rows on SIGTERM.
# =============================================================================
print("\n── C6-29: continuous dispatch; slow tenant in flight, others still served ─")

c629 = fresh_conn()
with c629.cursor() as _c:
    for _o in ("c629_slow", "c629_fast"):
        _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES (%s,%s,'whsec_t')",
                   (_o, _o))
_orig_629 = (W._dispatch_row, W.WORKER_BATCH_SIZE, W.WORKER_DISPATCH_CONCURRENCY, W._rr_after,
             W._stop)
W.WORKER_BATCH_SIZE, W.WORKER_DISPATCH_CONCURRENCY, W._rr_after = 10, 8, ""
_hold_629, _slow_in_629 = threading.Event(), threading.Event()
_active_629, _max_active_629, _done_629 = {}, {}, []
_lock_629 = threading.Lock()


def _dispatch_629(row, destination=None):
    _o = row["org_id"]
    with _lock_629:
        _active_629[_o] = _active_629.get(_o, 0) + 1
        _max_active_629[_o] = max(_max_active_629.get(_o, 0), _active_629[_o])
    try:
        if _o.endswith("_slow"):
            _slow_in_629.set()
            _hold_629.wait(4)               # the downstream is trickling
    finally:
        with _lock_629:
            _active_629[_o] -= 1
            _done_629.append((_o, row["id"]))


def _state_629(org):
    return _q1(c629, "SELECT COUNT(*) FROM outbox WHERE org_id=%s AND dispatched=1", (org,))


def _poll_until_629(disp, cond, seconds=3.0):
    """Poll (no waiting on the poll interval) until cond() holds; returns the poll count."""
    _n, _end = 0, time.monotonic() + seconds
    while time.monotonic() < _end:
        disp.poll(c629)
        _n += 1
        if cond():
            break
        time.sleep(0.02)
    return _n


W._dispatch_row = _dispatch_629
_d629 = W._Dispatcher()
try:
    L.process_stripe_event_batch(c629, [fake_event(eid=f"evt_c629_slow_{_i}") for _i in range(2)],
                                 org_id="c629_slow")
    _d629.poll(c629)
    chk("the slow tenant's task starts and stays in flight",
        _slow_in_629.wait(3) and _d629.in_flight == 1, str(_d629.in_flight))
    # Rows that arrive AFTER the slow task started: five for the fast tenant, one more
    #     for the slow tenant (which must wait for its in-flight task, not start a second).
    L.process_stripe_event_batch(c629, [fake_event(eid=f"evt_c629_fast_{_i}") for _i in range(5)],
                                 org_id="c629_fast")
    L.process_stripe_event_batch(c629, [fake_event(eid="evt_c629_slow_late")], org_id="c629_slow")
    _t0_629 = time.monotonic()
    _polls_629 = _poll_until_629(_d629, lambda: _state_629("c629_fast") == 5)
    chk("the fast tenant is delivered and recorded while the slow task is still in flight",
        _state_629("c629_fast") == 5 and _state_629("c629_slow") == 0
        and not _hold_629.is_set() and time.monotonic() - _t0_629 < 2.5,
        f"fast={_state_629('c629_fast')} slow={_state_629('c629_slow')} "
        f"after {time.monotonic() - _t0_629:.1f}s, {_polls_629} polls")
    chk("…within a few polls, none of which waited for the slow tenant", _polls_629 <= 10,
        str(_polls_629))
    _late = _q1(c629, "SELECT claimed_until IS NULL AND dispatched = 0 FROM outbox "
                      "WHERE transaction_id='evt_c629_slow_late'")
    chk("the slow tenant's new row is not claimed while its task is in flight", _late is True,
        str(_late))
    _hold_629.set()
    _poll_until_629(_d629, lambda: _state_629("c629_slow") == 3 and _d629.in_flight == 0)
    chk("once the slow task ends its rows are recorded; the late row follows in a new task",
        _state_629("c629_slow") == 3 and _d629.in_flight == 0, str(_state_629("c629_slow")))
    chk("a tenant never has two tasks dispatching at once",
        _max_active_629 == {"c629_slow": 1, "c629_fast": 1}, str(_max_active_629))
    _slow_ids = [_i for _o, _i in _done_629 if _o == "c629_slow"]
    chk("…and its rows went out in id order across the two tasks",
        len(_slow_ids) == 3 and _slow_ids == sorted(_slow_ids), str(_slow_ids))

    # SIGTERM while a task is mid-row: the row in flight finishes, the unsent rows come
    #     back untried (lease cleared, no attempt counted), and nothing new is claimed.
    _hold_629.clear()
    _slow_in_629.clear()
    L.process_stripe_event_batch(c629, [fake_event(eid=f"evt_c629_term_{_i}") for _i in range(3)],
                                 org_id="c629_slow")
    _d629.poll(c629)
    _slow_in_629.wait(3)
    W._stop = True
    L.process_stripe_event_batch(c629, [fake_event(eid="evt_c629_after_stop")], org_id="c629_fast")
    _d629.poll(c629)
    _hold_629.set()
    _d629.finish(c629, 5)
    with c629.cursor() as _c:
        _c.execute("SELECT COUNT(*) FILTER (WHERE dispatched = 1), "
                   "COUNT(*) FILTER (WHERE dispatched = 0 AND claimed_until IS NULL "
                   "AND attempts = 0) FROM outbox WHERE transaction_id LIKE 'evt_c629_term_%'")
        _term_629 = _c.fetchone()
    chk("SIGTERM: the row in flight finishes; the two unsent rows are released untried",
        _term_629 == (1, 2), str(_term_629))
    _after = _q1(c629, "SELECT claimed_until IS NULL AND dispatched = 0 FROM outbox "
                       "WHERE transaction_id='evt_c629_after_stop'")
    chk("…and a row that arrives once stopping has begun is not claimed", _after is True,
        str(_after))

    # The same through run_loop itself (its own connection, a real stop): a new tenant's
    #     rows are delivered while the slow task is mid-row, and stopping releases the rest.
    _hold_629.clear()
    _slow_in_629.clear()
    _orig_poll_629 = W.WORKER_POLL_INTERVAL
    W._stop, W.WORKER_POLL_INTERVAL = False, 0.2
    with c629.cursor() as _c:
        _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES "
                   "('c629_loop_slow','x','whsec_t'), ('c629_loop_fast','x','whsec_t')")
    L.process_stripe_event_batch(c629, [fake_event(eid=f"evt_c629_ls_{_i}") for _i in range(3)],
                                 org_id="c629_loop_slow")
    _loop_conn_629 = W._connect()
    _loop_629 = threading.Thread(target=W.run_loop, args=(_loop_conn_629,), name="c629-loop")
    _loop_629.start()
    try:
        _slow_in_629.wait(3)
        L.process_stripe_event_batch(c629, [fake_event(eid=f"evt_c629_lf_{_i}") for _i in range(3)],
                                     org_id="c629_loop_fast")
        _end_629 = time.monotonic() + 3
        while time.monotonic() < _end_629 and _state_629("c629_loop_fast") < 3:
            time.sleep(0.05)
        chk("run_loop delivers a new tenant's rows while another tenant's task is mid-row",
            _state_629("c629_loop_fast") == 3 and _state_629("c629_loop_slow") == 0,
            f"fast={_state_629('c629_loop_fast')} slow={_state_629('c629_loop_slow')}")
    finally:
        W._stop = True
        _hold_629.set()
        _loop_629.join(15)
        W._stop, W.WORKER_POLL_INTERVAL = False, _orig_poll_629
        _loop_conn_629.close()
    with c629.cursor() as _c:
        _c.execute("SELECT COUNT(*) FILTER (WHERE dispatched = 1), "
                   "COUNT(*) FILTER (WHERE dispatched = 0 AND claimed_until IS NULL "
                   "AND attempts = 0) FROM outbox WHERE transaction_id LIKE 'evt_c629_ls_%'")
        _loop_state_629 = _c.fetchone()
    chk("…and on stop run_loop exits after recording: one row sent, two released untried",
        not _loop_629.is_alive() and _loop_state_629 == (1, 2),
        f"alive={_loop_629.is_alive()} state={_loop_state_629}")
finally:
    _hold_629.set()
    (W._dispatch_row, W.WORKER_BATCH_SIZE, W.WORKER_DISPATCH_CONCURRENCY, W._rr_after,
     W._stop) = _orig_629
    _d629._pool.shutdown(wait=True, cancel_futures=True)
    c629.close()


# =============================================================================
# C6-30: every error response is recorded with a class and a type
#     Seven red nodes in the architecture diagram did not say whether the error was
#     recorded, and measuring them found two holes: a 413 never reached the observability
#     middleware (it sat inside the body cap: http_requests_total delta 0, no http_request
#     line) and neither did a 500 from an unhandled exception (ServerErrorMiddleware builds
#     that response outside every BaseHTTPMiddleware). The middleware is now pure ASGI and
#     the outermost layer. Each error path below is driven through the real app and must
#     move http_errors_total{status,error_class,error_type} by exactly one, move
#     http_requests_total by one, and leave exactly one http_request line with the same
#     class and type. None may land in the unclassified fallback.
# =============================================================================
print("\n── C6-30: error responses carry error_class and error_type ──────────────")

import ast as _ast_630  # noqa: E402
from prometheus_client import REGISTRY as _REG_630  # noqa: E402


class _Lines630(logging.Handler):
    """Collects the http_request records the middleware emits."""

    def __init__(self):
        super().__init__(logging.INFO)
        self.records = []

    def emit(self, record):
        if getattr(record, "event", None) == "http_request":
            self.records.append(record)


def _req_630(method, path, status):
    return _REG_630.get_sample_value("http_requests_total",
                                     {"method": method, "path": path, "status": str(status)}) or 0


def _err_630(status, error_class, error_type):
    return _REG_630.get_sample_value(
        "http_errors_total",
        {"status": str(status), "error_class": error_class, "error_type": error_type}) or 0


def _err_samples_630():
    """{(status, class, type): value} for every http_errors_total series."""
    return {(s.labels["status"], s.labels["error_class"], s.labels["error_type"]): s.value
            for m in L._HTTP_ERRORS.collect() for s in m.samples if s.name == "http_errors_total"}


_lines_630 = _Lines630()
logging.getLogger("ledger").addHandler(_lines_630)
_seen_types_630: set = set()


def _probe_630(label, send, status, error_class, error_type, method, path):
    """Send one request and check what it left behind."""
    _lines_630.records.clear()
    _before_630 = _err_samples_630()
    _req_before = _req_630(method, path, status)
    _r = send()
    _after_630 = _err_samples_630()
    _moved = {k: _after_630.get(k, 0) - _before_630.get(k, 0) for k in _after_630
              if _after_630.get(k, 0) != _before_630.get(k, 0)}
    _want = (str(status), error_class, error_type)
    chk(f"{label}: answered {status}", _r.status_code == status, f"got {_r.status_code}")
    chk(f"{label}: http_errors_total{{{status},{error_class},{error_type}}} +1, nothing else moved",
        _moved == {_want: 1}, str(_moved))
    chk(f"{label}: http_requests_total{{{method},{path},{status}}} +1",
        _req_630(method, path, status) - _req_before == 1,
        str(_req_630(method, path, status) - _req_before))
    _recs = _lines_630.records
    chk(f"{label}: exactly one http_request line, with status, error_class and error_type",
        len(_recs) == 1 and (_recs[0].status, getattr(_recs[0], "error_class", None),
                             getattr(_recs[0], "error_type", None)) == (status, error_class, error_type),
        str([(r.status, getattr(r, "error_class", None), getattr(r, "error_type", None))
             for r in _recs]))
    _seen_types_630.add(error_type)


c630 = fresh_conn()
with c630.cursor() as _c:
    _c.execute("INSERT INTO org (org_id, name, stripe_webhook_secret) VALUES "
               "('c630_t','c630_t','whsec_c630')")
    _c.execute("INSERT INTO dlq (org_id, transaction_id, reason, raw_payload, status) "
               "VALUES ('default','evt_c630_done','INVALID','{}','resolved') RETURNING id")
    _resolved_id_630 = _c.fetchone()[0]

_orig_630 = (L._pool, L.BILLING_API_KEY, L._ANY_ORG_KEYS, L._lookup_org_by_key)
L._pool = _MockPool(c630)
L._limiter.reset()
_cl630 = TestClient(L.app, raise_server_exceptions=False)
try:
    # --- 400: the same body for three causes, three different types in the record ----
    _b, _h = _signed(fake_event(eid="evt_c630_sig"), "whsec_the_wrong_secret")
    _probe_630("bad signature", lambda: _cl630.post("/webhook/stripe/c630_t", content=_b, headers=_h),
               400, "authentication", "invalid_signature", "POST", "/webhook/stripe/{org_id}")
    _probe_630("unknown tenant",
               lambda: _cl630.post("/webhook/stripe/c630_nobody", content=_b, headers=_h),
               400, "authentication", "unknown_tenant", "POST", "/webhook/stripe/{org_id}")
    _probe_630("malformed tenant id",
               lambda: _cl630.post("/webhook/stripe/bad.id", content=_b, headers=_h),
               400, "validation", "malformed_tenant_id", "POST", "/webhook/stripe/{org_id}")
    _bad_sig = _cl630.post("/webhook/stripe/c630_t", content=_b, headers=_h)
    _unknown = _cl630.post("/webhook/stripe/c630_nobody", content=_b, headers=_h)
    chk("…and the caller cannot tell them apart: same body for a bad signature and an unknown tenant",
        _bad_sig.text == _unknown.text and _bad_sig.status_code == _unknown.status_code,
        f"{_bad_sig.text!r} vs {_unknown.text!r}")

    # --- 401 / 403 / 429 on the auth path --------------------------------------------
    L.BILLING_API_KEY, L._ANY_ORG_KEYS = "c630_operator_key", False
    L._limiter.reset()
    _probe_630("no key", lambda: _cl630.get("/ledger/summary"),
               401, "authentication", "missing_api_key", "GET", "/ledger/summary")
    _probe_630("wrong key", lambda: _cl630.get("/ledger/summary", headers={"X-Api-Key": "nope"}),
               401, "authentication", "invalid_api_key", "GET", "/ledger/summary")
    _probe_630("wrong key on /metrics",
               lambda: _cl630.get("/metrics", headers={"X-Api-Key": "nope"}),
               401, "authentication", "invalid_api_key", "GET", "/metrics")
    L._limiter.reset()
    _budget_630 = int(L.RATE_LIMIT_AUTH_FAILURES.split("/")[0])
    for _i in range(_budget_630):
        _cl630.get("/ledger/summary", headers={"X-Api-Key": f"spend{_i}"})
    _probe_630("failed-auth budget spent",
               lambda: _cl630.get("/ledger/summary", headers={"X-Api-Key": "one-more"}),
               429, "rate_limit", "auth_failure_budget", "GET", "/ledger/summary")
    L._limiter.reset()
    L._ANY_ORG_KEYS, L._lookup_org_by_key = True, lambda conn, key: "c630_t"
    _probe_630("tenant key on /metrics",
               lambda: _cl630.get("/metrics", headers={"X-Api-Key": "a-tenant-key"}),
               403, "authorization", "operator_key_required", "GET", "/metrics")
    L.BILLING_API_KEY, L._ANY_ORG_KEYS, L._lookup_org_by_key = _orig_630[1], _orig_630[2], _orig_630[3]

    # --- 429 from slowapi's per-route limit -------------------------------------------
    L._limiter.reset()
    _ops_limit_630 = int(L.RATE_LIMIT_OPS.split("/")[0])
    for _i in range(_ops_limit_630):
        _cl630.get("/ledger/summary")
    _probe_630("per-route rate limit", lambda: _cl630.get("/ledger/summary"),
               429, "rate_limit", "rate_limited", "GET", "/ledger/summary")
    L._limiter.reset()

    # --- 413 and the 422s -------------------------------------------------------------
    _probe_630("body over the cap",
               lambda: _cl630.post("/webhook/stripe", content=b"x" * (L.MAX_REQUEST_BODY_BYTES + 1),
                                   headers={"Content-Type": "application/json"}),
               413, "size_limit", "body_too_large", "POST", "unmatched")
    _probe_630("envelope missing fields",
               lambda: _cl630.post("/webhook/stripe", json={}),
               422, "validation", "request_validation", "POST", "/webhook/stripe")
    _probe_630("body that is not JSON",
               lambda: _cl630.post("/webhook/stripe", content=b"{not json",
                                   headers={"Content-Type": "application/json"}),
               422, "validation", "request_validation", "POST", "/webhook/stripe")
    _probe_630("query parameter of the wrong type",
               lambda: _cl630.get("/dlq/entries?limit=abc"),
               422, "validation", "invalid_query", "GET", "/dlq/entries")
    _probe_630("limit below 1", lambda: _cl630.get("/dlq/entries?limit=0"),
               422, "validation", "invalid_query", "GET", "/dlq/entries")
    _probe_630("unknown status filter", lambda: _cl630.get("/dlq/entries?status=bogus"),
               422, "validation", "invalid_query", "GET", "/dlq/entries")
    _probe_630("path parameter of the wrong type", lambda: _cl630.post("/dlq/abc/retry"),
               422, "validation", "invalid_query", "POST", "/dlq/{dlq_id}/retry")

    # --- 404 / 405 / 409 --------------------------------------------------------------
    _probe_630("unknown dlq id", lambda: _cl630.post("/dlq/987654321/retry"),
               404, "not_found", "dlq_entry_not_found", "POST", "/dlq/{dlq_id}/retry")
    _probe_630("resolved dlq entry", lambda: _cl630.post(f"/dlq/{_resolved_id_630}/retry"),
               409, "conflict", "dlq_already_resolved", "POST", "/dlq/{dlq_id}/retry")
    _probe_630("route that does not exist", lambda: _cl630.get("/c630/no/such/route"),
               404, "not_found", "route_not_found", "GET", "unmatched")
    _probe_630("wrong method", lambda: _cl630.get("/webhook/stripe"),
               405, "validation", "method_not_allowed", "GET", "/webhook/stripe")

    # --- 503 x3 and 500 ---------------------------------------------------------------
    class _DeadPool630:
        def __init__(self, exc):
            self._exc = exc

        def getconn(self):
            raise self._exc

        def putconn(self, conn):
            pass

    L._pool = _DeadPool630(psycopg2.OperationalError("c630: server closed the connection"))
    _probe_630("database connection lost", lambda: _cl630.get("/ledger/summary"),
               503, "dependency", "database_unavailable", "GET", "/ledger/summary")
    L._pool = _DeadPool630(L._PoolBusy())
    _probe_630("pool exhausted", lambda: _cl630.get("/ledger/summary"),
               503, "dependency", "pool_exhausted", "GET", "/ledger/summary")
    L._pool = _MockPool(c630)
    with patch("stripe.Webhook.construct_event"), \
         patch("ledger.process_stripe_event", side_effect=RuntimeError("c630: DB write failed")):
        _probe_630("ingest write failed",
                   lambda: _cl630.post("/webhook/stripe", json=fake_event(eid="evt_c630_db")),
                   503, "dependency", "ingest_db_failure", "POST", "/webhook/stripe")
    with patch("stripe.Webhook.construct_event"), \
         patch("ledger.process_stripe_event", side_effect=ValueError("c630: not handled anywhere")):
        _probe_630("unhandled exception in a handler",
                   lambda: _cl630.post("/webhook/stripe", json=fake_event(eid="evt_c630_500")),
                   500, "internal", "unhandled_exception", "POST", "/webhook/stripe")

    # --- coverage of the taxonomy itself ----------------------------------------------
    chk("every type in the taxonomy was driven through the real app above",
        _seen_types_630 == set(L._ERROR_TYPES), str(sorted(set(L._ERROR_TYPES) ^ _seen_types_630)))
    _unclassified = {k: v for k, v in _err_samples_630().items() if k[1] == "unclassified"}
    chk("no known error path ended in the unclassified fallback", _unclassified == {},
        str(_unclassified))
    _classes_630 = {"authentication", "authorization", "validation", "size_limit", "rate_limit",
                    "not_found", "conflict", "dependency", "internal"}
    chk("every type maps to one of the nine classes", set(L._ERROR_TYPES.values()) <= _classes_630,
        str(set(L._ERROR_TYPES.values()) - _classes_630))

    # --- the fallback is visible when a path has no type ------------------------------
    from fastapi import HTTPException as _HTTPException_630  # noqa: E402

    def _teapot_630():
        raise _HTTPException_630(status_code=418, detail="untyped")

    def _gone_630():
        raise _HTTPException_630(status_code=500, detail="untyped")

    _routes_630 = [L.app.add_api_route("/c630-untyped-4xx", _teapot_630) or L.app.router.routes[-1],
                   L.app.add_api_route("/c630-untyped-5xx", _gone_630) or L.app.router.routes[-1]]
    try:
        _probe_630("a 4xx raised with no type", lambda: _cl630.get("/c630-untyped-4xx"),
                   418, "unclassified", "unclassified_4xx", "GET", "/c630-untyped-4xx")
        _probe_630("a 5xx raised with no type", lambda: _cl630.get("/c630-untyped-5xx"),
                   500, "unclassified", "unclassified_5xx", "GET", "/c630-untyped-5xx")
    finally:
        for _rt in _routes_630:
            L.app.router.routes.remove(_rt)
    _seen_types_630.discard("unclassified_4xx")
    _seen_types_630.discard("unclassified_5xx")

    # --- cardinality: nothing the caller controls reaches a label ----------------------
    _hostile = "evil" + "A" * 200 + "<script>"
    _cl630.post(f"/webhook/stripe/{_hostile}", content=_b, headers=_h)
    _cl630.get(f"/{_hostile}/route")
    _label_text = " ".join(f"{k[0]} {k[1]} {k[2]}" for k in _err_samples_630())
    chk("a hostile path and tenant id appear in no http_errors_total label",
        "evil" not in _label_text and "script" not in _label_text, _label_text[:200])
    _allowed_types = set(L._ERROR_TYPES) | {"unclassified_4xx", "unclassified_5xx"}
    chk("every error_type label ever recorded comes from the fixed set",
        {k[2] for k in _err_samples_630()} <= _allowed_types,
        str({k[2] for k in _err_samples_630()} - _allowed_types))

    # --- the middleware on its own: the two shapes a TestClient cannot produce ---------
    import asyncio as _asyncio_630  # noqa: E402

    _scope_630 = {"type": "http", "method": "GET", "path": "/c630-direct", "headers": [],
                  "client": ("203.0.113.7", 4321)}

    async def _recv_630():
        return {"type": "http.disconnect"}

    async def _send_630(message):
        pass

    async def _no_response_630(scope, receive, send):
        return None

    async def _raises_630(scope, receive, send):
        raise ValueError("c630: escaped every handler")

    _lines_630.records.clear()
    _pre_630 = (_req_630("GET", "unmatched", 200), sum(_err_samples_630().values()))
    _asyncio_630.run(L._Observability(_no_response_630)(dict(_scope_630), _recv_630, _send_630))
    chk("a request that ends with no response and no exception records nothing",
        not _lines_630.records and sum(_err_samples_630().values()) == _pre_630[1], "")
    _before_exc = _err_630(500, "internal", "unhandled_exception")
    _raised_630 = None
    try:
        _asyncio_630.run(L._Observability(_raises_630)(dict(_scope_630), _recv_630, _send_630))
    except ValueError as _e:
        _raised_630 = _e
    chk("an exception that escapes the app is recorded as a 500 and re-raised for uvicorn's log",
        isinstance(_raised_630, ValueError)
        and _err_630(500, "internal", "unhandled_exception") - _before_exc == 1
        and len(_lines_630.records) == 1 and _lines_630.records[0].status == 500,
        f"raised={_raised_630!r} lines={len(_lines_630.records)}")
    _lines_630.records.clear()
    _asyncio_630.run(L._Observability(_no_response_630)({"type": "lifespan"}, _recv_630, _send_630))
    chk("a non-HTTP scope (lifespan) passes through untouched", not _lines_630.records)

    # --- structure: no error can be raised without a type -------------------------------
    _tree_630 = _ast_630.parse(open(L.__file__, encoding="utf-8").read())
    _bare_630, _typed_630, _bad_630 = [], 0, []
    for _node in _ast_630.walk(_tree_630):
        if isinstance(_node, _ast_630.Call) and isinstance(_node.func, _ast_630.Name):
            if _node.func.id == "HTTPException":
                _bare_630.append(_node.lineno)
            elif _node.func.id == "_ApiError":
                _typed_630 += 1
                _lit = _node.args[2] if len(_node.args) > 2 else None
                _forwarded = isinstance(_lit, _ast_630.Name) and _lit.id == "error_type"
                if not _forwarded and not (isinstance(_lit, _ast_630.Constant)
                                           and _lit.value in L._ERROR_TYPES):
                    _bad_630.append(_node.lineno)
            elif _node.func.id == "_auth_failed":
                _lit = _node.args[2] if len(_node.args) > 2 else None
                if not (isinstance(_lit, _ast_630.Constant) and _lit.value in L._ERROR_TYPES):
                    _bad_630.append(_node.lineno)
    chk("ledger.py never constructs a bare HTTPException (the line numbers, if it does)",
        _bare_630 == [], str(_bare_630))
    chk("every _ApiError and _auth_failed call names a type that is in the taxonomy",
        _typed_630 >= 10 and _bad_630 == [], f"{_typed_630} _ApiError calls, bad lines {_bad_630}")
finally:
    L._pool, L.BILLING_API_KEY, L._ANY_ORG_KEYS, L._lookup_org_by_key = _orig_630
    L._limiter.reset()
    logging.getLogger("ledger").removeHandler(_lines_630)
    c630.close()


# =============================================================================
# C6-31: the ingress writes an access log that cannot leak a credential, and marks
#     the errors it produces itself with the application's class and type names
#     Caddy had no log directive, so a 413 or a 502 left no line. Its default log writes
#     every request header except Authorization and Cookie: measured live, X-Api-Key and
#     Stripe-Signature came out in plaintext, and a request with a Host that matches no
#     site went through a second, unfiltered logger. The Caddyfile is checked as text here
#     (this suite does not start Caddy); the behaviour was measured against the real
#     caddy:2-alpine image, and the evidence is described in the README and docs/testing.md.
# =============================================================================
print("\n── C6-31: Caddyfile access log, header filter, error class and type ─────")

import re as _re_631  # noqa: E402

_caddy_631 = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "Caddyfile"),
                  encoding="utf-8").read()
_caddy_code_631 = "\n".join(_ln for _ln in _caddy_631.splitlines() if not _ln.strip().startswith("#"))
_filters_631 = _re_631.findall(r"format filter \{\s*wrap json\s*request>headers delete\s*\}",
                               _caddy_code_631)
chk("the site log writes JSON to stdout", _re_631.search(
    r"log \{\s*output stdout\s*format filter", _caddy_code_631) is not None)
chk("the site log AND the default logger delete request>headers (X-Api-Key, Stripe-Signature)",
    len(_filters_631) == 2, f"{len(_filters_631)} filter blocks")
_pairs_631 = _re_631.findall(
    r"handle_errors (\d+)\s*\{\s*log_append error_class (\w+)\s*log_append error_type (\w+)\s*\}",
    _caddy_code_631)
chk("Caddy marks the errors it produces itself: 413 and 502",
    {p[0] for p in _pairs_631} == {"413", "502"}, str(_pairs_631))
chk("a class and type Caddy shares with the application match the application's table",
    all(L._ERROR_TYPES.get(_t, _c) == _c for _s, _c, _t in _pairs_631)
    and ("413", "size_limit", "body_too_large") in _pairs_631,
    str(_pairs_631))
chk("every class Caddy uses is one of the application's classes",
    {_c for _s, _c, _t in _pairs_631} <= _classes_630, str(_pairs_631))
chk("the timeouts that cut slow clients are unchanged",
    "read_header 10s" in _caddy_code_631 and "read_body   30s" in _caddy_code_631)


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
