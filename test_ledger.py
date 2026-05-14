"""
test_ledger.py — smoke tests for ledger.py (Phase 1: Pydantic Validation)
=========================================================================
Run: python test_ledger.py
"""

import io
import json
import sqlite3
import sys
import time
import pathlib

# Force UTF-8 output on Windows so Unicode separators print correctly
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")

import ledger as L   # Direct import — all dependencies installed

# ── helpers ───────────────────────────────────────────────────────────────────

ok = fail = 0

def chk(name: str, cond: bool, detail: str = "") -> None:
    global ok, fail
    if cond:
        ok   += 1
        print(f"  PASS  {name}")
    else:
        fail += 1
        print(f"  FAIL  {name}" + (f"  |  {detail}" if detail else ""))


def fresh_conn() -> sqlite3.Connection:
    return L._bootstrap(pathlib.Path(":memory:"))


def fake_event(
    eid: str = "evt_test_001",
    etype: str = "invoice.paid",
    customer: str = "cus_123",
    amount: int = 4900,
    currency: str = "usd",
    include_request: bool = True,
) -> dict:
    """Factory for fake Stripe events matching StripeEvent Pydantic structure."""
    event = {
        "id": eid,
        "type": etype,
        "data": {"object": {
            "customer": customer,
            "amount_paid": amount,
            "currency": currency,
        }},
    }
    if include_request:
        event["request"] = {"idempotency_key": f"idem_{eid}"}
    return event


# =============================================================================
# Tests
# =============================================================================

print("\n── Phase 1: Pydantic Validation (Entry Boundary) ────────────────────────")

conn = fresh_conn()
ev   = fake_event()

r1 = L.process_stripe_event(conn, ev)
chk("valid event → POSTED",  r1["outcome"] == "POSTED")
chk("transaction_id returned", r1["transaction_id"] == "evt_test_001")


print("\n── Phase 1: Currency Validation (Regex Pattern) ────────────────────────")

conn = fresh_conn()

# Valid currency (lowercase, 3 chars)
ev_valid = fake_event(currency="usd")
r = L.process_stripe_event(conn, ev_valid)
chk("currency 'usd' → POSTED", r["outcome"] == "POSTED")

# Invalid currency (uppercase)
ev_uppercase = fake_event(eid="evt_invalid_curr_1", currency="USD")
r = L.process_stripe_event(conn, ev_uppercase)
chk("currency 'USD' (uppercase) → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")

# Invalid currency (wrong length)
ev_short = fake_event(eid="evt_invalid_curr_2", currency="us")
r = L.process_stripe_event(conn, ev_short)
chk("currency 'us' (2 chars) → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")


print("\n── Phase 1: Customer ID Validation (Min Length) ────────────────────────")

conn = fresh_conn()

# Valid customer ID (min 4 chars)
ev_valid = fake_event(customer="cus_123")
r = L.process_stripe_event(conn, ev_valid)
chk("customer 'cus_123' → POSTED", r["outcome"] == "POSTED")

# Invalid customer ID (too short)
ev_short = fake_event(eid="evt_short_cus", customer="cus")
r = L.process_stripe_event(conn, ev_short)
chk("customer 'cus' (3 chars) → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")

# Invalid customer ID (empty)
ev_empty = fake_event(eid="evt_empty_cus", customer="")
r = L.process_stripe_event(conn, ev_empty)
chk("customer '' (empty) → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")


print("\n── Phase 1: Amount Validation (Fallback + Non-Negative) ────────────────")

conn = fresh_conn()

# Valid: amount_paid provided
ev_amount_paid = fake_event(eid="evt_amt_1", amount=5000)
r = L.process_stripe_event(conn, ev_amount_paid)
chk("amount_paid 5000 → POSTED", r["outcome"] == "POSTED")

# Invalid: negative amount_paid
ev_negative = fake_event(eid="evt_negative", amount=-5000)
r = L.process_stripe_event(conn, ev_negative)
chk("amount_paid -5000 → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")

# Valid: fallback to amount field (when amount_paid is None)
ev_amount_fallback = {
    "id": "evt_amount_fallback",
    "type": "invoice.paid",
    "data": {"object": {
        "customer": "cus_fallback",
        "amount_paid": None,  # Try fallback
        "amount": 3000,
        "currency": "usd",
    }},
    "request": {},
}
r = L.process_stripe_event(conn, ev_amount_fallback)
chk("amount (fallback from amount_paid) → POSTED", r["outcome"] == "POSTED")

# Invalid: both amount_paid and amount missing
ev_no_amount = {
    "id": "evt_no_amount",
    "type": "invoice.paid",
    "data": {"object": {
        "customer": "cus_no_amount",
        "amount_paid": None,
        "amount": None,
        "currency": "usd",
    }},
    "request": {},
}
r = L.process_stripe_event(conn, ev_no_amount)
chk("both amount fields None → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")


print("\n── Phase 1: EventType Enum Validation ─────────────────────────────────")

conn = fresh_conn()

# Valid: supported event types
ev_valid_type = fake_event(etype="invoice.paid")
r = L.process_stripe_event(conn, ev_valid_type)
chk("event type 'invoice.paid' → POSTED", r["outcome"] == "POSTED")

# Invalid: unsupported event type
ev_invalid_type = fake_event(eid="evt_bad_type", etype="payment.created")
r = L.process_stripe_event(conn, ev_invalid_type)
chk("event type 'payment.created' → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")


print("\n── Phase 1: Missing Required Fields ───────────────────────────────────")

conn = fresh_conn()

# Missing 'id'
ev_no_id = {
    "id": "",
    "type": "invoice.paid",
    "data": {"object": {"customer": "cus_123", "amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_id)
chk("empty id → DLQ_INVALID", r["outcome"] == "DLQ_INVALID")

# Missing 'type'
ev_no_type = {
    "id": "evt_no_type",
    "type": "",
    "data": {"object": {"customer": "cus_123", "amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_type)
chk("empty type → DLQ_INVALID", r["outcome"] == "DLQ_INVALID")

# Missing 'data.object.customer'
ev_no_customer = {
    "id": "evt_no_cus",
    "type": "invoice.paid",
    "data": {"object": {"amount_paid": 5000, "currency": "usd"}},
}
r = L.process_stripe_event(conn, ev_no_customer)
chk("missing customer → DLQ_INVALID", r["outcome"] == "DLQ_INVALID")


print("\n── Idempotency guard ────────────────────────────────────────────────")

conn = fresh_conn()
ev   = fake_event()

r1 = L.process_stripe_event(conn, ev)
chk("first insert → POSTED",  r1["outcome"] == "POSTED")
chk("transaction_id returned", r1["transaction_id"] == "evt_test_001")

r2 = L.process_stripe_event(conn, ev)   # replay
chk("duplicate → DLQ_DUPLICATE",       r2["outcome"] == "DLQ_DUPLICATE")
chk("DLQ row written",
    conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0] == 1)

# Ledger must still have exactly 1 row (not 2)
ledger_count = conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
chk("ledger has exactly 1 row after replay", ledger_count == 1)


print("\n── Outbox written atomically ────────────────────────────────────────")

outbox_count = conn.execute("SELECT COUNT(*) FROM outbox WHERE dispatched=0").fetchone()[0]
chk("outbox has 1 pending row",    outbox_count == 1)

row = conn.execute(
    "SELECT transaction_id, event_type FROM outbox WHERE dispatched=0"
).fetchone()
chk("outbox row has correct tx_id",    row[0] == "evt_test_001")
chk("outbox row has correct event_type", row[1] == "invoice.paid")


print("\n── Validation & DLQ routing ─────────────────────────────────────────")

conn2 = fresh_conn()

# Missing id
r = L.process_stripe_event(conn2, {"id": "", "type": "invoice.paid", "data": {}})
chk("empty id → DLQ_INVALID",      r["outcome"] == "DLQ_INVALID")

# Unknown event type (now caught by Pydantic Enum validation as DLQ_INVALID, not DLQ_UNKNOWN_TYPE)
r = L.process_stripe_event(conn2, fake_event(eid="evt_x", etype="payment.created"))
chk("unknown type → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']} instead of DLQ_INVALID")

dlq_count = conn2.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
chk("DLQ has 2 rows",  dlq_count == 2)


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
    chk(f"{etype} → {expected_status}", r["outcome"] == expected_status,
        f"got {r['outcome']}")


print("\n── Headless benchmark ───────────────────────────────────────────────")

N    = 5_000
conn4 = fresh_conn()

import random, string

def _fake(i):
    return fake_event(
        eid=f"evt_{''.join(random.choices(string.ascii_lowercase, k=12))}_{i}",
        etype=random.choice(list(L.EventType)).value,
        customer=f"cus_{i}",
        amount=random.randint(100, 50_000),
    )

events = [_fake(i) for i in range(N)]
t0 = time.perf_counter()
for ev in events:
    L.process_stripe_event(conn4, ev)
elapsed = time.perf_counter() - t0
tps = N / elapsed
chk(f"throughput ≥ 2,000 TPS  (got {tps:,.0f})", tps >= 2_000)
print(f"         {N:,} events in {elapsed:.3f}s  →  {tps:,.0f} TPS  "
      f"(single-core, SQLite in-memory)")

# =============================================================================
print("\n── Phase 2: DLQEntry model ──────────────────────────────────────────────")

from pydantic import ValidationError as PydanticValidationError

# Valid DLQEntry builds correctly
entry = L.DLQEntry(
    transaction_id="evt_test_001",
    reason=L.DLQReason.DUPLICATE,
    raw_payload={"id": "evt_test_001"},
)
chk("DLQEntry builds with valid data", entry.transaction_id == "evt_test_001")
chk("DLQEntry reason is enum value",  entry.reason == L.DLQReason.DUPLICATE)

# to_db() returns the right 4-tuple
db_row = entry.to_db()
chk("DLQEntry.to_db() is 4-tuple",         len(db_row) == 4)
chk("to_db() reason is string not enum",   db_row[1] == "DUPLICATE")
chk("to_db() payload is JSON string",      db_row[2] == '{"id": "evt_test_001"}')
chk("to_db() received_at is float",        isinstance(db_row[3], float))

# DLQReason rejects unknown values
try:
    L.DLQEntry(
        transaction_id="evt_x",
        reason="TYPO",           # not in enum
        raw_payload={},
    )
    chk("DLQEntry rejects unknown reason", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("DLQEntry rejects unknown reason", True)

# transaction_id must not be empty
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

# Valid LedgerEntry builds and serializes
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
chk("LedgerEntry builds with valid data",    le.transaction_id == "evt_le_001")
chk("LedgerEntry status is LedgerStatus",   le.status == L.LedgerStatus.POSTED)

# to_db() returns 9-tuple with string enum values
db_row = le.to_db()
chk("LedgerEntry.to_db() is 9-tuple",          len(db_row) == 9)
chk("to_db() event_type is string not enum",   db_row[1] == "invoice.paid")
chk("to_db() status is string not enum",       db_row[5] == "POSTED")
chk("to_db() amount_cents is int",             db_row[3] == 4900)
chk("to_db() created_at is float",             db_row[8] == 1234567890.0)

# amount_cents cannot be negative
try:
    L.LedgerEntry(
        transaction_id="evt_neg",
        event_type=L.EventType.INVOICE_PAID,
        customer_id="cus_abc",
        amount_cents=-1,
        currency="usd",
        status=L.LedgerStatus.POSTED,
        idempotency_key="idem_neg",
        payload="{}",
        created_at=0.0,
    )
    chk("LedgerEntry rejects negative amount_cents", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects negative amount_cents", True)

# currency must match ISO pattern
try:
    L.LedgerEntry(
        transaction_id="evt_curr",
        event_type=L.EventType.INVOICE_PAID,
        customer_id="cus_abc",
        amount_cents=100,
        currency="USD",     # uppercase — invalid
        status=L.LedgerStatus.POSTED,
        idempotency_key="idem_curr",
        payload="{}",
        created_at=0.0,
    )
    chk("LedgerEntry rejects uppercase currency", False, "should have raised ValidationError")
except (PydanticValidationError, ValueError):
    chk("LedgerEntry rejects uppercase currency", True)

# customer_id min_length=4
try:
    L.LedgerEntry(
        transaction_id="evt_cus",
        event_type=L.EventType.INVOICE_PAID,
        customer_id="cus",   # 3 chars — too short
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

conn5 = fresh_conn()

# Process valid event then replay to produce DUPLICATE
ev_dup = fake_event(eid="evt_dup_check")
L.process_stripe_event(conn5, ev_dup)
L.process_stripe_event(conn5, ev_dup)   # replay → DUPLICATE

# Process invalid event to produce INVALID
ev_bad = fake_event(eid="evt_invalid_check", currency="WRONG")
L.process_stripe_event(conn5, ev_bad)

rows = conn5.execute(
    "SELECT reason FROM dlq ORDER BY id"
).fetchall()
reasons = [r[0] for r in rows]
chk("DB DLQ has 2 rows",                        len(reasons) == 2)
chk("First DLQ row reason is DUPLICATE",        reasons[0] == "DUPLICATE")
chk("Second DLQ row reason is INVALID",         reasons[1] == "INVALID")


print("\n── Phase 2: LedgerStatus enum coverage ──────────────────────────────────")

chk("LedgerStatus.POSTED value",   L.LedgerStatus.POSTED.value == "POSTED")
chk("LedgerStatus.PENDING value",  L.LedgerStatus.PENDING.value == "PENDING")
chk("LedgerStatus.VOID value",     L.LedgerStatus.VOID.value == "VOID")

# Status map produces VOID for invoice.payment_failed
conn6 = fresh_conn()
ev_failed = fake_event(eid="evt_failed_001", etype="invoice.payment_failed")
r = L.process_stripe_event(conn6, ev_failed)
chk("invoice.payment_failed → VOID", r["outcome"] == "VOID", f"got {r['outcome']}")

ev_sub_updated = fake_event(eid="evt_sub_upd", etype="customer.subscription.updated")
r = L.process_stripe_event(conn6, ev_sub_updated)
chk("subscription.updated → PENDING", r["outcome"] == "PENDING", f"got {r['outcome']}")


print("\n── Phase 5: Integration — HTTP layer (TestClient) ──────────────────────")

from starlette.testclient import TestClient

# Patch module-level connection so HTTP tests use in-memory DB, not billing_ledger.db
http_conn = L._bootstrap(pathlib.Path(":memory:"))
L._conn = http_conn
client = TestClient(L.app)

# Health check
r = client.get("/health")
chk("GET /health → 200", r.status_code == 200)
chk("health response is ok", r.json().get("status") == "ok")

# Valid event through HTTP
ev_http = fake_event(eid="evt_http_001", customer="cus_http001", amount=9900)
r = client.post("/webhook/stripe", json=ev_http)
chk("POST valid event → HTTP 200",       r.status_code == 200)
chk("valid event → POSTED via HTTP",     r.json()["outcome"] == "POSTED",
    f"got {r.json()}")

# Duplicate via HTTP → still 200 (Stripe needs 200 to stop retrying)
r = client.post("/webhook/stripe", json=ev_http)
chk("POST duplicate → HTTP 200 (not 4xx)",      r.status_code == 200)
chk("duplicate → DLQ_DUPLICATE via HTTP",        r.json()["outcome"] == "DLQ_DUPLICATE",
    f"got {r.json()}")

# Invalid currency via HTTP → DLQ_INVALID
ev_bad_curr = fake_event(eid="evt_http_bad_curr", currency="USD")
r = client.post("/webhook/stripe", json=ev_bad_curr)
chk("POST uppercase currency → HTTP 200",        r.status_code == 200)
chk("uppercase currency → DLQ_INVALID via HTTP", r.json()["outcome"] == "DLQ_INVALID",
    f"got {r.json()}")

# $0 invoice via HTTP → cross-field validator fires
ev_zero = fake_event(eid="evt_http_zero", amount=0)
r = client.post("/webhook/stripe", json=ev_zero)
chk("POST $0 invoice → HTTP 200",               r.status_code == 200)
chk("$0 invoice → DLQ_INVALID via HTTP",         r.json()["outcome"] == "DLQ_INVALID",
    f"got {r.json()}")

# Bad customer prefix via HTTP
ev_bad_cus = fake_event(eid="evt_http_bad_cus", customer="notacus_001")
r = client.post("/webhook/stripe", json=ev_bad_cus)
chk("POST bad customer prefix → HTTP 200",       r.status_code == 200)
chk("bad prefix → DLQ_INVALID via HTTP",         r.json()["outcome"] == "DLQ_INVALID",
    f"got {r.json()}")

# Ledger summary endpoint
r = client.get("/ledger/summary")
chk("GET /ledger/summary → 200", r.status_code == 200)
summary = r.json()
chk("summary has 'ledger' key",         "ledger" in summary)
chk("summary has 'dlq_depth' key",      "dlq_depth" in summary)
chk("summary has 'outbox_pending' key", "outbox_pending" in summary)
chk("summary dlq_depth is 4",           summary["dlq_depth"] == 4,
    f"got {summary['dlq_depth']}")
chk("summary outbox_pending is 1",      summary["outbox_pending"] == 1,
    f"got {summary['outbox_pending']}")


print("\n── Phase 5: Concurrent insertion (threading) ────────────────────────────")

import threading
import tempfile
import os

# Each thread must have its own connection — shared connection + BEGIN = nested tx error.
# Use a temp file so multiple connections can point at the same database.
tmp_db = pathlib.Path(tempfile.mktemp(suffix=".db"))
L._bootstrap(tmp_db).close()   # create schema once

ev_concurrent      = fake_event(eid="evt_concurrent_001",
                                customer="cus_concurrent", amount=5000)
results_concurrent = []
errors_concurrent  = []

def _fire():
    try:
        conn = L._bootstrap(tmp_db)      # own connection per thread
        r = L.process_stripe_event(conn, ev_concurrent)
        results_concurrent.append(r["outcome"])
        conn.close()
    except Exception as exc:
        errors_concurrent.append(str(exc))

threads = [threading.Thread(target=_fire) for _ in range(5)]
for t in threads:
    t.start()
for t in threads:
    t.join()

# Read final ledger count before cleanup
verify_conn = L._bootstrap(tmp_db)
ledger_rows = verify_conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
verify_conn.close()

# Cleanup temp files
for suffix in ("", "-wal", "-shm"):
    try:
        pathlib.Path(str(tmp_db) + suffix).unlink()
    except FileNotFoundError:
        pass

chk("no exceptions from 5 concurrent inserts", len(errors_concurrent) == 0,
    str(errors_concurrent))
chk("exactly 1 POSTED outcome",      results_concurrent.count("POSTED") == 1,
    f"POSTED count: {results_concurrent.count('POSTED')}")
chk("remaining 4 are DLQ_DUPLICATE", results_concurrent.count("DLQ_DUPLICATE") == 4,
    f"DUPLICATE count: {results_concurrent.count('DLQ_DUPLICATE')}")
chk("ledger has exactly 1 row after 5 concurrent inserts", ledger_rows == 1,
    f"got {ledger_rows}")


print("\n── Phase 5: Outbox dispatch simulation ──────────────────────────────────")

dispatch_conn = L._bootstrap(pathlib.Path(":memory:"))
L.process_stripe_event(
    dispatch_conn,
    fake_event(eid="evt_dispatch_001", customer="cus_dispatch", amount=7500)
)

pending_before = dispatch_conn.execute(
    "SELECT COUNT(*) FROM outbox WHERE dispatched=0"
).fetchone()[0]
chk("outbox has 1 pending row before dispatch", pending_before == 1)

# Simulate downstream worker flipping dispatched=1
dispatch_conn.execute(
    "UPDATE outbox SET dispatched=1 WHERE transaction_id='evt_dispatch_001'"
)
dispatch_conn.commit()

pending_after = dispatch_conn.execute(
    "SELECT COUNT(*) FROM outbox WHERE dispatched=0"
).fetchone()[0]
chk("outbox has 0 pending rows after dispatch", pending_after == 0)

# Replay the same event — idempotency holds even after outbox dispatch
r = L.process_stripe_event(
    dispatch_conn,
    fake_event(eid="evt_dispatch_001", customer="cus_dispatch", amount=7500)
)
chk("replay after dispatch → DLQ_DUPLICATE (idempotency holds)",
    r["outcome"] == "DLQ_DUPLICATE", f"got {r['outcome']}")

ledger_rows = dispatch_conn.execute("SELECT COUNT(*) FROM ledger").fetchone()[0]
chk("ledger still has 1 row after replay post-dispatch", ledger_rows == 1,
    f"got {ledger_rows}")


print("\n── Phase 5: DLQ queryability and payload preservation ───────────────────")

dlq_conn = L._bootstrap(pathlib.Path(":memory:"))

# Three distinct invalid events → three DLQ entries
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_1", currency="WRONG"))
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_2", amount=0))
L.process_stripe_event(dlq_conn, fake_event(eid="evt_dlq_3", customer=""))

rows = dlq_conn.execute(
    "SELECT transaction_id, reason, raw_payload FROM dlq ORDER BY id"
).fetchall()

chk("DLQ has 3 rows",                    len(rows) == 3,   f"got {len(rows)}")
chk("all 3 reasons are INVALID",
    all(r[1] == "INVALID" for r in rows))
chk("raw_payload is a JSON string",
    all(isinstance(r[2], str) for r in rows))

parsed = [json.loads(r[2]) for r in rows]
chk("payload evt_dlq_1 id preserved",    parsed[0].get("id") == "evt_dlq_1")
chk("payload evt_dlq_2 id preserved",    parsed[1].get("id") == "evt_dlq_2")
chk("payload evt_dlq_3 id preserved",    parsed[2].get("id") == "evt_dlq_3")


print("\n── Phase 3: Cross-field validator — invoice amount > 0 ──────────────────")

conn7 = fresh_conn()

# invoice.paid with $0 must be rejected
ev_zero_invoice = fake_event(eid="evt_zero_paid", etype="invoice.paid", amount=0)
r = L.process_stripe_event(conn7, ev_zero_invoice)
chk("invoice.paid with amount=0 → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")

# invoice.payment_failed with $0 must also be rejected
ev_zero_failed = {
    "id": "evt_zero_failed",
    "type": "invoice.payment_failed",
    "data": {"object": {
        "customer": "cus_abc123",
        "amount_paid": 0,
        "currency": "usd",
    }},
}
r = L.process_stripe_event(conn7, ev_zero_failed)
chk("invoice.payment_failed with amount=0 → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")

# subscription events with amount=0 are fine — they are lifecycle events, not payments
ev_sub_zero = {
    "id": "evt_sub_zero",
    "type": "customer.subscription.created",
    "data": {"object": {
        "customer": "cus_sub001",
        "amount_paid": 0,
        "currency": "usd",
    }},
}
r = L.process_stripe_event(conn7, ev_sub_zero)
chk("subscription.created with amount=0 → POSTED (lifecycle event, not payment)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")

# invoice.paid with amount > 0 still works
ev_valid_invoice = fake_event(eid="evt_valid_paid", etype="invoice.paid", amount=4900)
r = L.process_stripe_event(conn7, ev_valid_invoice)
chk("invoice.paid with amount=4900 → POSTED", r["outcome"] == "POSTED",
    f"got {r['outcome']}")


print("\n── Phase 3: Cross-field validator — customer ID format ──────────────────")

conn8 = fresh_conn()

# Customer without 'cus_' prefix must be rejected
ev_bad_cus_fmt = fake_event(eid="evt_bad_cus_fmt", customer="abc1234567")
r = L.process_stripe_event(conn8, ev_bad_cus_fmt)
chk("customer 'abc1234567' (no cus_ prefix) → DLQ_INVALID",
    r["outcome"] == "DLQ_INVALID", f"got {r['outcome']}")

# Customer with correct 'cus_' prefix passes
ev_good_cus = fake_event(eid="evt_good_cus", customer="cus_abc123")
r = L.process_stripe_event(conn8, ev_good_cus)
chk("customer 'cus_abc123' → POSTED", r["outcome"] == "POSTED",
    f"got {r['outcome']}")

# Edge case: exactly 'cus_' with no suffix — still 4 chars, passes length but...
# 'cus_' = 4 chars, passes min_length=4, but is it a real ID?
# We accept it — the format validator only checks the prefix, not minimum suffix length.
ev_bare_prefix = fake_event(eid="evt_bare_cus", customer="cus_")
r = L.process_stripe_event(conn8, ev_bare_prefix)
chk("customer 'cus_' (prefix only, 4 chars) → POSTED (prefix valid, suffix optional)",
    r["outcome"] == "POSTED", f"got {r['outcome']}")


print("\n── Phase 3: Cross-field validators fire together ────────────────────────")

conn9 = fresh_conn()

# Both cross-field rules violated: invoice.paid + $0 + bad customer prefix
# Pydantic collects ALL errors — the error report will mention both
ev_double_bad = {
    "id": "evt_double_bad",
    "type": "invoice.paid",
    "data": {"object": {
        "customer": "notacustomer",  # no cus_ prefix
        "amount_paid": 0,            # zero amount for invoice
        "currency": "usd",
    }},
}
r = L.process_stripe_event(conn9, ev_double_bad)
chk("invoice.paid + $0 + bad customer → DLQ_INVALID", r["outcome"] == "DLQ_INVALID",
    f"got {r['outcome']}")
chk("reason mentions validation failures",
    "validation" in r.get("reason", "").lower() or "Pydantic" in r.get("reason", ""),
    f"got reason: {r.get('reason')}")


print(f"\n{'='*60}")
print(f"  {ok} passed | {fail} failed")
if fail:
    sys.exit(1)
