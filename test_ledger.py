"""
test_ledger.py — smoke tests for ledger.py (Phase 1: Pydantic Validation)
=========================================================================
Run: python test_ledger.py
"""

import json
import sqlite3
import sys
import time
import pathlib

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
        etype=random.choice(list(L.SUPPORTED_EVENT_TYPES)),
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
print(f"\n{'='*60}")
print(f"  {ok} passed | {fail} failed")
if fail:
    sys.exit(1)
