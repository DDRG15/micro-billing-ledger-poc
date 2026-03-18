"""
test_ledger.py — smoke tests for ledger.py (stdlib only, no pytest required)
=============================================================================
Run: python test_ledger.py
"""

import json
import sqlite3
import sys
import time
import pathlib

# Make sure ledger.py is importable without FastAPI/uvicorn at test time.
# We only import the framework-agnostic core.
import importlib, types

# Stub FastAPI so ledger.py imports cleanly even without the package installed.
for mod in ["fastapi", "fastapi.responses", "pydantic", "uvicorn"]:
    sys.modules.setdefault(mod, types.ModuleType(mod))

# Minimal stubs
_fastapi = sys.modules["fastapi"]
_fastapi.FastAPI     = lambda **kw: types.SimpleNamespace(
    post=lambda *a, **k: (lambda f: f),
    get =lambda *a, **k: (lambda f: f),
)
_fastapi.HTTPException = Exception
_fastapi.Request       = object
_fastapi.status        = types.SimpleNamespace(HTTP_200_OK=200)
sys.modules["fastapi.responses"].JSONResponse = dict
sys.modules["pydantic"].BaseModel = object
sys.modules["pydantic"].Field     = lambda *a, **k: None

import ledger as L   # noqa: E402  (import after stub setup)

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
) -> dict:
    return {
        "id":   eid,
        "type": etype,
        "data": {"object": {
            "customer":    customer,
            "amount_paid": amount,
            "currency":    "usd",
        }},
        "request": {"idempotency_key": f"idem_{eid}"},
    }


# =============================================================================
# Tests
# =============================================================================

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

# Unknown event type
r = L.process_stripe_event(conn2, fake_event(eid="evt_x", etype="payment.created"))
chk("unknown type → DLQ_UNKNOWN_TYPE", r["outcome"] == "DLQ_UNKNOWN_TYPE")

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
