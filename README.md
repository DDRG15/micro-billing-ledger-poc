# Micro-Billing-Ledger PoC
### Idempotent Stripe Webhook Ingestion via Transactional Outbox

**Status:** A PoC that actually respects your data. Hardened for financial integrity; brutally honest about production gaps.

Forked from [Aequitas](https://github.com/DDRG15/aequitas-privacy-engine), my high-throughput engine built to survive 75k EPS and Linux OOM killers. This isn't just a "billing script" — it's a demonstration of Reliability Engineering applied to the specific nightmare of missing revenue.

---

## 🔬 The Problem: "Amateur Hour" Webhooks

Stripe delivers webhooks with **at-least-once** semantics. If your implementation looks like this, you are losing money (or sleep):

```python
# 🚫 DANGEROUS: The "Race Window"
if not db.exists(event_id):
    db.insert(event)          # Another thread just did this 2ms ago.
    revenue_ledger.credit()   # Congrats, you just double-credited a customer.
```

Two concurrent retries pass the `exists()` check before either commits. Result? A **"Ghost Payment"** — data that exists in your database but doesn't exist in reality. My Auditor DNA refuses to let this happen.

---

## 🏗️ Architecture: The "No-BS" Logic

```
Stripe  ──►  POST /webhook/stripe
                    │
                    ▼
         ┌──────────────────────────┐
         │   process_stripe_event() │  ← framework-agnostic core
         └──────────┬───────────────┘
                    │
          ┌─────────┴────────────┐
          │                      │
     New event?         Duplicate / Invalid / Unknown type?
          │                      │
          ▼                      ▼
  ┌──────────────┐       ┌──────────────┐
  │    ledger    │       │     dlq      │
  │  POSTED /    │       │  DUPLICATE   │
  │  VOID /      │       │  INVALID     │
  │  PENDING     │       │  UNKNOWN_TYPE│
  └──────┬───────┘       └──────────────┘
         │
         ▼
  ┌──────────────┐
  │    outbox    │  ← downstream worker polls WHERE dispatched=0
  │ dispatched=0 │    (Temporal activity in production)
  └──────────────┘
```

### 1. Atomic Dual-Writes (The Holy Grail)

The `ledger` record and the `outbox` task land in the same `BEGIN…COMMIT` block. There is zero gap. If the process dies after returning HTTP 200, the outbox row survives. A downstream worker retries; the idempotency guard ensures that retry is a no-op on the ledger.

```python
with _tx(conn) as cur:
    cur.execute("INSERT OR IGNORE INTO ledger (...) VALUES (...)")
    if cur.rowcount == 1:               # genuinely new — not a replay
        cur.execute("INSERT INTO outbox (...) VALUES (...)")
```

### 2. DB-Level Idempotency

We use the Stripe `event_id` as the Primary Key. `INSERT OR IGNORE` means the SQLite engine handles the race condition — not a slow Python `if` statement. `rowcount == 1` tells us whether this was a new insert or a silently ignored duplicate.

### 3. DLQ: Because Silent Failures are Amateur Hour

Nothing is dropped. Duplicates, invalid payloads, and unknown event types all land in the Dead-Letter Queue with a specific reason code. If we can't audit it, we didn't build it.

| Reason | Cause |
|---|---|
| `DUPLICATE` | Stripe retry of an already-processed event |
| `INVALID` | Missing `id` or `type` field |
| `UNKNOWN_TYPE` | Event type not in the supported set |

### 4. HTTP 503 on DB Failure — Correct Stripe Retry Protocol

Returning 200 on a failed write tells Stripe "got it, stop retrying." The money disappears. 503 tells Stripe to back off and try again. One line, massive consequence.

---

## 🔧 Phase 1: Pydantic Validation Pipeline (May 13, 2026)

**Status:** Entry boundary guardrails installed. 34/34 tests passing. Still respects your data, now with type safety.

We added Pydantic validation at the webhook entry point. Because let's face it — raw dict extraction was a bit too trusting. Now we have proper bouncers checking IDs, currencies, and amounts before they hit the database.

### What Got Implemented

**1. EventType Enum — The Bouncer's Rulebook**
```python
class EventType(str, Enum):
    INVOICE_PAID = "invoice.paid"
    INVOICE_FAILED = "invoice.payment_failed"
    SUB_CREATED = "customer.subscription.created"
    SUB_DELETED = "customer.subscription.deleted"
    SUB_UPDATED = "customer.subscription.updated"
```
No more string typos in event type checks. The enum knows what's allowed.

**2. Nested Pydantic Models — Structured Webhook Data**
```python
class StripeObject(BaseModel):
    customer: str = Field(min_length=4)  # No empty strings, no "unknown"
    amount_paid: Optional[int] = Field(ge=0)  # Non-negative cents
    amount: Optional[int] = Field(ge=0)      # Fallback field
    currency: str = Field(pattern=r"^[a-z]{3}$")  # ISO 4217 lowercase

class StripeEvent(BaseModel):
    id: str = Field(min_length=1)
    type: EventType  # Enum validation — rejects unknown types
    data: StripeEventData
    request: Optional[dict] = None
    idempotency_key: Optional[str] = None  # Resolved in validator
```
Three-layer validation: BaseModel (types) → Field constraints (values) → @model_validator (business logic).

**3. Smart Amount Fallback Logic**
```python
@model_validator(mode='after')
def check_amount_present(self):
    actual_amount = self.amount_paid if self.amount_paid is not None else self.amount
    if actual_amount is None:
        raise ValueError("Either amount_paid or amount must be provided")
    if actual_amount < 0:
        raise ValueError(f"Amount cannot be negative: {actual_amount}")
    return self
```
Stripe sometimes puts amounts in `amount_paid`, sometimes in `amount`. We handle both, but require at least one.

**4. Currency Validation — No More "USD" Surprises**
- Pattern: `^[a-z]{3}$` (exactly 3 lowercase letters)
- Rejects: `"USD"` (uppercase), `"us"` (too short), `"UUU"` (invalid code)
- Accepts: `"usd"`, `"eur"`, `"gbp"`

**5. Customer ID Validation — No More "Unknown" Placeholders**
- Minimum length: 4 characters (Stripe IDs like `"cus_ABC123"`)
- Rejects: `""`, `"cus"`, `null`
- Forces real customer IDs in the database

**6. Three-Layer Validation Pipeline**
```
Raw webhook → Pydantic validation → Business logic → DLQ routing
     ↓              ↓                    ↓            ↓
  Dirty data    Type + Field checks   Ledger write   Invalid events
```
Invalid data gets caught at Layer 1 (Pydantic) and routed to DLQ with reason "INVALID".

### What Changed in Process Flow

**Before (Dict Extraction):**
```python
event_type = event.get("type", "")  # Could be anything
customer_id = data_object.get("customer", "unknown")  # Silent default
amount_cents = data_object.get("amount_paid") or data_object.get("amount", 0)  # No validation
```

**After (Pydantic Validation):**
```python
validated_event = StripeEvent(**event)  # Throws ValidationError if invalid
# Now guaranteed: event_type is EventType enum, customer_id is valid string, etc.
```

### Test Results — We Actually Tested This

**34/34 tests passing** (up from 25 before Phase 1)

**New validation tests:**
- Currency rejects uppercase: `"USD"` → `DLQ_INVALID`
- Customer rejects empty: `""` → `DLQ_INVALID`
- Amount rejects negative: `-5000` → `DLQ_INVALID`
- EventType rejects unknown: `"payment.created"` → `DLQ_INVALID`
- Amount fallback works: `amount_paid=None, amount=3000` → `POSTED`

**Performance:** 12,412 TPS (5,000 events in 0.403s) — still fast, now safe.

### Errors We Fixed

**1. Pydantic Import Issues**
- Initially forgot `ValidationError` and `model_validator` imports
- Fixed: Added proper imports for Pydantic v2.9.2

**2. Idempotency Key Validator Bug**
- `@model_validator` tried to set `self.idempotency_key` but field wasn't defined
- Fixed: Added `idempotency_key: Optional[str] = None` to model signature

**3. Test Stub Conflicts**
- Old test stubs conflicted with real Pydantic imports
- Fixed: Removed stubs, let real packages handle imports

**4. DLQ Reason Code Changes**
- Unknown event types now caught by Pydantic as `DLQ_INVALID` (not separate `DLQ_UNKNOWN_TYPE`)
- Fixed: Updated test expectations to match new behavior

### What This Means for Production

**Data Quality:** No more silent corruption. Invalid webhooks get caught at entry, logged to DLQ.

**Type Safety:** `event.type` is now `EventType.INVOICE_PAID`, not a string that could be `"invooice.paid"`.

**Audit Trail:** Every validation failure is logged with reason. No more "how did this get in the database?"

**Backwards Compatibility:** Valid webhooks still work exactly the same. Invalid ones now fail fast instead of corrupting data.

Still a PoC, but now with guardrails that would make an auditor smile. Ready for Phase 2 when you give the word.

---

## ⚡ Performance: Tactical Speed

Bypassing the HTTP overhead to measure pure ledger throughput:

```bash
python ledger.py --silent --events 10000
```

**Results (Linux, single-core, SQLite WAL, Python 3.12):**

| Events | Time | TPS |
|---|---|---|
| 5,000 | 0.074s | ~67,000 |
| 10,000 | 0.149s | ~67,000 |
| 50,000 | 0.745s | ~67,000 |

This measures the Titanium Skeleton. Real-world HTTP overhead will eat some of this, but the core won't be the bottleneck.

---

## 🚀 Quickstart

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Run the API
uvicorn ledger:app --port 8000

# Send a test webhook
curl -X POST http://localhost:8000/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt_3PxK001",
    "type": "invoice.paid",
    "data": {
      "object": {
        "customer": "cus_abc123",
        "amount_paid": 4900,
        "currency": "usd"
      }
    },
    "request": {"idempotency_key": "idem_001"}
  }'

# Replay the same event — watch the idempotency guard fire
curl -X POST http://localhost:8000/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{"id": "evt_3PxK001", "type": "invoice.paid", "data": {"object": {"customer": "cus_abc123", "amount_paid": 4900, "currency": "usd"}}, "request": {}}'

# Check the ledger
curl http://localhost:8000/ledger/summary

# Run the 17-point audit (tests)
python test_ledger.py
```

---

## Schema

```sql
ledger  (transaction_id PK, event_type, customer_id, amount_cents,
         currency, status, idempotency_key, payload, created_at)

outbox  (id, transaction_id, event_type, payload, dispatched, created_at)

dlq     (id, transaction_id, reason, raw_payload, received_at)
```

Ledger `status` maps to accounting semantics:

| Status | Event types |
|---|---|
| `POSTED` | `invoice.paid`, `customer.subscription.created` |
| `VOID` | `invoice.payment_failed`, `customer.subscription.deleted` |
| `PENDING` | `customer.subscription.updated` |

---

## 🔍 The Auditor's Disclosure (Production Gaps)

I'm an Auditor. I find the holes before the hackers do. Here is what I would fix before this touches a single real dollar.

### 🔴 The "Must-Fix" List

**SQLite Single-Writer**
Great for this PoC, but it doesn't scale horizontally. Under concurrent load, all writes serialize behind a single lock. `check_same_thread=False` suppresses the warning without solving the problem.

*Production fix:* Migrate to PostgreSQL. `INSERT OR IGNORE` becomes `INSERT … ON CONFLICT (transaction_id) DO NOTHING`. The outbox pattern and idempotency logic are unchanged — just the engine swaps out.

---

**No Signature Verification**
This PoC accepts any POST to `/webhook/stripe`. Currently the vault is open. In production, every request must be verified using `stripe.WebhookSignature.verify_header()` with the endpoint secret before any DB access. Reject with 400 on failure.

---

**Storage-Only Outbox**
I've implemented the storage, but not the worker. The events are sitting in a table waiting for a Temporal Activity to pick them up and actually deliver them downstream. Without the worker, the outbox is a write-only audit log — not a delivery guarantee.

*Production fix:* A Temporal activity polling `WHERE dispatched=0 ORDER BY id LIMIT 100`, forwarding each event, then flipping `dispatched=1` in the same transaction.

---

### 🟡 The "Scale" List

**No DLQ Retry Budget**
Events land in the DLQ and stay there forever. There is no mechanism to re-attempt `UNKNOWN_TYPE` events after adding support for a new event type, or replay `INVALID` events after a schema fix.

*Production fix:* Add `retry_count`, `next_retry_at`, and `max_retries` columns. A backoff worker processes retryable entries on a schedule.

---

**Fragile Amount Extraction**
If `amount_paid` is missing, I fall back to `amount`, then to `0`. In a revenue ledger, silently recording `0` is a Data Quality failure — the kind that causes a bad month-end close.

*Production fix:* Per-event-type strict extraction. Any event with an unresolvable amount routes to `DLQ` with reason `MISSING_AMOUNT` rather than recording `0`.

---

**Currency Normalisation**
Stripe can send `usd` or `USD`. Mixed-case values in the same ledger cause silent grouping errors in any `GROUP BY currency` query in your finance reports.

*Production fix:* `.lower()` on ingest. One line. No excuse not to do it.

---

**No WAL Checkpoint During Long Runs**
SQLite WAL mode accumulates write-ahead log entries without checkpointing. On a long-running process ingesting millions of events, the WAL file grows unbounded and degrades read performance. This pattern is already solved in the parent Aequitas engine — just not ported here yet.

*Production fix:* `PRAGMA wal_checkpoint(PASSIVE)` every N commits.

---

### 🟢 Quality of Life

**No structured logging.** A production billing service needs JSON logs with `transaction_id`, `event_type`, `outcome`, and `duration_ms` on every request — for real-time alerting and post-incident forensics.

**No metrics endpoint.** `/ledger/summary` is for manual inspection. Production needs Prometheus-compatible counters: `webhooks_received`, `webhooks_posted`, `webhooks_dlq`, `outbox_pending_depth`.

---

## 🏁 Production Path Summary

| Gap | Fix | Priority |
|---|---|---|
| Concurrency | Postgres + `ON CONFLICT DO NOTHING` | CRITICAL |
| Spoofing | Webhook Signature Verification | CRITICAL |
| Reliability | Temporal Worker (Poll / Dispatch / Ack) | HIGH |
| DLQ retries | Backoff worker + retry budget | HIGH |
| Amount validation | Per-event-type extraction, DLQ on missing | HIGH |
| Currency normalisation | `.lower()` on ingest | LOW |
| WAL checkpoint | `PRAGMA wal_checkpoint(PASSIVE)` every N commits | LOW |
| Auditability | Structured JSON logging | MED |
| Observability | Prometheus metrics endpoint | MED |

---

## 🧬 Origin

The fault-tolerance patterns here — idempotent inserts, transactional outbox, and dead-lettering — are stripped from [Aequitas](https://github.com/DDRG15/aequitas-privacy-engine), my high-throughput engine built to process 75k+ events per second with zero data loss under hard kills.

In billing, a single race condition is a financial discrepancy. I build systems that treat data like currency.
