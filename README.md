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
