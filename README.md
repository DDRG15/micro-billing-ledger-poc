# Micro-Billing-Ledger PoC
### Idempotent Stripe Webhook Ingestion — PostgreSQL · Pydantic v2 · FastAPI

**Stack:** Python 3.12 · FastAPI · Pydantic v2 · psycopg2 · PostgreSQL 16 · Docker Compose  
**Tests:** 101 / 101 — no mocks, no stubs, every assertion hits a live PostgreSQL database  
**Throughput:** ~5,000 TPS (batch) · 500+ TPS (single-event) · both on Docker-on-Windows localhost

---

## The Problem

Stripe delivers webhooks with **at-least-once** guarantees. If your ingestion layer looks like this, you are double-counting revenue:

```python
# WRONG — classic race window
if not db.exists(event_id):
    db.insert(event)          # another thread just did this 2ms ago
    revenue_ledger.credit()   # ghost payment: credited twice, exists in DB, not in reality
```

Two concurrent retries pass the `exists()` check before either commits. The result is a **Ghost Payment** — a ledger row that exists in your database but doesn't correspond to a real financial event. Recovery requires manual forensics.

---

## Quick Start

```bash
# 1. Start PostgreSQL
docker compose up -d postgres

# 2. Install Python dependencies
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 3. Run the full test suite — 101 tests, live PostgreSQL, no mocks
python test_ledger.py

# 4. Start the API
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/billing \
  uvicorn ledger:app --port 8000
```

### Send test webhooks

```bash
# Post a valid invoice
curl -X POST http://localhost:8000/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{
    "id": "evt_001",
    "type": "invoice.paid",
    "data": {
      "object": { "customer": "cus_abc123", "amount_paid": 4900, "currency": "usd" }
    }
  }'
# → {"outcome":"POSTED","transaction_id":"evt_001","reason":null}

# Replay the same event — idempotency guard fires
curl -X POST http://localhost:8000/webhook/stripe \
  -H "Content-Type: application/json" \
  -d '{"id":"evt_001","type":"invoice.paid","data":{"object":{"customer":"cus_abc123","amount_paid":4900,"currency":"usd"}}}'
# → {"outcome":"DLQ_DUPLICATE","transaction_id":"evt_001","reason":"Already processed..."}

# Inspect the ledger
curl http://localhost:8000/ledger/summary

# Inspect the DLQ
curl "http://localhost:8000/dlq/entries?limit=10"
```

---

## Architecture

```mermaid
flowchart TD
    S[Stripe Webhook] -->|POST /webhook/stripe| H[FastAPI Handler]
    H -->|dict payload| V{5-Layer Pydantic\nValidation Stack}

    V -->|ValidationError| DI[DLQ — INVALID\nraw payload preserved]
    V -->|Valid StripeEvent| P[process_stripe_event\nor\nprocess_stripe_event_batch]

    P -->|BEGIN| TX[PostgreSQL Transaction]
    TX -->|INSERT ... ON CONFLICT\nDO NOTHING| L[(ledger\ntransaction_id PRIMARY KEY)]
    TX -->|RETURNING transaction_id| R{Inserted?}

    R -->|in RETURNING set — new event| O[(outbox\ndispatched=0)]
    R -->|absent from RETURNING set — duplicate| DD[DLQ — DUPLICATE\naudit trail]

    TX -->|COMMIT| C[HTTP 200 to Stripe]

    O -->|Temporal Activity / worker| DS[Downstream System]
    DS -->|dispatched=1 in same TX| O

    DI --> OPS[Ops — GET /dlq/entries]
    DD --> OPS
```

### Why this handles concurrent load correctly

`INSERT INTO ledger ... ON CONFLICT (transaction_id) DO NOTHING` is a **database-level serialization point** — not an application lock, not a SELECT-then-INSERT race. Five threads firing the same `event_id` simultaneously all enter the transaction; PostgreSQL's PRIMARY KEY constraint ensures exactly one INSERT wins; all others produce `rowcount=0` (single-event path) or are absent from the `RETURNING` set (batch path). Zero application-level coordination required.

---

## 5-Layer Validation Stack

Every Stripe webhook passes through all five layers before touching the database. Any failure routes to DLQ — the raw payload is always preserved byte-perfect.

```
Layer 1 — Pydantic type coercion
  EventType enum                unknown type string → DLQ_INVALID (at model creation)
  StripeObject.customer         min_length=4 → rejects empty strings and short placeholders
  StripeObject.currency         pattern=r"^[a-z]{3}$" → rejects uppercase, 2-char, non-ISO codes
  StripeObject.amount_paid      ge=0 → rejects negative amounts at the Field level

Layer 2 — StripeObject @model_validator
  check_amount_present          either amount_paid OR amount must be non-null

Layer 3 — StripeEvent @model_validator (cross-field)
  resolve_idempotency_key       resolves from request.idempotency_key, falls back to event id
  check_invoice_amount_nonzero  invoice.paid + $0 → DLQ_INVALID (no revenue received)
  check_customer_id_format      customer must start with 'cus_' (structural Stripe API rule)

Layer 4 — Business logic
  _STATUS_MAP[event_type]       POSTED / VOID / PENDING per event type (type-safe dict)
  INSERT ... ON CONFLICT        DB-level idempotency guard — the serialization point

Layer 5 — DLQ routing
  ValidationError  → DLQ_INVALID    (raw payload preserved, reason code structured)
  ON CONFLICT hit  → DLQ_DUPLICATE  (Stripe retry audit trail, payload preserved)
```

---

## Performance Benchmarks

All numbers from `python test_ledger.py` against a live Docker PostgreSQL 16 container.  
Environment: Windows 10 · Docker-on-WSL2 · Python 3.12 · single core · ~8ms/round trip.

### Why single-event was slow (26 TPS)

```
Per event: BEGIN + INSERT ledger + INSERT outbox + COMMIT = 4 round trips
5,000 events × 4 round trips × ~8ms/round trip = ~160 seconds → 26 TPS
```

Each event was a separate synchronous fsync through WAL. The bottleneck was not CPU, not Pydantic, not the constraint check — it was the number of synchronous network round trips to a Docker container on WSL2.

### Why batch is fast (~5,000 TPS)

```
5,000 events → 12 round trips total:
  1  BEGIN
  5  bulk ledger INSERTs  (execute_values, page_size=1000, RETURNING transaction_id)
  5  bulk outbox INSERTs  (execute_values, page_size=1000)
  1  COMMIT
= ~1.0 second elapsed → ~5,000 TPS  (192× improvement)
```

`psycopg2.extras.execute_values` collapses N individual INSERT statements into `ceil(N / page_size)` multi-row statements. PostgreSQL does the same total work; the application makes 192× fewer network calls. `page_size=1000` is chosen because 9 ledger columns × 1,000 rows = 9,000 bind parameters — safely within PostgreSQL's 65,535-parameter statement limit.

### Results

| Path | Events | Time | TPS |
|---|---|---|---|
| Per-event (pre-batch) | 5,000 | ~195s | 26 |
| Batch (`execute_values`) | 5,000 | ~1.0s | ~5,000 |

The 500 TPS floor is a hard test assertion in `test_ledger.py` — not a metric, a test.

---

## Schema

```sql
-- Financial record. transaction_id is PRIMARY KEY and the idempotency guard.
-- ON CONFLICT (transaction_id) DO NOTHING makes replay a zero-lock no-op at DB level.
CREATE TABLE ledger (
    transaction_id  TEXT             PRIMARY KEY,
    event_type      TEXT             NOT NULL,
    customer_id     TEXT             NOT NULL,
    amount_cents    INTEGER          NOT NULL,
    currency        TEXT             NOT NULL DEFAULT 'usd',
    status          TEXT             NOT NULL,   -- POSTED | VOID | PENDING
    idempotency_key TEXT             NOT NULL,
    payload         TEXT             NOT NULL,   -- full original JSON, audit copy
    created_at      DOUBLE PRECISION NOT NULL
);

-- Written in the same BEGIN...COMMIT as the ledger row.
-- dispatched=0 = pending downstream delivery.
-- A worker flips to dispatched=1 in the same TX as delivery confirmation.
CREATE TABLE outbox (
    id             BIGSERIAL        PRIMARY KEY,
    transaction_id TEXT             NOT NULL,
    event_type     TEXT             NOT NULL,
    payload        TEXT             NOT NULL,
    dispatched     INTEGER          NOT NULL DEFAULT 0,
    created_at     DOUBLE PRECISION NOT NULL
);

-- Every rejection lands here. raw_payload is never corrected, never truncated.
-- Humans review DLQ entries. The system never assumes a DLQ entry is unimportant.
CREATE TABLE dlq (
    id             BIGSERIAL        PRIMARY KEY,
    transaction_id TEXT             NOT NULL,
    reason         TEXT             NOT NULL,   -- DUPLICATE | INVALID
    raw_payload    TEXT             NOT NULL,
    received_at    DOUBLE PRECISION NOT NULL
);
```

| Status | Event types | Meaning |
|---|---|---|
| `POSTED` | `invoice.paid`, `customer.subscription.created` | Revenue confirmed |
| `VOID` | `invoice.payment_failed`, `customer.subscription.deleted` | Reversed or failed |
| `PENDING` | `customer.subscription.updated` | Awaiting resolution |

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/webhook/stripe` | Ingest webhook. Always 200 for valid JSON; 503 on DB failure (correct Stripe retry protocol). |
| `GET` | `/health` | Liveness check |
| `GET` | `/ledger/summary` | Row counts by status, DLQ depth, outbox pending count |
| `GET` | `/dlq/entries?limit=N` | Inspect DLQ, newest first. Default 50, capped at 1,000. |

---

## Test Coverage

```
101 tests · python test_ledger.py · zero mocks · zero stubs · real PostgreSQL

Phase 1  (34 tests)   Entry boundary — Pydantic type + Field + enum validation
Phase 2  (26 tests)   Output models  — DLQEntry, LedgerEntry, to_db() serialization
Phase 3  ( 9 tests)   Cross-field    — invoice amount > 0, customer ID format
Phase 5  (32 tests)   Full stack     — HTTP, concurrent idempotency, outbox dispatch, DLQ queryability
```

**Concurrent idempotency test:**
```python
# 5 threads fire the same event_id simultaneously
threads = [threading.Thread(target=_fire) for _ in range(5)]
for t in threads: t.start()
for t in threads: t.join()

assert results.count("POSTED") == 1        # exactly one INSERT won the constraint race
assert results.count("DLQ_DUPLICATE") == 4  # all others: ON CONFLICT hit
assert ledger_row_count == 1               # verified by a separate connection
```

---

## Production Gap Checklist

| Gap | Priority | Fix |
|---|---|---|
| Stripe signature verification | **CRITICAL** | Uncomment 3 lines in `stripe_webhook()`, add `stripe` to requirements. Without this, anyone who knows the URL can POST fake events. |
| Outbox worker | **HIGH** | Temporal activity polling `WHERE dispatched=0 ORDER BY id LIMIT 100`, delivering each event, flipping `dispatched=1` in the same TX. |
| DLQ retry budget | **HIGH** | Add `retry_count`, `next_retry_at`, `max_retries` columns. Without this, DLQ is a graveyard, not a quarantine. |
| Per-event-type amount extraction | **HIGH** | Unknown amount → DLQ instead of silently recording $0. |
| Connection pooling | **MEDIUM** | Replace module-level single connection with `psycopg2.pool.ThreadedConnectionPool`. |
| Structured JSON logging | **MEDIUM** | JSON logs with `transaction_id`, `outcome`, `duration_ms` per request. |
| Prometheus `/metrics` | **MEDIUM** | `webhooks_received_total`, `webhooks_posted_total`, `dlq_depth`, `outbox_pending`. |
| Currency normalization | **LOW** | `.lower()` on ingest. One line. Mixed-case breaks `GROUP BY currency` in finance reports. |

---

## Origin

The fault-tolerance patterns here — idempotent inserts, transactional outbox, dead-lettering — are derived from [Aequitas](https://github.com/DDRG15/aequitas-privacy-engine), a high-throughput engine built to process 75k+ events per second with zero data loss under hard kills.

In billing, a single race condition is a financial discrepancy. This system treats data like currency.

---

*Diego Alonso Del Río García — May 2026*
