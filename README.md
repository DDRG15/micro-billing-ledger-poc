# Micro-Billing-Ledger PoC
### Idempotent Stripe Webhook Ingestion — PostgreSQL · Pydantic v2 · FastAPI

[![Tests](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/test.yml/badge.svg)](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/test.yml)
[![Lint](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/lint.yml/badge.svg)](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/lint.yml)
[![Stack](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/stack.yml/badge.svg)](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/stack.yml)

**Status: Proof of Concept.** This demonstrates specific architectural decisions — idempotent inserts, Transactional Outbox, SELECT FOR UPDATE SKIP LOCKED — in a working system. It is not production software. The Production Gap Checklist at the bottom of this file is now fully closed; what remains genuinely out of scope is listed there as *Not attempted*, with the reason.

**Stack:** Python 3.12–3.13 · FastAPI · Pydantic v2 · psycopg2 · PostgreSQL 16 · Docker Compose  
**Tests:** 463 / 463 — no mocks, no stubs, every assertion hits a live PostgreSQL database  
**Throughput:** 4,143 TPS batch · 112 TPS over HTTP at 16 concurrent clients (p50 112ms) · 24 TPS per-event · all reproducible via `python benchmark.py`, measured against the restricted role the service actually deploys as

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

**Prerequisites:** [Docker Desktop](https://www.docker.com/products/docker-desktop/) · Python 3.12 or 3.13

```bash
# 1. Configure environment — the dev defaults work as-is
cp .env.example .env
# Every compose variable also has a built-in dev default, so the stack still
# starts without this file. Copy it anyway: it is where you change the values
# marked *** CHANGE *** before anything is reachable from a network.

# 2. Start PostgreSQL and create the database the test suite runs on. The suite
#    TRUNCATEs every tenant table, so it refuses (exit code 2) any database whose
#    name does not end in _test or _bench — including `billing`, the one compose
#    creates for the stack.
docker compose up -d --wait postgres
docker compose exec postgres createdb -U postgres billing_test

# 3. Install Python dependencies
python -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt

# 4. Apply migrations to the test database. Required before the suite: the
#    row-level security policies and the restricted `billing_app` role exist only in
#    migration 0006, and the tests that prove isolation connect as that role.
#    Set DATABASE_URL inline, not with export: an exported DATABASE_URL also reaches
#    `docker compose up` and points the migrate container at localhost.
#    (PowerShell: $env:DATABASE_URL="..."; alembic upgrade head; Remove-Item Env:DATABASE_URL)
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/billing_test alembic upgrade head

# 5. Run the full test suite — live PostgreSQL. With no DATABASE_URL set it uses
#    billing_test on localhost:5432.
python test_ledger.py

# 6. Start the full stack (postgres + migrate + billing + worker + caddy)
docker compose up
# Confirm the database is enforcing tenant isolation, not just declaring it:
docker compose logs billing | grep -E "rls_enforced|rls_inert"

# — OR — run the API standalone without the full stack. It connects as the owner
#   unless APP_DATABASE_URL points at billing_app, and says so at startup. On the
#   demo credentials it refuses to start unless the run is declared dev; the check
#   reads INGRESS_BIND_ADDR (default 127.0.0.1), not uvicorn's --host, so keep
#   uvicorn on its default loopback bind when you declare dev.
BILLING_ENV=dev uvicorn ledger:app --port 8000

# 7. Teardown — stop containers and remove volumes
docker compose down          # keeps pgdata volume (data survives)
docker compose down -v       # wipes pgdata volume (full clean slate)
```

### Send test webhooks

`POST /webhook/stripe` verifies an HMAC-SHA256 signature on **every** request, including
when `STRIPE_WEBHOOK_SECRET` is still the placeholder. That is deliberate — an endpoint
that accepts unsigned requests whenever it is misconfigured fails silently, which is
worse than rejecting everything. So a bare `curl -d '{...}'` correctly returns `400`.

`send_test_webhook.py` signs requests the way Stripe does, using whatever secret the
stack is actually running with. Standard library only — no venv, no Stripe account:

```bash
python send_test_webhook.py --all
```

```
Valid invoice.paid:
  POSTED expected: HTTP 200  {"outcome":"POSTED","transaction_id":"evt_demo_...","reason":null}

Same event twice - the idempotency guard fires on the second:
  1st -> POSTED: HTTP 200        {"outcome":"POSTED",...}
  2nd -> DLQ_DUPLICATE: HTTP 200 {"outcome":"DLQ_DUPLICATE","reason":"Already processed..."}

Invalid payload (currency 'xx' is not a real ISO 4217 code):
  DLQ_INVALID expected: HTTP 200 {"outcome":"DLQ_INVALID","reason":"Pydantic validation failed: 1 errors"}

Failed invoice - amount lives in amount_due, not amount_paid:
  VOID expected, recorded at 7500: HTTP 200  {"outcome":"VOID",...}

Ledger summary:
  summary: HTTP 200  {"ledger":{"VOID":1,"POSTED":2},"dlq_depth":2,"dlq_total":2,"outbox_pending":1}

Prometheus metrics:
  metrics: HTTP 200
      webhook_events_total{outcome="POSTED"} 2.0
      webhook_events_total{outcome="DLQ_DUPLICATE"} 1.0
      webhook_events_total{outcome="DLQ_INVALID"} 1.0
      webhook_events_total{outcome="VOID"} 1.0
      dlq_depth 2.0
      dlq_total 2.0
      outbox_pending 1.0
```

Individual steps: `--duplicate`, `--invalid`, `--failed`, `--summary`, `--metrics`.
Add `--url https://localhost` to go through Caddy instead of the direct port.

The read endpoints need the API key (`dev_key_change_in_production` by default):

```bash
curl -H "X-Api-Key: dev_key_change_in_production" http://localhost:8000/ledger/summary
curl -H "X-Api-Key: dev_key_change_in_production" "http://localhost:8000/dlq/entries?limit=10"
```

---

## Architecture

```mermaid
flowchart TD
    S[Stripe Webhook] -->|HTTPS :443| CA[Caddy\nTLS termination + security headers]
    CA -->|HTTP :8000| H[FastAPI Handler]
    H -->|dict payload| V{5-Layer Pydantic\nValidation Stack}

    V -->|ValidationError| DI[DLQ — INVALID\nraw payload preserved]
    V -->|Valid StripeEvent| P[process_stripe_event\nor\nprocess_stripe_event_batch]

    P -->|BEGIN| TX[PostgreSQL Transaction]
    TX -->|INSERT ... ON CONFLICT\nDO NOTHING| L[(ledger\nPK: org_id + transaction_id)]
    TX -->|RETURNING transaction_id| R{Inserted?}

    R -->|in RETURNING set — new event| O[(outbox\ndispatched=0)]
    R -->|absent from RETURNING set — duplicate| DD[DLQ — DUPLICATE\naudit trail]

    TX -->|COMMIT| C[HTTP 200 to Stripe]

    O -->|worker.py polls every N seconds\nSELECT FOR UPDATE SKIP LOCKED| W[Worker Service\ndispatched=0 → dispatched=1]
    W -->|POST to that tenant's downstream_url| DS[Downstream System
per tenant]
    W -->|structured log if WORKER_LOG_ONLY| LOG[Log / Audit]

    DI --> OPS[Ops — GET /dlq/entries]
    DD --> OPS
```

### Why this handles concurrent load correctly

`INSERT INTO ledger ... ON CONFLICT (transaction_id) DO NOTHING` is a **database-level serialization point** — not an application lock, not a SELECT-then-INSERT race. Five threads firing the same `event_id` simultaneously all enter the transaction; PostgreSQL's PRIMARY KEY constraint ensures exactly one INSERT wins; all others produce `rowcount=0` (single-event path) or are absent from the `RETURNING` set (batch path). Zero application-level coordination required.

---

## Outbox Worker

The Transactional Outbox pattern guarantees that a `ledger` row and its corresponding `outbox` row are written atomically. Without a drain loop, that guarantee is half-finished: events are recorded but never delivered downstream.

`worker.py` is a standalone Docker service — not a thread inside the API process. An in-process thread shares the connection pool, dies with the web server, and cannot be restarted independently. A separate service has its own lifecycle, its own connection, and its own failure domain.

**`SELECT FOR UPDATE SKIP LOCKED`** is the concurrency mechanism:

```sql
-- Round-robin across tenants, then lock. ROW_NUMBER() interleaves tenants so one
-- tenant's backlog cannot own every batch; the ranking sits in a CTE because
-- PostgreSQL rejects FOR UPDATE in a query containing a window function.
WITH ranked AS (
    SELECT id, ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY id) AS rn
    FROM   outbox WHERE dispatched = 0
), picked AS (
    SELECT id FROM ranked ORDER BY rn, id LIMIT 100
)
SELECT o.id, o.transaction_id, o.event_type, o.payload, o.org_id, g.downstream_url
FROM   outbox o
LEFT   JOIN org g ON g.org_id = o.org_id
WHERE  o.id IN (SELECT id FROM picked)
ORDER  BY o.id
FOR UPDATE OF o SKIP LOCKED;   -- locks outbox rows only, never the org registry
```

`FOR UPDATE` locks the selected rows for the duration of the transaction. `SKIP LOCKED` skips rows already locked by another worker — rather than blocking, it moves on. Two worker replicas running simultaneously process different rows without any coordination. No event is dispatched twice.

The `dispatched=1` update happens in the **same `BEGIN…COMMIT`** as the dispatch confirmation. A crash before `COMMIT` leaves `dispatched=0` — the event is retried on the next poll. Delivery is at-least-once. The ledger's `ON CONFLICT DO NOTHING` makes receipt exactly-once.

**Fail-fast on misconfiguration:** if `DOWNSTREAM_URL` is empty and `WORKER_LOG_ONLY` is not `"true"`, the worker exits at startup with a structured error. The alternative — marking rows `dispatched=1` without forwarding them — is undetectable data loss.

**Resilience:** `psycopg2.OperationalError` triggers exponential backoff (5 → 10 → 20 → 60s cap) and a reconnect loop. `SIGTERM` sets a stop flag; the worker finishes the current batch then exits 0. This is the correct behavior for Docker rolling deploys and Kubernetes graceful termination.

---

## Security Controls

**Stripe HMAC-SHA256 Signature Verification** — Every inbound webhook is authenticated via `stripe.Webhook.construct_event()` using the endpoint's signing secret. Unsigned or tampered requests return HTTP 400 before touching the database. Verification stays on even when `STRIPE_WEBHOOK_SECRET` is left at its placeholder: an endpoint that accepts unsigned requests whenever it is misconfigured fails silently, which is worse than rejecting everything.

**API Key Authentication** — `/ledger/summary`, `/dlq/entries`, `/dlq/{id}/retry` and `/metrics` require an `X-Api-Key` header validated against the `BILLING_API_KEY` environment variable. Missing or invalid keys return HTTP 401. An empty `BILLING_API_KEY` disables the check entirely for local development, and the application logs a warning at startup when that happens — a fail-open default is defensible, a silent one is not.

**Row-Level Security** — Tenant isolation is enforced by PostgreSQL, not only by the queries. `ledger`, `outbox` and `dlq` carry default-deny policies with `USING` **and** `WITH CHECK`, so a query that forgets its `org_id` predicate returns nothing rather than everything, and an `INSERT` naming another tenant is refused by the database. The service connects as `billing_app` — `NOSUPERUSER NOBYPASSRLS`, DML on those three tables, `SELECT` on the org registry, and nothing else — while the owner credential stays with the migration step, which is the only thing that needs DDL.

The role is the part that matters, and it is the part that is easy to get wrong. A first attempt shipped the policies alone: they applied cleanly, appeared in `pg_policies`, and enforced nothing, because the application connected as a superuser and a superuser bypasses RLS regardless of `ENABLE` or `FORCE`. The full test suite passed — which was the alarm, not the result, since default-deny should have broken half the system. So the process now says out loud, at every boot, which of the two worlds it is in:

```bash
docker compose logs billing | grep -E "rls_enforced|rls_inert"
# rls_enforced  db_user=billing_app  superuser=false  policies=3
```

`rls_inert` is not a warning about a future risk; it is the statement that isolation has gone back to depending on every query remembering its own predicate. `.github/workflows/stack.yml` fails the build if either container reports it.

**These are engineering controls, not a compliance program.** They address request authenticity and access to operational data. They are not an ISMS, there is no audit trail of who read operational data, no formal risk assessment, and no external audit. See [SECURITY.md](SECURITY.md) for what this project deliberately does not attempt.

---

## Infrastructure Resilience

**Connection Pooling** — `psycopg2.pool.ThreadedConnectionPool(minconn=maxconn=DB_POOL_SIZE)` (default 20) replaces the single module-level connection. `minconn` and `maxconn` are deliberately equal: psycopg2 returns a connection to the idle pool only while `len(pool) < minconn` and closes every other one on release, so `minconn=1` made the pool retain exactly one connection and reopen the rest per request — see *Performance Benchmarks* for what that measured. Each request leases a connection via `getconn()` and returns it in a `finally` block via `putconn()`. No connection exhaustion under concurrent load; no leaked connections on exception paths.

**Async/Sync Thread Dispatch** — All database-hitting routes — `POST /webhook/stripe`, `/ledger/summary`, `/dlq/entries` — are declared `def`, not `async def`. FastAPI runs synchronous routes in an external thread pool, so psycopg2's blocking calls never stall the asyncio event loop. The webhook handler reads the raw body from `request._body` (cached by Starlette when it parses the request) for HMAC verification — no `await` needed.

**Rate Limiting** — `slowapi` enforces 100 req/min on `POST /webhook/stripe`, 20 req/min on `/ledger/summary`, `/dlq/entries` and `/dlq/{id}/retry`, and 60 req/min on `/metrics` (a scraper hits it every 15s), keyed by client IP. A webhook flood without rate limiting exhausts `maxconn=20` and drives `outbox_pending` unbounded. The 429 response instructs Stripe to back off; Stripe's own retry logic handles redelivery.

Behind Caddy the app runs with `--proxy-headers` and trusts `X-Forwarded-For` **only from Caddy's address**, set via `FORWARDED_ALLOW_IPS` and pinned to a static container IP. The limiter therefore keys on the real client rather than the proxy, without letting the client choose its own key.

The obvious shortcut here is wrong and worth stating plainly. `--forwarded-allow-ips=*` sets `always_trust=True` in uvicorn's `ProxyHeadersMiddleware`, which returns `x_forwarded_for_hosts[0]` — the leftmost, entirely caller-supplied entry. Any client can then rotate the header to mint a fresh bucket every 100 requests, or forge a specific customer's IP to poison theirs. Pinning to Caddy's address makes uvicorn walk the header from the right and stop at the first untrusted hop, which is the real client, because Caddy appends rather than overwrites.

One version-specific trap: uvicorn 0.30.6 compares this value as an **exact string**, not a CIDR range. `172.28.0.0/16` matches nothing, silently disabling `X-Forwarded-For` and collapsing the limiter into a single global bucket — the failure looks like a fix. Hence a literal address plus a fixed `ipv4_address` in `docker-compose.yml`.

Verified end to end, not just traced: two containers with distinct addresses hit Caddy, one exhausted its bucket to 429 keyed on its own IP while the other still passed, and a forged header from an untrusted peer no longer reset the limit. `.github/workflows/stack.yml` asserts the bypass stays closed.

**Log Rotation** — `RotatingFileHandler(maxBytes=10MB, backupCount=5)` caps log storage at 50 MB. At 500 TPS under DLQ write failures, each logging a full raw payload as JSON, an unbounded log file fills a typical root partition in under an hour. The 5-file backup retains enough history for forensics without infinite growth.

**Resource Limits** — `deploy.resources.limits: memory: 256M, cpus: 0.50` on the billing container. Without a memory ceiling, an OOM in the billing container can terminate the Docker host process. The hard limit ensures OOM kills the container in isolation.

**Secrets Management** — All credentials are read from `.env` via `${VAR}` substitution in `docker-compose.yml`. No credential appears in source code. `.env.example` is the committed template with safe development defaults; `.env` is gitignored. Rotating a secret requires editing one file and restarting the affected service.

**HTTPS / TLS** — Caddy sits in front of FastAPI as a reverse proxy. Set `CADDY_DOMAIN` in `.env` to a real domain; Caddy requests a Let's Encrypt certificate automatically — no `certbot`, no cron renewal job, no manual certificate rotation. Security headers enforced at the proxy: `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Strict-Transport-Security: max-age=31536000`, server fingerprint removed.

---

## 5-Layer Validation Stack

Every Stripe webhook passes through all five layers before touching the database. Any failure routes to DLQ with every field of the event Stripe sent — re-serialized, so complete but not byte-for-byte.

```
Layer 1 — Pydantic type coercion
  EventType enum                unknown type string → DLQ_INVALID (at model creation)
  StripeObject.customer         min_length=4 → rejects empty strings and short placeholders
  StripeObject.currency         pattern=r"^[a-z]{3}$" → rejects uppercase, 2-char, non-ISO codes
  StripeObject.amount_paid      ge=0 → rejects negative amounts at the Field level

Layer 2 — StripeEvent.check_amount_present (needs the event type)
  invoice.*                     one of amount_paid / amount_due / amount must be non-null
  customer.subscription.*       no amount is read — Stripe's Subscription object has none

Layer 3 — StripeEvent @model_validator (cross-field)
  resolve_idempotency_key       request.idempotency_key, else the event id — also when Stripe
                                sends request: null or idempotency_key: null (automatic events:
                                renewals, retries), which used to return 422 / 500
  check_invoice_amount_nonzero  invoice.paid + $0 → DLQ_INVALID (no revenue received)
  check_customer_id_format      customer must start with 'cus_' (structural Stripe API rule)

Layer 4 — Business logic
  _STATUS_MAP[event_type]       POSTED / VOID / LIFECYCLE per event type (type-safe dict)
  INSERT ... ON CONFLICT        DB-level idempotency guard — the serialization point

Layer 5 — DLQ routing
  ValidationError  → DLQ_INVALID    (raw payload preserved, reason code structured)
  ON CONFLICT hit  → DLQ_DUPLICATE  (Stripe retry audit trail, payload preserved)
```

---

## Performance Benchmarks

Every number below comes from `python benchmark.py`, which is in the repository. Run it
yourself; do not quote these figures for your own hardware.

The benchmark TRUNCATEs the ledger before every mode, so like the test suite it refuses
any database whose name does not end in `_test` or `_bench`. Its default is
`billing_bench`; create and migrate it the same way as `billing_test` in the Quick Start.
In `--url` mode, start the server on that same database, or the reset lands on one
database and the load on another.

```
python benchmark.py --events 5000 --http-events 1500 --concurrency 16
```

Measured on Windows 10 · Docker Desktop · Python 3.13.7 · PostgreSQL 16 in a container:

| Path | Events | Time | Throughput | Latency |
|---|---|---|---|---|
| Single-event (`process_stripe_event`) | 5,000 | 204.6s | **24 TPS** | — |
| Batch (`process_stripe_event_batch`) | 5,000 | 1.21s | **4,143 TPS** | — |
| HTTP end-to-end (`POST /webhook/stripe`) | 1,500 | 13.3s | **112 TPS** | p50 112ms · p95 194ms · p99 2,152ms |

The p99 is not a typo and is not smoothed away. It reproduces on every run while p95 stays
under 200ms, so a small number of requests take roughly twenty times the median. It is
recorded rather than trimmed because an unexplained tail is a finding, not a blemish —
naming it is what makes it someone's next piece of work.

**These figures are lower than the ones this table carried before, and row-level security
is not the reason.** The obvious suspicion, once the service started connecting as a
non-superuser, was that the RLS predicates and the extra `SET LOCAL` per transaction had
cost this. Measured rather than assumed, on the same machine minutes apart:

| Path | RLS inert (owner) | RLS enforced (`billing_app`) |
|---|---|---|
| Single-event | 21 TPS | 24 TPS |
| Batch | 3,335 TPS | 4,018 TPS |

Enforced came out *faster*, which only means the difference is smaller than the run-to-run
noise on this machine — three repeat runs spanned 2,863–3,788 TPS on the batch path alone.
The honest conclusion is that tenant isolation costs nothing measurable here, and that the
older figures came from a differently loaded machine rather than a faster codebase. They
were replaced instead of explained away, because a number nobody can reproduce is a claim.

### Why single-event is bounded by round trips, not CPU

```
Per event: BEGIN + SET LOCAL + INSERT ledger + INSERT outbox + COMMIT
5,000 events × ~5 synchronous round trips ≈ 205 seconds → 24 TPS
```

The bottleneck is not CPU, not Pydantic, not the constraint check — it is the count of
synchronous round trips to PostgreSQL, each ending in a WAL fsync.

### Why batch is 169× faster

```
5,000 events → 13 round trips total:
  1  BEGIN
  1  SET LOCAL app.org_id          (the tenant scope, once per transaction)
  5  bulk ledger INSERTs  (execute_values, page_size=1000, RETURNING transaction_id)
  5  bulk outbox INSERTs  (execute_values, page_size=1000)
  1  COMMIT
= 1.21 seconds elapsed → 4,143 TPS
```

The scope costs one round trip for the whole batch and one per single event, which is why
it is invisible in the batch figure and lost in the noise in the single-event one.

`execute_values` collapses N INSERT statements into `ceil(N / page_size)` multi-row
statements. PostgreSQL does the same total work; the application makes far fewer network
calls. `page_size=1000` is chosen because 9 ledger columns × 1,000 rows = 9,000 bind
parameters, safely within PostgreSQL's 65,535-parameter statement limit.

### What writing this benchmark found

The HTTP figure did not exist before, and producing it exposed a defect the other two
numbers could never have shown. Throughput **collapsed** as clients were added — 23 TPS
at one client, 15 at two, then **1 TPS at four, with p50 latency of 4.2 seconds**.

The cause was the connection pool. psycopg2 returns a connection to the idle pool only
while `len(self._pool) < self.minconn`; every other release calls `conn.close()`. With
`minconn=1, maxconn=20` the pool retained exactly one connection, so under concurrency
each request opened a fresh PostgreSQL connection and closed it on release — and
`_connect()` runs inside `getconn` while the pool lock is held, which serialized every
caller. Setting `minconn == maxconn` fixed it:

| Concurrency | Before | After |
|---|---|---|
| 1 | 23 TPS | 15 TPS |
| 2 | 15 TPS | 22 TPS |
| 4 | 1 TPS | **61 TPS** |
| 8 | 1 TPS | **106 TPS** |
| 16 | did not finish | **143 TPS** |

That table is the record of one fix on the day it was made, kept because the *shape* is
the point — flat-to-collapsing versus scaling with clients. The absolute numbers are not
current: the same machine now measures 112 TPS at 16 concurrent. Re-run `benchmark.py`
for figures to rely on rather than reading either column as today's performance.

Two diagnoses were wrong before that one was right. First suspect was the rate limiter's
in-memory storage, which does an O(n) list prepend under a lock — disproved by re-running
against a limiter with empty state and getting identical numbers. Second was Windows TCP,
since latency snapped to 4.2s and 8.4s, the SYN retransmission timers — disproved by
switching the benchmark client to connection reuse and getting identical numbers. What
settled it was the service's own `duration_ms` middleware reporting 4,200ms against a
client-side 4,224ms, which placed the time inside the process.

The lesson is the ordinary one: a benchmark measures the whole system including the thing
doing the measuring, and the first plausible explanation is usually not the true one.

The 500 TPS floor is a hard test assertion in `test_ledger.py` — not a metric, a test.

---

## Schema

```sql
-- Financial record. transaction_id is PRIMARY KEY and the idempotency guard.
-- ON CONFLICT (transaction_id) DO NOTHING makes replay a zero-lock no-op at DB level.
-- BIGINT: covers amounts up to ~$92 quadrillion (INTEGER max is only ~$21M).
-- CHECK (length(payload) < 50000): prevents unbounded growth from malicious payloads.
-- Tenant registry (migration 0004). The API key is stored only as a SHA-256, so a
-- database dump does not hand over working credentials. The Stripe signing secret
-- cannot be hashed — verifying an HMAC needs the value — so in production it belongs
-- in a secret manager with this column holding a reference.
CREATE TABLE org (
    org_id                TEXT        PRIMARY KEY,
    name                  TEXT        NOT NULL,
    api_key_sha256        TEXT        UNIQUE,
    stripe_webhook_secret TEXT        NOT NULL,
    active                BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- PRIMARY KEY is (org_id, transaction_id), NOT transaction_id alone.
--   transaction_id IS the idempotency guard, and Stripe event ids are unique per
--   ACCOUNT rather than globally. With a single-column key, one tenant's event would
--   hit ON CONFLICT against another tenant's and be discarded as a duplicate —
--   silent revenue loss, indistinguishable from the guard working correctly.
--   The composite key makes the guard per-tenant, which is what it always meant.
CREATE TABLE ledger (
    org_id          TEXT        NOT NULL DEFAULT 'default' REFERENCES org(org_id),
    transaction_id  TEXT        NOT NULL,
    event_type      TEXT        NOT NULL,
    customer_id     TEXT        NOT NULL,
    amount_cents    BIGINT,                 -- NULL exactly for customer.subscription.*
    currency        TEXT        NOT NULL DEFAULT 'usd',
    status          TEXT        NOT NULL,   -- POSTED | VOID | LIFECYCLE (PENDING: pre-Cycle-6 rows)
    idempotency_key TEXT        NOT NULL,
    payload         TEXT        NOT NULL CHECK (length(payload) < 50000),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ledger_amount_matches_kind   -- migration 0007, NOT VALID: new rows only
        CHECK ((amount_cents IS NULL) = (event_type LIKE 'customer.subscription.%')),
    PRIMARY KEY (org_id, transaction_id)
);

-- Written in the same BEGIN...COMMIT as the ledger row.
-- (org_id, transaction_id) REFERENCES ledger ON DELETE CASCADE:
--   enforces Transactional Outbox invariant at DB level — an outbox row
--   cannot exist without its corresponding ledger row.
--   Application logic alone is not sufficient; the FK constraint is the guarantee.
-- dispatched=0 = pending downstream delivery.
-- A worker flips to dispatched=1 in the same TX as delivery confirmation.
CREATE TABLE outbox (
    id             BIGSERIAL   PRIMARY KEY,
    org_id         TEXT        NOT NULL DEFAULT 'default',
    transaction_id TEXT        NOT NULL,
    event_type     TEXT        NOT NULL,
    payload        TEXT        NOT NULL,
    dispatched     INTEGER     NOT NULL DEFAULT 0,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Composite, following the ledger key above.
    FOREIGN KEY (org_id, transaction_id)
        REFERENCES ledger(org_id, transaction_id) ON DELETE CASCADE
);

-- Every rejection lands here. raw_payload is never corrected, never truncated.
-- Humans review DLQ entries. The system never assumes a DLQ entry is unimportant.
CREATE TABLE dlq (
    id             BIGSERIAL   PRIMARY KEY,
    org_id         TEXT        NOT NULL DEFAULT 'default',
    transaction_id TEXT        NOT NULL,
    reason         TEXT        NOT NULL,   -- DUPLICATE | INVALID | TRANSIENT | UNKNOWN_TYPE
    raw_payload    TEXT        NOT NULL,
    received_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- Retry budget (migration 0002)
    status         TEXT        NOT NULL DEFAULT 'pending',  -- pending | retrying | resolved | exhausted
    retry_count    INTEGER     NOT NULL DEFAULT 0,
    max_retries    INTEGER     NOT NULL DEFAULT 3,
    next_retry_at  TIMESTAMPTZ,
    -- Claim timestamp (migration 0003). retry_dlq_batch commits its claim before
    -- doing the work, so without this a crash mid-batch left rows in 'retrying'
    -- that no later batch would ever select again. NULL counts as infinitely
    -- stale, which recovers rows stranded before this column existed.
    claimed_at     TIMESTAMPTZ
);

-- Indexes for hot query paths
-- org_id leads every composite index: each query is scoped to one tenant, and an
-- index that does not lead with the predicate's first column cannot serve it.
CREATE INDEX IF NOT EXISTS idx_ledger_org_customer ON ledger(org_id, customer_id);
CREATE INDEX IF NOT EXISTS idx_outbox_org_dispatched ON outbox(org_id, dispatched, id) WHERE dispatched=0;
CREATE INDEX IF NOT EXISTS idx_dlq_org_retry ON dlq(org_id, status, next_retry_at) WHERE status='pending';
CREATE INDEX IF NOT EXISTS idx_dlq_stale_claim ON dlq(status, claimed_at) WHERE status='retrying';
CREATE INDEX IF NOT EXISTS idx_org_api_key ON org(api_key_sha256) WHERE api_key_sha256 IS NOT NULL AND active;
```

| Status | Event types | Meaning |
|---|---|---|
| `POSTED` | `invoice.paid` | Revenue confirmed, recorded at `amount_paid` |
| `VOID` | `invoice.payment_failed` | Payment failed, recorded at `amount_due` |
| `LIFECYCLE` | `customer.subscription.created` · `.updated` · `.deleted` | Account state changed; no money moved. `amount_cents` is NULL, and the row is forwarded downstream like any other |
| `PENDING` | none since Cycle 6 | Was `customer.subscription.updated`; kept so rows written before stay valid |

**Subscription events carry no amount, on purpose.** Stripe's Subscription object has no
top-level amount field — prices live in `items.data[].price` — and the ledger used to
demand one from every event, so every real `customer.subscription.*` event was
dead-lettered as `INVALID` and counted in `dlq_depth`. Computing an amount from the items
would be worse: it posts a list price as revenue next to the `invoice.paid` that actually
collects it, and the figure is wrong for trials, metered and tiered prices, coupons and
proration. The money is on the invoice rows; a subscription row records that an account
changed state, which is what downstream systems act on. The database enforces the split
(`ledger_amount_matches_kind`), and `SUM(amount_cents)` skips the NULLs.

Upgrading a deployment that ran before this change: its DLQ holds those subscription
events as `INVALID` entries. Replay them with `POST /dlq/{id}/retry`; each settles as
`LIFECYCLE` on the first attempt.

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/webhook/stripe` | Ingest for the default tenant. 200 for valid JSON, including duplicates and invalid events once they are in the DLQ; 503 on DB failure, and for an invalid event whose DLQ row could not be written (correct Stripe retry protocol). |
| `POST` | `/webhook/stripe/{org_id}` | Per-tenant ingest. Signature verified with that tenant's secret, so the path cannot be used to write into another tenant. Unknown org returns the same 400 as a bad signature — no enumeration. |
| `GET` | `/health` | Liveness check |
| `GET` | `/ledger/summary` | Row counts by status, DLQ depth, outbox pending count |
| `GET` | `/dlq/entries?limit=N` | Inspect DLQ, newest first. Default 50, capped at 1,000. |
| `POST` | `/dlq/{id}/retry` | Replay one entry. `409` if already `resolved` — the ledger holds it, so there is nothing to replay. `exhausted` stays replayable. |
| `GET` | `/metrics` | Prometheus exposition, operator only: `BILLING_API_KEY` → 200, a tenant key → 403. |

---

## Observability

**Structured logs.** `LOG_FORMAT=json` (the default in containers) emits one JSON object per line. Anything passed through `extra={...}` becomes a top-level field, so `transaction_id`, `outcome`, `duration_ms`, `path` and `status` are queryable directly:

```json
{"ts":"2026-08-09T18:07:47+00:00","level":"INFO","logger":"ledger","message":"request POST /webhook/stripe -> 200",
 "event":"http_request","method":"POST","path":"/webhook/stripe","status":200,"duration_ms":12.6,"client_ip":"203.0.113.9"}
```

Plain text is unparseable at the moment you most need it: *"which `transaction_id` failed, how often, and how slow was it"* becomes a regex against a human sentence, and that regex breaks the next time someone rewords a log line. Consumers here read keys, not prose. Records are always one line — log shippers split on newlines, so a multi-line record arrives as several unparseable fragments — and tracebacks are folded into a single string field for the same reason. `worker.py` reuses the same formatter: two services logging the same event in two shapes cannot be correlated on `transaction_id`, which is exactly the field you want when tracing an event from ingest through to dispatch.

**Metrics.** `GET /metrics` exposes `webhook_events_total{outcome}`, `http_requests_total{method,path,status}`, `http_request_duration_seconds{method,path}`, plus `dlq_depth`, `dlq_total` and `outbox_pending`.

`dlq_depth` counts only rows still needing action — `pending`, `retrying`, `exhausted`. `dlq_total` counts every row ever written. They are separate because they answer different questions and one was previously doing both jobs badly: a single number counting all rows grew monotonically with historical traffic and never came back down, since `prune_dlq` only ages out `DUPLICATE` rows. A resolved `INVALID` entry inflated "queue depth" forever. An alert set on a figure that only rises is an alert that eventually gets muted.

Labels use the route **template** (`/dlq/{dlq_id}/retry`), never the resolved path. Labelling by resolved path gives every id its own time series — the standard way to take down a Prometheus server with your own instrumentation.

The two queue gauges are read from the database on each scrape rather than tracked in memory. The worker drains the outbox and prunes the DLQ from a different process, so an in-memory count would diverge from the tables it claims to describe, and a queue depth that is confidently wrong is worse than no queue depth at all.

**Secret provenance.** The startup log emits a `secrets_loaded` event and `/metrics` exposes `billing_secrets_info`, each carrying a SHA-256 prefix of the live secrets and where they came from (`env` or `file`). This exists because the failure it catches is otherwise silent: **`docker compose restart` does not re-read `.env`** — it restarts the existing container with its original environment, so a rotated secret is not live until `docker compose up -d` recreates the container. Without a fingerprint, an operator who edits `.env` and restarts gets no error, no log and no metric telling them the old credential is still in use.

Secrets can also be supplied as files via `BILLING_API_KEY_FILE` and `STRIPE_WEBHOOK_SECRET_FILE` — the Docker/Kubernetes convention, and the only way to rotate without replacing the container, since a container's environment is immutable for its lifetime while a mounted file is not. A configured but unreadable `*_FILE` stops the service rather than falling back to the environment variable.

Publishing a hash is safe for a high-entropy secret and not for a guessable one; see [SECURITY.md](SECURITY.md).

`/metrics` is the operator's endpoint: it answers only `BILLING_API_KEY`. Its counters are summed over every tenant and `billing_secrets_info` fingerprints the operator's own secrets, so no tenant view of it exists — a tenant key gets `403`, no key or an unknown one `401`. The queue gauges cover every tenant; a tenant's own figures are `/ledger/summary`. With tenants holding keys and `BILLING_API_KEY` empty there is no operator credential, and `/metrics` answers `403` to everyone until one is set.

The header is `X-Api-Key`, which Prometheus's `authorization:` block cannot send (it sends `Authorization: <type> <credentials>`, so the example this section used to show got `401` on every scrape). Use `http_headers` — check that your Prometheus version's configuration reference lists it:

```yaml
scrape_configs:
  - job_name: billing
    http_headers:
      X-Api-Key:
        files: [/etc/prom/billing.key]
```

---

**DLQ Operationalization** — `GET /dlq/entries` is the ops interface for triage. Every rejected event lands in the DLQ with its `raw_payload` holding every field Stripe sent — never corrected, never truncated (re-serialized, so not byte-for-byte). If that DLQ write itself fails, the webhook answers 503 instead of 200, so Stripe redelivers an event nothing has recorded yet. In fintech, a rejected payment event without a recoverable audit trail is a compliance gap, not a logging inconvenience. An operator retrieves entries, inspects the reason code, fixes the upstream issue, and replays the original payload.

---

## Test Coverage

```
463 tests · python test_ledger.py · zero mocks · zero stubs · real PostgreSQL

Phase 1    Entry boundary — Pydantic type, Field, enum; currency / customer / amount rules
Phase 2    Output models  — DLQEntry, LedgerEntry construction + to_db() serialization
Phase 3    Cross-field    — invoice amount > 0, customer ID 'cus_' format
Phase 5    Full stack     — HTTP 200/400/422/503, HMAC signature, 5-thread concurrent
                            idempotency, batch duplicate partition, BIGINT, outbox, DLQ
Phase 7    Outbox worker  — drain_batch dispatch + dispatched_at, SKIP LOCKED, idempotent re-drain
Phase 10   Migrations     — alembic upgrade/downgrade round-trip, schema parity with _bootstrap()
Phase 11   DLQ retry      — TRANSIENT backoff, exhaustion, manual POST /dlq/{id}/retry
Phase 12   Observability  — JSON formatter (field promotion, one line, traceback folding,
                            unserializable degradation), /metrics shape, outcome labels,
                            route-template cardinality, gauge-vs-table agreement, auth
A12 / A13  Tenant delivery — a DLQ replay lands in the owning tenant (entry resolved AND
                            that tenant's ledger holding the row); each tenant's events reach
                            only its own endpoint; a quiet tenant is not starved by a noisy one
A11        Per-tenant ingest — signature proves the tenant; one tenant's secret cannot
                            post into another's path; unknown org is byte-identical to a
                            bad signature; same event id ingests for both tenants
A9 / A10   Tenancy       — same event id from two tenants both post and keep their own
                            amounts; replay within a tenant is still a duplicate; a tenant
                            cannot read or retry another's rows; X-Org-Id cannot override
                            the credential
A6 / A7    Ops surface   — secret *_FILE sources, fingerprints, unreadable-file refusal;
                            the pool hands back the same connection instead of reopening
A8         Config        — rate-limit defaults pinned so "configurable" stays unchanged
A4 / A5    Terminal state — 'resolved' survives _increment_retry; dlq_depth counts backlog,
                            dlq_total counts history; both gauges agree with /ledger/summary
A15 / A16  Row-level security — connected as the RESTRICTED role, because running these
                            as the owner would prove nothing: unset scope sees zero rows,
                            a query that FORGETS its org_id predicate still cannot cross
                            tenants, a cross-tenant INSERT is refused by WITH CHECK. A16
                            then drives the real endpoints, core functions and worker
                            drain over that connection — every application query site
                            has to carry a scope for any of it to answer at all
A17        Deploy-time rows — a rotated STRIPE_WEBHOOK_SECRET still reaches the default
                            tenant's registry row, other tenants stay untouched, and the
                            role that serves traffic cannot rewrite the registry
A18        /metrics is operator-only — tenant keys get 403 and no exposition, the
                            operator sees every tenant's backlog. Drives the real
                            connection pool rather than the test double, so the pool the
                            application actually builds is covered at all
A0         Amount extract — per-event-type field priority; unknown amount → DLQ, never $0
A1 / A2    DLQ integrity  — retry appends no rows; 'resolved' terminal (409); 'exhausted' replayable
A3         Claim recovery — stale claim reclaimed, live claim not stolen, batch not aborted
M1 / M5    Ops boundary   — X-Api-Key auth (401 missing/wrong, 200 correct), prune_dlq retention
C6-1       Disposable DB  — suite and benchmark refuse any database not named *_test / *_bench
                            (exit 2, before connecting; dbname= overrides and key=value DSNs too)
C6-2       Automatic events — request: null and idempotency_key: null → stored, keyed by event id
C6-3       Demo credentials — refused at startup unless BILLING_ENV=dev AND a loopback ingress
C6-4       Subscriptions  — Stripe's documented objects (fixtures/stripe/) ledgered as LIFECYCLE
                            with no amount; DB constraint; pre-fix DLQ entries settle on replay
C6-5       Full payload   — ledger, outbox and DLQ hold every field Stripe sent
C6-6       DLQ outage     — invalid event whose DLQ write fails → 503 (real lock, no mocks)
C6-7       Two workers    — a second replica drains while the first holds its batch
C6-8       Refused rows   — an event the database refuses (> 50 000 chars) → DLQ, not 500
C6-9       RLS truth      — superuser / BYPASSRLS / a table without RLS all read as inert;
                            provision.py rotates billing_app from APP_DATABASE_URL
```

**Mutation-verified.** A passing suite proves nothing until you watch it fail. Seventeen deliberate breaks — dropping `extra=` promotion, removing currency normalization, reverting amount extraction, unauthenticating `/metrics`, labelling by resolved path, ignoring `from_retry`, removing the stale-claim reclaim, removing the 409, removing the guarded recovery, dropping the `status <> 'resolved'` guard, letting `dlq_depth` count resolved rows again, reverting the pool to `minconn=1`, quietly changing a rate-limit default, making each `_tx` accept its tenant scope and silently never apply it, dropping the `/metrics` render lock, and dropping the pool's `autocommit` — each turned the suite red. The guarded-recovery mutant reproduced the original defect exactly: two rows stranded in `retrying` because one row's failure aborted the batch. The two scope mutants reproduced the state row-level security shipped in before it was in force: policies present, nothing enforced — the API's raised `new row violates row-level security policy`, the worker's silently drained zero rows.

Writing a mutation test is itself easy to get wrong in a way that looks like success. The first `/metrics` reproduction keyed its interleaving on a thread name, and FastAPI dispatches synchronous handlers to an anyio worker thread — so the instrumentation never fired, the leak never reproduced, and the run reported "no leak observed". A18 asserted that the interleave actually happened, because a concurrency test that silently stops being concurrent is worse than no test. Cycle 6 retired that check with the property it guarded: `/metrics` became operator-only, so no tenant reads those gauges any more.

Cycle 6 added fourteen more, one or more per fix, and each reproduced the defect it guards against rather than failing somewhere else: the suite's guard skipped (exit 1 instead of 2), `request: dict` restored (422 back), the idempotency `.get` default restored (500 back), dev mode forced on (six refusal checks), five ways of undoing the subscription change — one of them caught by the database constraint alone — the four-field payload restored, a DLQ write result ignored (200 back), the candidate-step lock removed (second worker "got 0" back), the refused-row handler removed (500 back), and the BYPASSRLS check removed (`"enforced": true` for a role that bypasses every policy).

**Why no mocks:** the concurrency bug does not live in `process_stripe_event()`. It lives at the thread-connection boundary — specifically in whether PostgreSQL's constraint engine serializes two concurrent inserts. A mock returns whatever you tell it to return; it cannot reproduce `ON CONFLICT DO NOTHING` semantics. The only proof that the idempotency guarantee holds is five real threads firing the same `event_id` at a real `PRIMARY KEY` constraint simultaneously.

**Test isolation:** each section calls `fresh_conn()`, which issues `TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY` — resets data and `BIGSERIAL` sequences. No shared state between sections. Every assertion starts from a known-empty database.

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

| Gap | Priority | Status | Fix |
|---|---|---|---|
| Runtime ingress (Caddy) | **CRITICAL** | Done | `CADDY_DOMAIN` never reached the Caddy container, so `{$CADDY_DOMAIN}` expanded to nothing, the site block degenerated into a global options block, and Caddy crash-looped on every `up` — HTTPS ingress had never worked. Fixed, plus an admin-API healthcheck so nothing gates on a container that silently never started. |
| `X-Forwarded-For` trust | **HIGH** | Done | Was `--forwarded-allow-ips "*"`, letting any caller pick its own rate-limit bucket. Now pinned to Caddy's static address. See *Rate Limiting* above. |
| Container healthcheck | **HIGH** | Done | Probed bare `localhost`, which musl resolves to `::1` while uvicorn binds IPv4 only — a healthy service reported `unhealthy` on every start. Now probes `127.0.0.1`. |
| DLQ self-amplification | **HIGH** | Done | Retrying an entry whose event already reached the ledger appended a *new* DLQ row while resolving the old one, so the queue grew by one row per retry and `dlq_depth` became inflatable by retry activity. A replay now never creates DLQ rows, and `resolved` is terminal — 409 at the endpoint and a `status <> 'resolved'` guard on the one UPDATE that wrote status without checking it. |
| Stranded retry claims | **HIGH** | Done | `retry_dlq_batch` commits its claim before doing the work; a failure in the recovery path left rows in `retrying` that the claim query never selected again. Migration 0003 adds `claimed_at`; stale claims are reclaimed automatically. |
| Outbox worker | **HIGH** | Done | Standalone Docker service — `SELECT FOR UPDATE SKIP LOCKED`, exponential backoff, SIGTERM-safe. |
| DLQ retry budget | **HIGH** | Done | `retry_count`/`max_retries`/`next_retry_at` columns, exponential backoff, `POST /dlq/{id}/retry`, and a worker retry loop are implemented. Only `TRANSIENT` entries auto-retry; nothing in the live ingest path classifies an event as `TRANSIENT` yet (single-event DB failures return 503 so Stripe retries). The mechanism is provisioned for a future non-Stripe / async source with no upstream retrier. |
| DLQ retention | **MEDIUM** | Done | `prune_dlq()` deletes `DUPLICATE` rows older than 90 days; the worker runs it at most once per hour. Without retention, Stripe's at-least-once redelivery grows the `dlq` table without bound. |
| Schema migrations | **HIGH** | Done | Alembic migrations in `alembic/versions/` manage the production schema; the `migrate` service runs `alembic upgrade head`, then `provision.py`, before the API starts. CI applies the migrations too — `_bootstrap()` mirrors the tables with `IF NOT EXISTS` guards but creates neither the RLS policies nor the `billing_app` role, so a suite running without alembic could not have tested either. The API no longer calls `_bootstrap()` when it runs as the restricted role: it has no DDL rights, and shipping owner credentials to the container that serves traffic would undo the point of the role. |
| Per-event-type amount extraction | **MEDIUM** | Done | A failed invoice carries `amount_paid=0` with the real figure in `amount_due`, so reading `amount_paid` for every type dead-lettered every `invoice.payment_failed` — the events dunning runs on. `_AMOUNT_FIELDS_BY_TYPE` now maps each type to its field priority. An event with no amount field at all still routes to the DLQ: an unknown amount recorded as $0 reconciles cleanly and is therefore invisible. |
| Structured JSON logging | **MEDIUM** | Done | `LOG_FORMAT=json` emits one JSON object per line; `transaction_id`, `outcome`, `duration_ms`, `path` and `status` are top-level fields. API and worker share the formatter so an event can be correlated from ingest to dispatch. |
| Prometheus `/metrics` | **MEDIUM** | Done | `webhook_events_total{outcome}`, `http_requests_total`, `http_request_duration_seconds`, `dlq_depth` (backlog), `dlq_total` (audit count), `outbox_pending`. Key-protected, labelled by route template. |
| Currency normalization | **LOW** | Done | Lowercased at the entry boundary by a `mode="before"` validator, so `USD` is accepted and stored as `usd` rather than dead-lettered. `LedgerEntry` still rejects un-normalized input as a second layer. |
| Dependency pins | **MEDIUM** | Done | `psycopg2-binary==2.9.9` had no cp313 wheel, so the documented "Python 3.12+" prerequisite could not be followed. Bumped to 2.9.12; supported range stated explicitly. |
| CI blind spot | **HIGH** | Done | No workflow built the image or started compose, which is why the three runtime findings above survived four audit cycles with green badges. `stack.yml` now builds, boots, and attacks the stack. |
| Multi-tenancy | — | Partial | `org_id` on every table with a composite `(org_id, transaction_id)` primary key, so the idempotency guard is per-tenant; the tenant is derived from the API key rather than a header; ops queries scoped; per-tenant ingest verified with that tenant's own signing secret; the worker delivers to each tenant's `downstream_url` and drains round-robin so one tenant's backlog cannot starve another. Remaining: no admin credential — see [SECURITY.md](SECURITY.md). |
| Row-level security | **HIGH** | Done | Isolation was a convention every query had to keep, and on 2026-08-11 the batch path forgot it in two branches — one tenant's payload readable by another, one Stripe event POSTED under two tenants. `ledger`, `outbox` and `dlq` now carry default-deny policies with `USING` and `WITH CHECK`, and the service connects as `billing_app` (`NOSUPERUSER NOBYPASSRLS`). The role is the part that matters: a superuser bypasses RLS regardless of `ENABLE` or `FORCE`, so policies alone enforce nothing. The process logs `rls_enforced` or `rls_inert` at every boot, and CI fails the build on `rls_inert`. |
| GDPR / LGPD erasure | — | Not attempted | Conflicts directly with the append-only ledger this system is built around. Resolving that needs a documented retention and pseudonymization policy first, not code. |
| PgBouncer + Kubernetes | — | Not attempted | Deployment topology, not application behavior. The `--workers 1` / `maxconn=20` note above is the prerequisite to settle first. |

---

## Origin

The fault-tolerance patterns here — idempotent inserts, transactional outbox, dead-lettering — are derived from [Aequitas](https://github.com/DDRG15/aequitas-privacy-engine), a high-throughput engine built to process 75k+ events per second with zero data loss under hard kills.

In billing, a single race condition is a financial discrepancy. This system treats data like currency.

---

*Diego Alonso Del Río García — August 9, 2026*
