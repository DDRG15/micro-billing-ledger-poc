# Micro-Billing-Ledger

[![Tests](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/test.yml/badge.svg)](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/test.yml)
[![Lint](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/lint.yml/badge.svg)](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/lint.yml)
[![Stack](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/stack.yml/badge.svg)](https://github.com/DDRG15/micro-billing-ledger-poc/actions/workflows/stack.yml)

A backend service that receives payment events from Stripe, records each one in a
PostgreSQL ledger exactly once, and forwards it to another system. Stripe sends the same
event more than once when it is unsure a delivery arrived, and servers restart and
databases fail at bad moments, so the hard requirement is that a payment event is never
lost and never counted twice. Several customers ("tenants") can share one deployment
without seeing each other's data.

**Status:** proof of concept. It is built and tested to behave correctly under failure,
but it has not carried real traffic, and the [limitations](#limitations) are listed below.
**Stack:** Python 3.12 and 3.13 · FastAPI · Pydantic v2 · PostgreSQL 16 · Docker Compose ·
Caddy. **Tests:** 817 checks in one script, plus a separate end-to-end failure harness.

---

## How it works

```mermaid
flowchart LR
    S["Stripe"] -->|"signed webhook"| C["Caddy<br/>TLS, size and time limits"]
    C --> A["API (ledger.py)<br/>verify, validate, record"]
    A -->|"one transaction"| DB[("PostgreSQL<br/>ledger + outbox")]
    A -.->|"rejected or repeated events"| D[("dead-letter queue")]
    DB --> W["Worker (worker.py)<br/>claim, deliver, record result"]
    W -->|"HTTP POST"| T["Tenant's downstream system"]
```

1. **Receive.** Stripe posts an event to Caddy, the web server in front of the service.
   Caddy handles HTTPS, refuses bodies over 1 MiB and cuts clients that send too slowly.
2. **Verify and validate.** The API checks Stripe's signature using the tenant's own
   signing secret, then validates the event: known type, a customer id, an amount, a
   three-letter currency.
3. **Record once.** In a single database transaction the API inserts the event into the
   `ledger` table and a "to deliver" note into the `outbox` table. The ledger's primary
   key is `(org_id, transaction_id)`, so a second copy of the same event is refused by
   the database itself, and the API answers `200` with `DLQ_DUPLICATE`. A payment is keyed
   too: at most one `POSTED` row per tenant and Stripe invoice, so a different event
   announcing a payment already recorded is answered `DUPLICATE_PAYMENT` and recorded
   nowhere but the `dlq`.
4. **Keep what was rejected.** Invalid events, repeats and event types the ledger does
   not record are stored in the `dlq` table with the full payload. Stripe still gets a
   `200`, because asking it to resend a bad event would not fix it. If the database
   cannot take the event, the API answers `503` and Stripe retries later.
5. **Deliver.** A separate worker process reads the outbox, posts each event to the
   tenant's `downstream_url`, and marks it delivered. Failures wait and retry; nothing
   is dropped.
6. **Operate.** Authenticated endpoints show counts, list and replay dead-lettered
   events, and expose Prometheus metrics.

Terms used above and below:

- **Idempotency:** handling the same event twice has the same effect as handling it once.
  Here the database's unique key enforces it, with no application lock.
- **Transactional outbox:** the event and its "to deliver" note are written in the same
  database transaction, so one cannot exist without the other.
- **Dead-letter queue (DLQ):** a table holding events the system set aside, with enough
  detail for a person to look at them or replay them.
- **Row-level security (RLS):** PostgreSQL filters every query by tenant, so a query that
  forgets its own tenant filter returns nothing instead of everything.
- **Lease:** a time-limited claim on a piece of work. A worker that crashes loses its
  lease when it expires, and another worker picks the work up.

The full request path, every component and every failure response are drawn in
[docs/architecture.md](docs/architecture.md), which GitHub renders. The same diagrams are
available as an interactive viewer, [docs/architecture.html](docs/architecture.html): hover
or focus a node to see what flows into it (blue) and out of it (amber). GitHub shows HTML
files as source, so download the file and open it in a browser.

---

## Quick start

You need Docker with Compose, and Python 3 for the test script. Nothing else is installed
on the host.

```bash
cp .env.example .env        # optional: compose has the same dev defaults built in
docker compose up -d        # PostgreSQL, migrations, API, worker, Caddy
docker compose ps           # wait until billing shows "healthy"
```

Send signed test events. Stripe signs every request, and the service verifies the
signature even with the placeholder secret, so a bare `curl` correctly gets `400`.
`send_test_webhook.py` signs the way Stripe does, using the secret the stack runs with
(standard library only, no Stripe account):

```bash
python send_test_webhook.py --all
```

```
Valid invoice.paid:
  POSTED expected: HTTP 200  {"outcome":"POSTED","transaction_id":"evt_demo_...","reason":null}
Same event twice - the idempotency guard fires on the second:
  1st -> POSTED: HTTP 200  {"outcome":"POSTED",...}
  2nd -> DLQ_DUPLICATE: HTTP 200  {"outcome":"DLQ_DUPLICATE",...}
Invalid payload (currency 'xx' is not a real ISO 4217 code):
  DLQ_INVALID expected: HTTP 200  {"outcome":"DLQ_INVALID",...}
Failed invoice - amount lives in amount_due, not amount_paid:
  VOID expected, recorded at 7500: HTTP 200  {"outcome":"VOID",...}
```

(Abridged. `--all` also prints the ledger summary and the metrics.) Then look at the
results with the API key from `.env.example`:

```bash
curl -H "X-Api-Key: dev_key_change_in_production" http://localhost:8000/ledger/summary
curl -H "X-Api-Key: dev_key_change_in_production" "http://localhost:8000/dlq/entries?limit=10"
docker compose logs worker | grep dispatch       # the worker's delivery log lines
```

The worker runs in log-only mode by default (`WORKER_LOG_ONLY=true`): it marks events
delivered and logs them without calling anyone. Set `DOWNSTREAM_URL`, or a tenant's
`org.downstream_url`, to send them to a real endpoint. `--url https://localhost` sends
through Caddy instead of the direct port. `docker compose down -v` removes everything.

### Running the tests

The suite truncates tables, so it refuses any database whose name does not end in `_test`
or `_bench`. Create one next to the stack's database and apply the migrations first:

```bash
docker compose up -d --wait postgres
docker compose exec postgres createdb -U postgres billing_test
pip install -r requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/billing_test alembic upgrade head
python test_ledger.py
```

---

## What it guarantees

Each guarantee names the mechanism and the section of `test_ledger.py` that checks it
(see [docs/testing.md](docs/testing.md) for the full list).

| Guarantee | Mechanism | Checked by |
|---|---|---|
| An event is never counted twice, even when five requests carry it at the same moment | Primary key `(org_id, transaction_id)` and `INSERT ... ON CONFLICT DO NOTHING`; the database serializes the race | Phase 5 (five threads, one event id, one ledger row); A9 |
| A payment is never counted twice, however many events announce it: one `POSTED` row per tenant and invoice | Unique index `uq_ledger_one_posted_per_invoice` on `(org_id, invoice_id)` for `POSTED` rows, and a CHECK that every `POSTED` or `VOID` row names its invoice (migration 0012). A second event for a paid invoice is answered `200` as `DUPLICATE_PAYMENT`, with no ledger row, no outbox row and one resolved `dlq` row | C6-32 (every order of the four events of one payment; eight concurrent events per invoice, also as `billing_app`; batch and DLQ replay; two tenants; the migration) |
| The ledger and the delivery queue never disagree | Ledger row and outbox row written in one transaction | The "Outbox written atomically" check |
| An event the database could not store is retried by Stripe, not acknowledged | `503` with `Retry-After` on pool exhaustion or a dead connection; `503` when a rejected event could not even be written to the DLQ | C6-6, C6-20 |
| A rejected event is kept in full, and a repeated one leaves a single row | Raw payload stored in `dlq`; unique partial indexes allow one `DUPLICATE`, one `UNSUPPORTED` and one `DUPLICATE_PAYMENT` row per event | C6-5, C6-27, C6-28, C6-32 |
| `dlq_depth` counts only rows that need a person: Stripe redelivering an event does not raise it | `DUPLICATE`, `UNSUPPORTED` and `DUPLICATE_PAYMENT` rows are written `resolved` on every path; migration 0013 settled the `pending` `DUPLICATE` rows written before | C6-33 |
| A delivery is retried until it succeeds, and never dropped | Lease, per-row result commit, backoff of 30, 120, 600 then 3,600 s with no attempt limit | Phase 7, C6-12 |
| A broken downstream cannot starve the others or fake a success | Per-tenant tasks on a bounded thread pool; a 15 s dispatch deadline; a redirect counts as a failure; rows are handed back on shutdown | C6-15, C6-16, C6-17, C6-22, C6-29 |
| Tenants cannot read or write each other's rows, even through a bug | Row-level security enforced for the restricted `billing_app` role; the tenant comes from the API key, never from a header | A9 to A16 |
| Hostile input is bounded | 1 MiB body cap (Caddy and the app), slow-client timeouts at Caddy, event ids limited to printable ASCII, failed authentications rate-limited before the database lookup, interactive API docs off outside dev | C6-14, C6-19, C6-21, C6-24, C6-25, C6-26 |
| Every error response is recorded with a class and a type, including the 413 and the 500 | Pure ASGI observability layer outside the body cap; a fixed table of types; Caddy access log with request headers removed | C6-30, C6-31 |
| The service will not run on published demo credentials unless the run is declared local | Start-up refusal unless `BILLING_ENV=dev` and the ingress is on loopback | C6-3 |

**Delivery is at least once, not exactly once.** A worker that crashes after delivering
but before recording the result will deliver that event again once its lease expires.
Every delivery carries `X-Transaction-Id`, so the receiver can discard repeats.

---

## How it was tested

**The suite.** `test_ledger.py` is a single script that prints one line per check and
exits non-zero on a failure: 817 checks against a live PostgreSQL 16. The guarantees above
are behaviour of the database (a unique key serializing concurrent inserts, policies
applying to a restricted role, row locks skipping claimed rows), so the suite runs
against one rather than a mock. CI runs it on Python 3.12 and 3.13, runs ruff and bandit,
and builds and attacks the real Compose stack (`.github/workflows/`).

**Mutation testing.** Reverting a fix and watching its test fail is how you know the test
tests something. For the later fixes that was done by hand; there is no automated mutation
run in the repository. Details are in [docs/testing.md](docs/testing.md).

### Failure testing

The unit-level suite proves each mechanism; the harness in [`e2e/`](e2e/README.md) checks
that they hold together. It runs the full stack with a real downstream receiver, then
loads, interrupts and attacks it from outside. Three things it found, measured on a local
Compose run before they were fixed:

- One unsigned 100 MB request killed the API container for lack of memory.
- A downstream that answered `302` made the worker mark events delivered that never
  arrived.
- With 64 concurrent clients, 4,526 of 20,000 requests were answered `500` or `502`.

After the fixes, from the last full run (one machine, 12 cores, 14 GB RAM, default
Compose limits, API container capped at 0.5 CPU and 256 MB):

| Scenario | Result |
|---|---|
| 20,000 signed webhooks, 64 concurrent clients | all `200`; each in the ledger exactly once; no duplicate deliveries |
| 10,000 signed webhooks, 200 concurrent clients | all `200`; p99 under 2 s |
| PostgreSQL killed for 6 s under load | only `503` during the outage; no acknowledged event lost |
| Unsigned 100 to 300 MB bodies, direct and through Caddy | `413` or a cut upload; no restart; memory flat |
| Slow-header client through Caddy | connection closed after about 10 s |

No acknowledged event was lost or delivered twice in any run. The numbers depend on the
machine; the properties are what the run checks. `benchmark.py` measures throughput on
your own hardware, so this repository publishes no throughput figure.

---

## Operating it

| Method | Path | Auth | What it does |
|---|---|---|---|
| `POST` | `/webhook/stripe/{org_id}` | Stripe signature | Ingest for one tenant, verified with that tenant's signing secret. An unknown tenant returns the same `400` as a bad signature. |
| `POST` | `/webhook/stripe` | Stripe signature | Ingest for the `default` tenant. |
| `GET` | `/ledger/summary` | API key | Ledger counts by status, DLQ depth, outbox backlog, for the caller's tenant. |
| `GET` | `/dlq/entries` | API key | Dead-lettered events, newest first. Filter with `status` (`pending`, `retrying`, `resolved`, `exhausted`, `actionable`), page with `before_id`, `limit` 1 to 1,000. |
| `POST` | `/dlq/{id}/retry` | API key | Replay one entry. `409` if it is already `resolved`. |
| `GET` | `/metrics` | operator key | Prometheus metrics for all tenants. A tenant key gets `403`. |
| `GET` | `/health` | none | Liveness. It does not check the database. |
| `GET` | `/docs`, `/redoc`, `/openapi.json` | none | Only when `BILLING_ENV=dev` and the ingress is on loopback; otherwise `404`. |

Webhook responses: `200` for any event that was handled, including rejected ones;
`400` for a bad signature or unknown tenant; `413` over 1 MiB; `422` for a malformed
envelope; `429` over the rate limit; `503` when the database cannot take the event.

**Outcomes** in the `200` body: `POSTED` (invoice paid; at most one per tenant and
invoice), `VOID` (payment failed, recorded at `amount_due`, one row per
`invoice.payment_failed` event, never revenue), `LIFECYCLE` (subscription created, updated
or deleted, stored with no amount), `DLQ_INVALID` (failed validation: a `pending` DLQ row
that needs a person), and three outcomes that need nobody, each stored as a `resolved` DLQ
row, not counted in `dlq_depth`, answered `409` on manual retry and pruned after 90 days:
`DLQ_DUPLICATE` (Stripe redelivered an event the ledger already holds), `DUPLICATE_PAYMENT`
(a new event for an invoice whose payment is already `POSTED`) and `UNSUPPORTED` (a
well-formed Stripe event of a type the ledger does not record).

**Metrics.** `webhook_events_total{outcome}`, `http_requests_total{method,path,status}`,
`http_errors_total{status,error_class,error_type}` (one sample per response with status 400
or above, see "Errors: class and type" below),
`http_request_duration_seconds` (labelled by route template, not by resolved path),
`dlq_depth` (rows needing action: `pending`, `retrying` or `exhausted`; a redelivery
or a duplicate payment does not move it), `dlq_total`, `outbox_pending` and
`billing_secrets_info` (a hash prefix of the live secrets, to confirm a rotation took
effect). The queue gauges are read from the database on each scrape. Prometheus is not
part of the Compose stack. To scrape with the operator key, send it as a header:

```yaml
scrape_configs:
  - job_name: billing
    http_headers:
      X-Api-Key:
        files: [/etc/prom/billing.key]
```

**Logs.** `LOG_FORMAT=json` (the Compose default) writes one JSON object per line from
both the API and the worker. At start-up the API logs whether row-level security is in
force:

```bash
docker compose logs billing | grep -E "rls_enforced|rls_inert"
```

Every response leaves one `http_request` line with `method`, `path` (the route template),
`status`, `duration_ms` and `client_ip`. When the status is 400 or above the same line
also carries `error_class` and `error_type`:

```bash
docker compose logs billing | grep '"event": "http_request"' | grep error_type
```

Caddy writes one JSON access line per request to its stdout, with the request headers
removed (`X-Api-Key` and `Stripe-Signature` would otherwise be logged in plaintext):

```bash
docker compose logs caddy | grep '"http.log.access'
```

**Errors: class and type.**

Each error response is recorded with a class (what kind of failure) and a type (which
one). Both come from a fixed table in `ledger.py`, never from the request, so the metric's
cardinality is bounded. "Recorded" means one log line and one `http_errors_total` sample.
It does not mean alerted: Prometheus is not part of the Compose stack, so nothing pages
anyone until something outside this repository scrapes `/metrics` and evaluates a rule.

| Status | Class | Type | Origin |
|---|---|---|---|
| 400 | `authentication` | `invalid_signature` | Signature does not verify against the tenant's secret |
| 400 | `authentication` | `unknown_tenant` | `org_id` names no active tenant. The response body is the same as `invalid_signature`, so a caller cannot enumerate tenants; only the log and the metric tell them apart |
| 400 | `validation` | `malformed_tenant_id` | `org_id` does not match the tenant id pattern. Same response body as above |
| 401 | `authentication` | `missing_api_key` | No `X-Api-Key` header |
| 401 | `authentication` | `invalid_api_key` | Key matches no tenant and not `BILLING_API_KEY` |
| 403 | `authorization` | `operator_key_required` | A tenant key on `/metrics` |
| 404 | `not_found` | `route_not_found` | No such route, including `/docs` outside a dev run |
| 404 | `not_found` | `dlq_entry_not_found` | `POST /dlq/{id}/retry` for an id this tenant does not have |
| 405 | `validation` | `method_not_allowed` | The route exists, the method does not |
| 409 | `conflict` | `dlq_already_resolved` | Retry of a `resolved` DLQ entry |
| 413 | `size_limit` | `body_too_large` | Body over `MAX_REQUEST_BODY_BYTES`, or a malformed `Content-Length` |
| 422 | `validation` | `request_validation` | The JSON body is not a valid envelope |
| 422 | `validation` | `invalid_query` | A query or path parameter is invalid (`limit`, `status`, a non-integer `dlq_id`) |
| 429 | `rate_limit` | `rate_limited` | A per-route limit (`RATE_LIMIT_WEBHOOK`, `RATE_LIMIT_OPS`, `RATE_LIMIT_METRICS`) |
| 429 | `rate_limit` | `auth_failure_budget` | The client spent its failed-authentication budget |
| 500 | `internal` | `unhandled_exception` | An exception no handler caught |
| 503 | `dependency` | `database_unavailable` | `psycopg2.OperationalError` or `InterfaceError` |
| 503 | `dependency` | `pool_exhausted` | Every pooled connection stayed checked out for `DB_POOL_WAIT_SECONDS` |
| 503 | `dependency` | `ingest_db_failure` | The ingest path's own `RuntimeError` (event or DLQ row not stored) |

A response that reaches the middleware without a type is recorded as class
`unclassified`, type `unclassified_4xx` or `unclassified_5xx`. A test fails if any known
path ends there, and another test rejects a bare `HTTPException` in `ledger.py`.

A few useful queries:

```promql
sum by (error_type) (rate(http_errors_total[5m]))
http_errors_total{error_class="dependency"}
http_errors_total{error_type="unclassified_4xx"} > 0
```

Caddy marks the errors it produces itself with the same names, as extra fields on its
access line: `413` is `size_limit` / `body_too_large`, and `502` (the `billing` container
unreachable) is `dependency` / `upstream_unavailable`. Anything the application returns,
including its own `4xx` and `5xx`, is a normal proxied response: Caddy logs the status
and the application's line carries the class and type.

**What is not recorded, measured against the `caddy:2-alpine` image (Caddy 2.11.9):**

| Case | What Caddy and the application leave |
|---|---|
| Headers not finished within `read_header` (10 s) | Nothing. Caddy closes the connection with no response, and writes no line at the default log level or at debug level (only the TLS handshake) |
| Body not finished within `read_body` (30 s) | Caddy answers `499`; one access line with `status` 499, `bytes_read` below the `Content-Length` and a duration of about 30 s. No class or type: Caddy's error routes do not run for 499. The application never sees the request |
| Client closes the connection mid-body | Same `499` line from Caddy, after however long the client took. Duration tells it apart from the 30 s case. Directly against the application there is no line and no counter |
| A request that is not valid HTTP | uvicorn answers `400` and writes its own `Invalid HTTP request received` warning. No `http_request` line, no class, no type |
| A request for a `Host` that matches no site | Caddy answers an empty `200`; its default logger writes the line with status 0 |

**Adding a tenant.** There is no admin API; a tenant is a row in `org`. The `INSERT` is
in `.env.example`. Restart the API afterwards: whether any tenant holds a key is read once
at start-up.

**Data model.** Four tables, created by the migrations in `alembic/versions/`:
`org` (tenant registry: API key hash, Stripe signing secret, downstream URL), `ledger`
(one row per recorded event; amounts in cents as `BIGINT`; `invoice_id`, the Stripe
invoice an invoice row belongs to, on which at most one `POSTED` row per tenant is
allowed), `outbox` (delivery state per
event: attempts, next attempt, lease, last error) and `dlq`. The migrations are the source
of truth for the schema.

---

## Configuration

Every environment variable the services read. "Compose" says whether
`docker-compose.yml` passes it through; the others take effect if you add them to the
service's `environment:` block.

| Variable | Default | Purpose | Compose |
|---|---|---|---|
| `DATABASE_URL` | `postgresql://postgres:postgres@localhost:5432/billing` | Owner connection: migrations, `provision.py`, local runs | yes (`migrate`) |
| `APP_DATABASE_URL` | `DATABASE_URL`; in Compose `billing_app` | Restricted role the API and worker connect as; this is what puts row-level security in force | yes |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | `postgres`, `postgres`, `billing` | Credentials and name of the Compose PostgreSQL | yes |
| `STRIPE_WEBHOOK_SECRET` | `whsec_YOUR_SECRET_HERE` | Signing secret of the `default` tenant (written to the registry by `provision.py`) | yes |
| `STRIPE_WEBHOOK_SECRET_FILE` | empty | Read the secret from this file instead; an unreadable file stops the service | yes |
| `BILLING_API_KEY` | empty; Compose `dev_key_change_in_production` | API key of the `default` tenant and the operator key for `/metrics` | yes |
| `BILLING_API_KEY_FILE` | empty | Read the key from this file instead | yes |
| `BILLING_ENV` | empty; Compose `dev` | `dev` allows demo credentials and the API docs, if the ingress is also on loopback | yes |
| `INGRESS_BIND_ADDR` | `127.0.0.1` | Address Caddy's ports are published on; a non-loopback value revokes dev mode | yes |
| `CADDY_DOMAIN` | `localhost` | Domain Caddy gets a certificate for | yes |
| `FORWARDED_ALLOW_IPS` | Compose `172.28.0.10` | Proxy address uvicorn trusts for `X-Forwarded-For` (an exact address, not a range) | yes |
| `LOG_FORMAT` | `text`; Compose `json` | `json` or plain text logs | yes |
| `DB_POOL_SIZE` | `20` (worker: `1` in Compose) | Connections in the API's pool | yes |
| `DB_POOL_WAIT_SECONDS` | `5` | How long a request waits for a free connection before `503` | no |
| `MAX_REQUEST_BODY_BYTES` | `1048576` | Request body cap in the app (Caddy has its own 1 MiB cap) | no |
| `RATE_LIMIT_WEBHOOK` | `100/minute` | Per client IP, webhook routes | yes |
| `RATE_LIMIT_OPS` | `20/minute` | Per client IP, summary and DLQ routes | yes |
| `RATE_LIMIT_METRICS` | `60/minute` | Per client IP, `/metrics` | yes |
| `RATE_LIMIT_AUTH_FAILURES` | `20/minute` | Failed authentications per client IP before `429` | no |
| `DOWNSTREAM_URL` | empty | Delivery URL for tenants without their own `downstream_url` | yes |
| `WORKER_LOG_ONLY` | `false`; Compose `true` | Allows the worker to run with no URL, logging events instead of delivering them; with `false` and no URL it refuses to start | yes |
| `WORKER_BATCH_SIZE` | `100` | Outbox rows claimed per poll | yes |
| `WORKER_POLL_INTERVAL` | `5` | Seconds to wait when there is nothing to do | yes |
| `WORKER_DISPATCH_CONCURRENCY` | `8` | Tenants delivered to at once | no |
| `DISPATCH_SOCKET_TIMEOUT` | `10` | Seconds per connect or read to a downstream | no |
| `DISPATCH_DEADLINE_SECONDS` | `15` | Total seconds one delivery may take | no |
| `WORKER_LEASE_SECONDS` | `WORKER_BATCH_SIZE * DISPATCH_DEADLINE_SECONDS + 60` (1,560) | How long a claim keeps other workers off a row | no |

`.env.example` documents each value that matters before a deployment is reachable from a
network.

---

## Security

The main controls, in one place. [SECURITY.md](SECURITY.md) has the reasoning, what is
deliberately not covered, and how to report a vulnerability.

- Every webhook's Stripe signature is verified with the tenant's own secret, also when the
  secret is still the placeholder.
- Operational endpoints need an API key. The tenant is derived from the key, never from a
  header. `/metrics` needs the operator key.
- PostgreSQL enforces tenant isolation (row-level security) for a role that is neither
  superuser nor `BYPASSRLS`; the API says at every start-up whether it is in force.
- The service refuses to start on the published demo credentials unless the run is
  declared local.
- Request size and speed are limited at Caddy and in the app; failed logins are
  rate-limited.
- Every error response is recorded with a class and a type. Caddy's access log leaves out
  the request headers, so `X-Api-Key` and `Stripe-Signature` never reach it.
- The container runs as a non-root user and `/docs` is off outside dev.

These are engineering controls, not a compliance programme.

---

## Limitations

- **There is no PostHog adapter**, despite the repository's folder name. The worker sends
  each event as generic JSON over HTTP POST (headers `X-Event-Type`, `X-Transaction-Id`,
  `X-Org-Id`).
- **Five Stripe event types are recorded:** `invoice.paid`, `invoice.payment_failed` and
  `customer.subscription.created`, `.updated` and `.deleted`. Other types are answered
  `200` and stored as `UNSUPPORTED`. The fixtures in `fixtures/stripe/` are built from
  Stripe's API reference. The service has also been reconciled against a Stripe sandbox
  (test mode, API version `2026-10-28`) with [`e2e/stripe_sandbox.py`](e2e/README.md#stripe-sandbox-reconciliation):
  1,817 events of 70 types plus 60 Stripe resends, all answered `200`; Stripe's own count
  of paid invoices, failed payments and subscription events equals the ledger's, with the
  same event ids and the same amounts to the cent (81 paid invoices, 128,500 cents). It
  has not received live-mode traffic, and the run raised the webhook rate limit and
  called the API directly, not through Caddy.
  [docs/architecture.md](docs/architecture.md#verified-against-a-stripe-test-mode-account)
  has the full result.
- **No local spool when PostgreSQL is down.** The API answers `503` and Stripe's retries
  are the buffer. Accepting the event with `200` and holding it in memory would lose it
  on a restart, and Stripe never resends an event it was told arrived. A durable spool on
  local disk would remove the wait for Stripe's retry, at the cost of a second store to
  replay, monitor and secure; for a proof of concept that is not worth it.
- **One shared API key per tenant.** No user accounts, roles or key-rotation flow, and no
  log of who read what.
- **Tenants are added with SQL**, and a tenant's Stripe signing secret is stored in plain
  text in `org` (verifying a signature needs the value; in production it belongs in a
  secret manager).
- **Delivery is at least once**; receivers must deduplicate on `X-Transaction-Id`.
- **A failed payment is recorded once per event, not once per attempt.** Each
  `invoice.payment_failed` event is a `VOID` row and is delivered downstream. Stripe's
  `attempt_count` counts only automatic retries, so nothing identifies an attempt more
  finely than its event, and two events Stripe sent for the same failed attempt would make
  two `VOID` rows. No total the service exposes adds `VOID` amounts. Each delivery carries
  the full invoice, `attempt_count` included, for a receiver that has to tell attempts
  apart.
- **The DLQ's automatic retry loop has nothing to retry today.** It handles `TRANSIENT`
  entries, and nothing in the ingest path writes that reason; a database failure returns
  `503` so Stripe retries. Rejected events are replayed by hand.
- **One API process and one worker**, with a 256 MB, 0.5 CPU limit on the API container.
  It has not been run behind PgBouncer or on Kubernetes.
- **Secrets are read once at start-up.** Rotating means recreating the container.
- **No erasure path.** The ledger is append-only by design, which conflicts with
  GDPR-style deletion until a retention policy is decided.
- **Prometheus is not included** in the Compose stack.

---

## Roadmap

1. **Checkout and PaymentIntent event types** (next), with captured payloads saved as
   fixtures in place of the reference-built ones. The sandbox run already delivers
   `payment_intent.*` and `charge.*` events, which are stored as `UNSUPPORTED` today.
2. Deferred connection-pool start-up, then retiring `_bootstrap()`.
3. An optional PostHog adapter.

What, when, where, why and done-when for each item are in
[docs/architecture.md](docs/architecture.md#proposals-order-and-dependencies).

---

## Origin and licence

The fault-tolerance patterns (idempotent inserts, transactional outbox, dead-lettering)
come from [Aequitas](https://github.com/DDRG15/aequitas-privacy-engine), an earlier
project by the same author.

Licensed under the [MIT License](LICENSE). Diego Alonso Del Río García.
