# Testing

The README summarizes how the system is tested. This page is the longer version: what
each section of the suite covers, how the suite is built, the checks that run outside it
(Caddy, static analysis, a Stripe sandbox), and how the benchmark works.

## The suite

`test_ledger.py` is a single script, not a pytest project. It prints one line per check
and a final `N passed | M failed` line, and exits non-zero on any failure. It needs a
live PostgreSQL, because the guarantees it checks (a unique constraint serializing
concurrent inserts, row-level security applied to a restricted role, row locks that skip
claimed rows) are behaviour of the database, and a test double cannot establish them.

```bash
docker compose up -d --wait postgres
docker compose exec postgres createdb -U postgres billing_test
pip install -r requirements.txt
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/billing_test alembic upgrade head
python test_ledger.py
```

The migration step is required. The row-level security policies and the restricted
`billing_app` role exist only in migration 0006, and the sections that prove tenant
isolation connect as that role. Set `DATABASE_URL` inline rather than exporting it: an
exported value also reaches `docker compose up` and points the `migrate` container at
`localhost`.

**A disposable database is enforced.** The suite truncates `ledger`, `outbox` and `dlq`
between sections, so it refuses to run (exit code 2, before it connects) against any
database whose name does not end in `_test` or `_bench`. `benchmark.py` applies the same
rule. The check lives in `disposable_db.py`; it checks `APP_DATABASE_URL` too when it is set,
and it refuses a DSN it cannot parse (the key=value form) rather than guess.

**CI** (`.github/workflows/test.yml`) runs the suite on Python 3.12 and 3.13 against
PostgreSQL 16, after applying the migrations. `lint.yml` runs ruff and bandit, both
also over the `e2e/` scripts. `stack.yml` builds the Docker image, starts the full Compose stack and attacks it, which
covers what the suite cannot: the real ingress, the container health checks and the
`X-Forwarded-For` trust setting.

**Real database, some test doubles.** Idempotency tests run real inserts. Row-level
security tests connect as the restricted role, because running them as the owner would
prove nothing. Worker concurrency tests use separate PostgreSQL connections. Many HTTP
sections inject one controlled real connection through `_MockPool`, and some patch Stripe
signature verification to isolate routing and response behaviour; the sections that test
signatures themselves do not.

**Isolation between sections.** Database sections call `fresh_conn()`, which truncates
`outbox`, `dlq` and `ledger` with `RESTART IDENTITY` and removes non-default tenants
before the section builds its fixtures.

### The concurrent idempotency test

Five threads, each with its own database connection, release through a barrier and insert
the same event id. A separate connection then checks the table.

```python
assert results_concurrent.count("POSTED") == 1         # exactly one INSERT won the constraint
assert results_concurrent.count("DLQ_DUPLICATE") == 4  # all others hit ON CONFLICT
assert ledger_rows == 1                                # counted by a separate connection
```

### What each section covers

The labels are the ones used in the section headers of `test_ledger.py`, so they can be
found with `grep`.

| Section | What it checks |
|---|---|
| Phase 1 | Entry boundary: Pydantic types, field limits, the `EventType` enum, currency, customer and amount rules |
| Phase 2 | Output models: `DLQEntry` and `LedgerEntry` construction and `to_db()` serialization |
| Phase 3 | Cross-field rules: a failed invoice needs an amount above zero, a paid invoice may be zero, customer ids start with `cus_` |
| Phase 4 | Batch results are indexed by input position, an empty batch returns an empty list, a database failure in the handler is a 503 |
| Phase 5 | Full stack over HTTP: 200 and 422, a bad signature is a 400, concurrent idempotency, batch duplicate partition, `BIGINT` amounts, outbox, DLQ |
| Phase 7 | Outbox worker: an empty queue, dispatch sets `dispatched` and `dispatched_at`, a second drain returns 0 |
| Alembic migrations | `upgrade head` reaches the head revision, the three tables and the `dispatched_at` and `claimed_at` columns exist, `downgrade base` and `upgrade head` again |
| DLQ retry | `TRANSIENT` entries get `max_retries` 3 and `INVALID` ones 0, `retry_dlq_batch` replays and resolves, exhaustion, `POST /dlq/{id}/retry` resolves a `POSTED` replay |
| JSON logging and Prometheus `/metrics` | JSON log formatter, `/metrics` shape, outcome labels, route-template label cardinality, gauges agree with the tables |
| M1, M5 | API key boundary (401 and 200), `prune_dlq` retention |
| A0 | Amount extraction per event type; an unknown amount goes to the DLQ, never to `$0` |
| A1, A2, A4 | DLQ integrity: a retry appends no rows, `resolved` is terminal at every write site (409 at the endpoint) |
| A3 | Stale retry claims are reclaimed, live ones are not stolen |
| A5 | `dlq_depth` counts backlog, `dlq_total` counts history |
| A6 | Secret sources (`*_FILE`), fingerprints, an unreadable file stops the service |
| A7, A8 | The pool keeps its connections; rate-limit defaults are pinned |
| A9, A10, A11 | Tenancy: the same event id from two tenants posts for both, a tenant cannot read or retry another's rows, per-tenant signing secrets, an unknown org looks like a bad signature |
| A12, A13, A14 | A DLQ replay lands in the owning tenant, each tenant's events reach only its own endpoint, batch-born DLQ rows carry their tenant |
| A15, A16 | Row-level security as the restricted role: an unset scope sees nothing, a forgotten `org_id` predicate still cannot cross tenants, a cross-tenant insert is refused; then the real endpoints, core functions and worker run under it |
| A17 | A rotated `STRIPE_WEBHOOK_SECRET` reaches the default tenant, other tenants are untouched |
| A18 | `/metrics` is operator-only, tested through the application's real connection pool |
| C6-1 | The suite and the benchmark refuse a database that is not disposable |
| C6-2 | Events with `request: null` or `idempotency_key: null` are stored, keyed by event id |
| C6-3 | Demo credentials are refused unless `BILLING_ENV=dev` and the ingress is on loopback |
| C6-4 | Stripe's documented subscription objects are ledgered with no amount |
| C6-5 | Ledger, outbox and DLQ hold every field Stripe sent |
| C6-6 | An invalid event whose DLQ write fails is answered 503 |
| C6-7 | A second worker replica drains while the first holds its batch: `SKIP LOCKED` hands it other rows |
| C6-8 | An event the database refuses goes to the DLQ, not to a 500 |
| C6-9 | The RLS self-check reports superuser, `BYPASSRLS` and unprotected tables as inert; `provision.py` rotates the `billing_app` password |
| C6-10 | A signed `$0` `invoice.paid` is posted at 0 |
| C6-11 | DLQ queries: status filters, the actionable set, a cursor that walks 1,203 entries once |
| C6-12, C6-13 | Outbox leases: no locks held across HTTP, a quiet tenant is served despite an older failing backlog, tenants rotate across claims |
| C6-14 | The request body is capped while it is read |
| C6-15 | A redirecting downstream is a failed dispatch |
| C6-16 | A slow downstream is cut at a deadline and cannot hold other tenants |
| C6-17 | A worker stopped mid-batch hands its unsent rows back |
| C6-18 | A large real invoice is ledgered, not dead-lettered |
| C6-19 | Malformed input gets a 4xx, the same one for every tenant |
| C6-20 | A busy or dead database is a 503 with `Retry-After`, never a 500 |
| C6-21 | Failed authentications are rate-limited before the database lookup |
| C6-22, C6-29 | Tenants are dispatched concurrently; a slow tenant's task in flight does not delay claiming the others |
| C6-23 | API-only start-up notices come from the API process, not the worker |
| C6-24 | `/docs`, `/redoc` and the OpenAPI schema exist only in a declared dev run |
| C6-25 | The ingress cuts slow clients with server timeouts |
| C6-26 | An event id outside printable ASCII is `INVALID` |
| C6-27 | A replayed event leaves one `DUPLICATE` row |
| C6-28 | Event types the ledger does not record are `UNSUPPORTED`, not `INVALID` |
| C6-30 | Every error path, driven through the real app, moves `http_errors_total` by one with the right `status`, `error_class` and `error_type`, moves `http_requests_total` by one (the 413 and the 500 included), and leaves exactly one `http_request` line with the same class and type; none ends in the `unclassified` fallback; a hostile path or tenant id never becomes a label; a request that ends with no response records nothing; no bare `HTTPException` in `ledger.py` |
| C6-31 | The Caddyfile writes a JSON access log with the request headers removed (site logger and default logger), marks its own `413` and `502` with the application's class and type, and keeps the slow-client timeouts |

## Caddy behaviour the suite cannot check

The suite does not start Caddy. C6-31 checks the Caddyfile as text. What Caddy actually
records for each failure was measured against the `caddy:2-alpine` image (Caddy 2.11.9)
with the Compose stack running, using raw TLS sockets for the slow and oversized cases.
The results are in the README, under "Errors: class and type". The header filter was
measured the same way: requests carrying canary values in `X-Api-Key` and `Stripe-Signature`
left 0 occurrences of either in the access log, including a request for a `Host` that
matches no site, which goes through the default logger. Re-measure after changing the Caddy
image or the Caddyfile: the 499 case, the missing line for a `read_header` cut and the
default logger that writes the line for an unknown `Host` are all behaviour of Caddy, not of
this repository.

## Static analysis and its control

CI runs ruff and bandit (`lint.yml`). Bandit runs at medium severity and above (`-ll`) and
reports no issue on the current tree; six findings are suppressed with an explicit rule id.

A scanner that reports nothing is evidence only if it has been seen to report something.
The control is a file with the three patterns that matter most here:

```bash
mkdir canary && cat > canary/canary.py <<'PY'
import subprocess


def run(user_input, cur, name):
    subprocess.run(user_input, shell=True)
    eval(user_input)
    cur.execute("SELECT * FROM ledger WHERE org_id = '" + name + "'")
PY
bandit -r canary/canary.py -ll
```

With bandit 1.7.8 (the version pinned in `requirements-dev.txt`) the run exits 1 and
reports B602 (`shell=True`, high), B307 (`eval`, medium) and B608 (SQL built by string
concatenation, medium). If one of the three is missing, the scan configuration has changed
and a clean result on the real tree no longer means what it did. This check is manual; CI
does not repeat it. Run it in an empty directory and delete the canary afterwards: it is
deliberately vulnerable code.

## Manual check against a Stripe sandbox

The fixtures are built from Stripe's API reference, so the suite shows that the code accepts
what the reference documents, not what Stripe sends. The check that closes that gap needs a
Stripe test-mode account and the Stripe CLI, so it is not part of CI. Result of the last run
(test mode, API version `2026-10-28`, `invoice.paid`, `invoice.payment_failed` and the three
`customer.subscription.*` types generated with `stripe trigger`):

| Measured | Result |
|---|---|
| Events the CLI forwarded | 73, all answered `200` |
| Ledger rows | 10: 4 `POSTED`, 1 `VOID`, 5 `LIFECYCLE` |
| `UNSUPPORTED` rows in `dlq` | 63, across 17 event types |
| Lost or recorded twice | 0 and 0 |
| Amounts against the Stripe dashboard | gross volume USD 65.00 equals the four `POSTED` rows; the failed payment of USD 20.00 equals the `VOID` row |

To repeat it:

```bash
stripe listen --all-snapshot --forward-to localhost:8000/webhook/stripe   # prints a whsec_ secret
# Leave the listener running. Set STRIPE_WEBHOOK_SECRET in .env to that secret and run
# docker compose up -d, then use a second terminal for the rest.
stripe trigger invoice.paid
stripe trigger invoice.payment_failed
stripe trigger customer.subscription.created
stripe trigger customer.subscription.updated
stripe trigger customer.subscription.deleted
curl -H "X-Api-Key: dev_key_change_in_production" http://localhost:8000/ledger/summary
```

CLI 1.55 refuses to start `stripe listen` without `--events` or `--all-snapshot`. The run
passes when every forwarded event shows `[200]` in the listener output and the ledger counts
plus `dlq_total` add up to the number of events forwarded, on a database that held nothing
before the run. The outcome counts are also on `/metrics` as `webhook_events_total{outcome}`.
The captured payloads were not saved as fixtures;
[architecture.md](architecture.md#verified-against-a-stripe-test-mode-account) has the
longer description.

## Mutation testing

A passing test proves little until you have seen it fail. For the later fixes, the check
was to revert the fix, run the new test, and confirm it fails for the reason it is meant
to catch, not somewhere else. That is mutation testing done by hand: there is no
automated mutation run in this repository, and CI does not repeat it.

C6-30 was checked this way. Registering the observability layer inside the body cap again
fails 3 checks, all on the 413. Removing the code that records class and type fails 53.
Making the type marking a no-op fails 50, with every error landing in the `unclassified`
fallback.

One trap is worth knowing. A test that reports "no leak observed" can mean the leak does
not exist or that the instrumentation never ran. The `/metrics` isolation test therefore
asserts that the interleaving it depends on actually happened.

## End-to-end harness

`e2e/` runs the whole stack with a real downstream receiver and loads, interrupts and
attacks it from outside. See [`e2e/README.md`](../e2e/README.md) for the scenarios and the
results of the last full run.

## Benchmark

`benchmark.py` measures three paths and prints the environment it ran in:

| Path | Function | Bound by |
|---|---|---|
| Single event | `process_stripe_event` in a loop | Round trips to PostgreSQL: `BEGIN`, `SET LOCAL`, two inserts, `COMMIT` |
| Batch | `process_stripe_event_batch` | Far fewer round trips: `execute_values` collapses N inserts into `ceil(N / page_size)` statements |
| HTTP | `POST /webhook/stripe` against a running service | The whole stack: uvicorn, validation, HMAC, pool, rate limiter |

```bash
python benchmark.py --events 5000 --http-events 1500 --concurrency 16
```

The webhook limit is 100 requests per minute per client by default, which would make the
HTTP mode measure the rate limiter. Start the target with `RATE_LIMIT_WEBHOOK` raised, and
on the same database the benchmark resets (it defaults to `billing_bench`, created and
migrated like `billing_test`). Throughput depends on the machine, so this repository does
not publish figures; run the script to get yours. `test_ledger.py` carries one hard
floor: the batch path must exceed 500 events per second.

One finding from writing the HTTP mode is kept because it still shapes the code. With a
pool of `minconn=1, maxconn=20`, psycopg2 keeps only one idle connection and closes every
other one on release, and `getconn` opens a new PostgreSQL connection while holding the
pool lock, so concurrent requests queue behind each other's connection set-up. The pool is
now built with `minconn` equal to `maxconn` (`DB_POOL_SIZE`), and section A7 of the suite
checks that the pool hands back the same connection instead of reopening one.
