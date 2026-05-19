# FULL SPECTRUM PRE-FLIGHT ANALYSIS
## Micro-Billing-Ledger PoC — Private Reading Copy
### Diego Alonso Del Río García — Mayo 2026

> **Context:** Pre-push audit for a hiring committee at a Series A, Tier-2 B2B credit
> infrastructure fintech (LatAm, similar to AltScore). SOC 2 Type II + ISO 27001 certified.
> Audited from four engineering personas. Unfiltered.

---

## VERDICT: NO-GO

**Three items cause immediate rejection. Everything else is defensible.**

1. `LOGIC_AUDIT.md` says "No PostgreSQL code exists in this repository" — in a repo where every test hits PostgreSQL.
2. Dead `BLUEPRINT_ANALYSIS.md` references in six places across tracked source files.
3. The Docker container crashes on startup — billing user cannot write `billing_ledger.log` to `/app`.

Fix these three and the rest is a strong codebase. Ship without them and the committee concludes either the tests are fabricated or the documentation has no integrity.

---

## PERSONA 1 — LEAD CYBERSECURITY ENGINEER (SecOps / ISO 27001)

### CRITICAL

**[SEC-C1] Unauthenticated financial write endpoint**
`ledger.py:1180–1186` — Stripe HMAC verification is commented out. `POST /webhook/stripe` accepts any JSON from any IP with zero authentication. An adversary who knows the URL can POST arbitrary `invoice.paid` events and write real ledger rows. This is not a "production gap" — it is an open injection vector for financial fraud. ISO 27001 A.14.1.2 requires authentication at system entry points.

**[SEC-C2] Placeholder secret hardcoded in source with no validation**
`ledger.py:511` — `STRIPE_WEBHOOK_SECRET = "whsec_YOUR_SECRET_HERE"` is the fallback when no env var is set. Zero validation that the secret was replaced before use. A misconfigured deploy silently uses the placeholder and accepts all unsigned requests. ISO 27001 A.9.4.3: secrets must never be embedded in source.

**[SEC-C3] Stripe secret hardcoded in docker-compose.yml — tracked by git**
`docker-compose.yml:63` — `STRIPE_WEBHOOK_SECRET: whsec_YOUR_SECRET_HERE` is committed to git. Even as a placeholder, this trains contributors to put secrets in compose files. Any accidental push of a real secret here creates permanent git history exposure. SOC 2 CC6.1 requires controls preventing secrets from entering version control.

**[SEC-C4] Database credentials hardcoded in Dockerfile and docker-compose.yml**
`Dockerfile:88`, `docker-compose.yml:32–33` — `postgres:postgres` is committed in two files. A developer who `docker build && docker push` ships an image with the default credentials baked into the image layer. ISO 27001 A.9.4.3 violation.

### HIGH

**[SEC-H1] `/dlq/entries` exposes customer PII with zero authentication**
`ledger.py:1239–1272` — Returns raw webhook payloads including customer IDs, amounts, and full event JSON. No API key, no Bearer token, no IP allowlist. Under LGPD (Brazil's GDPR equivalent, applicable to LatAm fintechs), PII endpoints require access controls. SOC 2 CC6.3 and ISO 27001 A.9.4.1 both require this.

**[SEC-H2] `/ledger/summary` and `/health` are unauthenticated**
Read-only endpoints that reveal system state (DLQ depth, outbox pending count) are information-disclosure vectors. An adversary can probe `dlq_depth` to infer processing failures or attack windows.

**[SEC-H3] No rate limiting on any endpoint**
No `SlowAPI`, no nginx `limit_req`, no upstream API gateway config. A malicious actor can flood `POST /webhook/stripe` with crafted payloads, exhausting the single database connection and filling the DLQ table.

**[SEC-H4] `limit=-1` on `/dlq/entries` causes HTTP 500**
`ledger.py:1256` — `min(-1, 1000)` returns `-1`. PostgreSQL raises `ERROR: LIMIT must not be negative` on a parameterized query. Unhandled exception → FastAPI returns HTTP 500 with a stack trace. No test covers this.

### MEDIUM

**[SEC-M1] PII stored in plaintext in the database**
`ledger.payload` and `dlq.raw_payload` store full Stripe webhook JSON including customer identifiers. Under LGPD, PII must be classifiable and subject to right-to-erasure. There is no mechanism to redact a specific customer's data from these TEXT columns without breaking the audit trail.

**[SEC-M2] `billing_ledger.log` may contain PII**
`ledger.py:876–881` — On DLQ write failure, full `raw_payload` is written to the log at ERROR level. Log files are not encrypted, not rotated, not PII-sanitized. ISO 27001 A.12.4 violation.

**[SEC-M3] No security headers on any HTTP response**
No `X-Content-Type-Options`, `X-Frame-Options`, `Strict-Transport-Security`, or `Content-Security-Policy` headers. FastAPI doesn't add these by default.

### Stale Code References — Documentation Integrity Failures

`BLUEPRINT_ANALYSIS.md` was deleted. Its ghost remains in six places in tracked source. An auditor following these references finds dead links, which undermines confidence in every other claim in the codebase:

| File | Line | Dead Reference |
|---|---|---|
| `ledger.py` | 172 | `# See BLUEPRINT_ANALYSIS.md §5 for the .lower() normalization fix.` |
| `ledger.py` | 483–485 | Section 2 configuration comment references `BLUEPRINT_ANALYSIS.md §2` |
| `ledger.py` | 1165–1167 | `stripe_webhook()` docstring: `"see BLUEPRINT_ANALYSIS.md §2"` |
| `ledger.py` | 1210 | `ledger_summary()` docstring: `"see BLUEPRINT_ANALYSIS.md §7"` |
| `test_ledger.py` | 185 | `# See BLUEPRINT_ANALYSIS.md §5 for the .lower() normalization fix.` |
| `Dockerfile` | 106–107 | CMD comment references `BLUEPRINT_ANALYSIS.md §1` |

### ISO 27001 Gap Summary

| Control | Gap |
|---|---|
| A.9.4.1 | No application-level access control on financial endpoints |
| A.9.4.3 | Secrets hardcoded in source and docker-compose.yml |
| A.14.1.2 | No authentication at system entry point (webhook) |
| A.8.2 | No data classification for customer PII in payload columns |
| A.12.4 | Log files contain PII without sanitization controls |

---

## PERSONA 2 — SITE RELIABILITY ENGINEER (SRE / DevOps)

### CRITICAL

**[SRE-C1] Docker container crashes on startup — billing user cannot write log file**
`Dockerfile:62–94` — The Dockerfile creates WORKDIR `/app` as root, then switches to `USER billing`. `billing_ledger.log` is opened via `logging.FileHandler("billing_ledger.log")` at module import time (`ledger.py:75`). The billing user has no write permission to `/app` (owned by root, mode `drwxr-xr-x`). This throws `PermissionError` on startup. **The Docker container cannot start.** This has gone undetected because the test suite runs locally, not in Docker.

**[SRE-C2] Single psycopg2 connection with no reconnect logic**
`ledger.py:1135` — `_conn = _bootstrap()` creates one connection at module load. If PostgreSQL restarts, the connection idles past `tcp_keepalives_idle`, or the network flaps, `_conn` becomes a dead socket. Every subsequent request raises `OperationalError`. No reconnect, no `SELECT 1` ping, no PgBouncer, no retry decorator. The service silently returns HTTP 503 on every request until the process is manually restarted.

**[SRE-C3] Synchronous psycopg2 calls inside `async def` handlers block the event loop**
`ledger.py:1158–1199` — All FastAPI routes are `async def` but call synchronous psycopg2 operations directly with no `run_in_executor`. This blocks the asyncio event loop for the duration of every DB call. Under any concurrency, all other requests stall behind each DB call.

### HIGH

**[SRE-H1] No restart policy in docker-compose.yml**
Neither `postgres` nor `billing` has `restart: unless-stopped`. A container OOM or crash stays down permanently. For a financial system this is unacceptable.

**[SRE-H2] No resource limits in docker-compose.yml**
No `deploy.resources.limits` for memory or CPU on either service. An unbounded billing container can OOM the host.

**[SRE-H3] No readiness probe for the billing service**
The `billing` container has no `healthcheck`. `/health` returns `{"status": "ok"}` regardless of DB connectivity. A dead DB connection returns 200 on `/health` and 503 on every actual request — Kubernetes or any orchestrator cannot distinguish these states.

**[SRE-H4] PostgreSQL port 5432 exposed to `0.0.0.0`**
`docker-compose.yml:37` — `"5432:5432"` binds to all network interfaces. Any machine on the same network can connect to PostgreSQL with `postgres:postgres`. Should be `"127.0.0.1:5432:5432"`.

**[SRE-H5] No log rotation — `billing_ledger.log` grows unboundedly**
`logging.FileHandler("billing_ledger.log")` appends forever. No `RotatingFileHandler`, no logrotate config. Under load, this fills the disk and kills the service.

**[SRE-H6] Outbox table has no cleanup mechanism**
Dispatched outbox rows are never deleted. `SELECT COUNT(*) FROM outbox WHERE dispatched=0` performs a full table scan with no index as delivered rows accumulate. There is no `DELETE FROM outbox WHERE dispatched=1 AND created_at < NOW() - INTERVAL '7 days'` or partition strategy.

**[SRE-H7] Stale Dockerfile CMD comment**
`Dockerfile:103–107` — Comment says `--workers 1: single worker because SQLite is a single-writer engine` and references `BLUEPRINT_ANALYSIS.md §1`. Both claims are now wrong — engine is PostgreSQL, file is deleted.

### MEDIUM

**[SRE-M1] Test suite leaks PostgreSQL connections**
Every `fresh_conn()` call creates a new psycopg2 connection. Across all test phases, ~12 connections are opened and only some are explicitly closed. Running the suite repeatedly in CI can exhaust PostgreSQL's default `max_connections=100`.

**[SRE-M2] No Docker image tagging strategy**
`docker-compose.yml` uses `build: .` with no image name or tag. Every build produces an untagged image. No rollback capability.

**[SRE-M3] No graceful shutdown handling**
No SIGTERM handler. `_conn` is never explicitly closed on shutdown. Mid-transaction shutdown leaves uncommitted transactions that PostgreSQL eventually rolls back, but produces client-visible errors with no structured log.

---

## PERSONA 3 — LEAD SDET

### CRITICAL

**[TEST-C1] Concurrent test has no synchronization barrier — threads may not fire simultaneously**
`test_ledger.py:828–832` — The test does `t.start()` in a loop then `t.join()` in a loop. On a single-core machine or under OS scheduling, threads execute sequentially not concurrently. The test needs `threading.Barrier(5)` inside `_fire()` so all 5 threads release simultaneously. Without this, you are testing "5 sequential inserts" not "5 concurrent inserts racing on the same primary key." This is the entire point of the test, and it is not guaranteed.

### HIGH

**[TEST-H1] No test for duplicate `transaction_id` within the same batch**
`process_stripe_event_batch` receives two events with identical `transaction_id` in one input list. Both pass Pydantic. The bulk INSERT inserts one and ON CONFLICT skips the other. The `inserted_ids` set has the ID once. The partition loop classifies one as new and one as duplicate — correct behavior, completely untested. Stripe can legitimately send the same event twice in a webhooks batch.

**[TEST-H2] No test for DB connection failure mid-transaction**
No test simulates `psycopg2.OperationalError` during a write. The code handles it by converting to `RuntimeError` → HTTP 503 — correct behavior, never verified. SOC 2 requires evidence that failure modes are tested, not just handled.

**[TEST-H3] No test verifying batch results are indexed by input position**
`process_stripe_event_batch` returns `list[dict]` indexed by input position. The benchmark test only checks TPS. There is no assertion that `results[3]` corresponds to `events[3]`. A scrambled result ordering bug passes all existing tests.

**[TEST-H4] Benchmark test verifies throughput but not data integrity**
`test_ledger.py:476` — The 500 TPS assertion passes if the batch function returns 5,000 result dicts quickly. It does not verify that 5,000 rows were actually written to the ledger. A catastrophically broken batch function returning dummy results without touching the DB would pass this test.

**[TEST-H5] Phase 3 tests execute after Phase 5 in the test file**
Test file order: Phase 1 → Phase 2 → Phase 5 (integration) → Phase 3 (cross-field validators). This is accidental ordering. Future tests that depend on Phase 3 state encounter interference from Phase 5's data. It also looks careless to a hiring committee reading top-to-bottom.

### MEDIUM

**[TEST-M1] No test for `limit=-1` on `/dlq/entries`**
`min(-1, 1000) = -1` → PostgreSQL raises on `LIMIT -1` → HTTP 500. Should be a test asserting 422 (invalid parameter), not 500.

**[TEST-M2] No test for `amount_paid` exceeding PostgreSQL INTEGER max**
`amount_paid = 2_147_483_648` passes Pydantic (`ge=0`), builds a valid `LedgerEntry`, then fails the PostgreSQL INSERT with `integer out of range` → unhandled `OperationalError` → HTTP 500. Not tested. (Also drives the ARCH-H1 finding for `BIGINT`.)

**[TEST-M3] No test verifying `idempotency_key` is stored correctly in the ledger row**
The resolver sets `idempotency_key` from `request.idempotency_key` or falls back to `id`. No test verifies the stored DB value. If the resolver breaks, all idempotency keys silently fall back to event IDs with no test catching it.

**[TEST-M4] No test for empty batch input**
`process_stripe_event_batch(conn, [])` returns `[]` by early-exit guard. Not tested.

**[TEST-M5] No test for `_write_dlq` failure path**
`_write_dlq` swallows its own failure and logs at ERROR. No test simulates a DLQ INSERT failure and verifies the main path continues. The "zero data loss" guarantee is documented but not verified.

### LOW

**[TEST-L1] `LOGIC_AUDIT.md` contradicts current codebase — SDET concern**
`LOGIC_AUDIT.md:57–79` states "No PostgreSQL code exists in this repository" and quotes SQLite SQL. A hiring committee reading the test suite and then this document will conclude the test results are fabricated. This is a documentation integrity issue that directly undermines test credibility.

---

## PERSONA 4 — PRINCIPAL BACKEND ARCHITECT

### CRITICAL

**[ARCH-C1] `LOGIC_AUDIT.md` is an active liability that poisons the entire repository**
`LOGIC_AUDIT.md:57` explicitly states: *"No PostgreSQL code exists in this repository."* It names `ledger.py:770` and quotes SQLite SQL. The document also says it was "generated by Claude Sonnet 4.6." Any hiring committee reading this will conclude either (a) the README, tests, and TPS numbers are fabricated, or (b) the repo's documentation has no integrity. This document must be deleted or completely rewritten before the repo goes public. **No other finding in this audit has higher reputational risk.**

### HIGH

**[ARCH-H1] `amount_cents INTEGER` — integer overflow for enterprise B2B amounts**
`ledger.py:563` — PostgreSQL `INTEGER` is 4 bytes, max 2,147,483,647 = $21,474,836.47 in cents (~$21M USD). For a B2B credit infrastructure company processing enterprise invoices, a single transaction can exceed this. One overflow causes a hard INSERT failure with no graceful fallback. This column must be `BIGINT` (8 bytes, max ~$92 trillion in cents).

**[ARCH-H2] No index on `ledger.customer_id`**
`ledger.py:559–571` — The schema has only one index: the PRIMARY KEY on `transaction_id`. Any query retrieving all ledger rows for a specific customer (`WHERE customer_id = 'cus_XYZ'`) is a full table scan O(N). For a B2B credit engine, customer-level financial history queries are the primary read path. At 5,000 TPS ingestion, the table reaches millions of rows quickly.

**[ARCH-H3] No index on `outbox.dispatched`**
`ledger.py:577–586` — The outbox worker query (`WHERE dispatched=0 ORDER BY id LIMIT 100`) performs a full table scan on an ever-growing table of delivered rows. This is the definition of a "write-only table that degrades over time." A partial index `CREATE INDEX ON outbox (dispatched, id) WHERE dispatched=0` is the fix.

**[ARCH-H4] No referential integrity between outbox and ledger**
`outbox.transaction_id` is TEXT with no `REFERENCES ledger(transaction_id)`. The transactional outbox invariant (outbox row exists ↔ ledger row exists) is maintained by the application's atomic write — not enforced by the database. A schema migration that drops or renames `ledger` leaves orphan outbox rows silently.

**[ARCH-H5] Dead `BLUEPRINT_ANALYSIS.md` references in application source code**
Six dead references across `ledger.py`, `test_ledger.py`, and `Dockerfile`. These are in production application code and docstrings. A senior engineer reviewing this repo will search for the file, find it gone, and flag the documentation as untrustworthy.

**[ARCH-H6] Module-level `_conn` — no pooling, no health check, no reconnect**
`ledger.py:1135` — One psycopg2 connection serves all requests. The connection can go stale permanently. The architecture cannot horizontally scale without rethinking this pattern. There is no `pg_stat_activity`-visible pool to monitor. Canonical fix: `psycopg2.pool.ThreadedConnectionPool` or migrate to `asyncpg`.

### MEDIUM

**[ARCH-M1] `created_at DOUBLE PRECISION` — wrong type for financial timestamps**
`ledger.py:569` — `DOUBLE PRECISION` has no timezone awareness, no PostgreSQL timestamp function compatibility (`date_trunc`, `AT TIME ZONE`), and produces floating-point comparison artifacts in financial reports. Correct type for a financial ledger is `TIMESTAMPTZ DEFAULT NOW()`.

**[ARCH-M2] `_STATUS_MAP` exhaustiveness not enforced at compile time**
`ledger.py:656–662` — No `mypy` in the toolchain, no CI step running it. `dict[EventType, LedgerStatus]` does not enforce that all `EventType` values are keys. Adding a new `EventType` and forgetting `_STATUS_MAP` produces a `KeyError` at runtime, not a type error at development time.

**[ARCH-M3] Batch function assigns identical `created_at` to all events in a batch**
`ledger.py:953` — `now = time.time()` is captured once before the loop. All 5,000 events receive the same `created_at` timestamp. Event ordering within a batch is ambiguous from `created_at` alone. Only `outbox.id BIGSERIAL` preserves insertion order.

**[ARCH-M4] `request` field type inconsistency between FastAPI and StripeEvent models**
`ledger.py:1154` — `StripeWebhookPayload.request` is `dict` (never None). `StripeEvent.request` is `Optional[dict]`. The `if self.request is None` branch in `resolve_idempotency_key` is dead code for the HTTP path. Harmless but confusing to future maintainers and a type-contract mismatch.

**[ARCH-M5] `payload TEXT NOT NULL` — unbounded column, no size constraint**
Each ledger row stores the full raw webhook payload. Stripe webhooks can be several KB. No `CHECK (length(payload) < N)`, no server-side limit, no compression. At 5,000 TPS sustained ingestion, storage growth is significant without a data retention policy.

### LOW

**[ARCH-L1] `--workers 1` comment is architecturally wrong for PostgreSQL**
`Dockerfile:103` — Comment says single worker is required because SQLite is single-writer. PostgreSQL is a multi-client server. The correct production configuration is multiple uvicorn workers (typically `nproc` workers). The comment causes a production engineer to deploy under-utilized infrastructure.

**[ARCH-L2] `get_amount() -> int` return type annotation is incorrect**
`ledger.py:208–217` — Returns `None` if both `amount_paid` and `amount` are None. `check_amount_present` makes this impossible via `StripeEvent`, but `StripeObject` can be constructed directly without the validator. The `-> int` annotation is a lie to the type checker.

---

## GO / NO-GO VERDICT MATRIX

**Overall Verdict: NO-GO — 3 blockers, 11 HIGH findings**

| File | Issue | Persona | Priority |
|---|---|---|---|
| `LOGIC_AUDIT.md` | DELETE or rewrite — says "No PostgreSQL code exists" in a PostgreSQL repo | Architect | **BLOCKER** |
| `ledger.py` | Remove 4 dead `BLUEPRINT_ANALYSIS.md` references: lines 172, 483–485, 1165–1167, 1210 | SecOps | **BLOCKER** |
| `test_ledger.py` | Remove dead `BLUEPRINT_ANALYSIS.md` reference (line 185) | SecOps | **BLOCKER** |
| `Dockerfile` | Fix `billing_ledger.log` write permissions — billing user can't write to `/app`; container crashes on start | SRE | **HIGH** |
| `Dockerfile` | Remove dead `BLUEPRINT_ANALYSIS.md` reference in CMD comment (line 106–107); fix `--workers 1` SQLite rationale | SecOps | **HIGH** |
| `docker-compose.yml` | Replace hardcoded `STRIPE_WEBHOOK_SECRET: whsec_YOUR_SECRET_HERE` with `${STRIPE_WEBHOOK_SECRET}` env var | SecOps | **HIGH** |
| `ledger.py` | Validate `limit >= 1` in `/dlq/entries` before the SQL query — prevent `LIMIT -1` PostgreSQL error | SecOps/SDET | **HIGH** |
| `ledger.py` | Change `amount_cents INTEGER` → `BIGINT` in CREATE TABLE DDL | Architect | **HIGH** |
| `ledger.py` | Add missing DB indexes: `ledger(customer_id)` and `outbox(dispatched, id) WHERE dispatched=0` | Architect | **HIGH** |
| `docker-compose.yml` | Add `restart: unless-stopped` to both services | SRE | **MEDIUM** |
| `docker-compose.yml` | Bind PostgreSQL to `127.0.0.1:5432:5432` not `0.0.0.0` | SecOps | **MEDIUM** |
| `ledger.py` | Add connection reconnect/validation logic (or `ThreadedConnectionPool`) | SRE | **MEDIUM** |
| `test_ledger.py` | Add `threading.Barrier(5)` to concurrent insertion test | SDET | **MEDIUM** |
| `test_ledger.py` | Add test: same `transaction_id` twice in one batch input | SDET | **MEDIUM** |
| `test_ledger.py` | Add test: `limit=-1` on `/dlq/entries` → 422 not 500 | SDET | **MEDIUM** |
| `test_ledger.py` | Add test: `amount_paid > 2_147_483_647` path | SDET | **MEDIUM** |
| `test_ledger.py` | Fix phase ordering — Phase 3 tests appear after Phase 5 | SDET | **MEDIUM** |
| `ledger.py` | Replace `created_at DOUBLE PRECISION` with `TIMESTAMPTZ DEFAULT NOW()` | Architect | **LOW** |
| `Dockerfile` | Create `/var/log/billing/` directory owned by billing user for log file | SRE | **LOW** |

---

## WHAT IS STRONG (FOR BALANCE)

This analysis is blunt by design. For completeness, what would not be challenged in a SOC 2 audit:

- **Idempotency implementation is correct.** `ON CONFLICT (transaction_id) DO NOTHING` + `RETURNING` is the right pattern. The concurrent test (despite needing a barrier) does verify the end state correctly.
- **Pydantic validation stack is thorough.** 5 layers, correct use of `@model_validator(mode='after')`, cross-field rules are sound.
- **`_tx()` context manager is correct.** BEGIN/yield/COMMIT-or-ROLLBACK with autocommit=True is the right psycopg2 pattern.
- **DLQ design is correct.** Best-effort append, never raises on the hot path, raw payload preserved byte-perfect, structured reason codes.
- **Batch function's `RETURNING` usage is architecturally correct.** The explanation of why Alternative A (post-INSERT SELECT) and Alternative B (pre-INSERT tracking) both have race windows is accurate.
- **Multi-stage Dockerfile is correct.** Non-root user, compiler not in runtime image — both are production-correct patterns.
- **`.gitignore` is comprehensive.** `.env`, `*.log`, `*.db` are all covered.

---

*Private reading copy — Diego Alonso Del Río García — Mayo 2026*
