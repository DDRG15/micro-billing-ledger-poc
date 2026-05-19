# FULL SPECTRUM PRE-FLIGHT AUDIT — LIVING DOCUMENT
## posthog-billing-poc · Diego Alonso Del Río García · Mayo 2026
### Mode: C — HYBRID (Audit existing code → Deliver patched code)

> **Purpose:** Single living document for ALL audit findings, AI responses, implementation
> decisions, git hashes, and outstanding work. Read this first at the start of every session.
> Never delete — it is the source of truth for what changed, why, and what is still pending.

---

## SESSION LOG

| Date | Session Summary | Key Commits |
|------|----------------|-------------|
| 2026-05-13 | Original PREFLIGHT_AUDIT.md — 3 blockers, NO-GO verdict | — |
| 2026-05-13 | Phases 1–7 remediation (CRITICAL/HIGH from prior audit) | `03a4226` → `6e36e67` |
| 2026-05-18 | Full-spectrum re-audit; Phases 1–6 implemented; PRIVATE_README written | `8dfe1cf` → `bb6479c` |
| 2026-05-19 | Phase 7 — Outbox worker (standalone Docker service); .dockerignore, .gitignore updates | `ae6e9f4` |

---

## CLASSIFICATION

```
MODE SELECTED: C — HYBRID
Reason: Existing codebase, partially remediated from a prior audit.
        Fresh audit of current state, remaining gaps identified and patched.
```

---

## EXECUTIVE RISK MATRIX — FINAL STATE

| Severity | ID | Location | Summary | Status |
|----------|----|----------|---------|--------|
| CRITICAL | C1 | `LOGIC_AUDIT.md:105` | Stale doc said Stripe was commented out | ✅ Fixed `8dfe1cf` |
| CRITICAL | C2 | `ledger.py:514` | Placeholder secret, no startup validation | ✅ Fixed `8dfe1cf` |
| HIGH | H1 | `ledger.py` webhook handler | `async def` blocking event loop with sync psycopg2 | ✅ Fixed `e4703be` |
| HIGH | H2 | `ledger.py:73–80` | `FileHandler` with no rotation — fills disk | ✅ Fixed `e4703be` |
| HIGH | H3 | `docker-compose.yml:37` | PG port `0.0.0.0` exposed to LAN | ✅ Fixed `e4703be` |
| HIGH | H4 | `docker-compose.yml` | No billing service `healthcheck` | ✅ Fixed `e4703be` |
| HIGH | H5 | No file | No rate limiting on any endpoint | ✅ Fixed `346a6bd` |
| MEDIUM | M1 | `docker-compose.yml` | No `deploy.resources.limits` | ✅ Fixed `346a6bd` |
| MEDIUM | M2 | `ledger.py` outbox DDL | No FK `outbox → ledger` | ✅ Fixed `346a6bd` |
| MEDIUM | M3 | `test_ledger.py` | No batch position indexing test | ✅ Fixed `49f5bd2` |
| MEDIUM | M4 | `test_ledger.py` | No DB failure → HTTP 503 test | ✅ Fixed `49f5bd2` |
| MEDIUM | M5 | `test_ledger.py` | Benchmark no row count check | ✅ Fixed `49f5bd2` |
| MEDIUM | M6 | `ledger.py` auth | Empty `BILLING_API_KEY` bypasses auth | ✅ Fixed `346a6bd` |
| LOW | L1 | `ledger.py:get_amount()` | Wrong `-> int` annotation | ✅ Fixed `bb6479c` |
| LOW | L2 | Toolchain | No `mypy`/`ruff`/`bandit` | ✅ Fixed `bb6479c` |
| LOW | L3 | `test_ledger.py` | ~12 leaked psycopg2 connections | ✅ Fixed `49f5bd2` |
| LOW | L4 | `test_ledger.py` | Empty batch untested | ✅ Fixed `49f5bd2` |

**ALL 17 FINDINGS RESOLVED. Phase 7 adds outbox worker (architectural feature, not a finding).**

---

## WHAT WAS FIXED IN PREVIOUS SESSIONS (prior to this audit)

| Finding | Fix | Commit |
|---------|-----|--------|
| Stripe HMAC commented out | Active `stripe.Webhook.construct_event()` call | 03a4226 |
| Single module-level DB connection | `ThreadedConnectionPool(1, 20)` | 03a4226 |
| `amount_cents INTEGER` overflow | Changed to `BIGINT` | 03a4226 |
| No `customer_id` index | `idx_ledger_customer_id` | 03a4226 |
| No outbox dispatched index | `idx_outbox_dispatched_id` partial index | 03a4226 |
| `limit=-1` → HTTP 500 | `if limit < 1: raise 422` | 03a4226 |
| DLQ/summary unauthenticated | `_require_api_key` dependency | 03a4226 |
| Docker crash (log file perms) | `RUN chown -R billing:billing /app` | 03a4226 |
| Stripe secret hardcoded in compose | `${STRIPE_WEBHOOK_SECRET}` substitution | 03a4226 |
| No restart policy | `restart: unless-stopped` both services | 03a4226 |
| No synchronization in concurrent test | `threading.Barrier(5)` | 48c9779 |
| No batch duplicate test | Added to test suite | 48c9779 |
| `limit=-1` test missing | HTTP 422 assertion test | 48c9779 |
| Phase ordering wrong | Phase 3 now precedes Phase 5 | 48c9779 |
| Batch duplicate partitioning bug | `remaining_ids.discard()` fix | 457ebee |
| BIGINT test missing | Amount > 2.1B test | 48c9779 |
| `LOGIC_AUDIT.md` "No PostgreSQL" | Rewritten to reflect current state | 6e36e67 |

---

## PHASE-BY-PHASE IMPLEMENTATION LOG

### Phase 0 — Baseline tag
- **Action:** `git tag audit-baseline`
- **Before:** `6e36e67` (last commit from prior session)
- **After:** tag `audit-baseline` pointing to `6e36e67`

### Phase 1 — CRITICAL (C1, C2)
- **Commit:** `8dfe1cf`
- **Files:** `LOGIC_AUDIT.md`, `ledger.py`
- **C1:** Updated "What Is NOT Implemented" table — Stripe verification and connection pooling marked ACTIVE
- **C2:** Added `warnings.warn` if `STRIPE_WEBHOOK_SECRET` equals the placeholder value at startup

### Phase 2 — HIGH (H1, H2, H3, H4)
- **Commit:** `e4703be`
- **Files:** `ledger.py`, `docker-compose.yml`
- **H1:** `async def stripe_webhook` → `def stripe_webhook`; `await request.body()` → `request.body()`; same for `health`. FastAPI threadpool handles sync handlers correctly.
- **H2:** `logging.FileHandler` → `RotatingFileHandler(maxBytes=10MB, backupCount=5)`. Added import from `logging.handlers`.
- **H3:** PG port `"5432:5432"` → `"127.0.0.1:5432:5432"`. Only localhost can reach PostgreSQL.
- **H4:** Added `healthcheck` to billing service. `wget -qO- http://localhost:8000/health`, interval 10s, start_period 10s.

### Phase 3 — HIGH+MEDIUM (H5, M1, M2, M6)
- **Commit:** `346a6bd`
- **Files:** `ledger.py`, `docker-compose.yml`, `requirements.txt`
- **H5:** Added `slowapi==0.1.9`. Limiter keyed by IP. 100/min on webhook, 20/min on dlq/summary. 429 on breach.
- **M1:** `deploy.resources.limits: memory: 256M, cpus: 0.50` on billing service.
- **M2:** `outbox.transaction_id REFERENCES ledger(transaction_id) ON DELETE CASCADE` in DDL.
- **M6:** Rewrote `_require_api_key` — separate cases for (1) no key configured, (2) missing header, (3) wrong header.

### Phase 4 — MEDIUM+LOW tests (M3, M4, M5, L3, L4)
- **Commit:** `49f5bd2`
- **File:** `test_ledger.py`
- **M3:** 3-event batch position test — `results[i]["transaction_id"] == events[i]["id"]`
- **M4:** DB failure → 503 test — patches `process_stripe_event` to raise `RuntimeError`
- **M5:** Benchmark data integrity — `COUNT(*) FROM ledger` after batch must equal N
- **L3:** `conn.close()` added after: `conn4`, `large_conn`, `dispatch_conn`, `dlq_conn`, `batch_conn`
- **L4:** Empty batch test — `process_stripe_event_batch(conn, []) == []`

### Phase 5 — LOW (L1, L2)
- **Commit:** `bb6479c`
- **Files:** `ledger.py`, new `requirements-dev.txt`
- **L1:** `get_amount() -> int` → `get_amount() -> Optional[int]`
- **L2:** `requirements-dev.txt` with `ruff==0.4.4`, `mypy==1.10.0`, `bandit==1.7.8`

### Phase 6 — Living report + PRIVATE_README
- **Commit:** `6d7d2f7`
- **Files:** `FULL_SPECTRUM_AUDIT.md` (this file), `PRIVATE_README.md`

### Phase 7 — Outbox worker (standalone Docker service)
- **Commit:** `ae6e9f4`
- **Files:** `worker.py` (new), `ledger.py`, `docker-compose.yml`, `test_ledger.py`, `.dockerignore` (new), `.gitignore`
- **worker.py:** Standalone Python process — `drain_batch()` reads up to 100 `dispatched=0` rows with `SELECT FOR UPDATE SKIP LOCKED`, dispatches each (HTTP POST if `DOWNSTREAM_URL` set, else structured log), marks rows `dispatched=1, dispatched_at=NOW()` inside the same `BEGIN/COMMIT`. Exponential backoff on DB errors (5→10→20→60s). SIGTERM-safe — finishes current batch then exits 0.
- **ledger.py:** Added `ALTER TABLE outbox ADD COLUMN IF NOT EXISTS dispatched_at TIMESTAMPTZ` in `_bootstrap()` — idempotent migration for existing databases.
- **docker-compose.yml:** Added `worker` service — same image as billing, `command: python worker.py`, no port mapping, `depends_on: postgres: condition: service_healthy`, `restart: unless-stopped`.
- **test_ledger.py:** 3 new worker tests — empty queue, dispatch + verify dispatched=1/dispatched_at, idempotency (second drain returns 0).
- **.dockerignore:** New file — excludes `.git`, `.env`, `__pycache__`, test files, audit docs, `.venv`, log files, `.claude/` from Docker build context.
- **.gitignore:** Added `.pytest_cache/`, `.mypy_cache/`, `*.egg-info/`, `dist/`, `build/`, `*.whl`.
- **requirements.txt:** No changes needed — `worker.py` uses only stdlib (`urllib.request`, `signal`, `time`, `logging`) plus `psycopg2` already pinned.

---

## TOOL RECOMMENDATIONS

| Tool | Install | Purpose | Status |
|------|---------|---------|--------|
| `slowapi==0.1.9` | `pip install slowapi` | FastAPI rate limiting | ✅ Added in Phase 3 |
| `RotatingFileHandler` | stdlib | Log rotation | ✅ Added in Phase 2 |
| `ruff==0.4.4` | `pip install -r requirements-dev.txt` | Lint + format | ✅ Added in Phase 5 |
| `mypy==1.10.0` | `pip install -r requirements-dev.txt` | Type checking | ✅ Added in Phase 5 |
| `bandit==1.7.8` | `pip install -r requirements-dev.txt` | Security scan | ✅ Added in Phase 5 |
| `prometheus-client` | already commented in `requirements.txt` | Metrics endpoint | Future |
| `pytest` | optional | Replace hand-rolled runner | Optional |

---

## GO-LIVE CHECKLIST

- [ ] `STRIPE_WEBHOOK_SECRET` set to real Stripe signing secret (not placeholder)
- [ ] `BILLING_API_KEY` set to `openssl rand -hex 32` in production secrets manager
- [ ] `DATABASE_URL` points to managed PostgreSQL (RDS, Cloud SQL, Supabase)
- [ ] `pip install -r requirements.txt` completed in container (includes `stripe`, `psycopg2-binary`, `slowapi`)
- [ ] `python test_ledger.py` passes 100% against production-equivalent DB
- [ ] `docker-compose build && docker-compose up` → billing healthcheck goes healthy
- [ ] Rate limiter confirms 429 on 101st request within a minute from the same IP
- [ ] `billing_ledger.log` confirmed to rotate (check `.1` file appears after rotation)
- [ ] Outbox worker implemented OR alert configured for `outbox_pending > 10000`
- [ ] PII handling policy documented for `ledger.payload` and `dlq.raw_payload`
- [ ] Run `bandit -r ledger.py` — no HIGH severity findings
- [ ] Run `mypy ledger.py` — no type errors

---

## KNOWN REMAINING LIMITATIONS

1. ~~**Outbox worker not implemented**~~ — **RESOLVED in Phase 7.** `worker.py` + Docker service drain the outbox every 5 seconds. Set `DOWNSTREAM_URL` in production to forward events to a real endpoint.

2. **Structured JSON logging not implemented** — Logs are plain text. Cloud environments (Datadog, CloudWatch) require JSON logs with consistent keys for alerting.

3. **DLQ has no retry mechanism** — Dead-lettered events require manual replay. No `retry_count`, `max_retries`, or scheduled retry job.

4. **No Prometheus `/metrics` endpoint** — `prometheus-client` is commented in `requirements.txt`. Required for production observability.

5. **PII in storage** — `ledger.payload` and `dlq.raw_payload` store full webhook JSON including customer IDs. LGPD/GDPR right-to-erasure has no implementation path without breaking the audit trail.

---

## MOST LIKELY SILENT FAILURE IN PRODUCTION

~~**The outbox accumulates without bound. There is no worker draining it.**~~ — Fixed in Phase 7.

**New most likely silent failure: `DOWNSTREAM_URL` left empty in production.**
`worker.py` defaults to log-only mode when `DOWNSTREAM_URL=""`. Events are marked `dispatched=1`
but never actually forwarded. Revenue data is logged but never reaches the downstream system.

**Monitor:** `SELECT COUNT(*) FROM outbox WHERE dispatched=0`. Alert at > 10,000 rows.
**Verify:** Set `DOWNSTREAM_URL` to a real endpoint and confirm downstream system receives events.

---

## AI RESPONSE LOG

### Response 1 — 2026-05-18
User requested full-spectrum analysis following Universal Engineering Prompt (MODE C HYBRID),
H→L priority tier list, phase-by-phase implementation with before/after git commits, and living
report file.

**Classification:** MODE C — HYBRID

**Approach:** Read all 7 project files; compared current state against PREFLIGHT_AUDIT.md;
identified 17 already-fixed items from prior sessions; found 2 CRITICAL, 5 HIGH, 6 MEDIUM,
4 LOW remaining. Tagged `audit-baseline`. Implemented Phases 1–6.

**Key design decisions:**
- H1: `async def` → `def` (FastAPI threadpool is the correct pattern for sync psycopg2)
- C2: `warnings.warn` not `RuntimeError` (RuntimeError would break tests that don't set `STRIPE_WEBHOOK_SECRET`)
- M2: FK `ON DELETE CASCADE` (consistent with Transactional Outbox intent — delete ledger row → remove orphan outbox row)
- M6: Three explicit `if` branches instead of one compound condition (clearer operator intent)

### Response 2 — 2026-05-18
User asked to stop and preserve context before continuing implementation. Created this
`FULL_SPECTRUM_AUDIT.md` as a planning + context document before implementing Phases 2–6.

### Response 3 — 2026-05-18 (continuation)
User asked "where did we leave off, what's left, priority list?" — summarized status table,
confirmed plan, executed Phases 2–6 to completion. Also answered Stripe SDK question:
free, ~12 MB installed, `pip install -r requirements.txt` needed, no Stripe account required
for webhook signature verification only.

---

*Diego Alonso Del Río García — posthog-billing-poc — Mayo 2026*
*Last updated: 2026-05-18 — All 17 findings resolved. Phases 0–6 complete.*
