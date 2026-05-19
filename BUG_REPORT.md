# BUG REPORT — posthog-billing-poc
## Status: 1 MEDIUM open · 3 LOW open · 2 informational
### Last updated: 2026-05-19 · Read this first when you come back.

---

## WHAT TO FIX FIRST (in order)

```
┌─────────────────────────────────────────────────────────────────┐
│  B1  MEDIUM  worker.py / run_loop()  ← START HERE              │
│  B2  LOW     worker.py / _dispatch_row()                        │
│  B4  LOW     worker.py / drain_batch()                          │
│  B6  LOW     worker.py / main()       (type-only, mypy fix)     │
└─────────────────────────────────────────────────────────────────┘
B3 and B5 are informational — no code change needed.
```

---

## B1 — MEDIUM — Broken connection reused after failed reconnect

**File:** [worker.py](worker.py)
**Function:** `run_loop()`
**Status:** OPEN

### What is broken

When a `psycopg2.OperationalError` occurs (e.g. DB goes away), `run_loop` tries to
reconnect. If the reconnect itself fails, the exception is silently swallowed with
`pass` — and `conn` still points to the old, broken connection. The next loop
iteration calls `drain_batch(broken_conn)`, which raises again immediately,
triggering the backoff — but it never actually gets a new connection.

```python
# CURRENT CODE — broken if inner _connect() raises:
except psycopg2.OperationalError as exc:
    log.error("db_error retrying in %.0fs: %s", backoff, exc)
    time.sleep(backoff)
    backoff = min(backoff * 2, 60.0)
    try:
        conn = _connect()          # if this raises...
    except psycopg2.OperationalError:
        pass                       # conn is still the broken one ← BUG
```

### How to fix

Replace the inner `try/except` with a loop that keeps trying until a new
connection is established, using the same backoff already in scope:

```python
# FIXED:
except psycopg2.OperationalError as exc:
    log.error("db_error retrying in %.0fs: %s", backoff, exc)
    time.sleep(backoff)
    backoff = min(backoff * 2, 60.0)
    new_conn = None
    while new_conn is None and not _stop:
        try:
            new_conn = _connect()
            log.info("reconnected to database")
        except psycopg2.OperationalError as reconnect_exc:
            log.error("reconnect_failed retrying in %.0fs: %s", backoff, reconnect_exc)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60.0)
    if new_conn is not None:
        conn = new_conn
```

---

## B2 — LOW — Wrong log label for DNS / connection-refused dispatch errors

**File:** [worker.py](worker.py)
**Function:** `_dispatch_row()`
**Status:** OPEN

### What is broken

`urllib.request.urlopen` raises `urllib.error.URLError` (not `urllib.error.HTTPError`)
when the downstream server is unreachable (DNS failure, connection refused, timeout).
This exception is not caught in `_dispatch_row`, so it bubbles up through
`drain_batch` (correct — transaction rolls back, row is retried) and reaches
`run_loop`'s bare `except Exception`, which logs it as `"unexpected_error"`.
It's handled correctly but the log is misleading — it looks like a worker bug,
not a dispatch failure.

### How to fix

In `_dispatch_row`, catch `urllib.error.URLError` alongside `HTTPError` and
raise a `RuntimeError` with a clear message — same as the HTTP 4xx/5xx path:

```python
# In _dispatch_row, replace the try/except block:
try:
    with urllib.request.urlopen(req, timeout=10) as resp:
        status = resp.status
except urllib.error.HTTPError as exc:
    status = exc.code
except urllib.error.URLError as exc:
    raise RuntimeError(
        f"downstream unreachable for transaction_id={row['transaction_id']}: {exc.reason}"
    )
```

---

## B4 — LOW — No-op UPDATE when all rows in a batch fail dispatch

**File:** [worker.py](worker.py)
**Function:** `drain_batch()`
**Status:** OPEN (non-blocking)

### What is broken

If every row in a batch fails `_dispatch_row` (e.g. downstream is down),
`dispatched_ids` is an empty list. The code still issues:

```sql
UPDATE outbox SET dispatched=1, dispatched_at=NOW() WHERE id = ANY([])
```

PostgreSQL handles `ANY([])` correctly (updates zero rows), so data is safe.
But it opens a BEGIN/COMMIT transaction for a write that does nothing.

### How to fix

One-line guard before the `UPDATE`:

```python
if dispatched_ids:
    cur.execute(
        "UPDATE outbox SET dispatched=1, dispatched_at=NOW() WHERE id=ANY(%(ids)s)",
        {"ids": dispatched_ids},
    )
```

The guard already exists for the log line — just move the `UPDATE` inside it.
(Check current code: the UPDATE may already be inside the `if dispatched_ids:` block.
If so, this is already fixed — verify before acting.)

---

## B6 — LOW — mypy flags `conn` as Optional at `run_loop()` call site

**File:** [worker.py](worker.py)
**Function:** `main()`
**Status:** OPEN (type-only, no runtime impact)

### What is broken

```python
conn: Optional[psycopg2.extensions.connection] = None
while conn is None:
    try:
        conn = _connect()
    ...
run_loop(conn)   # mypy: Argument 1 has incompatible type "Optional[connection]"
```

The `while conn is None` loop guarantees `conn` is set before `run_loop` is
called, but mypy can't infer that.

### How to fix

Add an `assert` after the loop — zero runtime cost, narrows the type:

```python
assert conn is not None  # loop above guarantees this
run_loop(conn)
```

---

## B3 — INFORMATIONAL — `ADD COLUMN IF NOT EXISTS` requires PostgreSQL 9.6+

**File:** [ledger.py](ledger.py)
**Function:** `_bootstrap()`
**Status:** No action needed (docker-compose pins PG 16)

`ALTER TABLE outbox ADD COLUMN IF NOT EXISTS dispatched_at` uses a syntax only
available since PostgreSQL 9.6. The compose file pins `postgres:16-alpine`.
Only relevant if someone runs against a very old PostgreSQL instance.

---

## B5 — INFORMATIONAL — `dispatched_at` migration coupled to `ledger._bootstrap()`

**File:** [test_ledger.py](test_ledger.py)
**Status:** No action needed (POC scope)

The `ALTER TABLE outbox ADD COLUMN IF NOT EXISTS dispatched_at` migration runs
inside `ledger._bootstrap()`. If `worker.py` is ever extracted to its own
repository, this migration must move with it (or be duplicated in `worker.py`'s
own startup sequence). Not a problem at current scope.

---

## WHEN YOU COME BACK — SUGGESTED ORDER

1. Fix **B1** (15 min) — real correctness bug, worker can loop forever on broken conn
2. Fix **B2** (10 min) — misleading log noise, easy catch clause addition
3. Fix **B4** (5 min) — verify first; may already be fixed by the existing `if dispatched_ids:` guard
4. Fix **B6** (2 min) — one `assert` line, makes mypy happy

Total estimated time: ~30 minutes for all four.

---

*Diego Alonso Del Río García — posthog-billing-poc — 2026-05-19*
*Generated at end of session before break. Companion to FULL_SPECTRUM_AUDIT.md.*
