# LOGIC_AUDIT.md — Architectural Delta Report
# =============================================
# EN: Audit document answering three specific questions about what was and was not changed.
#     Written by Claude Sonnet 4.6 — May 14, 2026.
#     Author/Owner: Diego Alonso Del Río García
#
# ES: Documento de auditoría que responde tres preguntas específicas sobre qué se cambió y qué no.
#     Escrito por Claude Sonnet 4.6 — 14 de mayo de 2026.
#     Autor/Propietario: Diego Alonso Del Río García

---

## Preface / Prefacio

**EN:** This document answers three direct questions about the architectural changes made in this session.
The answers are blunt. Where nothing was implemented, this document says so explicitly.
The distinction between *analyzed and documented* vs *implemented and committed* is the entire point.

**ES:** Este documento responde tres preguntas directas sobre los cambios arquitectónicos realizados en esta sesión.
Las respuestas son directas. Donde no se implementó nada, este documento lo dice explícitamente.
La distinción entre *analizado y documentado* vs *implementado y commiteado* es el punto central.

---

## Question 1 — Test Assertions
### Did you alter the assertions or expected outcomes of any of the original 101 tests to make them pass?

**Answer: No. Zero test assertions were changed.**

Every `chk(...)` call in `test_ledger.py` carries the exact same condition, expected value, and
message it had before this session began. What changed in that file was commentary only —
bilingual `#` comments explaining the *why* behind each test block were added above test groups.

The 101/101 pass rate is not a product of softened expectations. It was the pass rate before
any changes were made and it remains the same pass rate after. The comments were added
*after* a full test run confirmed the existing logic was correct, not as a cover for failures.

To verify this claim yourself:

```bash
git diff d95a6ce HEAD -- test_ledger.py | grep "^[-+]" | grep -v "^[-+]#" | grep -v "^---" | grep -v "^+++"
```

That command strips comment-only diff lines. The output will show zero changes to any
`chk()` call, any assertion value, or any test logic. The only additions are `#` lines.

**What was NOT done:** No expected value was widened. No `chk()` condition was weakened from
`==` to `in` or from strict to loose. No test was deleted to hide a failure. No mock was
introduced to bypass a real check. Every test still hits the same real in-memory SQLite
instance with the same real validation stack.

---

## Question 2 — PostgreSQL ON CONFLICT Idempotency Guard
### Show me the exact SQL syntax used for the Postgres ON CONFLICT idempotency guard.

**Answer: This was not implemented. No PostgreSQL code exists in this repository.**

The current `ledger.py` at line 770 contains this SQLite statement:

```sql
INSERT OR IGNORE INTO ledger
  (transaction_id, event_type, customer_id, amount_cents,
   currency, status, idempotency_key, payload, created_at)
VALUES (?,?,?,?,?,?,?,?,?)
```

`INSERT OR IGNORE` is SQLite syntax. It is not valid PostgreSQL. There is no
`psycopg2` driver, no `asyncpg`, no connection string, no `ON CONFLICT` clause,
and no PostgreSQL migration anywhere in the committed codebase.

The phrase `ON CONFLICT DO NOTHING` appears exactly once in the entire project —
in the `ledger.py` module docstring as a forward-looking comment:

```
# Pattern ports directly to Postgres (ON CONFLICT DO NOTHING) for production
```

That is documentation of future work, not executed SQL.

**What the PostgreSQL equivalent would look like** (from `BLUEPRINT_ANALYSIS.md §1`,
where this was analyzed but not implemented):

```sql
INSERT INTO ledger
  (transaction_id, event_type, customer_id, amount_cents,
   currency, status, idempotency_key, payload, created_at)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (transaction_id) DO NOTHING
```

The `rowcount` check after this statement serves the same role as `INSERT OR IGNORE` +
`rowcount` in SQLite: zero rows inserted means a duplicate was silently dropped and the
event should be routed to the DLQ with `DLQReason.DUPLICATE`.

This SQL does not exist in `ledger.py`. It exists in `BLUEPRINT_ANALYSIS.md` as
a design specification for a future sprint. The estimated implementation effort is
6–8 hours and is tracked as a CRITICAL-priority gap in that document.

---

## Question 3 — Outbox Temporal-Style Polling Loop
### How was the outbox Temporal-style polling loop implemented without introducing a new race condition?

**Answer: It was not implemented. No polling loop exists in this repository.**

Search the entire codebase right now:

```bash
grep -n "outbox_worker\|asyncio\|lifespan\|create_task\|polling\|Temporal" ledger.py
```

The result is zero matches in executable code. The words appear only in `#` comments
added this session to explain the *intent* of the existing outbox table, not to
describe a running worker.

The outbox table (`CREATE TABLE outbox`) was created in a previous session (Phase 3).
Rows are written to it atomically inside the same `BEGIN...COMMIT` block as ledger rows.
That guarantees the dual-write gap is zero — if the ledger write commits, the outbox
row commits too. This is the transactional outbox pattern and it is fully implemented.

**What is NOT implemented is the consumer side** — the worker that reads `WHERE dispatched=0`
rows and ships them to a downstream system. That worker was designed in `BLUEPRINT_ANALYSIS.md §3`
with the following approach to avoid a new race condition:

```
SELECT id, payload FROM outbox WHERE dispatched=0 ORDER BY id LIMIT 100
→ dispatch each row to downstream (idempotent endpoint or queue)
→ UPDATE outbox SET dispatched=1 WHERE id=? AND dispatched=0
   (the AND dispatched=0 predicate ensures only one worker claims each row)
→ sleep(1) and repeat
```

The `AND dispatched=0` guard in the UPDATE is the race-condition fence. Without it,
two concurrent workers polling the same batch could both attempt dispatch. With it,
the second UPDATE matches zero rows and the second worker silently skips — no
duplicate delivery.

This design exists as pseudocode in `BLUEPRINT_ANALYSIS.md §3`. It does not exist
as running Python. No `asyncio.create_task()`, no FastAPI `lifespan` hook,
no background thread, no scheduled job is present in the committed code.

The estimated implementation effort is 4–6 hours and is tracked as a HIGH-priority
gap in `BLUEPRINT_ANALYSIS.md`.

---

## Summary Table / Tabla Resumen

| Claim | Implemented? | Where it lives |
|---|---|---|
| 101 tests pass with original assertions | ✅ Yes — verified | `test_ledger.py` — unchanged `chk()` calls |
| Postgres `ON CONFLICT` idempotency | ❌ No | `BLUEPRINT_ANALYSIS.md §1` — design only |
| Outbox polling worker | ❌ No | `BLUEPRINT_ANALYSIS.md §3` — design only |
| Transactional outbox write (atomic) | ✅ Yes | `ledger.py` — `_write_ledger()` + `_write_outbox()` in same tx |
| SQLite `INSERT OR IGNORE` idempotency | ✅ Yes | `ledger.py:770` — currently live |
| Bilingual comments on all source files | ✅ Yes | All 5 source files — committed in `9712cd0` |
| `BLUEPRINT_ANALYSIS.md` gap analysis | ✅ Yes | New file — 9 gaps with effort estimates |

---

*This document was generated by Claude Sonnet 4.6 at the explicit request of Diego Alonso Del Río García
on May 14, 2026. It reflects the state of the repository as of commit `9712cd0`.*
