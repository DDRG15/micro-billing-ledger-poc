# LOGIC_AUDIT.md — Architectural State Reference
## Micro-Billing-Ledger PoC
### Diego Alonso Del Río García — Mayo 2026

---

## Purpose / Propósito

EN: This document is the authoritative reference for the current architectural state of the
    repository. It is a living document — update it when the architecture changes. It answers
    three questions: what is implemented, how it works, and what is not yet implemented.

ES: Este documento es la referencia autoritativa para el estado arquitectónico actual del
    repositorio. Es un documento vivo — actualizarlo cuando la arquitectura cambie. Responde
    tres preguntas: qué está implementado, cómo funciona, y qué aún no está implementado.

---

## Current Engine / Motor Actual

EN: **PostgreSQL 16** (via psycopg2-binary). SQLite was used during Phases 1–3 for
    in-memory test speed. It was fully replaced by PostgreSQL in commit `6554181`.
    There is no SQLite code anywhere in this repository. All tests hit a live
    PostgreSQL instance running in Docker (postgres:16-alpine).

ES: **PostgreSQL 16** (vía psycopg2-binary). SQLite se usó durante las Fases 1–3 para
    velocidad de tests en memoria. Fue completamente reemplazado por PostgreSQL en el
    commit `6554181`. No hay código SQLite en ningún lugar de este repositorio. Todos
    los tests golpean una instancia PostgreSQL real corriendo en Docker (postgres:16-alpine).

---

## Idempotency Guard / Guardia de Idempotencia

EN: The idempotency mechanism is `INSERT INTO ledger (...) VALUES (...) ON CONFLICT
    (transaction_id) DO NOTHING`. The `transaction_id` column is the PRIMARY KEY.
    PostgreSQL's constraint engine serializes concurrent inserts — exactly one INSERT
    wins the race; all others produce `rowcount=0` (single-event path) or are absent
    from the `RETURNING` set (batch path). No application-level lock is needed.

ES: El mecanismo de idempotencia es `INSERT INTO ledger (...) VALUES (...) ON CONFLICT
    (transaction_id) DO NOTHING`. La columna `transaction_id` es la PRIMARY KEY.
    El motor de restricciones de PostgreSQL serializa las inserciones concurrentes — exactamente
    un INSERT gana la carrera; todos los demás producen `rowcount=0` (ruta por evento) o
    están ausentes del conjunto `RETURNING` (ruta en lote). No se necesita bloqueo a nivel
    de aplicación.

---

## Transaction Model / Modelo de Transacción

EN: All three tables (ledger, outbox, dlq) are written inside a single `BEGIN…COMMIT`
    block provided by `_tx()`. The `_tx()` context manager uses `conn.autocommit = True`
    at the connection level, then issues explicit `BEGIN` / `COMMIT` or `ROLLBACK` to
    group multiple statements into one atomic unit. A crash before COMMIT leaves nothing;
    a crash after COMMIT leaves all rows intact. This is the Transactional Outbox pattern.

ES: Las tres tablas (ledger, outbox, dlq) se escriben dentro de un único bloque
    `BEGIN…COMMIT` proporcionado por `_tx()`. El gestor de contexto `_tx()` usa
    `conn.autocommit = True` a nivel de conexión, luego emite `BEGIN` / `COMMIT` o
    `ROLLBACK` explícitos para agrupar múltiples sentencias en una unidad atómica.
    Un crash antes del COMMIT no deja nada; un crash después del COMMIT deja todas las
    filas intactas. Este es el patrón Outbox Transaccional.

---

## Batch Path / Ruta en Lote

EN: `process_stripe_event_batch()` uses `psycopg2.extras.execute_values` to collapse N
    individual INSERT round trips into `ceil(N/page_size)` bulk statements. At
    `page_size=1000` and N=5,000: ~12 round trips vs 20,000 for the per-event path.
    Throughput: 26 TPS (per-event, Docker-WSL2) → ~5,000 TPS (batch). The 192×
    improvement comes entirely from reducing synchronous network round trips.

    Duplicate partitioning uses `RETURNING transaction_id`: PostgreSQL reports exactly
    what THIS transaction inserted — atomic, no race window, no extra round trip.

ES: `process_stripe_event_batch()` usa `psycopg2.extras.execute_values` para colapsar N
    round trips de INSERT individuales en `ceil(N/page_size)` sentencias masivas. Con
    `page_size=1000` y N=5,000: ~12 round trips vs 20,000 para la ruta por evento.
    Throughput: 26 TPS (por evento, Docker-WSL2) → ~5,000 TPS (lote). La mejora de 192×
    viene enteramente de reducir los round trips de red síncronos.

    La partición de duplicados usa `RETURNING transaction_id`: PostgreSQL reporta exactamente
    lo que ESTA transacción insertó — atómico, sin ventana de carrera, sin round trip extra.

---

## Test Isolation / Aislamiento de Tests

EN: Each test section calls `fresh_conn()` which issues `TRUNCATE TABLE outbox, dlq,
    ledger RESTART IDENTITY`. This resets all table data and BIGSERIAL sequences.
    No mocking. No stubs. Every assertion hits a real PostgreSQL database.

ES: Cada sección de test llama `fresh_conn()` que emite `TRUNCATE TABLE outbox, dlq,
    ledger RESTART IDENTITY`. Esto resetea todos los datos de tablas y secuencias BIGSERIAL.
    Sin mocking. Sin stubs. Cada aserción golpea una base de datos PostgreSQL real.

---

## What Is NOT Implemented / Lo que NO Está Implementado

| Feature / Característica | Status / Estado |
|---|---|
| Stripe webhook signature verification | **ACTIVE** — `stripe.Webhook.construct_event()` validates HMAC-SHA256 on every request; `stripe==10.12.0` is in `requirements.txt` |
| Outbox worker (dispatch loop) | Schema exists (dispatched=0/1), worker not implemented |
| Connection pooling | **ACTIVE** — `ThreadedConnectionPool(minconn=1, maxconn=20)` at module level |
| Prometheus `/metrics` endpoint | Not implemented (`prometheus-client` commented out in requirements.txt) |
| Structured JSON logging | Plain text logging only (`RotatingFileHandler` added in Phase 2) |
| DLQ retry budget (retry_count, max_retries) | DLQ is append-only — no retry mechanism |

---

## Commit History / Historial de Commits

```
478bd29  docs: full bilingual rewrite of IMPLEMENTATION_REPORT, remove BLUEPRINT_ANALYSIS
5e47362  docs: Phase 6 — lead architect documentation pass
c8f1b30  perf: batch ingestion via execute_values — 26 TPS → 4,992 TPS
6554181  feat: PostgreSQL migration — rip out SQLite, implement ON CONFLICT idempotency
0d86845  docs: Add LOGIC_AUDIT.md — honest architectural delta report (SQLite era, now superseded)
9712cd0  docs: bilingual comments, BLUEPRINT_ANALYSIS.md, README and IMPLEMENTATION_REPORT updates
```

---

*Diego Alonso Del Río García — PostHog Billing PoC — Mayo 2026*
