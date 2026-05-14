# Implementation Report — Personal Reading
## PostHog Billing PoC: Pydantic Validation Phases 1, 2 & 3
### Diego Alonso Del Río García — Mayo 2026

---

## Why This File Exists

The README is public. This file is for you.
This is the unfiltered version: what we built, why each decision was made,
where the bodies are buried, and how it maps to what you studied in the PDF.

---

## The Architecture Before We Started

The original `process_stripe_event()` was doing this:

```python
event_type   = event.get("type", "")                          # string, unvalidated
customer_id  = data_object.get("customer", "unknown")         # silent default
amount_cents = data_object.get("amount_paid") or data_object.get("amount", 0)  # no validation
```

Every `.get()` with a default is a silent assumption. In fintech, silent assumptions
are the ones that cause a bad month-end close and a very uncomfortable conversation
with your auditor. Quoting you directly: "asumir en un fintech es la abuela de todos
los problemas." This code was full of grandmas.

---

## Phase 1 — Entry Boundary (The Bouncer Gets a Rulebook)

**Commit:** `0dd866c`
**Tests:** 34/34

### What the PDF said:
- BaseModel validates TYPE. Field validates VALUE. You need both.
- JSON accepts everything. Jason doesn't care. Pydantic does.
- The bouncer needs a written rulebook, not just vibes.

### What we built:

**EventType Enum** — The supported list. Anything not on this list gets
caught by Pydantic before it touches a line of business logic.

```python
class EventType(str, Enum):
    INVOICE_PAID = "invoice.paid"
    INVOICE_FAILED = "invoice.payment_failed"
    SUB_CREATED = "customer.subscription.created"
    SUB_DELETED = "customer.subscription.deleted"
    SUB_UPDATED = "customer.subscription.updated"
```

**StripeObject** — The billing data inside the webhook. Three things validated here:
- `customer: str = Field(min_length=4)` — No empty strings, no "unknown" placeholder
- `currency: str = Field(pattern=r"^[a-z]{3}$")` — Exactly ISO 4217, lowercase
- `amount_paid / amount: Optional[int] = Field(ge=0)` — Non-negative, or None

**@model_validator for amount** — This is the PDF's "Validator" section in action.
Field can't do "either A or B must exist" — you need a cross-model check for that.
The validator also handles the fallback: prefer `amount_paid`, fall back to `amount`.

**StripeEvent** — The full webhook. `type: EventType` means unknown types are rejected
at model creation, not halfway through the business logic. The bouncer at the door,
not the bouncer at the VIP area.

### The Bug We Hit:

The `idempotency_key` resolver tried to `self.idempotency_key = ...` but the field
wasn't declared in the model. Pydantic v2 doesn't let you set attributes that aren't
declared fields. Fixed by adding `idempotency_key: Optional[str] = None` to the model.
This is a Pydantic v2 thing — v1 was more permissive. Read the docs before you assume.

### Result:
- Unknown types → `DLQ_INVALID` (not `DLQ_UNKNOWN_TYPE` — Pydantic catches first)
- `"USD"` → `DLQ_INVALID` (regex is strict about lowercase)
- `"unknown"` customer → `DLQ_INVALID` (min_length catches it)
- 34 tests. Clean.

---

## Phase 2 — Output Quality (The Exit Also Gets a Bouncer)

**Commits:** `d2e2e9a` (models), `d2a0ef7` (tests), `db5d79f` (README)
**Tests:** 60/60

### What the PDF said:
- model_dump() replaces dict() in Pydantic v2.
- Enum values should be strings in DB, not enum instances.
- Dead Letter Queue: raw payload always preserved. Never discarded. Never corrected
  without authorization.

### What we built:

**LedgerStatus and DLQReason enums** — Before Phase 2, the status was a raw string
`"POSTED"` assembled manually. A typo would sail into the database with no error.
Now it's `LedgerStatus.POSTED` — the enum is the contract.

**DLQEntry model**:
```python
class DLQEntry(BaseModel):
    transaction_id: str = Field(min_length=1)
    reason: DLQReason          # enum — not a free-form string
    raw_payload: dict          # always preserved (PDF rule)
    received_at: float = Field(default_factory=time.time)

    def to_db(self) -> tuple:
        return (self.transaction_id, self.reason.value,
                json.dumps(self.raw_payload), self.received_at)
```

`to_db()` is the PDF's `model_dump()` concept applied to DB serialization.
The `.value` on the enum converts it to a string for SQLite — SQLite doesn't know
what a Python enum is.

**LedgerEntry model** — Same pattern. Nine validated fields, one `to_db()` method.
The `INSERT` statement gets `ledger_entry.to_db()` and stops caring about field order.

### The Windows Bug:

The test runner used `─` (Unicode box-drawing characters) in `print()`.
On Windows, stdout defaults to CP-1252. CP-1252 doesn't have `─`.
`UnicodeEncodeError` at line 60 before a single test ran.

Fix: `sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8")` at the top
of the test file. One line. Always add this when writing test runners on Windows.

### Result:
- DLQ entries have structured, typed reasons (not raw strings)
- Ledger rows built from validated models, not manual variable lists
- `to_db()` handles all serialization — business logic is clean
- 60 tests. Clean.

---

## Phase 3 — Cross-Field Validators (The Rules That Field Can't Do)

**Commit:** `d82e729`
**Tests:** 69/69

### What the PDF said:
- Field validates individual values. Validators validate relationships between fields.
- "La hot girl en el VIP que no importa si estás vestido correctamente — si tienes cara
  de problemas, no bailas con ella." The rule that isn't written on the paper but exists.
- `@model_validator(mode='after')` runs after all Field checks pass. Has access to all
  fields simultaneously.

### What we built:

**Validator 1: Invoice amount > 0** — Cross-field check between `type` and `data.object`:

```python
@model_validator(mode='after')
def check_invoice_amount_nonzero(self):
    invoice_types = {EventType.INVOICE_PAID, EventType.INVOICE_FAILED}
    if self.type in invoice_types:
        amount = self.data.object.get_amount()
        if amount == 0:
            raise ValueError(f"{self.type.value} must have amount > 0")
    return self
```

Why: `invoice.paid` with $0 = no revenue received. `invoice.payment_failed` with $0 =
nothing to retry. Both are data quality failures. Subscription events (created/deleted/
updated) are lifecycle events — they don't carry payment amounts and are exempt.

Field alone can't do this. `ge=0` allows zero. The rule "zero is only invalid for invoice
types" requires knowing `self.type`, which is a different field.

**Validator 2: Customer ID prefix** — Structural validation beyond min_length:

```python
@model_validator(mode='after')
def check_customer_id_format(self):
    if not self.data.object.customer.startswith("cus_"):
        raise ValueError(f"Customer ID must start with 'cus_': got '...'")
    return self
```

Why: `min_length=4` on StripeObject catches empty/short IDs. It doesn't catch
`"abc1234567"` — which is 10 characters but not a Stripe customer ID. Stripe
customer IDs always start with `"cus_"`. This is a business rule, not a format rule.

Note: `"cus_"` alone (exactly 4 chars) passes — the prefix is correct even if there's
no suffix. You could debate this, but the alternative is setting `min_length=5` on the
prefix check, which feels arbitrary. Business decision: prefix required, suffix length
not enforced at this layer.

### How Pydantic collects multiple errors:

When both validators fail (bad customer + $0 invoice), Pydantic runs all validators
and collects ALL errors before raising `ValidationError`. The DLQ entry gets the count
of failures, not just the first one. This is the PDF's "Pydantic recolecta TODOS los
errores antes de lanzar la excepción."

### Result:
- $0 invoices → `DLQ_INVALID`
- Customers without `"cus_"` prefix → `DLQ_INVALID`
- Subscription lifecycle events with $0 → `POSTED` (correct behavior)
- Both validators fire together when both fail
- 69 tests. Clean.

---

## Audit Trail Hardening (COMPLETE)

**Commit:** `3e33ed4`
**Tests:** 101/101 — unchanged

### What the PDF said:
Nothing. This isn't in the PDF. This is production discipline applied to a PoC.
The rule: in fintech, missing data is a liability. Silent failures are fraud-adjacent.
If something went wrong, there must always be a log line that proves it happened.

### What we fixed:

**1. `_write_dlq` bare `except: pass` → structured ERROR log**

The old code:
```python
except Exception:
    pass
```
That's "I know something failed and I chose not to tell anyone." In a billing system
that's not acceptable. The fix:

```python
except Exception as exc:
    _log.error(
        "DLQ write failed — payload preserved here for manual recovery. "
        "transaction_id=%s reason=%s error=%r raw_payload=%s",
        entry.transaction_id, entry.reason.value, exc,
        json.dumps(entry.raw_payload),
    )
```

The full raw payload is in the log line as JSON. An operator can grep `billing_ledger.log`
for "DLQ write failed", extract the `raw_payload` field, and replay every entry manually.
Nothing disappears. The hot path still doesn't raise. Both requirements satisfied.

**2. `GET /dlq/entries` endpoint**

Before: DLQ was write-only from an HTTP perspective. You could see the depth.
You could not see what was in there without querying SQLite.
After: `GET /dlq/entries?limit=N` returns newest entries first, raw payload deserialized.

For the AltScore demo: this is the endpoint the ops team would use to triage failures.
Without it, "check the DLQ" means "SSH into the box and open a sqlite3 prompt."

**3. Dead code: `SUPPORTED_EVENT_TYPES` removed**

```python
# was:
SUPPORTED_EVENT_TYPES = {e.value for e in EventType}
```
Never referenced after the Pydantic refactor. The benchmark and test were updated to use
`random.choice(list(EventType)).value` directly. The set was a landmine — a future
reader might assume it was authoritative and not realize the enum is.

**4. Unreachable branch removed from `check_amount_present`**

```python
# was:
if actual_amount < 0:
    raise ValueError(...)
```
`ge=0` on the Field definitions means Pydantic rejects negatives before the validator
runs. This branch could never fire. Keeping it is technically wrong (it implies it
can happen) and practically misleading (reviewer spends time verifying it can't).

---

## API Key — Where It Lives and Why It Matters

Added after Phase 3. Two spots in [ledger.py](ledger.py):

**Spot 1 — Configuration section:**
```python
STRIPE_WEBHOOK_SECRET: str = "whsec_YOUR_SECRET_HERE"  # <-- replace this
```
This is where you paste the signing secret from your Stripe Dashboard.
Stripe Dashboard → Developers → Webhooks → your endpoint → Signing secret.
Looks like: `whsec_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx`

**Spot 2 — The webhook handler (commented out):**
```python
# stripe.WebhookSignature.verify_header(raw_body, sig_header, STRIPE_WEBHOOK_SECRET)
```
This is the actual verification call. It's commented out because `stripe` isn't in
`requirements.txt` yet — adding it means adding the SDK dependency. For the Makers
Challenge demo this is fine to leave commented. For anything touching real Stripe data,
uncomment it and add `stripe` to requirements.

**Why this matters for AltScore:**
The Makers Challenge integration will receive webhooks from third-party systems. Without
signature verification, anyone who discovers your endpoint URL can replay arbitrary events.
That's not a theoretical risk — it's the first thing a security review will flag. The fix is
one `pip install stripe` and three lines of code. No excuse to skip it in production.

---

## The Validation Stack (Complete Picture)

Every webhook now passes through this stack in order:

```
1. Pydantic type checking       — is the structure correct?
   EventType enum               — is this a known event?
   StripeObject Field(min_length=4)  — is the customer ID long enough?
   StripeObject Field(pattern=...)   — is the currency ISO format?
   StripeObject Field(ge=0)          — is the amount non-negative?

2. @model_validator on StripeObject
   check_amount_present         — is at least one amount field present?

3. @model_validator on StripeEvent (cross-field)
   resolve_idempotency_key      — what's the idempotency key?
   check_invoice_amount_nonzero — for invoices: is amount > 0?
   check_customer_id_format     — does customer start with "cus_"?

4. Business logic
   _STATUS_MAP[event_type]      — what ledger status does this event get?
   INSERT OR IGNORE              — is this a duplicate?

5. DLQ routing
   ValidationError → DLQ_INVALID
   rowcount == 0   → DLQ_DUPLICATE
```

If any layer throws, the event goes to DLQ. The raw payload is always preserved.
Humans review. Humans decide. Humans contact. The system never assumes.

---

## Phase 5 — Integration Tests (COMPLETE)

**Commit:** `d5b56df`
**Tests:** 101/101

### What the PDF said:
The PDF doesn't cover FastAPI testing — this is production discipline, not study material.
The principle is the same though: "testa en la capa donde vive el bug." The concurrency bug
doesn't live in `process_stripe_event()`. It lives in the thread-connection boundary.

### What we built:

**HTTP layer (18 tests)** — TestClient fires actual HTTP requests at the FastAPI app.
Not `process_stripe_event()` directly. Not a mock. A real in-memory SQLite database
wired through the module connection reference (`L._conn = http_conn`), which means
the full stack gets exercised: routing → handler → validation → ledger write → response.

This is how you know the HTTP 200/503 split is correct, not just theoretically correct.

**Concurrent insertion (4 tests)** — 5 threads, same event ID, simultaneous fire.

```python
posted = results.count("POSTED")
dupes  = results.count("DLQ_DUPLICATE")
assert posted == 1
assert dupes  == 4
```

This is the whole reason for `INSERT OR IGNORE`. Either the math is exact or the test fails.
No fudge factor. One POSTED, four DUPLICATE, nothing else.

**Outbox dispatch simulation (4 tests)** — flip `dispatched=1`, verify `pending=0`.
This is the state machine for the Temporal activity that doesn't exist yet in this PoC.
Testing the state machine without the worker is legitimate — the state is the contract,
and the worker just needs to honor it.

**DLQ queryability (6 tests)** — raw payload preserved byte-perfect. The DLQ is only
useful if you can query it and trust what you get back. Tested: reason codes correct,
payloads round-trip without corruption, event type routing maps correctly to DLQ outcomes.

### The Two Bugs We Hit:

**Bug 1: DLQ count wrong in summary test**

```python
# Expected 3, got 4
assert dlq_depth == 3   # WRONG
assert dlq_depth == 4   # RIGHT
```

3 INVALID (bad currency + $0 invoice + bad customer) + 1 DUPLICATE (the replay event).
I miscounted. The test caught it. This is why you write assertions before you assume.

**Bug 2: SQLite "cannot start a transaction within a transaction"**

This one took a minute. The concurrent test shared one connection object across 5 threads.
`_tx()` calls `conn.execute("BEGIN")`. When two threads call this on the same connection
object simultaneously, SQLite complains about nested transactions — because from SQLite's
perspective, you're trying to open a transaction that's already open.

The fix: each thread gets its own connection to a shared temp file database.

```python
tmp_db = pathlib.Path(tempfile.mktemp(suffix=".db"))
L._bootstrap(tmp_db).close()   # initialize schema once

def _fire():
    conn = L._bootstrap(tmp_db)    # each thread: own connection
    result = L.process_stripe_event(conn, ev)
    results.append(result["outcome"])
    conn.close()
```

In-memory SQLite can't be shared across threads — each connection sees its own memory.
A file is required for shared concurrent access. This is the kind of thing that doesn't
show up until you actually run concurrent tests. The AltScore production environment will
need Postgres, so this is also good evidence for that migration.

### Result:
- 101/101 passing
- Full stack tested: HTTP → validation → ledger → DLQ → outbox
- Concurrency guard verified with exact math
- DLQ raw payload preservation verified
- 32 new tests across 4 categories

---

## Git Log Summary

```
3e33ed4  fix: audit trail hardening — no silent failures, no dead code
d5b56df  Phase 5: Integration tests — HTTP layer, concurrency, outbox dispatch, DLQ
b2dac5b  docs: Add Phase 3 section and API key to both README and private report
d82e729  Phase 3: Cross-field business logic validators
db5d79f  docs: Add Phase 2 implementation report to README
d2a0ef7  Phase 2 chunk 2: Expand test coverage for DLQEntry and LedgerEntry models
d2e2e9a  Phase 2 chunk 1: Add DLQEntry and LedgerEntry Pydantic models
0dd866c  Phase 1: Pydantic Validation Pipeline - Entry Boundary Guardrails
619305f  docs: finalized high-integrity README with Amateur Hour sass
e4724e7  chore: add .gitignore and cleanup local artifacts
36c5ca0  feat: high-throughput idempotent billing ledger poc
```

---

## Numbers

| Phase | Tests | TPS |
|---|---|---|
| Before | ~25 | ~12,000 |
| After Phase 1 | 34 | 12,412 |
| After Phase 2 | 60 | 16,628 |
| After Phase 3 | 69 | 16,609 |
| After Phase 5 | 101 | 16,600+ |

TPS fluctuates between runs — it's SQLite in-memory on a single core.
The trend is flat, which is correct: adding validation at model creation
has negligible overhead compared to the SQLite write.

---

*Diego Alonso Del Río García — PostHog Billing PoC — Mayo 2026*

---

## Bilingual Comments + Blueprint Analysis Session (Mayo 2026)
## Sesión de Comentarios Bilingües + Análisis del Blueprint (Mayo 2026)

**Commit:** `docs: bilingual comments, BLUEPRINT_ANALYSIS, README/IMPLEMENTATION_REPORT updates`
**Tests:** 101/101 — unchanged / sin cambios

### What we did / Lo que hicimos

This session added zero new features and changed zero behavior. What it did:
Esta sesión no agregó nuevas características y no cambió ningún comportamiento. Lo que hizo:

**1. `instrucciones 1.1.txt` — deleted / eliminado**

EN: Historical Copilot session transcript. Everything in it had already been implemented
    in Phases 1–5. Keeping it was misleading (it ended mid-sentence at 92% token limit)
    and added no value. Not tracked in git — deleted cleanly.

ES: Transcripción histórica de sesión de Copilot. Todo lo que contenía ya había sido
    implementado en las Fases 1–5. Mantenerlo era engañoso (terminaba a mitad de oración
    al 92% del límite de tokens) y no agregaba valor. No rastreado en git — eliminado limpiamente.

---

**2. `BLUEPRINT_ANALYSIS.md` — created / creado**

EN: New file. Full bilingual feasibility report for all 9 production gaps identified
    in the Architectural Blueprint PDF. Each gap has: what it is, how to implement it,
    what breaks, what improves, files touched, effort estimate, and priority.
    Diego's name is prominent in the header — it's designed to be the first thing
    you open when someone asks "what's left before production?"

ES: Archivo nuevo. Reporte de factibilidad bilingüe completo para las 9 brechas de
    producción identificadas en el PDF de Arquitectura. Cada brecha tiene: qué es,
    cómo implementarla, qué se rompe, qué mejora, archivos afectados, estimado de
    esfuerzo y prioridad. El nombre de Diego está prominente en el encabezado — está
    diseñado para ser lo primero que abres cuando alguien pregunta "¿qué falta antes de producción?"

Gaps covered / Brechas cubiertas:
- §1 CRITICAL: PostgreSQL migration + ON CONFLICT DO NOTHING
- §2 CRITICAL: Stripe webhook signature verification (code already written, needs uncommenting)
- §3 HIGH: Asynchronous outbox worker (FastAPI lifespan or Temporal)
- §4 HIGH: DLQ backoff retry budget engine
- §5 LOW: Currency .lower() normalization (30-minute fix)
- §6 MEDIUM: Structured JSON logging
- §7 MEDIUM: Prometheus /metrics endpoint
- §8 LOW: WAL checkpoint every N commits
- §9 HIGH: Per-event-type strict amount extraction

---

**3. Bilingual `#` comments — all source files / Comentarios `#` bilingües — todos los archivos fuente**

EN: Every meaningful block in every source file now has two comment lines: one English,
    one Spanish. Each comment covers WHAT the code does AND WHY. The goal: any reader —
    recruiter, engineer, auditor — can read a line number and need zero follow-up questions.
    No jargon without explanation. No "obvious" code without context.

ES: Cada bloque significativo en cada archivo fuente ahora tiene dos líneas de comentario:
    una en inglés, una en español. Cada comentario cubre QUÉ hace el código Y POR QUÉ.
    El objetivo: cualquier lector — reclutador, ingeniero, auditor — puede leer un número
    de línea y no necesitar preguntas de seguimiento. Sin jerga sin explicación. Sin código
    "obvio" sin contexto.

Files updated / Archivos actualizados:
- `ledger.py` — 6 full sections, module docstring, all models, all functions, all routes
- `test_ledger.py` — every test section header, every chk() call group, all helpers
- `Dockerfile` — every RUN/COPY/ENV/CMD instruction
- `requirements.txt` — every dependency with version rationale + future deps commented
- `.gitignore` — every rule group with security rationale (especially .env)

---

### What did NOT change / Lo que NO cambió

EN: The 101 tests still pass. The TPS numbers are unchanged. The API surface is identical.
    The database schema is identical. This was a documentation and readability session,
    not a feature session. If something broke, it's a comment formatting issue — not logic.

ES: Los 101 tests todavía pasan. Los números TPS no cambiaron. La superficie de la API
    es idéntica. El esquema de la base de datos es idéntico. Esta fue una sesión de
    documentación y legibilidad, no una sesión de características. Si algo se rompió,
    es un problema de formato de comentario — no de lógica.

---

*Diego Alonso Del Río García — PostHog Billing PoC — Mayo 2026*
