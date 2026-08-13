# Security Policy

## Status of this project

This is a **proof of concept**, not production software. It is published to demonstrate
specific architectural decisions — idempotent inserts at the database constraint, the
Transactional Outbox pattern, and dead-letter handling. Read the *Production Gap
Checklist* in [README.md](README.md) before considering any of it for real traffic.

That said, the controls it does implement are meant to work, and a report that one of
them does not is worth sending.

## Reporting a vulnerability

Open a [private security advisory](https://github.com/DDRG15/micro-billing-ledger-poc/security/advisories/new)
rather than a public issue, so the problem is not disclosed before it can be addressed.

A useful report names the affected file and line, the input or condition that triggers
the behaviour, and what an attacker gains. A reproduction beats a description.

Expect an acknowledgement within a week. This is a personal project, so there is no
paid support and no bug bounty.

## Supported versions

Only the current `main` branch. There are no releases and no backports.

## What this project does and does not defend against

Stating the boundary is more useful than implying an unlimited one.

**Enforced.** HMAC-SHA256 signature verification on every webhook, including when the
signing secret is left at its placeholder — an endpoint that accepts unsigned requests
whenever it is misconfigured fails silently, which is worse than rejecting everything.
API key on the operational endpoints (`/ledger/summary`, `/dlq/entries`,
`/dlq/{id}/retry`, `/metrics`). Per-client rate limiting keyed on `X-Forwarded-For`
trusted only from the proxy's own address. TLS and security headers at the proxy.
The container runs as a non-root user.

**Tenant isolation (partial).** `ledger`, `outbox` and `dlq` carry `org_id`, and the
ledger primary key is `(org_id, transaction_id)` so the idempotency guard is per-tenant —
without that, two tenants presenting the same Stripe event id would silently discard one
another's revenue. The caller's tenant is derived from their API key and is never accepted
from a header: a tenant supplied by the caller is not a boundary. Every operational query
is scoped to the derived tenant, and `POST /dlq/{id}/retry` scopes in the WHERE clause so
another tenant's entry is indistinguishable from a nonexistent one — a check applied after
the fetch would leak existence through the status code.

**Ingest is per-tenant.** `POST /webhook/stripe/{org_id}` verifies the HMAC with that
tenant's registered signing secret. This is self-validating — a Stripe signature only
checks out against the account that produced the event — so the path segment routes the
request without being trusted, and a caller cannot write into a tenant whose secret it
does not hold. An unknown or inactive org returns exactly what a bad signature returns,
so the endpoint cannot be used to enumerate tenants; the distinction appears only in the
server log.

**Delivery is per tenant.** `org.downstream_url` gives each tenant its own destination,
and the outbox worker resolves it per row. Forwarding every tenant's events to one shared
URL would have been the largest leak in the system — the outbox payload carries customer
ids and amounts — since isolating the database means nothing if the data is then handed to
a shared endpoint. NULL falls back to the process-wide `DOWNSTREAM_URL`, so a
single-operator deployment is unchanged. Dispatch carries `X-Org-Id` so the receiver knows
which tenant it is being handed.

The worker also drains round-robin across tenants rather than by global id order. That is
an availability property rather than a confidentiality one: a shared queue drained in id
order lets a tenant with a large backlog own every batch, making a noisy tenant an outage
for the quiet ones.

**The database enforces isolation, not only the queries.** Every `SELECT` and `INSERT`
used to carry its own `org_id` predicate, which holds exactly as long as nobody forgets
one — and on 2026-08-11 somebody did: the batch path built its DLQ rows without `org_id`
in two branches, so one tenant's raw payload became readable by another and the same
Stripe event was POSTED under two tenants. No linter, type checker or constraint could
have caught it, because the guarantee was a habit.

`ledger`, `outbox` and `dlq` now carry row-level security policies with `USING` **and**
`WITH CHECK` — reading and writing both — and the service connects as `billing_app`, a
role created `NOSUPERUSER NOBYPASSRLS` holding DML on those three tables and `SELECT` on
the org registry. The role is the part that matters: a superuser bypasses RLS entirely,
`ENABLE` and `FORCE` included, so an earlier attempt that shipped only the policies
enforced nothing while appearing complete. Unset scope means *no rows*, not *all rows*;
treating "unset" as "unrestricted" would reproduce the original failure exactly. Work
that legitimately crosses tenants — the outbox drain, the DLQ retry claim, retention
pruning — opts in through one named token, so `grep ALL_ORGS` lists everything that
leaves a tenant.

The process states which of the two worlds it came up in, at startup, every boot:
`rls_enforced` with the connected role, or `rls_inert` at CRITICAL. Check it after any
deploy that touches `APP_DATABASE_URL`:

```bash
docker compose logs billing | grep -E "rls_enforced|rls_inert"
```

**What RLS here does not do.** It scopes what a session may see; it does not decide who
may open one. Anyone holding the `billing_app` credential reads every tenant by asking
for `ALL_ORGS` — so that password is a real secret, and migration 0006 creates it with a
placeholder published in this repository. Rotate it before the database is reachable
from anywhere; the service warns at startup for as long as it is unrotated. The owner
credential still bypasses RLS by design, because migrations need DDL — it belongs to the
migration step and to no process serving traffic.

**Not attempted, within tenancy.** There is no admin credential, so no operator-wide view
across tenants exists. No authentication beyond a single shared API key per tenant — no
user accounts, no roles, no rotation flow. No audit log of who read operational data. No
encryption at rest beyond whatever PostgreSQL is configured to provide. No GDPR/LGPD
erasure path; the ledger is deliberately append-only and resolving that conflict needs a
retention policy decided first, not code.

**Dev defaults are dev defaults.** `.env.example` ships working placeholder values so
the stack starts on a clean clone. Every one marked `*** CHANGE ***` must be replaced
before anything is reachable from a network. `BILLING_API_KEY` left empty disables
authentication on the operational endpoints entirely; the application logs a warning at
startup when that happens, and that warning is not decorative.

## Secrets and rotation

Secrets come from environment variables, or from a file when `<NAME>_FILE` is set —
`BILLING_API_KEY_FILE`, `STRIPE_WEBHOOK_SECRET_FILE`. The file form is the
Docker/Kubernetes secrets convention and it exists for a concrete reason: a container's
environment is immutable for its lifetime, so a secret delivered by environment variable
cannot be rotated without replacing the container. A mounted file can change underneath a
running process.

A configured but unreadable `*_FILE` makes the service refuse to start rather than fall
back to the environment variable. Starting on the wrong credential and logging nothing is
a worse outcome than not starting.

**`docker compose restart` does not re-read `.env`.** It restarts the existing container
with its original environment, so a rotated secret is not live until the container is
recreated with `docker compose up -d`. Before this was made observable, that mistake
produced no error, no log line and no metric — the operator believed a rotation had landed
while the old credential kept working.

To confirm which secret a process is actually running with, without printing it: the
startup log emits a `secrets_loaded` event, and `/metrics` exposes `billing_secrets_info`.
Both carry a SHA-256 prefix and the source (`env` or `file`), which you can compare against
a locally computed hash. Alert on that fingerprint changing when you did not deploy, and on
it *failing* to change after a rotation you did perform.

A published hash is only safe for a high-entropy secret, which is what this project asks
for (`openssl rand -hex 32`). For a guessable value, publishing its hash invites an offline
dictionary attack — the fingerprint does not make a weak secret safe, and nothing else will
either.

**Not attempted:** live reload. Secrets are read once at startup. Rotation means recreating
the container or restarting the process. Re-reading a file on every request would add an
I/O call to the hot path and a new class of failure — a half-written secret file read
mid-rotation — to solve a problem that a deploy already solves.

## Dependencies

Production dependencies are pinned to exact versions in `requirements.txt`. Pinning
prevents surprise breakage, but it also freezes vulnerabilities in place: a pinned
version stops receiving fixes the moment one is published. Transitive dependencies are
the harder case — they are not listed in the manifest, so a scanner watching this
repository may not flag them at all. `starlette` reached this project that way through
FastAPI's pin range. Check the resolved tree, not only the manifest.
