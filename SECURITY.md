# Security Policy

## Status of this project

This is a **proof of concept**, not production software. It is published to demonstrate
specific architectural decisions — idempotent inserts at the database constraint, the
Transactional Outbox pattern, and dead-letter handling. Read the *Limitations* section in
[README.md](README.md) before considering any of it for real traffic.

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

**Request size, speed and surface.** A request body over 1 MiB is refused with `413`, at
Caddy and again in the application (`MAX_REQUEST_BODY_BYTES`) for traffic that does not
pass through Caddy; the application counts bytes as they arrive, so an unauthenticated
body cannot grow the process past the cap. Caddy closes a connection whose headers take
longer than 10 s or whose body takes longer than 30 s, and drops idle connections after
2 minutes. Failed authentications are counted per client and answered `429` after
`RATE_LIMIT_AUTH_FAILURES` (default 20 a minute) before any database lookup, so guessing a
key costs the attacker the budget and costs the service no query. An event id must be
printable ASCII of 1 to 255 characters; anything else is `INVALID`, because the id travels
into a delivery header and into log lines. `/docs`, `/redoc` and `/openapi.json` are served
only in a declared local dev run and answer `404` everywhere else. A signed payload that is
replayed within Stripe's signature tolerance leaves one `DUPLICATE` or one `UNSUPPORTED`
row per event, not one per request (unique partial indexes, migrations 0010 and 0011).

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
which tenant it is being handed, and `X-Transaction-Id` for receiver deduplication. Delivery
is at-least-once: a crash after HTTP success but before the finish commit, or dispatch
that outlives its lease, can deliver an event again. The worker forwards generic JSON;
there is no PostHog adapter.

The worker selects eligible candidates per tenant, then ranks them round-robin before
applying the batch limit; the rotation continues across claims, so more backlogged
tenants than batch slots still each get a turn. Claims commit before HTTP; `claimed_until` keeps other workers
off a claimed row until the lease expires. Each result commits separately. Tenants are
delivered to concurrently on a bounded thread pool (`WORKER_DISPATCH_CONCURRENCY`),
rows of one tenant stay in order, and a tenant whose downstream is slow or dead holds one
slot, not the worker. A delivery has a total deadline (`DISPATCH_DEADLINE_SECONDS`, 15 s)
and follows no redirect, so a downstream can neither stall the worker indefinitely nor
steer it to another host with a `3xx`. Dispatch
failures back off for 30, 120, 600 and then 3,600 seconds, repeating the final delay
without an attempt limit. Backed-off and leased rows are excluded from selection. These
are availability controls: a tenant's failing backlog does not fill every later batch.
A stale failure can update only an undelivered row whose lease still matches its claim;
a late success can mark an undelivered row delivered even after another worker claims it.

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

**RLS governs rows, not process-wide state.** `/metrics` exposes counters and queue gauges
aggregated across tenants, plus `billing_secrets_info` for the operator's secrets. When
authentication is configured, only the operator key (`BILLING_API_KEY`) is accepted;
a tenant key gets 403, and missing or unknown keys get 401. With no keys configured
anywhere, the endpoint remains open in development mode. The test suite checks operator
access and tenant refusal through the application's real connection pool. A tenant's own queue
counts are available through `/ledger/summary`.

One limit remains: `rls_inert` is a
log line and a CI gate, not an alert: a deployment that quietly drops
`APP_DATABASE_URL` reverts isolation to the application's own predicates, and nothing
pages anyone. Grep for it after any deploy that touches database configuration.

**Not attempted, within tenancy.** There is no cross-tenant ledger or DLQ API credential;
the operator key grants aggregate `/metrics` access. No authentication beyond a single shared API key per tenant — no
user accounts, no roles, no rotation flow. No audit log of who read operational data. No
encryption at rest beyond whatever PostgreSQL is configured to provide. No GDPR/LGPD
erasure path; the ledger is deliberately append-only and resolving that conflict needs a
retention policy decided first, not code.

**Dev defaults are dev defaults, and the service enforces it.** `.env.example` ships
working placeholder values so the stack starts on a clean clone. Every one of them is a
working credential to anyone who has read this repository: the placeholder signing secret
verifies signatures, so anyone can forge webhooks into the default tenant; the default
API key opens the operational endpoints; migration 0006's `billing_app` password opens
the database.

The billing service therefore refuses to start while it would serve traffic on any of
them — a placeholder, empty or non-`whsec_` signing secret (in the environment or on any
active org row), the default API key, an API key under 32 characters, no API key at all,
or the `billing_app` bootstrap password — unless two declarations both hold:

- `BILLING_ENV=dev` — the operator declared a development run. Unset means strict.
- `INGRESS_BIND_ADDR` is a loopback address — docker-compose publishes Caddy's ports on
  this address, and passes the same value to the service. Publishing the ingress beyond
  the machine revokes dev mode in the same edit.

docker-compose defaults `BILLING_ENV` to `dev` and the ingress to `127.0.0.1`, so a fresh
clone starts and nothing beyond the machine can reach it. The refusal names each
offending setting, never its value (`docker compose logs billing | grep demo_credentials`).
The start-up warnings about placeholder credentials come from the API process only; the
worker, which imports the same module but serves no HTTP, does not print them.

Known limit: the check reads `INGRESS_BIND_ADDR`, not the address uvicorn itself binds.
Run outside compose, `BILLING_ENV=dev uvicorn ledger:app --host 0.0.0.0` passes it and
serves the demo credentials to the network. Declaring dev is a statement the operator
makes; the service cannot see every way a port can be published.

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
