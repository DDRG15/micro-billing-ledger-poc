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

**Not attempted.** No multi-tenancy, so there is no tenant isolation boundary to breach
or defend. No authentication beyond a single shared API key — no user accounts, no
roles, no rotation. No audit log of who read operational data. No encryption at rest
beyond whatever PostgreSQL is configured to provide. No GDPR/LGPD erasure path; the
ledger is deliberately append-only and resolving that conflict needs a retention policy
decided first, not code.

**Dev defaults are dev defaults.** `.env.example` ships working placeholder values so
the stack starts on a clean clone. Every one marked `*** CHANGE ***` must be replaced
before anything is reachable from a network. `BILLING_API_KEY` left empty disables
authentication on the operational endpoints entirely; the application logs a warning at
startup when that happens, and that warning is not decorative.

## Dependencies

Production dependencies are pinned to exact versions in `requirements.txt`. Pinning
prevents surprise breakage, but it also freezes vulnerabilities in place: a pinned
version stops receiving fixes the moment one is published. Transitive dependencies are
the harder case — they are not listed in the manifest, so a scanner watching this
repository may not flag them at all. `starlette` reached this project that way through
FastAPI's pin range. Check the resolved tree, not only the manifest.
