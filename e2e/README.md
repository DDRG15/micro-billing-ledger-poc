# End-to-end failure harness

Runs the full stack (Caddy, API, worker, PostgreSQL) with a real downstream receiver,
so the pipeline can be loaded, interrupted and attacked from outside, the way it fails
in production. The unit-level suite (`test_ledger.py`) proves each mechanism; this
harness checks that they hold together.

Everything runs under a separate Compose project (`billing-e2e`), with its own
volumes and freshly generated secrets. `make_env.sh` sets `BILLING_ENV=production`, so the
API runs with the demo-credential refusal active, as a real deployment does. The harness
uses the same host ports as the normal stack (80, 443, 8000, 5432), so stop that one first.

## Files

| File | Purpose |
|---|---|
| `make_env.sh` | Writes `e2e/e2e.env` with random secrets (git-ignored) |
| `dc.sh` | `docker compose` for the harness: project name, override, env file |
| `compose.override.yml` | Adds the `downstream` receiver and points the worker at it |
| `downstream.py` | The receiver: logs every delivery; can fail, stall, trickle or redirect |
| `loadgen.py` | Sends signed `invoice.paid` events from N concurrent clients |
| `check_run.py` | After a run: counts events answered 200 but missing from the ledger, events in the ledger more than once, and deliveries received downstream (with duplicates) |
| `recv/` | Created by `make_env.sh`, git-ignored: the receiver's `mode` file and its `received.log` |

## Run it

```bash
./e2e/make_env.sh
./e2e/dc.sh up -d --build --wait

python e2e/loadgen.py -n 50 -c 4 --prefix evt_smoke --url https://localhost/webhook/stripe --out smoke.jsonl
python e2e/check_run.py smoke.jsonl --prefix evt_smoke

./e2e/dc.sh down -v        # remove containers and volumes
```

## Scenarios

**Saturation.** The webhook route allows 100 requests per minute per client by
default, which a load test hits immediately. To measure the service rather than the
limiter, raise it for the run and recreate the API:

```bash
printf 'RATE_LIMIT_WEBHOOK=10000000/minute\n' >> e2e/e2e.env
./e2e/dc.sh up -d --wait billing
python e2e/loadgen.py -n 20000 -c 64 --prefix evt_sat --url https://localhost/webhook/stripe --out sat.jsonl
python e2e/check_run.py sat.jsonl --prefix evt_sat
```

Remove the line again afterwards; a rate-limit test run with it still in place
measures nothing.

**Database loss under load.** Start a run in one terminal and, a few seconds in,
`docker kill billing-e2e-postgres-1`, then `docker start billing-e2e-postgres-1`.
Expected: requests during the outage get `503` with `Retry-After` (Stripe retries
those), never `500`, and `check_run.py` reports no event answered 200 that is missing
from the ledger.

**Downstream failures.** `echo fail > e2e/recv/mode` (503s, rows back off with the
error in `outbox.last_error`), `echo slow:5 > e2e/recv/mode`, `echo ok > e2e/recv/mode`.
Per tenant, set `org.downstream_url` to `http://downstream:9000/redirect` (a 302 must
count as a failed delivery) or `http://downstream:9000/drip` (a response that never
finishes: cut at the 15 s dispatch deadline without holding back other tenants).

**Hostile input.** Unsigned bodies over 1 MiB get `413` without the API's memory
growing; a client that sends its headers slowly is cut after 10 s by Caddy; malformed
signatures, keys and tenant ids get the same `4xx` for every tenant.

## Results from the last full run

Measured on one machine (12 cores, 14 GB RAM) against the default compose limits
(API container: 0.5 CPU, 256 MB):

| Scenario | Result |
|---|---|
| 20,000 signed webhooks, 64 concurrent clients | all `200`; each in the ledger exactly once; no duplicate deliveries |
| 10,000 signed webhooks, 200 concurrent clients | all `200`; p99 under 2 s |
| PostgreSQL killed for 6 s under load | only `503` during the outage; no acknowledged event lost |
| Unsigned 100–300 MB bodies, direct and through Caddy | `413` or cut upload; no restart; memory flat |
| Slow-header client through Caddy | connection closed after ~10 s |

Numbers depend on the machine; the properties (no lost or duplicated events, `503`
rather than `500`, bounded memory) are what the run checks.

## Stripe sandbox reconciliation

`stripe_sandbox.py` runs the service against a real Stripe sandbox (test mode only; it
never passes `--live` and aborts on a live-mode event) and checks that the books match
what Stripe says it emitted.

Requirements: the Stripe CLI logged in to a sandbox, Docker, and the normal stack
running with `STRIPE_WEBHOOK_SECRET` set to the CLI's signing secret
(`stripe listen --print-secret`). `stripe listen` signs every forwarded event with
that one secret, and it reaches the service from a single address, so raise the webhook
limit for the run (`RATE_LIMIT_WEBHOOK=100000/minute`); a burst above the default 100
per minute is refused, and the script counts any non-200 answer as a failure.

```bash
cp .env.example .env
export STRIPE_WEBHOOK_SECRET="$(stripe listen --print-secret | grep -oE 'whsec_[A-Za-z0-9]+')"
RATE_LIMIT_WEBHOOK=100000/minute docker compose up -d --build --wait
stripe listen --all-snapshot --forward-to localhost:8000/webhook/stripe > listen.log 2>&1 &

python e2e/stripe_sandbox.py --listen-log listen.log --out-dir evidence
```

`--stripe-cmd` (or `STRIPE_CLI`) replaces `stripe`, for a containerised CLI;
`--psql-cmd` replaces `docker compose exec -T postgres psql -U postgres -d billing`.
Run it from the directory holding `docker-compose.yml`. The listener log can also come
from a command (`--listen-log-cmd "docker logs <container>"`). The exit status is 0
only when every control passes. The evidence directory holds the report, the masked
listener log and the Stripe events the run compared against; nothing in it is a secret.

| Phase | What it does |
|---|---|
| 1. Volume | `stripe trigger` over about 100 event types, four at a time (lowered if Stripe rate-limits), until more than `--target-events` (default 1000) events were forwarded |
| Catch-up | every event Stripe emitted that never reached the service is resent, up to `--catchup-rounds` (default 5) rounds |
| 2. Resends | `stripe events resend` for `--resends` (default 60) delivered events, `invoice.paid` and subscription events included, then the same catch-up |
| 3. Controls | the checks below |

The catch-up exists because `stripe listen` is a development tunnel. Under a burst of about
ten events a second it dropped 329 of 1,912 events and never retried them, while the
service answered every request it received. Stripe retries a registered webhook endpoint
on its own; the tunnel does not, so the script does it, and the report counts what the
listener dropped (`listener_catch_up`). Stopping the listener for 25 seconds mid-run
dropped 83 events; all 83 were resent in one round and every control passed.

Some `stripe trigger` fixtures (`payment_intent.*`, `charge.succeeded`, `setup_intent.*`,
`quote.*`) are refused by the sandbox at API version `2026-10-28`, because they use
endpoints Stripe has retired; Stripe may email the account about those calls. The events
still arrive as side effects of the invoice and subscription triggers. `--types` limits the
run to fixtures that work.

| Control | Pass condition |
|---|---|
| a | every forwarded event has exactly one row, in `ledger` or in `dlq` (not counting `DUPLICATE` rows) |
| b | Stripe's count of `invoice.paid`, `invoice.payment_failed` and `customer.subscription.{created,updated,deleted}` since the start equals the ledger's POSTED, VOID and LIFECYCLE rows, and the event id sets are equal |
| c | `amount_paid` of the `invoice.paid` events equals the POSTED `amount_cents`, and `amount_due` of the failed payments equals the VOID `amount_cents`, to the cent |
| d | every delivery the listener logged was answered 200 |
| e | no pending outbox rows, and no ledger row without an outbox row |
| g | every event Stripe emitted in the window reached the service, directly or through the catch-up |
| r | each resend was redelivered, answered 200, and left the ledger unchanged |
