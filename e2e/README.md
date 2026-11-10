# End-to-end failure harness

Runs the full stack (Caddy, API, worker, PostgreSQL) with a real downstream receiver,
so the pipeline can be loaded, interrupted and attacked from outside, the way it fails
in production. The unit-level suite (`test_ledger.py`) proves each mechanism; this
harness checks that they hold together.

Everything runs under a separate Compose project (`billing-e2e`), with its own
volumes and freshly generated secrets. It uses the same host ports as the normal stack
(80, 443, 8000, 5432), so stop that one first.

## Files

| File | Purpose |
|---|---|
| `make_env.sh` | Writes `e2e/e2e.env` with random secrets (git-ignored) |
| `dc.sh` | `docker compose` for the harness: project name, override, env file |
| `compose.override.yml` | Adds the `downstream` receiver and points the worker at it |
| `downstream.py` | The receiver: logs every delivery; can fail, stall, trickle or redirect |
| `loadgen.py` | Sends signed `invoice.paid` events from N concurrent clients |
| `check_run.py` | After a run: every event answered 200 is in the ledger once and was delivered |

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
