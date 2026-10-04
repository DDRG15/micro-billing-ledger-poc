#!/usr/bin/env python3
"""
benchmark.py — measure the three ingest paths, reproducibly.

WHY THIS EXISTS
    The throughput figures in README.md came from a single manual run that nobody could
    repeat. A number you cannot reproduce is a claim, not a measurement, and the first
    person to try it on different hardware finds out. This script re-measures every
    figure the README states, prints the environment it measured on, and is the only
    thing those numbers are allowed to come from.

WHAT IT MEASURES
    1. single-event  process_stripe_event() in a loop. One BEGIN + 2 INSERTs + COMMIT
                     per event: four synchronous round trips, so this is bounded by
                     network latency to PostgreSQL, not by CPU or by Pydantic.
    2. batch         process_stripe_event_batch(). execute_values collapses N inserts
                     into ceil(N/page_size) statements, so the same work costs a
                     fraction of the round trips.
    3. http          POST /webhook/stripe against a running service, signed exactly as
                     Stripe signs. This is the only end-to-end number: it includes
                     uvicorn, Pydantic, HMAC verification, the connection pool and the
                     rate limiter. The README had no figure for this path at all.

    The gap between (1) and (3) is the HTTP stack's cost. The gap between (1) and (2) is
    why the batch endpoint exists.

RATE LIMITING
    /webhook/stripe allows 100/minute by default, which would make an HTTP benchmark
    measure the limiter rather than the service. Start the target with a raised limit:

        RATE_LIMIT_WEBHOOK=100000/minute uvicorn ledger:app --port 8000

    On the demo credentials the service refuses to start unless the run is declared
    dev (BILLING_ENV=dev, see .env.example), and it must run on the same _bench
    database this script resets.

    The script detects a 429 and tells you, rather than reporting a throughput number
    that is really just the limit.

USAGE
    python benchmark.py                       # all three, 2000 events each
    python benchmark.py --events 5000
    python benchmark.py --skip-http           # DB paths only, no running service needed
    python benchmark.py --url http://localhost:8000
"""

import argparse
import json
import os
import platform
import random
import statistics
import string
import sys
import time
from concurrent.futures import ThreadPoolExecutor

import disposable_db  # noqa: E402

# Every mode below TRUNCATEs ledger, outbox and dlq first — including --url, which resets
#     the database at DATABASE_URL before driving the server. Refuse anything not declared
#     disposable before ledger opens its pool. In --url mode, start the server on the same
#     _bench database, or the reset lands on one database and the load on another.
disposable_db.require_disposable_database(
    "postgresql://postgres:postgres@localhost:5432/billing_bench")

import ledger as L  # noqa: E402  — after DATABASE_URL, which it reads at import


def fake_event(index: int) -> dict:
    """A structurally valid event with a unique id, so nothing is deduplicated away.

    Duplicates would measure the idempotency guard rejecting work, not the cost of
    doing it, and the resulting number would look better than the truth.
    """
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=18))
    return {
        "id":   f"evt_bench_{suffix}",
        "type": "invoice.paid",
        "data": {"object": {
            "customer":    f"cus_bench{index:07d}",
            "amount_paid": random.randint(100, 100_000),
            "currency":    "usd",
        }},
        "request": {"idempotency_key": f"idem_{suffix}"},
    }


def _reset(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE outbox, dlq, ledger RESTART IDENTITY")


def _work_connection():
    """The connection the measured paths run on — the same role the service uses.

    This matters to the number. Since Phase 13.7 the service connects as `billing_app`,
    a non-superuser, so every transaction it opens also issues SET LOCAL app.org_id and
    every statement is filtered through the row-level security policies on ledger, outbox
    and dlq. Measuring on the owner connection instead skips both: a superuser bypasses
    RLS entirely, so the figure would describe a configuration nobody deploys and would
    be optimistic by exactly the amount the isolation costs.

    Falls back to the owner DSN when APP_DATABASE_URL is unset, which is also what the
    application does — and the banner says which one was used, so the output is never
    ambiguous about what it measured.
    """
    import psycopg2
    conn = psycopg2.connect(L.APP_DATABASE_URL)
    conn.autocommit = True          # _tx opens transactions with a literal BEGIN
    return conn


def bench_single(work, owner, n: int) -> dict:
    """Measured on `work`; reset and verified on `owner`.

    The count cannot come from `work`: under default-deny, a COUNT with no scope
    established sees zero rows and would report that nothing was written.
    """
    events = [fake_event(i) for i in range(n)]
    _reset(owner)
    start = time.perf_counter()
    for event in events:
        L.process_stripe_event(work, event)
    elapsed = time.perf_counter() - start
    with owner.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ledger")
        written = cur.fetchone()[0]
    return {"events": n, "elapsed": elapsed, "tps": n / elapsed, "written": written}


def bench_batch(work, owner, n: int) -> dict:
    events = [fake_event(i) for i in range(n)]
    _reset(owner)
    start = time.perf_counter()
    L.process_stripe_event_batch(work, events)
    elapsed = time.perf_counter() - start
    with owner.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM ledger")
        written = cur.fetchone()[0]
    return {"events": n, "elapsed": elapsed, "tps": n / elapsed, "written": written}


def bench_http(url: str, n: int, concurrency: int, secret: str, timeout: float = 120.0) -> dict:
    """Drive POST /webhook/stripe with connection reuse.

    Uses httpx with a keep-alive pool rather than urllib. That is not a style choice —
    urllib opens a fresh TCP connection per request, and on Windows a few concurrent
    threads doing that hit connection-establishment limits: measured latency snapped to
    ~4.2s and ~8.4s, which are the SYN retransmission timers, and throughput "collapsed"
    from 15 TPS at 2 clients to 1 TPS at 4. None of that came from the service. Any load
    generator worth trusting reuses connections, because real clients do.
    """
    import hashlib
    import hmac

    import httpx

    events = [fake_event(i) for i in range(n)]
    limits = httpx.Limits(max_connections=concurrency,
                          max_keepalive_connections=concurrency)

    def post(client: "httpx.Client", event: dict):
        body = json.dumps(event).encode()
        ts = int(time.time())
        mac = hmac.new(secret.encode(), f"{ts}.".encode() + body, hashlib.sha256).hexdigest()
        t0 = time.perf_counter()
        try:
            resp = client.post(
                "/webhook/stripe", content=body,
                headers={"Content-Type": "application/json",
                         "Stripe-Signature": f"t={ts},v1={mac}"},
            )
            return resp.status_code, (time.perf_counter() - t0) * 1000
        except Exception as exc:  # noqa: BLE001 — a timeout is a result, not a crash
            return type(exc).__name__, (time.perf_counter() - t0) * 1000

    with httpx.Client(base_url=url, limits=limits, timeout=timeout) as client:
        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            results = list(pool.map(lambda ev: post(client, ev), events))
        elapsed = time.perf_counter() - start

    codes: dict = {}
    for code, _ in results:
        codes[code] = codes.get(code, 0) + 1
    lat = sorted(d for _, d in results)
    return {
        "events": n, "elapsed": elapsed, "tps": n / elapsed, "codes": codes,
        "concurrency": concurrency,
        "p50": statistics.median(lat),
        "p95": lat[int(0.95 * len(lat)) - 1],
        "p99": lat[int(0.99 * len(lat)) - 1],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--events", type=int, default=2000)
    parser.add_argument("--http-events", type=int, default=600,
                        help="separate count for the HTTP path, which is far slower "
                             "than the DB paths (default 600)")
    parser.add_argument("--url", default="http://localhost:8000")
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--skip-http", action="store_true")
    args = parser.parse_args()

    print("=" * 74)
    print("  Micro-Billing-Ledger — ingest benchmark")
    print("=" * 74)
    print(f"  python      {platform.python_version()} on {platform.system()} {platform.release()}")
    print(f"  database    {L.DATABASE_URL.rsplit('@', 1)[-1]}")
    print(f"  events      {args.events:,} per DB path, {args.http_events:,} over HTTP")
    print("  Numbers are specific to this machine and this database. Re-run rather")
    print("  than quoting someone else's figures.")

    conn = L._bootstrap()          # owner: DDL, TRUNCATE and the row counts
    work = _work_connection()      # the role the service actually runs as
    _rls = L._rls_status()
    print(f"  db role     {_rls['db_user']}  "
          f"({'RLS enforced' if _rls['enforced'] else 'RLS INERT — superuser bypasses it'})")
    if not _rls["enforced"]:
        print("              Set APP_DATABASE_URL to the billing_app DSN to measure the")
        print("              configuration that is actually deployed. Without it these")
        print("              figures omit the cost of tenant isolation.")
    print()

    single = bench_single(work, conn, args.events)
    print(f"  single-event  {single['elapsed']:7.2f}s  ->  {single['tps']:9,.0f} TPS   "
          f"({single['written']:,} rows)")

    batch = bench_batch(work, conn, args.events)
    print(f"  batch         {batch['elapsed']:7.2f}s  ->  {batch['tps']:9,.0f} TPS   "
          f"({batch['written']:,} rows)")
    print(f"                batch is {batch['tps'] / single['tps']:.0f}x the single-event path")

    http = None
    if not args.skip_http:
        secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "whsec_YOUR_SECRET_HERE")
        _reset(conn)
        http = bench_http(args.url, args.http_events, args.concurrency, secret)
        ok = http["codes"].get(200, 0)
        print(f"  http e2e      {http['elapsed']:7.2f}s  ->  {http['tps']:9,.0f} TPS   "
              f"({ok:,}/{http['events']:,} 200s, {http['concurrency']} concurrent)")
        print(f"                latency ms  p50={http['p50']:.1f}  p95={http['p95']:.1f}  "
              f"p99={http['p99']:.1f}")
        if http["codes"].get(429):
            print(f"                {http['codes'][429]:,} requests were RATE LIMITED — this figure")
            print("                measures the limiter, not the service. Restart the target with")
            print("                RATE_LIMIT_WEBHOOK=100000/minute and run again.")
        if any(not isinstance(c, int) for c in http["codes"]):
            print(f"                transport errors: "
                  f"{ {k: v for k, v in http['codes'].items() if not isinstance(k, int)} }")

    _reset(conn)
    work.close()
    conn.close()

    print()
    print("  Why single-event is bounded by round trips, not CPU:")
    print("    BEGIN + INSERT ledger + INSERT outbox + COMMIT = 4 synchronous round trips")
    print("    per event. execute_values collapses the batch path into")
    print(f"    ceil(N/1000) statements, which is where the {batch['tps'] / single['tps']:.0f}x comes from.")
    if http:
        print("    HTTP adds uvicorn, Pydantic, HMAC and the pool on top of single-event:")
        print(f"    {single['tps']:,.0f} -> {http['tps']:,.0f} TPS at {http['concurrency']} concurrent clients.")
    print("=" * 74)
    return 0


if __name__ == "__main__":
    sys.exit(main())
