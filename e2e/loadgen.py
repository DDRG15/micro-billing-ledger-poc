"""Signed Stripe webhook load generator for the end-to-end harness.

Sends N invoice.paid events, signed the way Stripe signs them, from C concurrent
clients, and prints throughput, latency percentiles and the count of each HTTP status
and ledger outcome. With --out it also writes one JSON line per request, which is how
a run is checked for lost acknowledgements: every event answered 200 must be in the
ledger exactly once.

    python e2e/loadgen.py -n 20000 -c 64 --url https://localhost/webhook/stripe --out run.jsonl

The signing secret is read from e2e/e2e.env (created by e2e/make_env.sh), or --secret.
"""
import argparse
import hashlib
import hmac
import http.client
import json
import os
import ssl
import threading
import time
import urllib.parse
from collections import Counter


def _load_env(path: str) -> dict:
    env = {}
    with open(path) as f:
        for line in f:
            if "=" in line and not line.lstrip().startswith("#"):
                key, value = line.strip().split("=", 1)
                env[key] = value
    return env


def _event(prefix: str, i: int) -> dict:
    return {
        "id": f"{prefix}_{i:07d}", "object": "event", "type": "invoice.paid",
        "created": int(time.time()), "livemode": False, "api_version": "2024-06-20",
        "data": {"object": {"id": f"in_{prefix}_{i}", "object": "invoice",
                            "customer": "cus_load0001", "amount_paid": 1000 + i % 97,
                            "amount_due": 1000 + i % 97, "currency": "usd"}},
        "request": {"id": None, "idempotency_key": None},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--url", default="http://127.0.0.1:8000/webhook/stripe")
    parser.add_argument("-n", type=int, default=1000, help="events to send")
    parser.add_argument("-c", type=int, default=16, help="concurrent clients")
    parser.add_argument("--prefix", default="evt_load", help="event id prefix (unique per run)")
    parser.add_argument("--secret", default=None, help="signing secret (default: e2e.env)")
    parser.add_argument("--dup", type=int, default=1, help="send each event this many times")
    parser.add_argument("--xff", action="store_true", help="send a spoofed X-Forwarded-For")
    parser.add_argument("--out", default=None, help="write one JSON line per request here")
    args = parser.parse_args()

    secret = args.secret or _load_env(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "e2e.env"))["STRIPE_WEBHOOK_SECRET"]
    url = urllib.parse.urlparse(args.url)
    tls = ssl._create_unverified_context()  # nosec B323 — the local stack uses Caddy's self-signed certificate

    def connect() -> http.client.HTTPConnection:
        if url.scheme == "https":
            return http.client.HTTPSConnection(url.hostname, url.port or 443, context=tls, timeout=30)
        return http.client.HTTPConnection(url.hostname, url.port or 80, timeout=30)

    jobs = [i for i in range(args.n) for _ in range(args.dup)]
    lock, next_job, results = threading.Lock(), [0], []

    def client(k: int) -> None:
        conn = connect()
        while True:
            with lock:
                if next_job[0] >= len(jobs):
                    return
                i = jobs[next_job[0]]
                next_job[0] += 1
            body = json.dumps(_event(args.prefix, i))
            ts = int(time.time())
            sig = hmac.new(secret.encode(), f"{ts}.{body}".encode(), hashlib.sha256).hexdigest()
            headers = {"Content-Type": "application/json", "Stripe-Signature": f"t={ts},v1={sig}"}
            if args.xff:
                headers["X-Forwarded-For"] = f"10.{k}.{i % 250}.{i % 199}"
            t0 = time.perf_counter()
            try:
                conn.request("POST", url.path, body, headers)
                resp = conn.getresponse()
                data = resp.read()
                status = resp.status
                outcome = json.loads(data).get("outcome") if data[:1] == b"{" else None
            except Exception as exc:  # a reset or timeout is a result to count, not a crash
                status, outcome = f"EXC:{type(exc).__name__}", None
                conn.close()
                conn = connect()
            results.append({"i": i, "st": status, "outcome": outcome,
                            "ms": (time.perf_counter() - t0) * 1000})

    threads = [threading.Thread(target=client, args=(k,)) for k in range(args.c)]
    started = time.perf_counter()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.perf_counter() - started

    if args.out:
        with open(args.out, "w") as f:
            for row in results:
                f.write(json.dumps(row) + "\n")
    latencies = sorted(r["ms"] for r in results)

    def pct(q: float) -> float:
        return latencies[min(len(latencies) - 1, int(len(latencies) * q))]

    print(f"sent={len(results)} in {elapsed:.1f}s = {len(results) / elapsed:.0f} req/s | "
          f"p50={pct(.5):.0f}ms p95={pct(.95):.0f}ms p99={pct(.99):.0f}ms max={latencies[-1]:.0f}ms")
    print("status:", dict(Counter(str(r["st"]) for r in results)))
    print("outcome:", dict(Counter(str(r["outcome"]) for r in results)))


if __name__ == "__main__":
    main()
