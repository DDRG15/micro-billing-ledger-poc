#!/usr/bin/env python3
"""
send_test_webhook.py — drive the running stack with correctly signed webhooks.

Standard library only. No venv, no pip install, no Stripe account.

WHY THIS EXISTS
    POST /webhook/stripe verifies an HMAC-SHA256 signature on every request, including
    when STRIPE_WEBHOOK_SECRET is still the placeholder from .env.example. That is
    deliberate (see the comment above _PLACEHOLDER_SECRET in ledger.py): an endpoint
    that accepts unsigned requests whenever it is misconfigured is worse than one that
    rejects everything, because the failure is silent.

    The consequence is that a plain `curl -d '{...}'` gets 400, which makes the system
    look broken to anyone trying it for the first time. The fix is not to weaken
    verification for demos — it is to sign the request properly. This script computes
    the same signature Stripe would, using whatever secret the stack is actually
    running with, so the happy path works out of the box AND the signature check stays
    on and observably working.

USAGE
    python send_test_webhook.py                # one valid invoice.paid  -> POSTED
    python send_test_webhook.py --duplicate    # send it twice           -> POSTED, DLQ_DUPLICATE
    python send_test_webhook.py --invalid      # bad currency            -> DLQ_INVALID
    python send_test_webhook.py --failed       # failed invoice          -> VOID (amount_due)
    python send_test_webhook.py --summary      # GET /ledger/summary with the API key
    python send_test_webhook.py --metrics      # GET /metrics with the API key
    python send_test_webhook.py --all          # the full tour, in order

    --url https://localhost   # go through Caddy instead of the direct port
                              # (self-signed certificate is not verified)
"""

import argparse
import hashlib
import hmac
import json
import os
import ssl
import time
import urllib.error
import urllib.request

DEFAULT_URL = "http://localhost:8000"
PLACEHOLDER_SECRET = "whsec_YOUR_SECRET_HERE"


def load_env(path: str = ".env") -> dict:
    """Read KEY=VALUE lines from .env if it exists.

    The stack runs with compose's built-in defaults when .env is absent, so this
    falls back to exactly those same defaults — otherwise the script would sign with
    one secret while the server verified with another, and the 400 would look like a
    bug in the server rather than a mismatch in the caller.
    """
    values = {}
    if os.path.exists(path):
        with open(path, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    return values


ENV = load_env()


def config(name: str, fallback: str) -> str:
    """Environment wins, then .env, then the compose default."""
    return os.environ.get(name) or ENV.get(name) or fallback


SECRET = config("STRIPE_WEBHOOK_SECRET", PLACEHOLDER_SECRET)
API_KEY = config("BILLING_API_KEY", "dev_key_change_in_production")

# Self-signed certificate when CADDY_DOMAIN=localhost — expected, so do not verify.
_INSECURE = ssl.create_default_context()
_INSECURE.check_hostname = False
_INSECURE.verify_mode = ssl.CERT_NONE


def stripe_signature(payload: str, secret: str, timestamp: int) -> str:
    """Build a Stripe-Signature header value.

    Stripe signs the string "<timestamp>.<raw body>", not the body alone, so the
    timestamp cannot be swapped out to replay an old capture.
    """
    signed = f"{timestamp}.{payload}".encode()
    digest = hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
    return f"t={timestamp},v1={digest}"


def post_webhook(base_url: str, event: dict, label: str) -> None:
    body = json.dumps(event)
    timestamp = int(time.time())
    request = urllib.request.Request(
        f"{base_url}/webhook/stripe",
        data=body.encode(),
        headers={
            "Content-Type": "application/json",
            "Stripe-Signature": stripe_signature(body, SECRET, timestamp),
        },
        method="POST",
    )
    _send(request, label)


def get_endpoint(base_url: str, path: str, label: str, raw: bool = False) -> None:
    request = urllib.request.Request(
        f"{base_url}{path}",
        headers={"X-Api-Key": API_KEY},
        method="GET",
    )
    _send(request, label, raw=raw)


def _send(request: urllib.request.Request, label: str, raw: bool = False) -> None:
    context = _INSECURE if request.full_url.startswith("https") else None
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:  # nosec B310 — URL is the operator-supplied --url flag, not user input
            payload = response.read().decode()
            status = response.status
    except urllib.error.HTTPError as exc:          # 4xx/5xx still carry a useful body
        payload, status = exc.read().decode(), exc.code
    except urllib.error.URLError as exc:
        print(f"  {label}: cannot reach {request.full_url} - {exc.reason}")
        print("  Is the stack up? Try: docker compose up -d")
        return

    if raw:
        interesting = [
            line for line in payload.splitlines()
            if line and not line.startswith("#")
            and line.split("{")[0].split(" ")[0] in {
                "webhook_events_total", "dlq_depth", "outbox_pending",
            }
        ]
        print(f"  {label}: HTTP {status}")
        for line in interesting[:8]:
            print(f"      {line}")
        return

    try:
        payload = json.dumps(json.loads(payload), separators=(",", ":"))
    except json.JSONDecodeError:
        pass
    print(f"  {label}: HTTP {status}  {payload}")


def valid_event(event_id: str) -> dict:
    return {
        "id": event_id,
        "type": "invoice.paid",
        "data": {"object": {
            "id": f"in_{event_id}", "customer": "cus_abc123", "amount_paid": 4900,
            "currency": "usd",
        }},
        "request": {"idempotency_key": f"idem_{event_id}"},
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--url", default=DEFAULT_URL,
                        help=f"base URL (default {DEFAULT_URL}; use https://localhost for Caddy)")
    parser.add_argument("--duplicate", action="store_true", help="send the same event twice")
    parser.add_argument("--invalid", action="store_true", help="send an event that fails validation")
    parser.add_argument("--failed", action="store_true", help="send a failed invoice (amount_due)")
    parser.add_argument("--summary", action="store_true", help="GET /ledger/summary")
    parser.add_argument("--metrics", action="store_true", help="GET /metrics")
    parser.add_argument("--all", action="store_true", help="run everything in order")
    args = parser.parse_args()

    if SECRET == PLACEHOLDER_SECRET:
        print("Signing with the placeholder secret from .env.example - fine for a local")
        print("demo. Set STRIPE_WEBHOOK_SECRET to your real signing secret before live")
        print("traffic; this script picks it up automatically.\n")

    suffix = int(time.time())
    ran_any = False

    if args.all or not any([args.duplicate, args.invalid, args.failed,
                            args.summary, args.metrics]):
        print("Valid invoice.paid:")
        post_webhook(args.url, valid_event(f"evt_demo_{suffix}"), "POSTED expected")
        ran_any = True

    if args.all or args.duplicate:
        print("\nSame event twice - the idempotency guard fires on the second:")
        event = valid_event(f"evt_dup_{suffix}")
        post_webhook(args.url, event, "1st -> POSTED")
        post_webhook(args.url, event, "2nd -> DLQ_DUPLICATE")
        ran_any = True

    if args.all or args.invalid:
        print("\nInvalid payload (currency 'xx' is not a real ISO 4217 code):")
        bad = valid_event(f"evt_bad_{suffix}")
        bad["data"]["object"]["currency"] = "xx"
        post_webhook(args.url, bad, "DLQ_INVALID expected")
        ran_any = True

    if args.all or args.failed:
        print("\nFailed invoice - amount lives in amount_due, not amount_paid:")
        post_webhook(args.url, {
            "id": f"evt_failed_{suffix}",
            "type": "invoice.payment_failed",
            "data": {"object": {"id": f"in_failed_{suffix}", "customer": "cus_abc123",
                                "amount_paid": 0, "amount_due": 7500, "currency": "usd"}},
        }, "VOID expected, recorded at 7500")
        ran_any = True

    if args.all or args.summary:
        print("\nLedger summary:")
        get_endpoint(args.url, "/ledger/summary", "summary")
        ran_any = True

    if args.all or args.metrics:
        print("\nPrometheus metrics:")
        get_endpoint(args.url, "/metrics", "metrics", raw=True)
        ran_any = True

    if ran_any:
        print("\nInspect the dead letter queue:")
        print(f'  curl -H "X-Api-Key: {API_KEY}" "{args.url}/dlq/entries?limit=10"')


if __name__ == "__main__":
    main()
