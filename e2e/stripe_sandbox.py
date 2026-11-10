"""Reconcile the billing service against a real Stripe sandbox.

    python e2e/stripe_sandbox.py --listen-log-cmd "docker logs pbpoc-stripe-listen"

A running `stripe listen --all-snapshot --forward-to <service>/webhook/stripe` forwards
every event the sandbox emits to the service. This script drives the sandbox and then
checks the books, in three phases:

  1. Volume and variety. `stripe trigger` over many event types until more than
     --target-events events were forwarded, four at a time by default.
  2. Real duplicates. `stripe events resend` for --resends events already delivered.
  3. Reconciliation. The service's database against what Stripe says it emitted: every
     forwarded event has exactly one row, event counts and money match to the cent, every
     delivery was answered 200, and the outbox is drained.

Exit status is 0 only when every control passes. Sandbox (test mode) only: the script
never passes --live and aborts if Stripe returns a live-mode event.
"""
import argparse
import concurrent.futures
import json
import os
import random
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from collections import Counter, defaultdict

# The five types the ledger records, in the order they are exercised first.
CORE_TYPES = (
    "invoice.paid",
    "invoice.payment_failed",
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
)

# Relative frequency of each core type in the volume phase (per round).
CORE_WEIGHTS = {
    "invoice.paid": 4,
    "invoice.payment_failed": 4,
    "customer.subscription.created": 4,
    "customer.subscription.updated": 2,
    "customer.subscription.deleted": 2,
}

# Every other fixture `stripe trigger --help` lists that makes sense in a sandbox. Types
# the sandbox refuses are dropped by the probe and reported. Left out on purpose:
# account.*, issuing_*, identity.*, test_helpers.*, reporting.*, payout/topup/transfer
# and balance events, which change the account itself or need products it lacks.
OTHER_TYPES = (
    "payment_intent.succeeded",
    "payment_intent.payment_failed",
    "payment_intent.created",
    "payment_intent.canceled",
    "payment_intent.requires_action",
    "payment_intent.processing",
    "payment_intent.amount_capturable_updated",
    "payment_intent.partially_funded",
    "charge.succeeded",
    "charge.failed",
    "charge.captured",
    "charge.refunded",
    "charge.refund.updated",
    "charge.updated",
    "charge.dispute.created",
    "charge.dispute.updated",
    "charge.dispute.closed",
    "checkout.session.completed",
    "checkout.session.expired",
    "checkout.session.async_payment_failed",
    "checkout.session.async_payment_succeeded",
    "customer.created",
    "customer.updated",
    "customer.deleted",
    "customer.discount.created",
    "customer.discount.updated",
    "customer.discount.deleted",
    "customer.tax_id.created",
    "customer.tax_id.deleted",
    "customer.source.created",
    "customer.source.updated",
    "customer.subscription.trial_will_end",
    "customer.subscription.paused",
    "invoice.created",
    "invoice.finalized",
    "invoice.payment_action_required",
    "invoice.payment_succeeded",
    "invoice.sent",
    "invoice.updated",
    "invoice.voided",
    "invoice.marked_uncollectible",
    "invoice.deleted",
    "invoiceitem.created",
    "invoiceitem.deleted",
    "credit_note.created",
    "credit_note.updated",
    "credit_note.voided",
    "coupon.created",
    "coupon.updated",
    "coupon.deleted",
    "plan.created",
    "plan.updated",
    "plan.deleted",
    "price.created",
    "price.updated",
    "product.created",
    "product.updated",
    "product.deleted",
    "promotion_code.created",
    "promotion_code.updated",
    "payment_method.attached",
    "payment_method.detached",
    "payment_method.updated",
    "payment_link.created",
    "payment_link.updated",
    "refund.created",
    "refund.updated",
    "setup_intent.created",
    "setup_intent.succeeded",
    "setup_intent.canceled",
    "setup_intent.requires_action",
    "setup_intent.setup_failed",
    "subscription_schedule.created",
    "subscription_schedule.updated",
    "subscription_schedule.canceled",
    "subscription_schedule.aborted",
    "subscription_schedule.completed",
    "subscription_schedule.expiring",
    "subscription_schedule.released",
    "tax_rate.created",
    "tax_rate.updated",
    "quote.created",
    "quote.finalized",
    "quote.accepted",
    "quote.canceled",
    "cash_balance.funds_available",
    "customer_cash_balance_transaction.created",
    "billing_portal.configuration.created",
    "billing_portal.configuration.updated",
    "billing_portal.session.created",
)

SUPPORTED_SUB_TYPES = (
    "customer.subscription.created",
    "customer.subscription.updated",
    "customer.subscription.deleted",
)

SECRET_RE = re.compile(r"\b(whsec|sk|rk|pk)_[A-Za-z0-9_]+")
RATE_LIMIT_RE = re.compile(r"rate_limit|rate limit|too many requests|\b429\b", re.I)
SENT_RE = re.compile(r"-->\s+(\S+)\s+\[(evt_[A-Za-z0-9]+)\]")
ANSWER_RE = re.compile(r"<--\s+\[(\d+)\]\s+POST\s+(\S+)\s+\[(evt_[A-Za-z0-9]+)\]")


def short_error(text):
    """The API error message if the CLI printed one, else the tail of its output."""
    m = re.search(r'"message":\s*"((?:[^"\\]|\\.)*)"', text)
    return mask(m.group(1) if m else text.strip()[-300:])[:300]


def mask(text):
    """Hide anything shaped like a Stripe secret before it is printed or stored."""
    return SECRET_RE.sub(r"\1_***", text)


class HarnessError(Exception):
    """A precondition failed; the run cannot produce a verdict."""


class Gate:
    """A concurrency limit that can be lowered while workers are running."""

    def __init__(self, limit):
        self.limit = limit
        self.active = 0
        self.cond = threading.Condition()

    def __enter__(self):
        with self.cond:
            while self.active >= self.limit:
                self.cond.wait()
            self.active += 1

    def __exit__(self, *exc):
        with self.cond:
            self.active -= 1
            self.cond.notify_all()

    def halve(self):
        with self.cond:
            before = self.limit
            self.limit = max(1, self.limit // 2)
            return before, self.limit


def parse_json(text):
    """The first JSON object in a CLI's stdout (the CLI may print a banner first)."""
    start = text.find("{")
    if start < 0:
        raise HarnessError("no JSON in CLI output: " + mask(text[:200]))
    try:
        return json.loads(text[start:])
    except ValueError as exc:
        raise HarnessError("unparseable JSON in CLI output: " + mask(str(exc))) from exc


def parse_listener(text):
    """Deliveries and answers the listener logged, per event id."""
    sent = Counter()
    types = {}
    answers = defaultdict(list)
    errors = 0
    ready = False
    for line in text.splitlines():
        if "Ready!" in line:
            ready = True
        m = SENT_RE.search(line)
        if m:
            sent[m.group(2)] += 1
            types[m.group(2)] = m.group(1)
            continue
        m = ANSWER_RE.search(line)
        if m:
            answers[m.group(3)].append(int(m.group(1)))
            continue
        if "ERROR" in line:
            errors += 1
    return {"sent": sent, "types": types, "answers": answers,
            "errors": errors, "ready": ready}


class Harness:
    def __init__(self, args):
        self.args = args
        self.stripe_cmd = [os.path.expanduser(t) for t in shlex.split(args.stripe_cmd)]
        self.psql_cmd = shlex.split(args.psql_cmd)
        self.gate = Gate(args.concurrency)
        self.lock = threading.Lock()
        self.trigger_log = []
        self.rate_limit_events = []
        self.yields = {}
        self.out_dir = args.out_dir
        os.makedirs(self.out_dir, exist_ok=True)
        self.t0 = time.time()
        self.start_epoch = int(self.t0)
        self.phase_seconds = {}
        self.baseline_ids = set()

    # ---- process helpers -------------------------------------------------

    def say(self, msg):
        print(f"[{time.time() - self.t0:7.1f}s] {mask(msg)}", file=sys.stderr, flush=True)

    def stripe(self, argv, timeout=180):
        try:
            p = subprocess.run(self.stripe_cmd + argv, capture_output=True, text=True,
                               timeout=timeout, check=False)
        except subprocess.TimeoutExpired:
            return 124, "", "timeout"
        except OSError as exc:
            raise HarnessError(f"cannot run the Stripe CLI command: {exc}") from exc
        return p.returncode, p.stdout, p.stderr

    def psql(self, sql, variables=None):
        argv = self.psql_cmd + ["-X", "-A", "-t", "-F", "\t", "-v", "ON_ERROR_STOP=1"]
        for key, value in (variables or {}).items():
            argv += ["-v", f"{key}={value}"]
        try:
            p = subprocess.run(argv, input=sql, capture_output=True, text=True,
                               timeout=120, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise HarnessError(f"cannot run the psql command: {exc}") from exc
        if p.returncode != 0:
            raise HarnessError("psql failed: " + mask(p.stderr[-400:]))
        return [line.split("\t") for line in p.stdout.splitlines() if line.strip()]

    def read_listener(self):
        if self.args.listen_log:
            try:
                with open(self.args.listen_log, encoding="utf-8", errors="replace") as f:
                    return mask(f.read())
            except OSError as exc:
                raise HarnessError(f"cannot read the listener log: {exc}") from exc
        argv = [os.path.expanduser(t) for t in shlex.split(self.args.listen_log_cmd)]
        try:
            p = subprocess.run(argv, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                               text=True, timeout=120, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise HarnessError(f"cannot run the listener log command: {exc}") from exc
        if p.returncode != 0:
            raise HarnessError("listener log command failed: " + mask(p.stdout[-300:]))
        return mask(p.stdout)

    # ---- Stripe API --------------------------------------------------------

    def stripe_events_since(self, since):
        """Every event Stripe holds with created >= since, paginated, slimmed down."""
        events, after = [], None
        while True:
            argv = ["events", "list", "--limit", "100", "-d", f"created[gte]={since}"]
            if after:
                argv += ["--starting-after", after]
            rc, out, err = self.stripe(argv)
            if rc != 0:
                raise HarnessError("events list failed: " + mask((err or out)[-300:]))
            page = parse_json(out)
            for e in page.get("data", []):
                if e.get("livemode"):
                    raise HarnessError("live-mode event returned; refusing to continue")
                obj = (e.get("data") or {}).get("object") or {}
                events.append({
                    "id": e["id"], "type": e["type"], "created": e["created"],
                    "amount_paid": obj.get("amount_paid"),
                    "amount_due": obj.get("amount_due"),
                })
            if not page.get("has_more") or not page.get("data"):
                return events
            after = page["data"][-1]["id"]

    # ---- triggers ----------------------------------------------------------

    def trigger(self, etype, phase):
        """Run one `stripe trigger`, backing off and lowering concurrency on rate limits."""
        began = time.time()
        attempts, limited, error, rc = 0, False, "", 0
        for attempt in range(1, 5):
            attempts = attempt
            with self.gate:
                rc, out, err = self.stripe(["trigger", etype], timeout=240)
            if rc == 0:
                error = ""
                break
            error = short_error(err + out)
            if RATE_LIMIT_RE.search(err + out):
                limited = True
                with self.lock:
                    before, after = self.gate.halve()
                    self.rate_limit_events.append({
                        "type": etype, "attempt": attempt,
                        "concurrency_before": before, "concurrency_after": after,
                    })
                time.sleep(min(30, 2 ** attempt) + random.uniform(0, 1))
                continue
            break
        result = {"type": etype, "phase": phase, "ok": rc == 0, "attempts": attempts,
                  "rate_limited": limited, "seconds": round(time.time() - began, 2),
                  "error": error}
        with self.lock:
            self.trigger_log.append(result)
        return result

    def probe(self, types):
        """Trigger each type once, one at a time, and measure how many events it emits."""
        seen = {}
        for etype in types:
            since = int(time.time()) - 3
            result = self.trigger(etype, "probe")
            time.sleep(1.0)
            try:
                fresh = [e for e in self.stripe_events_since(since) if e["id"] not in seen]
            except HarnessError as exc:
                self.say(f"probe: could not list events after {etype}: {exc}")
                fresh = []
            for e in fresh:
                seen[e["id"]] = e["type"]
            self.yields[etype] = len(fresh) if result["ok"] else 0
            flag = "ok" if result["ok"] else "FAILED"
            self.say(f"probe {etype}: {flag}, {self.yields[etype]} events")

    def forwarded_new(self):
        parsed = parse_listener(self.read_listener())
        return parsed, set(parsed["sent"]) - self.baseline_ids

    def volume(self, working_others):
        """Trigger core types (weighted) plus a rotation of the others until the goals hold."""
        args = self.args
        core_submitted = Counter()
        submitted = 0

        def schedule():
            rotation = 0
            while True:
                for etype in CORE_TYPES:
                    for _ in range(CORE_WEIGHTS[etype]):
                        yield etype
                for _ in range(3):
                    if working_others:
                        yield working_others[rotation % len(working_others)]
                        rotation += 1

        sched = schedule()
        inflight = set()
        parsed, new_ids = self.forwarded_new()

        def enough():
            return (len(new_ids) > args.target_events
                    and all(core_submitted[t] >= args.min_core_triggers for t in CORE_TYPES))

        last_report = 0.0
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
            while True:
                while (not enough() and len(inflight) < self.gate.limit
                       and submitted < args.max_triggers):
                    etype = next(sched)
                    if etype in CORE_TYPES:
                        core_submitted[etype] += 1
                    inflight.add(pool.submit(self.trigger, etype, "volume"))
                    submitted += 1
                if not inflight:
                    break
                done, inflight = concurrent.futures.wait(
                    inflight, timeout=2, return_when=concurrent.futures.FIRST_COMPLETED)
                parsed, new_ids = self.forwarded_new()
                if time.time() - last_report > 15:
                    last_report = time.time()
                    self.say(f"volume: {submitted} triggers submitted, {len(new_ids)} events "
                             f"forwarded, concurrency {self.gate.limit}")
                if submitted >= args.max_triggers and not inflight:
                    break
        return submitted

    def settle(self, label):
        """Wait until the listener log stops changing and every delivery has an answer."""
        deadline = time.time() + self.args.settle_timeout
        last_state, stable_since = None, time.time()
        while time.time() < deadline:
            parsed = parse_listener(self.read_listener())
            deliveries = sum(parsed["sent"].values())
            answered = sum(len(v) for v in parsed["answers"].values())
            state = (deliveries, answered)
            if state != last_state:
                last_state, stable_since = state, time.time()
            elif deliveries == answered and time.time() - stable_since >= self.args.settle_seconds:
                self.say(f"{label}: listener settled at {deliveries} deliveries")
                return True
            time.sleep(2)
        self.say(f"{label}: listener did not settle within {self.args.settle_timeout}s")
        return False

    # ---- database ------------------------------------------------------------

    def db_snapshot(self):
        org = {"org": self.args.org_id}
        ledger = {}
        for row in self.psql(
                "SELECT transaction_id, event_type, status, COALESCE(amount_cents::text, ''), "
                "COALESCE(payload::jsonb->>'created', '') FROM ledger WHERE org_id = :'org'",
                org):
            ledger[row[0]] = {"type": row[1], "status": row[2],
                              "amount": int(row[3]) if row[3] else None,
                              "created": int(row[4]) if row[4] else None}
        dlq = Counter()
        invalid = []
        for row in self.psql(
                "SELECT transaction_id, reason, status FROM dlq WHERE org_id = :'org'", org):
            dlq[(row[0], row[1])] += 1
            if row[1] == "INVALID":
                invalid.append(row[0])
        return {"ledger": ledger, "dlq": dlq, "invalid": invalid}

    def ledger_window_sums(self):
        rows = self.psql(
            "SELECT status, count(*), COALESCE(sum(amount_cents), 0) FROM ledger "
            "WHERE org_id = :'org' AND (payload::jsonb->>'created')::bigint >= :since "
            "GROUP BY status ORDER BY status",
            {"org": self.args.org_id, "since": self.start_epoch})
        return {r[0]: {"count": int(r[1]), "cents": int(r[2])} for r in rows}

    def outbox_state(self):
        org = {"org": self.args.org_id}
        pending = int(self.psql(
            "SELECT count(*) FROM outbox WHERE org_id = :'org' AND dispatched = 0", org)[0][0])
        orphans = int(self.psql(
            "SELECT count(*) FROM ledger l LEFT JOIN outbox o "
            "ON o.org_id = l.org_id AND o.transaction_id = l.transaction_id "
            "WHERE l.org_id = :'org' AND o.id IS NULL", org)[0][0])
        return pending, orphans

    # ---- phases ----------------------------------------------------------------

    def preflight(self):
        rc, out, err = self.stripe(["events", "list", "--limit", "1"])
        if rc != 0:
            raise HarnessError("Stripe CLI is not usable: " + mask((err or out)[-300:]))
        for e in parse_json(out).get("data", []):
            if e.get("livemode"):
                raise HarnessError("the CLI session returns live-mode data; use a test session")
        count = self.psql("SELECT count(*) FROM org WHERE org_id = :'org'",
                          {"org": self.args.org_id})
        if not count or count[0][0] != "1":
            raise HarnessError(f"org {self.args.org_id!r} does not exist in the database")
        parsed = parse_listener(self.read_listener())
        if not parsed["ready"]:
            raise HarnessError("the listener log has no 'Ready!' line; is `stripe listen` up?")
        self.baseline_ids = set(parsed["sent"])
        self.say(f"preflight ok; listener already holds {len(self.baseline_ids)} earlier events")

    def phase_trigger(self):
        began = time.time()
        others = list(self.args.types) if self.args.types else list(OTHER_TYPES)
        self.say(f"phase 1: probing {len(CORE_TYPES) + len(others)} event types")
        self.probe(list(CORE_TYPES) + others)
        failed = sorted(r["type"] for r in self.trigger_log
                        if r["phase"] == "probe" and not r["ok"])
        working_others = [t for t in others if t not in failed]
        self.say(f"phase 1: {len(failed)} types refused by the sandbox; volume run starts")
        submitted = self.volume(working_others)
        self.say(f"phase 1: {submitted} volume triggers submitted")
        self.settle("phase 1")
        self.phase_seconds["1_triggers"] = round(time.time() - began, 1)
        return failed

    def pick_resends(self, parsed, ids):
        """Resend targets: invoice.paid and subscription events first, then spread the rest."""
        by_type = defaultdict(list)
        for i in sorted(ids):
            by_type[parsed["types"][i]].append(i)
        quota = [("invoice.paid", 15), ("invoice.payment_failed", 5)]
        quota += [(t, 5) for t in SUPPORTED_SUB_TYPES]
        chosen = []
        for etype, n in quota:
            chosen += by_type.pop(etype, [])[:n]
        chosen = chosen[:self.args.resends]
        rest = [by_type[t] for t in sorted(by_type)]
        while len(chosen) < self.args.resends and any(rest):
            for bucket in rest:
                if bucket and len(chosen) < self.args.resends:
                    chosen.append(bucket.pop(0))
        return chosen

    def phase_resend(self, targets):
        began = time.time()
        results = {}

        def one(evt_id):
            with self.gate:
                rc, out, err = self.stripe(["events", "resend", evt_id])
            ok = rc == 0
            if ok:
                try:
                    ok = parse_json(out).get("id") == evt_id
                except (HarnessError, ValueError):
                    ok = False
            return evt_id, {"ok": ok, "error": "" if ok else mask((err or out)[-200:])}

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.args.concurrency) as pool:
            for evt_id, res in pool.map(one, targets):
                results[evt_id] = res
        self.say(f"phase 2: {len(targets)} resends issued")
        self.settle("phase 2")
        self.phase_seconds["2_resends"] = round(time.time() - began, 1)
        return results

    # ---- reconciliation -----------------------------------------------------------

    def run(self):
        self.preflight()
        failed_types = self.phase_trigger()

        parsed_1, run_ids = self.forwarded_new()
        deliveries_1 = {i: parsed_1["sent"][i] for i in run_ids}
        before = self.db_snapshot()

        targets = self.pick_resends(parsed_1, run_ids)
        resend_results = self.phase_resend(targets)

        began = time.time()
        parsed = parse_listener(self.read_listener())
        run_ids = set(parsed["sent"]) - self.baseline_ids
        after = self.db_snapshot()
        stripe_events = self.stripe_events_since(self.start_epoch)
        self.say(f"phase 3: Stripe holds {len(stripe_events)} events since the start")
        sums = self.ledger_window_sums()
        pending, orphans = self.outbox_state()
        deadline = time.time() + self.args.outbox_timeout
        while pending and time.time() < deadline:
            time.sleep(3)
            pending, orphans = self.outbox_state()
        self.phase_seconds["3_reconcile"] = round(time.time() - began, 1)

        report = self.reconcile(parsed, run_ids, deliveries_1, before, after,
                                stripe_events, sums, pending, orphans,
                                targets, resend_results, failed_types)
        self.write_evidence(parsed, stripe_events, report)
        return report

    def reconcile(self, parsed, run_ids, deliveries_1, before, after, stripe_events,
                  sums, pending, orphans, targets, resend_results, failed_types):
        controls = []

        def control(cid, name, ok, detail):
            controls.append({"id": cid, "name": name, "ok": bool(ok), "detail": detail})

        ledger, dlq = after["ledger"], after["dlq"]
        dlq_by_id = defaultdict(Counter)
        for (i, r), n in dlq.items():
            dlq_by_id[i][r] += n
        window = {e["id"]: e for e in stripe_events}

        # Volume: more than the target, and every core type actually triggered.
        ok_by_type = Counter(r["type"] for r in self.trigger_log if r["ok"])
        core_missing = [t for t in CORE_TYPES if ok_by_type[t] == 0]
        control("v", "volume", len(run_ids) > self.args.target_events and not core_missing,
                f"{len(run_ids)} distinct events forwarded (target > {self.args.target_events});"
                f" core types never triggered: {core_missing or 'none'}")

        # (a) exactly one row per forwarded event: ledger or non-duplicate DLQ row.
        lost, multi, placement = [], [], Counter()
        for evt_id in sorted(run_ids):
            in_ledger = 1 if evt_id in ledger else 0
            dlq_rows = {r: n for r, n in dlq_by_id[evt_id].items() if r != "DUPLICATE"}
            total = in_ledger + sum(dlq_rows.values())
            if total == 0:
                lost.append(evt_id)
            elif total > 1:
                multi.append(evt_id)
            else:
                placement["ledger" if in_ledger else "dlq:" + next(iter(dlq_rows))] += 1
        dup_orphans = sorted(
            i for i in run_ids if dlq_by_id[i]["DUPLICATE"] and parsed["sent"][i] < 2)
        control("a", "one row per forwarded event", not lost and not multi,
                f"{len(run_ids)} events: {dict(placement)}; lost {len(lost)}; "
                f"with more than one row {len(multi)}; DUPLICATE rows for events delivered "
                f"once {len(dup_orphans)}")

        # (b) Stripe's own count of the supported types against the ledger, same window.
        stripe_counts = Counter(e["type"] for e in stripe_events)
        in_window = {i: r for i, r in ledger.items()
                     if r["created"] is not None and r["created"] >= self.start_epoch}
        ledger_counts = Counter(r["type"] for r in in_window.values())
        status_counts = {s: v["count"] for s, v in sums.items()}
        compared = ("invoice.paid", "invoice.payment_failed") + SUPPORTED_SUB_TYPES
        mismatch = {t: (stripe_counts[t], ledger_counts[t]) for t in compared
                    if stripe_counts[t] != ledger_counts[t]}
        stripe_ids = {e["id"] for e in stripe_events if e["type"] in compared}
        ledger_ids = {i for i, r in in_window.items() if r["type"] in compared}
        not_forwarded = sorted(set(window) - run_ids)
        stripe_sub = sum(stripe_counts[t] for t in SUPPORTED_SUB_TYPES)
        control("b", "event counts, Stripe vs ledger",
                not mismatch and stripe_ids == ledger_ids,
                f"invoice.paid Stripe {stripe_counts['invoice.paid']} vs POSTED "
                f"{status_counts.get('POSTED', 0)}; invoice.payment_failed Stripe "
                f"{stripe_counts['invoice.payment_failed']} vs VOID {status_counts.get('VOID', 0)}"
                f"; customer.subscription.{{created,updated,deleted}} Stripe {stripe_sub} vs "
                f"LIFECYCLE {status_counts.get('LIFECYCLE', 0)}; id sets equal: "
                f"{stripe_ids == ledger_ids}; per-type mismatches: {mismatch or 'none'}")
        control("g", "listener forwarded every event Stripe emitted",
                not not_forwarded,
                f"{len(window)} events in the Stripe window, {len(not_forwarded)} never forwarded "
                f"(first: {not_forwarded[:3]})")

        # (c) money, cent for cent.
        stripe_paid = sum(e["amount_paid"] or 0 for e in stripe_events
                          if e["type"] == "invoice.paid")
        stripe_due = sum(e["amount_due"] or 0 for e in stripe_events
                         if e["type"] == "invoice.payment_failed")
        ledger_posted = sums.get("POSTED", {}).get("cents", 0)
        ledger_void = sums.get("VOID", {}).get("cents", 0)
        control("c", "money, Stripe vs ledger (cents)",
                stripe_paid == ledger_posted and stripe_due == ledger_void,
                f"invoice.paid amount_paid Stripe {stripe_paid} vs POSTED amount_cents "
                f"{ledger_posted}; invoice.payment_failed amount_due Stripe {stripe_due} vs "
                f"VOID amount_cents {ledger_void}")

        # (d) every delivery answered 200.
        deliveries = sum(parsed["sent"].values())
        answers = [a for v in parsed["answers"].values() for a in v]
        codes = Counter(answers)
        unanswered = sum(max(0, parsed["sent"][i] - len(parsed["answers"].get(i, [])))
                         for i in parsed["sent"])
        non200 = sum(n for c, n in codes.items() if c != 200)
        control("d", "all deliveries answered 200",
                non200 == 0 and unanswered == 0 and parsed["errors"] == 0,
                f"{deliveries} deliveries logged, {len(answers)} answers: {dict(codes)}; "
                f"non-200 {non200}; unanswered {unanswered}; listener ERROR lines "
                f"{parsed['errors']}")

        # (e) outbox drained, and every ledger row has its outbox row.
        control("e", "outbox drained", pending == 0 and orphans == 0,
                f"pending outbox rows {pending}; ledger rows without an outbox row {orphans}")

        # (r) resends: all issued, all answered 200, ledger untouched.
        issued_ok = sum(1 for r in resend_results.values() if r["ok"])
        redelivered = [i for i in targets if parsed["sent"][i] > deliveries_1.get(i, 0)]
        resend_bad = [i for i in targets
                      if any(a != 200 for a in parsed["answers"].get(i, []))]
        ledger_rows_before = {i: r for i, r in before["ledger"].items()}
        ledger_changed = {i for i in set(ledger) | set(ledger_rows_before)
                          if ledger.get(i) != ledger_rows_before.get(i)}
        added = Counter()
        for evt_id in targets:
            for r, n in dlq_by_id[evt_id].items():
                added[r] += n - before["dlq"].get((evt_id, r), 0)
        control("r", "resends are idempotent",
                len(targets) >= self.args.resends and issued_ok == len(targets)
                and len(redelivered) == len(targets) and not resend_bad and not ledger_changed,
                f"{len(targets)} resends issued ({issued_ok} accepted by Stripe), "
                f"{len(redelivered)} redelivered to the service, non-200 answers {len(resend_bad)}"
                f"; ledger rows before {len(before['ledger'])} after {len(ledger)}, rows "
                f"changed {len(ledger_changed)}; DLQ rows added by the resends: {dict(added)}")

        types_delivered = Counter(parsed["types"][i] for i in run_ids)
        ids_by_type = defaultdict(list)
        for i in run_ids:
            ids_by_type[parsed["types"][i]].append(i)
        table = []
        for etype in sorted(ids_by_type):
            in_d = Counter()
            for i in ids_by_type[etype]:
                for r, n in dlq_by_id[i].items():
                    if r != "DUPLICATE":
                        in_d[r] += n
            table.append({"type": etype, "stripe": stripe_counts[etype],
                          "forwarded": len(ids_by_type[etype]),
                          "ledger": sum(1 for i in ids_by_type[etype] if i in ledger),
                          "dlq": dict(in_d)})
        triggers = defaultdict(lambda: {"ok": 0, "failed": 0})
        for r in self.trigger_log:
            triggers[r["type"]]["ok" if r["ok"] else "failed"] += 1
        return {
            "controls": controls, "ok": all(c["ok"] for c in controls),
            "start_epoch": self.start_epoch, "org_id": self.args.org_id,
            "distinct_events_forwarded": len(run_ids), "deliveries": deliveries,
            "distinct_types_forwarded": len(types_delivered),
            "answers_by_code": dict(codes),
            "ledger_by_status": sums, "stripe_types": dict(stripe_counts),
            "per_type": table, "triggers": dict(triggers), "yields": self.yields,
            "failed_probe_types": failed_types, "rate_limit_events": self.rate_limit_events,
            "final_concurrency": self.gate.limit,
            "invalid_dlq_ids": after["invalid"][:20], "lost_ids": lost[:20],
            "multi_row_ids": multi[:20], "not_forwarded_ids": not_forwarded[:20],
            "phase_seconds": self.phase_seconds,
            "total_seconds": round(time.time() - self.t0, 1),
        }

    # ---- output ---------------------------------------------------------------------

    def write_evidence(self, parsed, stripe_events, report):
        def dump(name, text):
            with open(os.path.join(self.out_dir, name), "w", encoding="utf-8") as f:
                f.write(mask(text))

        dump("listener.log", self.read_listener())
        dump("stripe_events.jsonl", "".join(json.dumps(e) + "\n" for e in stripe_events))
        dump("triggers.jsonl", "".join(json.dumps(r) + "\n" for r in self.trigger_log))
        dump("report.json", json.dumps(report, indent=2, sort_keys=True))
        dump("summary.txt", render_summary(report))


def render_summary(report):
    lines = ["", "=" * 72, "STRIPE SANDBOX RECONCILIATION", "=" * 72]
    lines.append(f"distinct events forwarded: {report['distinct_events_forwarded']}  "
                 f"deliveries (incl. resends): {report['deliveries']}  "
                 f"distinct types: {report['distinct_types_forwarded']}")
    lines.append(f"answers by status code: {report['answers_by_code']}")
    lines.append(f"ledger by status (window): {report['ledger_by_status']}")
    lines.append(f"duration: {report['total_seconds']}s  phases: {report['phase_seconds']}")
    lines.append(f"Stripe rate limits hit: {len(report['rate_limit_events'])}  "
                 f"concurrency at end: {report['final_concurrency']}")
    lines.append(f"types the sandbox refused: {report['failed_probe_types'] or 'none'}")
    lines.append("-" * 72)
    for c in report["controls"]:
        lines.append(f"{'PASS' if c['ok'] else 'FAIL'}  ({c['id']}) {c['name']}")
        lines.append(f"        {c['detail']}")
    lines.append("-" * 72)
    lines.append("RESULT: " + ("ALL CONTROLS PASSED" if report["ok"] else "CONTROL FAILURE"))
    return "\n".join(lines) + "\n"


def build_parser():
    p = argparse.ArgumentParser(
        description="Drive a Stripe sandbox against the billing service and reconcile the books.")
    p.add_argument("--stripe-cmd", default=os.environ.get("STRIPE_CLI", "stripe"),
                   help="Stripe CLI command (env STRIPE_CLI; default: stripe)")
    p.add_argument("--psql-cmd", default=os.environ.get(
        "PSQL_CMD", "docker compose exec -T postgres psql -U postgres -d billing"),
        help="command that opens psql on the service database (env PSQL_CMD)")
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--listen-log", help="file holding the `stripe listen` output")
    src.add_argument("--listen-log-cmd",
                     help="command that prints the `stripe listen` output, "
                          "e.g. 'docker logs pbpoc-stripe-listen'")
    p.add_argument("--target-events", type=int, default=1000,
                   help="forward more than this many events (default 1000)")
    p.add_argument("--min-core-triggers", type=int, default=12,
                   help="triggers per core type in the volume phase (default 12)")
    p.add_argument("--max-triggers", type=int, default=600,
                   help="hard cap on volume-phase triggers (default 600)")
    p.add_argument("--concurrency", type=int, default=4,
                   help="parallel Stripe CLI calls (default 4)")
    p.add_argument("--resends", type=int, default=60,
                   help="events to resend in phase 2 (default 60)")
    p.add_argument("--types", nargs="+",
                   help="replace the built-in list of non-core event types")
    p.add_argument("--org-id", default="default", help="tenant to reconcile (default: default)")
    p.add_argument("--settle-seconds", type=int, default=12,
                   help="listener quiet time that counts as settled (default 12)")
    p.add_argument("--settle-timeout", type=int, default=300,
                   help="longest wait for the listener to settle (default 300)")
    p.add_argument("--outbox-timeout", type=int, default=120,
                   help="longest wait for the outbox to drain (default 120)")
    p.add_argument("--out-dir", default=None,
                   help="where evidence is written (default: a new temporary directory)")
    return p


def main():
    args = build_parser().parse_args()
    if args.out_dir is None:
        args.out_dir = tempfile.mkdtemp(prefix="stripe_sandbox_")
    if args.resends < 1 or args.concurrency < 1 or args.target_events < 1:
        print("--resends, --concurrency and --target-events must be at least 1",
              file=sys.stderr)
        return 2
    harness = Harness(args)
    try:
        report = harness.run()
    except HarnessError as exc:
        print(f"ABORTED: {mask(str(exc))}", file=sys.stderr)
        return 2
    print(render_summary(report))
    print(f"evidence written to {args.out_dir}")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
