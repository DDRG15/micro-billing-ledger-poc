"""Check a load run for lost or duplicated events.

    python e2e/check_run.py run.jsonl --prefix evt_load

Every event answered 200 must be in the ledger exactly once, and each must reach the
downstream receiver (once the worker has drained). Reads the ledger through
`./e2e/dc.sh exec postgres psql` and the receiver's e2e/recv/received.log.
"""
import argparse
import json
import os
import re
import subprocess
from collections import Counter

HERE = os.path.dirname(os.path.abspath(__file__))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("run", help="the --out file of a loadgen run")
    parser.add_argument("--prefix", required=True, help="the --prefix of that run")
    args = parser.parse_args()

    with open(args.run) as f:
        acked = {f"{args.prefix}_{r['i']:07d}" for r in map(json.loads, f) if r["st"] == 200}
    if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", args.prefix):
        raise SystemExit("--prefix must match [A-Za-z0-9_]{1,64}")
    # The prefix is restricted to [A-Za-z0-9_] above, so it cannot break out of the literal.
    query = f"SELECT transaction_id FROM ledger WHERE transaction_id LIKE '{args.prefix}\\_%'"  # nosec B608
    out = subprocess.run([os.path.join(HERE, "dc.sh"), "exec", "-T", "postgres", "psql", "-U",
                          "postgres", "-d", "billing", "-Atc", query],
                         capture_output=True, text=True, check=True).stdout.split()
    in_ledger = Counter(out)
    delivered = Counter()
    log = os.path.join(HERE, "recv", "received.log")
    if os.path.exists(log):
        with open(log) as f:
            for line in f:
                row = json.loads(line)
                if row["outcome"] == "ok" and (row["tx"] or "").startswith(args.prefix + "_"):
                    delivered[row["tx"]] += 1
    print(f"answered 200:              {len(acked)}")
    print(f"200 but missing in ledger: {len(acked - set(in_ledger))}")
    print(f"in ledger more than once:  {sum(1 for c in in_ledger.values() if c > 1)}")
    print(f"delivered downstream:      {len(delivered)} (duplicates: {sum(c - 1 for c in delivered.values())})")


if __name__ == "__main__":
    main()
