"""
disposable_db.py — refuse to run destructive tooling against a database that matters.

test_ledger.py and benchmark.py both TRUNCATE ledger, outbox and dlq, and the suite also
deletes every tenant except the default one. Both used to default to the `billing`
database — the one `docker compose up` creates, and the one the README told you to run
them against — so the documented workflow erased the local ledger on every run, and a
mistyped DATABASE_URL could erase any other database the credentials reached.

The rule is a naming convention, checked before anything connects: a database whose name
ends in `_test` or `_bench` has been declared disposable, and nothing else has. It is
deliberately not a host check. The compose stack's real database also lives on localhost,
so the host says nothing about whether the data is worth keeping.

Call require_disposable_database() BEFORE `import ledger`: ledger reads DATABASE_URL and
APP_DATABASE_URL at import and opens its connection pool there.
"""

import os
import sys
from typing import List
from urllib.parse import parse_qs, urlsplit

DISPOSABLE_SUFFIXES = ("_test", "_bench")

# Distinct from 1, which is what an uncaught exception exits with. A test can then tell
#     "the guard refused" apart from "something crashed while connecting".
GUARD_EXIT_CODE = 2


def _database_names(dsn: str) -> List[str]:
    """Every database name a libpq URI could connect to. Empty if it is not a URI.

    libpq lets a `dbname=` query parameter override the path, so both count. Checking
    only the path would let `.../billing_test?dbname=billing` through to `billing`.
    """
    parts = urlsplit(dsn)
    if parts.scheme not in ("postgres", "postgresql"):
        return []
    return [parts.path.lstrip("/")] + parse_qs(parts.query).get("dbname", [])


def refusal(dsn: str) -> str:
    """Why this DSN is not disposable, or '' if it is.

    Never echoes the DSN itself — it carries a password, and this message is printed.
    """
    names = _database_names(dsn)
    if not names:
        # key=value DSNs are valid libpq, but parsing them by hand is where a guard
        #     grows the hole it exists to close. Ask for the form this module can read.
        return "it is not a postgresql:// URI, so its database name cannot be checked"
    for name in names:
        if not name:
            return "it names no database (libpq would fall back to PGDATABASE or the user name)"
        if not name.endswith(DISPOSABLE_SUFFIXES):
            return f"it points at database {name!r}, whose name does not end in _test or _bench"
    return ""


def require_disposable_database(default_url: str) -> str:
    """Point DATABASE_URL at a disposable database, or exit before anything connects.

    Fills DATABASE_URL from default_url when unset, so `ledger` reads the same value this
    function checked. APP_DATABASE_URL is checked too when set: the restricted-role
    connection writes rows, and it is the one a half-edited environment forgets.
    """
    url = os.environ.get("DATABASE_URL", "").strip() or default_url
    os.environ["DATABASE_URL"] = url
    app_url = os.environ.get("APP_DATABASE_URL", "").strip()

    for var, dsn in (("DATABASE_URL", url), ("APP_DATABASE_URL", app_url)):
        if not dsn:
            continue
        reason = refusal(dsn)
        if reason:
            print(
                f"refusing to run: {var} is not disposable: {reason}.\n"
                f"This script TRUNCATEs ledger, outbox and dlq. Point it at a database "
                f"created for the purpose, e.g. billing_test (see README, 'Running the tests').",
                file=sys.stderr,
            )
            raise SystemExit(GUARD_EXIT_CODE)
    return url
