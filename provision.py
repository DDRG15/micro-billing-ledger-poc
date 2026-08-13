"""
provision.py — deploy-time database writes that need owner rights
=================================================================
Runs as DATABASE_URL (the owner), immediately after `alembic upgrade head`, and never
from a process that serves traffic. Exits non-zero on failure so the compose gate
`migrate: condition: service_completed_successfully` holds the API back.

WHY THIS FILE EXISTS
--------------------
Until Phase 13.7 the API container connected as `postgres` and called _bootstrap() at
import, which did two unrelated jobs under one name: it created the tables, and it
synchronised the default tenant's Stripe signing secret from STRIPE_WEBHOOK_SECRET.

13.7 pointed the service at `billing_app` — NOSUPERUSER, NOBYPASSRLS, and holding only
SELECT on the org registry — because a superuser bypasses row-level security entirely
and leaves migration 0006's policies decorative. That correctly removed the service's
ability to run _bootstrap. It also silently removed the second job, and the two failure
modes are not comparable: missing DDL is loud (alembic owns the schema and the migrate
service already runs it), while a missing secret sync is silent and arrives later.

Migration 0004 seeds the default org with the literal 'whsec_YOUR_SECRET_HERE' under
ON CONFLICT DO NOTHING, and alembic re-runs nothing once a revision is applied. So
without this step a fresh deployment verifies every legacy /webhook/stripe request
against a placeholder, and a rotation of STRIPE_WEBHOOK_SECRET never reaches the
registry at all. Both surface as Stripe apparently sending bad signatures — a failure
that points at the sender rather than at the row that is actually wrong.

The org registry stays read-only to the application. Writing it is a deploy-time act by
whoever holds the owner credential, which is exactly the boundary 13.7 drew.
"""
import os
import sys

import psycopg2

DEFAULT_ORG_ID = "default"


def _read_secret(name: str, default: str = "") -> str:
    """Resolve a secret from `<NAME>_FILE` if set, otherwise from `<NAME>`.

    Mirrors ledger._read_secret, including its refusal to fall back. If a deployment
    mounts its secret as a file, provisioning has to read the same value the API will
    verify against, or this step writes a secret nothing checks signatures with.
    """
    path = os.environ.get(f"{name}_FILE", "").strip()
    if path:
        try:
            with open(path, encoding="utf-8") as handle:
                return handle.read().strip()
        except OSError as exc:
            raise RuntimeError(
                f"{name}_FILE is set to {path!r} but could not be read: {exc}. "
                f"Refusing to fall back to ${name} — provisioning the wrong secret "
                f"silently is worse than failing the deploy."
            ) from exc
    return os.environ.get(name, default)


def sync_default_org_secret(conn, secret: str) -> None:
    """Point the default tenant at the operator's current STRIPE_WEBHOOK_SECRET.

    DO UPDATE, not DO NOTHING. For the default org the signing secret IS
    STRIPE_WEBHOOK_SECRET by definition, so a stale row would make the legacy
    /webhook/stripe route verify against a secret the operator already rotated away
    from. Every other tenant is left untouched: their secrets live only in the registry
    and no environment variable speaks for them.
    """
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO org (org_id, name, stripe_webhook_secret) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (org_id) DO UPDATE SET stripe_webhook_secret = EXCLUDED.stripe_webhook_secret",
            (DEFAULT_ORG_ID, "Default organisation", secret),
        )


def main() -> int:
    dsn = os.environ.get("DATABASE_URL", "")
    if not dsn:
        print("provision: DATABASE_URL is not set — this step needs the OWNER connection, "
              "not APP_DATABASE_URL", file=sys.stderr)
        return 1

    secret = _read_secret("STRIPE_WEBHOOK_SECRET", "whsec_YOUR_SECRET_HERE")

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    try:
        sync_default_org_secret(conn, secret)
    finally:
        conn.close()

    # The value never appears in output. Which secret is live is answered by the
    #     fingerprint the API logs at startup (see ledger._secret_fingerprint).
    print(f"provision: default org signing secret synchronised "
          f"({'placeholder' if secret == 'whsec_YOUR_SECRET_HERE' else 'set'})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
