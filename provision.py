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
from urllib.parse import unquote, urlsplit

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import encrypt_password

DEFAULT_ORG_ID = "default"
APP_ROLE = "billing_app"
# Migration 0006 creates billing_app with this password; it is printed in that file.
APP_ROLE_BOOTSTRAP_PASSWORD = "billing_app"


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


def sync_app_role_password(conn, app_dsn: str) -> str:
    """Give billing_app the password APP_DATABASE_URL carries. Returns what it did.

    A migration cannot carry a secret, so 0006 creates the role with a password that is
    printed in the repository, and rotating it was a manual ALTER ROLE the docs asked
    for and nothing performed. The deploy now does it: the operator puts the real
    password in APP_DATABASE_URL, the service connects with it, and this step makes the
    role match before the service starts.

    The password never crosses the wire in clear: encrypt_password hashes it client-side
    (the server's password_encryption, SCRAM on PostgreSQL 16), so the ALTER ROLE that
    reaches the server — and any statement log — carries only the hash.

    Left alone when APP_DATABASE_URL is absent, names another role (this step speaks for
    billing_app only), or still carries the bootstrap password: the service refuses
    to serve traffic on that password outside dev, and rotating to it changes nothing.
    """
    if not app_dsn:
        return "skipped: APP_DATABASE_URL not given to this step"
    parts = urlsplit(app_dsn)
    if parts.username != APP_ROLE:
        return f"skipped: APP_DATABASE_URL connects as {parts.username!r}, not {APP_ROLE}"
    password = unquote(parts.password or "")
    if not password or password == APP_ROLE_BOOTSTRAP_PASSWORD:
        return "unchanged: APP_DATABASE_URL still carries the bootstrap password"
    hashed = encrypt_password(password, APP_ROLE, conn)
    with conn.cursor() as cur:
        cur.execute(sql.SQL("ALTER ROLE {} PASSWORD %s").format(sql.Identifier(APP_ROLE)),
                    (hashed,))
    return "rotated"


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
        role_result = sync_app_role_password(conn, os.environ.get("APP_DATABASE_URL", ""))
    finally:
        conn.close()

    # The value never appears in output. Which secret is live is answered by the
    #     fingerprint the API logs at startup (see ledger._secret_fingerprint).
    print(f"provision: default org signing secret synchronised "
          f"({'placeholder' if secret == 'whsec_YOUR_SECRET_HERE' else 'set'})")
    print(f"provision: {APP_ROLE} password {role_result}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
