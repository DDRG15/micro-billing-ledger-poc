"""row_level_security

Revision ID: 0006
Revises: 0005
Create Date: 2026-08-11

Creates a non-superuser application role and puts tenant isolation behind PostgreSQL
row-level security. Phase 13.6.

WHY. Until now isolation lived entirely in application queries: every SELECT and INSERT
    carried its own org_id predicate. That holds exactly as long as nobody forgets one,
    and on 2026-08-11 somebody did — process_stripe_event_batch built its DLQ rows
    without org_id in the INVALID and DUPLICATE branches, so one tenant's raw payload
    became readable by another and the same Stripe event ended up POSTED under two
    tenants. Two audits caught it; three passes of my own review did not. No linter,
    type checker or constraint could have caught it, because the guarantee was a habit.

THE PART THAT ACTUALLY MATTERS IS THE ROLE, NOT THE POLICY.
    A first attempt at this migration wrote ENABLE + FORCE ROW LEVEL SECURITY with a
    default-deny policy, applied cleanly, and the whole test suite passed — which was
    the alarm, not the result. The application connects as `postgres`, and a SUPERUSER
    bypasses RLS entirely; BYPASSRLS is implicit and FORCE does not change it. The
    policies existed, showed up in pg_policies, and enforced nothing.

    Shipping that would have been worse than shipping nothing: the repository would
    document a guarantee the database does not provide, and the manual org_id predicates
    — the only real defence — would look redundant and start being dropped.

    So this migration creates `billing_app` with NOSUPERUSER NOBYPASSRLS and grants it
    only DML on the four tables. Migrations keep running as the owner, because they need
    DDL; only the application connects as the restricted role.

DEFAULT DENY. The policy compares org_id against current_setting('app.org_id'). Absent,
    current_setting(..., true) returns NULL, every comparison is NULL, and no rows are
    visible. A policy treating "unset" as "unrestricted" would reproduce the original
    failure: forgetting to establish scope would silently restore full access. Genuinely
    cross-tenant work (outbox drain, DLQ retry claim, retention pruning) opts in by
    setting app.org_id to '*'.

USING **and** WITH CHECK. USING governs what is readable; WITH CHECK governs what may be
    written. USING alone would still allow inserting a row belonging to someone else —
    which is precisely what the batch bug did.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0006"
down_revision: Union[str, Sequence[str], None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

APP_ROLE = "billing_app"
TENANT_TABLES = ("ledger", "outbox", "dlq")

PREDICATE = """(
    current_setting('app.org_id', true) = '*'
    OR org_id = NULLIF(current_setting('app.org_id', true), '')
)"""


def upgrade() -> None:
    # ── the restricted application role ─────────────────────────────────────────
    #     Password is a bootstrap placeholder; a real deployment rotates it and puts it
    #     in APP_DATABASE_URL. The role cannot create databases, cannot bypass RLS, and
    #     owns nothing — ownership stays with the migration role so DDL is unavailable
    #     to the application even if its credentials leak.
    op.execute(f"""
        DO $$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{APP_ROLE}') THEN
                CREATE ROLE {APP_ROLE} LOGIN PASSWORD 'billing_app'
                    NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
            ELSE
                ALTER ROLE {APP_ROLE} NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS;
            END IF;
        END $$;
    """)
    op.execute(f"GRANT USAGE ON SCHEMA public TO {APP_ROLE}")
    for table in TENANT_TABLES:
        op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON {table} TO {APP_ROLE}")
    # The org registry is read-only to the application: tenants are provisioned by an
    #     operator, never by the service handling traffic.
    op.execute(f"GRANT SELECT ON org TO {APP_ROLE}")
    # BIGSERIAL columns need their sequences.
    op.execute(f"GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO {APP_ROLE}")

    # ── the policies ────────────────────────────────────────────────────────────
    for table in TENANT_TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        # FORCE so the policy also applies to the table owner. Not sufficient on its own
        #     — see the header — but required once the app runs as a non-owner too.
        op.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table}")
        op.execute(f"""
            CREATE POLICY tenant_isolation ON {table}
            USING ({PREDICATE})
            WITH CHECK ({PREDICATE})
        """)


def downgrade() -> None:
    for table in TENANT_TABLES:
        op.execute(f"DROP POLICY IF EXISTS tenant_isolation ON {table}")
        op.execute(f"ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY")
        op.execute(f"ALTER TABLE {table} DISABLE ROW LEVEL SECURITY")
        op.execute(f"REVOKE ALL ON {table} FROM {APP_ROLE}")
    op.execute(f"REVOKE ALL ON org FROM {APP_ROLE}")
    op.execute(f"REVOKE USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public FROM {APP_ROLE}")
    op.execute(f"REVOKE USAGE ON SCHEMA public FROM {APP_ROLE}")
    # The role itself is left in place: dropping it would fail if anything still holds a
    #     grant elsewhere, and an unused login role is harmless next to a failed rollback.
