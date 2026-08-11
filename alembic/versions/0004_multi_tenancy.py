"""multi_tenancy

Revision ID: 0004
Revises: 0003
Create Date: 2026-08-11

Adds org_id to ledger, outbox and dlq, and an `org` registry that maps a credential to
a tenant. Phase 13.

THE DECISION THAT MATTERS: the ledger primary key becomes (org_id, transaction_id).

    Adding org_id as an ordinary column while leaving transaction_id as the sole primary
    key would look correct and lose money. transaction_id IS the idempotency guard —
    INSERT ... ON CONFLICT (transaction_id) DO NOTHING. Stripe event ids are unique
    within an account, not across accounts, so two tenants can legitimately present the
    same id. With a single-column key the second tenant's event hits ON CONFLICT, is
    discarded as a duplicate and dead-lettered as DUPLICATE.

    That failure is silent, financial, and indistinguishable from the idempotency guard
    working exactly as designed. A composite key makes the guard per-tenant, which is
    what it always meant.

    outbox references ledger, so its foreign key becomes composite too.

WHY AN org TABLE RATHER THAN A HEADER: a tenant supplied by the caller is not an
    isolation boundary — anyone holding a shared key could read another tenant's data by
    changing a header. org_id is derived from the credential instead. The registry stores
    a SHA-256 of the API key (lookups compare hashes, the key itself is never stored) and
    the tenant's Stripe signing secret.

    The signing secret has to be recoverable to verify an HMAC, so it cannot be hashed.
    In production it belongs in a secret manager with this column holding a reference;
    storing the value here is a proof-of-concept shortcut and is called out as one in
    SECURITY.md rather than hidden.

BACKFILL: existing rows predate tenancy and belong to exactly one operator, so they are
    assigned DEFAULT_ORG_ID ('default'). The column is added nullable, backfilled, then
    set NOT NULL — adding it NOT NULL in one step fails on a non-empty table.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0004"
down_revision: Union[str, Sequence[str], None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

DEFAULT_ORG_ID = "default"


def upgrade() -> None:
    # ── org registry ────────────────────────────────────────────────────────────
    op.execute("""
        CREATE TABLE IF NOT EXISTS org (
            org_id                TEXT        PRIMARY KEY,
            name                  TEXT        NOT NULL,
            api_key_sha256        TEXT        UNIQUE,
            stripe_webhook_secret TEXT        NOT NULL,
            active                BOOLEAN     NOT NULL DEFAULT TRUE,
            created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)
    # Credential lookup happens on every authenticated request: index the hash.
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_org_api_key
        ON org(api_key_sha256) WHERE api_key_sha256 IS NOT NULL AND active
    """)
    # The tenant that owns every pre-existing row. Its secret is a placeholder; a real
    #     deployment updates it. Nothing can authenticate as it until api_key_sha256 is set.
    op.execute(f"""
        INSERT INTO org (org_id, name, stripe_webhook_secret)
        VALUES ('{DEFAULT_ORG_ID}', 'Default organisation', 'whsec_YOUR_SECRET_HERE')
        ON CONFLICT (org_id) DO NOTHING
    """)

    # ── org_id on the three data tables ─────────────────────────────────────────
    #     Nullable, then backfilled, then NOT NULL. Adding NOT NULL without a default
    #     to a populated table fails outright.
    for table in ("ledger", "outbox", "dlq"):
        op.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS org_id TEXT")
        op.execute(f"UPDATE {table} SET org_id = '{DEFAULT_ORG_ID}' WHERE org_id IS NULL")
        op.execute(f"ALTER TABLE {table} ALTER COLUMN org_id SET NOT NULL")
        op.execute(f"ALTER TABLE {table} ALTER COLUMN org_id SET DEFAULT '{DEFAULT_ORG_ID}'")

    # ── recompose the keys ──────────────────────────────────────────────────────
    #     The outbox foreign key must be dropped before the ledger primary key it
    #     depends on, and recreated against the new composite key afterwards.
    op.execute("ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_transaction_id_fkey")
    op.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS ledger_pkey")
    op.execute("ALTER TABLE ledger ADD PRIMARY KEY (org_id, transaction_id)")
    op.execute("""
        ALTER TABLE outbox
        ADD CONSTRAINT outbox_org_transaction_fkey
        FOREIGN KEY (org_id, transaction_id)
        REFERENCES ledger(org_id, transaction_id) ON DELETE CASCADE
    """)
    op.execute("""
        ALTER TABLE ledger
        ADD CONSTRAINT ledger_org_fkey FOREIGN KEY (org_id) REFERENCES org(org_id)
    """)

    # ── org-leading indexes ─────────────────────────────────────────────────────
    #     Every query is scoped by org, so org_id leads each composite index; the old
    #     single-column versions can no longer serve those predicates efficiently.
    op.execute("CREATE INDEX IF NOT EXISTS idx_ledger_org_customer ON ledger(org_id, customer_id)")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_outbox_org_dispatched
        ON outbox(org_id, dispatched, id) WHERE dispatched = 0
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_dlq_org_retry
        ON dlq(org_id, status, next_retry_at) WHERE status = 'pending'
    """)
    op.execute("CREATE INDEX IF NOT EXISTS idx_dlq_org ON dlq(org_id, id DESC)")
    op.execute("DROP INDEX IF EXISTS idx_ledger_customer_id")
    op.execute("DROP INDEX IF EXISTS idx_outbox_dispatched_id")


def downgrade() -> None:
    # Collapsing back to a single-column key can only succeed if no transaction_id is
    #     shared between tenants. Fail loudly rather than silently discarding a tenant's
    #     rows to make the key fit.
    op.execute("""
        DO $$
        DECLARE dupes INT;
        BEGIN
            SELECT COUNT(*) INTO dupes FROM (
                SELECT transaction_id FROM ledger GROUP BY transaction_id HAVING COUNT(*) > 1
            ) t;
            IF dupes > 0 THEN
                RAISE EXCEPTION
                    'Cannot downgrade: % transaction_id values exist in more than one org. '
                    'Collapsing to a single-column primary key would destroy rows.', dupes;
            END IF;
        END $$;
    """)
    op.execute("DROP INDEX IF EXISTS idx_dlq_org")
    op.execute("DROP INDEX IF EXISTS idx_dlq_org_retry")
    op.execute("DROP INDEX IF EXISTS idx_outbox_org_dispatched")
    op.execute("DROP INDEX IF EXISTS idx_ledger_org_customer")
    op.execute("CREATE INDEX IF NOT EXISTS idx_ledger_customer_id ON ledger(customer_id)")
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_outbox_dispatched_id
        ON outbox(dispatched, id) WHERE dispatched = 0
    """)

    op.execute("ALTER TABLE outbox DROP CONSTRAINT IF EXISTS outbox_org_transaction_fkey")
    op.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS ledger_org_fkey")
    op.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS ledger_pkey")
    op.execute("ALTER TABLE ledger ADD PRIMARY KEY (transaction_id)")
    op.execute("""
        ALTER TABLE outbox
        ADD CONSTRAINT outbox_transaction_id_fkey
        FOREIGN KEY (transaction_id) REFERENCES ledger(transaction_id) ON DELETE CASCADE
    """)

    for table in ("ledger", "outbox", "dlq"):
        op.execute(f"ALTER TABLE {table} DROP COLUMN IF EXISTS org_id")
    op.execute("DROP INDEX IF EXISTS idx_org_api_key")
    op.execute("DROP TABLE IF EXISTS org")
