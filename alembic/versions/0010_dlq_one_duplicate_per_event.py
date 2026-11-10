"""dlq_one_duplicate_per_event

Revision ID: 0010
Revises: 0009
Create Date: 2026-11-09

At most one DUPLICATE dead-letter row per (org_id, transaction_id).

WHY. Every redelivery of an already-ledgered event appended another DUPLICATE row. A
    signed payload captured on the wire stays valid for Stripe's 300 s signature
    tolerance, so replaying it as fast as the webhook rate limit allows (100/minute per
    IP by default) wrote a row per request: the answer to a replay is DLQ_DUPLICATE and
    the table grew with every one. The rows carry nothing the first one does not: the
    event is already in the ledger, which is the record that matters.

The insert paths now use ON CONFLICT DO NOTHING, so a repeat is still answered
    DLQ_DUPLICATE and still leaves exactly one audit row.

Existing data first. A unique index cannot be created over duplicates, so the oldest
    DUPLICATE row of each (org_id, transaction_id) is kept and the rest are deleted.
    They are audit-only copies of an event the ledger already holds, and prune_dlq
    deletes the same rows by age. set_config('app.org_id', '*') puts the delete under
    the cross-tenant scope of migration 0006's policy, for an owner role that RLS
    still applies to (FORCE ROW LEVEL SECURITY); a superuser bypasses it either way.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0010"
down_revision: Union[str, Sequence[str], None] = "0009"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("SELECT set_config('app.org_id', '*', true)")
    op.execute("""
        DELETE FROM dlq a
        USING dlq b
        WHERE a.reason = 'DUPLICATE' AND b.reason = 'DUPLICATE'
          AND a.org_id = b.org_id
          AND a.transaction_id = b.transaction_id
          AND a.id > b.id
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_dlq_one_duplicate_per_event
        ON dlq (org_id, transaction_id)
        WHERE reason = 'DUPLICATE'
    """)


def downgrade() -> None:
    # The deleted rows were audit-only copies and are not restored.
    op.execute("DROP INDEX IF EXISTS uq_dlq_one_duplicate_per_event")
