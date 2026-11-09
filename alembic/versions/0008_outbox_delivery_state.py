"""outbox_delivery_state

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-06

Outbox rows carry their own delivery state. Cycle 6, items 6a and 6b.

WHY. drain_batch selected a batch FOR UPDATE and dispatched it over HTTP inside that
    transaction: 100 rows x a 10 s timeout held row locks for up to ~17 minutes. A failed
    dispatch left the row exactly as it was, so it was retried on every poll; a tenant
    whose downstream kept failing kept its rows at the head of the queue, and with more
    than one window of them (batch x 10) no other tenant's rows were ever selected.

WHAT.
    attempts         failed dispatches so far
    next_attempt_at  not before this time (backoff after a failure); NULL = now
    claimed_until    a worker's lease; other workers skip the row until it expires
    last_error       the last dispatch failure, truncated
    idx_outbox_pending_by_org  (org_id, id) WHERE dispatched = 0 — the per-tenant
                     candidate lookup the claim query runs once per tenant.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0008"
down_revision: Union[str, Sequence[str], None] = "0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0")
    op.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ")
    op.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS claimed_until TIMESTAMPTZ")
    op.execute("ALTER TABLE outbox ADD COLUMN IF NOT EXISTS last_error TEXT")
    op.execute("CREATE INDEX IF NOT EXISTS idx_outbox_pending_by_org "
               "ON outbox (org_id, id) WHERE dispatched = 0")


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_outbox_pending_by_org")
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS last_error")
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS claimed_until")
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS next_attempt_at")
    op.execute("ALTER TABLE outbox DROP COLUMN IF EXISTS attempts")
