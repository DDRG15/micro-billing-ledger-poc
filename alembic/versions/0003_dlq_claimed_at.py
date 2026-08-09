"""dlq_claimed_at

Revision ID: 0003
Revises: 0002
Create Date: 2026-08-09

Adds claim-timestamp tracking to the dlq table so a stalled retry can be recovered.

retry_dlq_batch claims rows by setting status='retrying' and COMMITS that claim
before processing them one by one — it has to, because process_stripe_event opens
its own transaction. If the process dies, or the recovery UPDATE itself fails
(likely: the outage that caused the transient failure is often still going), the
claimed rows keep status='retrying' forever. The claim query only ever selected
status='pending', so nothing would look at them again — not stuck-and-retried,
but stuck-and-invisible, counted as neither pending nor resolved nor exhausted.

claimed_at records when the claim was taken. The claim query now also picks up
'retrying' rows whose claim has gone stale, so the system recovers on its own,
including rows already stranded by a crash that happened before this migration
(their claimed_at is NULL, which is treated as infinitely stale).
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0003"
down_revision: Union[str, Sequence[str], None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE dlq
        ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ
    """)
    # Covers the stale-claim half of the retry_dlq_batch WHERE clause:
    #     status='retrying' AND (claimed_at IS NULL OR claimed_at < NOW() - interval).
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_dlq_stale_claim
        ON dlq(status, claimed_at)
        WHERE status = 'retrying'
    """)


def downgrade() -> None:
    # Release any live claims first: without claimed_at the reclaim path is gone,
    #     so leaving rows in 'retrying' would strand them permanently again.
    op.execute("UPDATE dlq SET status = 'pending' WHERE status = 'retrying'")
    op.execute("DROP INDEX IF EXISTS idx_dlq_stale_claim")
    op.execute("ALTER TABLE dlq DROP COLUMN IF EXISTS claimed_at")
