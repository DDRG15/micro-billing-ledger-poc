"""dlq_retry_columns

Revision ID: 0002
Revises: 0001
Create Date: 2026-05-23

Adds retry tracking columns to the dlq table.
- status:        'pending' | 'retrying' | 'resolved' | 'exhausted'
- retry_count:   how many retry attempts have been made
- max_retries:   ceiling — entries with retry_count >= max_retries are marked exhausted
- next_retry_at: when the next retry attempt is allowed (NULL = not scheduled)
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002"
down_revision: Union[str, Sequence[str], None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE dlq
        ADD COLUMN IF NOT EXISTS status        TEXT        NOT NULL DEFAULT 'pending',
        ADD COLUMN IF NOT EXISTS retry_count   INTEGER     NOT NULL DEFAULT 0,
        ADD COLUMN IF NOT EXISTS max_retries   INTEGER     NOT NULL DEFAULT 3,
        ADD COLUMN IF NOT EXISTS next_retry_at TIMESTAMPTZ
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_dlq_retry
        ON dlq(status, next_retry_at)
        WHERE status = 'pending'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_dlq_retry")
    op.execute("""
        ALTER TABLE dlq
        DROP COLUMN IF EXISTS next_retry_at,
        DROP COLUMN IF EXISTS max_retries,
        DROP COLUMN IF EXISTS retry_count,
        DROP COLUMN IF EXISTS status
    """)
