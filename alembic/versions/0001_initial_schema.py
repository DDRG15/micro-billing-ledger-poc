"""initial_schema

Revision ID: 0001
Revises:
Create Date: 2026-05-23

Creates ledger, outbox, and dlq tables with all indexes and constraints.
Replaces the schema previously managed by _bootstrap() in ledger.py.
"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001"
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE IF NOT EXISTS ledger (
            transaction_id  TEXT        PRIMARY KEY,
            event_type      TEXT        NOT NULL,
            customer_id     TEXT        NOT NULL,
            amount_cents    BIGINT      NOT NULL,
            currency        TEXT        NOT NULL DEFAULT 'usd',
            status          TEXT        NOT NULL,
            idempotency_key TEXT        NOT NULL,
            payload         TEXT        NOT NULL CHECK (length(payload) < 50000),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS outbox (
            id              BIGSERIAL   PRIMARY KEY,
            transaction_id  TEXT        NOT NULL
                            REFERENCES ledger(transaction_id) ON DELETE CASCADE,
            event_type      TEXT        NOT NULL,
            payload         TEXT        NOT NULL,
            dispatched      INTEGER     NOT NULL DEFAULT 0,
            dispatched_at   TIMESTAMPTZ,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS dlq (
            id              BIGSERIAL   PRIMARY KEY,
            transaction_id  TEXT        NOT NULL,
            reason          TEXT        NOT NULL,
            raw_payload     TEXT        NOT NULL,
            received_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_ledger_customer_id
        ON ledger(customer_id)
    """)

    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_outbox_dispatched_id
        ON outbox(dispatched, id)
        WHERE dispatched = 0
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_outbox_dispatched_id")
    op.execute("DROP INDEX IF EXISTS idx_ledger_customer_id")
    op.execute("DROP TABLE IF EXISTS dlq")
    op.execute("DROP TABLE IF EXISTS outbox")
    op.execute("DROP TABLE IF EXISTS ledger")
