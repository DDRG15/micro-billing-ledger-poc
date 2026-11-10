"""dlq_one_unsupported_per_event

Revision ID: 0011
Revises: 0010
Create Date: 2026-11-09

At most one UNSUPPORTED dead-letter row per (org_id, transaction_id).

WHY. UNSUPPORTED is the reason code for well-formed Stripe events of a type the ledger
    does not record (charge.succeeded and the like). Such an event is answered 200, so
    Stripe does not redeliver it, but a signed body can be replayed within the 300 s
    tolerance, and without a key every replay would append another full-payload row
    (the DUPLICATE case migration 0010 closed). Same fix: a partial unique index, with
    the insert paths already using ON CONFLICT DO NOTHING.

No de-duplication step: no UNSUPPORTED rows exist before this revision. The reason is
    plain TEXT with no CHECK constraint, so the new value needs no schema change.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0011"
down_revision: Union[str, Sequence[str], None] = "0010"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_dlq_one_unsupported_per_event
        ON dlq (org_id, transaction_id)
        WHERE reason = 'UNSUPPORTED'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS uq_dlq_one_unsupported_per_event")
