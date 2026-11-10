"""dlq_duplicate_rows_resolved

Revision ID: 0013
Revises: 0012
Create Date: 2026-11-10

DUPLICATE dead-letter rows are resolved, not pending.

WHY. A DUPLICATE row records Stripe redelivering an event the ledger already holds. It was
    written with status 'pending' and max_retries 0, so the retry worker never selected it
    and nothing ever moved it, while dlq_depth, documented as the rows still needing action,
    counted it. Every redelivery Stripe makes raised the alarm by one, for good, with nothing
    wrong. The application now
    writes DUPLICATE rows as 'resolved', like UNSUPPORTED and DUPLICATE_PAYMENT; this
    revision settles the rows written before it.

Only 'pending' rows change. An 'exhausted' DUPLICATE row exists only because an operator
    replayed it and the replay failed, which is worth a look, so it stays where it is.
    claimed_at is cleared with the status, as every other resolving write does.

set_config('app.org_id', '*') puts the update under the cross-tenant scope of revision
    0006's policy, as in revision 0010.

Downgrade changes no data: which rows were 'pending' before this revision is not recorded,
    and putting resolved audit rows back into dlq_depth would only restore the false alarm.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0013"
down_revision: Union[str, Sequence[str], None] = "0012"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("SELECT set_config('app.org_id', '*', true)")
    op.execute("""
        UPDATE dlq SET status = 'resolved', claimed_at = NULL
        WHERE reason = 'DUPLICATE' AND status = 'pending'
    """)


def downgrade() -> None:
    # Data only, and not reversible: see the module docstring.
    pass
