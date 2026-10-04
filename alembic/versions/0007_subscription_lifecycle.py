"""subscription_lifecycle

Revision ID: 0007
Revises: 0006
Create Date: 2026-10-04

Subscription events are ledgered with no amount. Cycle 6, item 5d.

WHY. A Stripe Subscription object carries no top-level amount — prices live in
    items.data[].price — and the ledger demanded one from every event, so every real
    customer.subscription.* event was dead-lettered as INVALID and counted in dlq_depth.
    Synthesizing an amount from the items would post a list price as revenue next to
    the invoice.paid that actually collects it, and the figure is wrong for trials,
    metered and tiered prices, coupons and proration. A subscription row records that an
    account changed state. The money is on the invoice rows.

WHAT.
    amount_cents DROP NOT NULL — a subscription row stores NULL, not 0. Zero is an
        amount; NULL says there is none, and SUM() skips it.
    ledger_amount_matches_kind — NULL exactly when the event type is a subscription
        event. The application enforces the same split (ledger._LIFECYCLE_TYPES); the
        constraint is what still holds when some future code path forgets.

NOT VALID. The ledger is append-only, and this revision does not rewrite rows to satisfy
    a rule that did not exist when they were written. No real Stripe subscription event
    reached the ledger before it (every one failed validation), so a row that disagrees
    was written by a test or a non-Stripe caller. PostgreSQL enforces a NOT VALID check
    on every INSERT and UPDATE from now on; to list old rows that disagree, run
    `ALTER TABLE ledger VALIDATE CONSTRAINT ledger_amount_matches_kind`.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0007"
down_revision: Union[str, Sequence[str], None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ledger ALTER COLUMN amount_cents DROP NOT NULL")
    op.execute(
        "ALTER TABLE ledger ADD CONSTRAINT ledger_amount_matches_kind "
        "CHECK ((amount_cents IS NULL) = (event_type LIKE 'customer.subscription.%')) "
        "NOT VALID"
    )


def downgrade() -> None:
    # NOT NULL cannot come back while subscription rows hold NULL, and inventing a figure
    #     for them is the exact defect this revision removes. Refuse rather than guess.
    op.execute("""
        DO $$
        BEGIN
            IF EXISTS (SELECT 1 FROM ledger WHERE amount_cents IS NULL) THEN
                RAISE EXCEPTION 'Cannot downgrade 0007: the ledger holds subscription rows '
                                'with no amount. Decide what they become first.';
            END IF;
        END $$;
    """)
    op.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS ledger_amount_matches_kind")
    op.execute("ALTER TABLE ledger ALTER COLUMN amount_cents SET NOT NULL")
