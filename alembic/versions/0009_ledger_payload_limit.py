"""ledger_payload_limit

Revision ID: 0009
Revises: 0008
Create Date: 2026-11-09

The ledger's payload limit rises from 50,000 characters to 8 MiB.

WHY. CHECK (length(payload) < 50000) refused real events: a signed invoice.paid with
    forty line items (60 KB) was answered 200, dead-lettered as INVALID with
    max_retries 0, and a manual replay hit the same CHECK, so a $4,999 payment never
    reached the ledger and Stripe never retried it. The limit was meant to stop
    oversized input, which is now refused at the edge: Caddy and the app cap request
    bodies at 1 MiB. 8 MiB is above the worst case a 1 MiB body can serialize to
    (json.dumps escapes a non-ASCII character as up to six ASCII ones), so no body the
    edge accepts can be refused here.

NOT VALID skips re-checking existing rows, all of which satisfy the stricter old limit.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0009"
down_revision: Union[str, Sequence[str], None] = "0008"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS ledger_payload_check")
    op.execute("ALTER TABLE ledger ADD CONSTRAINT ledger_payload_check "
               "CHECK (length(payload) <= 8388608) NOT VALID")


def downgrade() -> None:
    # NOT VALID: rows written under the 8 MiB limit stay; new rows meet the old one.
    op.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS ledger_payload_check")
    op.execute("ALTER TABLE ledger ADD CONSTRAINT ledger_payload_check "
               "CHECK (length(payload) < 50000) NOT VALID")
