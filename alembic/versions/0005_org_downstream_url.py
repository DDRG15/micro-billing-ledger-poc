"""org_downstream_url

Revision ID: 0005
Revises: 0004
Create Date: 2026-08-11

Adds org.downstream_url so each tenant's events are forwarded to that tenant's own
endpoint.

WHY THIS IS THE POINT OF TENANCY IN THE WORKER. Until now every tenant's events went
    to one global DOWNSTREAM_URL. That is not a data leak in the single-operator
    deployment this started as — one operator, one destination — but in a multi-tenant
    one it is the largest leak in the system: tenant A's billing events, customer ids
    and amounts delivered to tenant B's endpoint. Isolation that stops at the database
    and then hands everything to a shared destination is not isolation.

NULL means "use the process-wide DOWNSTREAM_URL", which keeps existing deployments
    working unchanged: nothing is set, so nothing changes. A tenant opts in by setting
    its own URL.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0005"
down_revision: Union[str, Sequence[str], None] = "0004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("ALTER TABLE org ADD COLUMN IF NOT EXISTS downstream_url TEXT")


def downgrade() -> None:
    op.execute("ALTER TABLE org DROP COLUMN IF EXISTS downstream_url")
