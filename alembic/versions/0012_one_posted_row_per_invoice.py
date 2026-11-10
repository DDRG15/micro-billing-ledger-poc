"""one_posted_row_per_invoice

Revision ID: 0012
Revises: 0011
Create Date: 2026-11-10

A Stripe payment contributes at most one POSTED row per tenant, identified by the invoice
that represents it, not by the event that announces it.

WHY. The ledger's only key was (org_id, transaction_id), and transaction_id is the Stripe
    EVENT id. Stripe announces one payment with several events: in a test-mode run each
    paid invoice arrived as invoice.paid, invoice.payment_succeeded,
    payment_intent.succeeded and charge.succeeded, four distinct event ids. Only
    invoice.paid is recorded today, so nothing was counted twice, but nothing in the schema
    prevented it either: two invoice.paid events with different ids for one invoice (Stripe
    does not promise one per invoice) became two POSTED rows, two outbox deliveries and the
    same money counted twice, and the event key could not see it.

WHAT.
    ledger.invoice_id TEXT: data.object.id of an invoice event, NULL for a subscription
        row. Backfilled from the stored payload.
    uq_ledger_one_posted_per_invoice: UNIQUE (org_id, invoice_id) WHERE status = 'POSTED'.
        The application inserts with ON CONFLICT DO NOTHING (no target) and reports the
        skip as DUPLICATE_PAYMENT, so the outcome is 200, no second ledger row and no
        outbox row.
    ledger_invoice_rows_name_their_invoice: CHECK (status NOT IN ('POSTED','VOID') OR
        invoice_id IS NOT NULL). A unique index ignores NULLs, so without this a POSTED row
        with no invoice id would sit outside the index. NOT VALID, like revision 0007: rows
        written before this revision whose payload names no invoice are not rewritten;
        every new row is checked.
    uq_dlq_one_duplicate_payment_per_event: the trace row of a DUPLICATE_PAYMENT, one per
        (org_id, transaction_id), the rule revisions 0010 and 0011 apply to DUPLICATE and
        UNSUPPORTED.

VOID is not part of the key. Each invoice.payment_failed event is a failed attempt dunning
    acts on, and no field identifies an attempt more finely than its event: attempt_count
    counts automatic retries only, so two manual failures share it. VOID amounts are not
    added into any total the service exposes.

EXISTING DOUBLE COUNTS STOP THE UPGRADE. If the backfill finds two POSTED rows for one
    (org_id, invoice_id), the index cannot be built, and choosing which row is the truth
    is a finance decision, not a migration's. The upgrade raises with the number of
    invoices and the query that lists them; nothing is changed (the revision runs in one
    transaction).

set_config('app.org_id', '*') puts the backfill and the check under the cross-tenant scope
    of revision 0006's policy, for an owner role RLS still applies to (FORCE ROW LEVEL
    SECURITY); a superuser bypasses it either way. billing_app needs no new grant: its
    table-level privileges on ledger cover a column added later.
"""
from typing import Sequence, Union

from alembic import op

revision: str = "0012"
down_revision: Union[str, Sequence[str], None] = "0011"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("SELECT set_config('app.org_id', '*', true)")
    op.execute("ALTER TABLE ledger ADD COLUMN IF NOT EXISTS invoice_id TEXT")
    # CASE, not AND: SQL does not promise to evaluate an AND left to right, and a payload
    #     that is not JSON must leave the row NULL rather than abort the migration.
    op.execute("""
        UPDATE ledger SET invoice_id = CASE
            WHEN NOT pg_input_is_valid(payload, 'jsonb') THEN NULL
            WHEN jsonb_typeof(payload::jsonb #> '{data,object,id}') = 'string'
                THEN NULLIF(payload::jsonb #>> '{data,object,id}', '')
            END
        WHERE invoice_id IS NULL AND event_type LIKE 'invoice.%'
    """)
    op.execute("""
        DO $$
        DECLARE
            n_invoices integer;
            sample     text;
        BEGIN
            SELECT count(*), string_agg(org_id || '/' || invoice_id, ', ')
              INTO n_invoices, sample
              FROM (SELECT org_id, invoice_id FROM ledger
                     WHERE status = 'POSTED' AND invoice_id IS NOT NULL
                     GROUP BY org_id, invoice_id HAVING count(*) > 1
                     ORDER BY org_id, invoice_id) d;
            IF n_invoices > 0 THEN
                RAISE EXCEPTION 'Cannot upgrade to 0012: % invoice(s) already have more than one POSTED row (org/invoice: %). '
                    'The same payment is counted twice. Decide which row stands before upgrading; list them with: '
                    'SELECT org_id, invoice_id, transaction_id, amount_cents, created_at FROM ledger '
                    'WHERE status = ''POSTED'' AND (org_id, invoice_id) IN (SELECT org_id, invoice_id FROM ledger '
                    'WHERE status = ''POSTED'' GROUP BY 1, 2 HAVING count(*) > 1) ORDER BY 1, 2, 5',
                    n_invoices, left(sample, 500);
            END IF;
        END $$
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ledger_one_posted_per_invoice
        ON ledger (org_id, invoice_id)
        WHERE status = 'POSTED'
    """)
    # Guarded: a local run's _bootstrap() may have created it on a database still at 0011.
    op.execute("""
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_constraint
                           WHERE conname = 'ledger_invoice_rows_name_their_invoice') THEN
                ALTER TABLE ledger ADD CONSTRAINT ledger_invoice_rows_name_their_invoice
                    CHECK (status NOT IN ('POSTED', 'VOID') OR invoice_id IS NOT NULL) NOT VALID;
            END IF;
        END $$
    """)
    op.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_dlq_one_duplicate_payment_per_event
        ON dlq (org_id, transaction_id)
        WHERE reason = 'DUPLICATE_PAYMENT'
    """)
    # How many POSTED or VOID rows the backfill could not name, for the operator running it.
    op.execute("""
        DO $$
        DECLARE n integer;
        BEGIN
            SELECT count(*) INTO n FROM ledger
             WHERE status IN ('POSTED', 'VOID') AND invoice_id IS NULL;
            IF n > 0 THEN
                RAISE NOTICE '0012: % POSTED or VOID row(s) written before this revision name no invoice in their payload; they stay outside uq_ledger_one_posted_per_invoice', n;
            END IF;
        END $$
    """)


def downgrade() -> None:
    # invoice_id is derived from the payload, which stays, so dropping it loses nothing a
    #     re-upgrade cannot rebuild. DUPLICATE_PAYMENT rows in dlq are kept: they are
    #     resolved audit rows, and the earlier code lists them as plain text.
    op.execute("DROP INDEX IF EXISTS uq_dlq_one_duplicate_payment_per_event")
    op.execute("ALTER TABLE ledger DROP CONSTRAINT IF EXISTS ledger_invoice_rows_name_their_invoice")
    op.execute("DROP INDEX IF EXISTS uq_ledger_one_posted_per_invoice")
    op.execute("ALTER TABLE ledger DROP COLUMN IF EXISTS invoice_id")
