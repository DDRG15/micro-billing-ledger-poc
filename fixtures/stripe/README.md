# Stripe event fixtures

Five webhook events in the shape Stripe documents, used by `test_ledger.py` (section
C6-4) as raw request bodies:

| File | Type | Notes |
|---|---|---|
| `customer.subscription.created.json` | Subscription | `request` carries an id and an idempotency key (API-initiated) |
| `customer.subscription.updated.json` | Subscription | Automatic renewal: `request: {"id": null, "idempotency_key": null}`; `data.previous_attributes` set |
| `customer.subscription.deleted.json` | Subscription | `status: canceled`, `canceled_at` / `ended_at` set |
| `invoice.paid.json` | Invoice | `amount_paid` 1000, automatic event |
| `invoice.payment_failed.json` | Invoice | `amount_paid` 0, `amount_due` 1000, `next_payment_attempt` set |

**Source.** Built on 2026-10-03 from the sample objects in Stripe's API reference
(Event, Subscription and Invoice object pages on docs.stripe.com). The documented field
sets were kept: nothing trimmed, nothing added that the reference does not list. Ids,
customers, timestamps and per-type fields (status, amounts, cancellation) were changed
so each file is distinct.

**What they establish.** A Subscription object has no top-level `amount`, `amount_paid`,
`amount_due` or `total`; prices live in `items.data[].price.unit_amount`. Earlier test
fixtures gave subscriptions a top-level `amount`, which is how every real subscription
event could fail validation while the suite passed.

**Known mismatch.** `api_version` is `"2019-02-19"`, copied from the Event reference's
own sample, while the object shapes follow the current reference (for example
`items.data[].current_period_end`). Nothing in this codebase branches on
`api_version`. Replace these with events captured from a Stripe test-mode account
(`stripe trigger customer.subscription.updated`) when one is available.
