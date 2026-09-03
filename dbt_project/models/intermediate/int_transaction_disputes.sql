-- One row per transaction that has ever had a dispute filed against it,
-- carrying the settlement status it needs to distinguish "chargeback that
-- reversed an un-paid-out charge" from "chargeback lost after the merchant
-- was already paid" (bad debt) — see macros/categorize_revenue.sql.

with disputes as (

    select * from {{ ref('stg_payments__disputes') }}

),

transactions_settled as (

    select * from {{ ref('int_transactions_settled') }}

),

joined as (

    select
        d.dispute_id,
        d.transaction_id,
        d.reason,
        d.status as dispute_status,
        {{ cents_to_amount('d.amount_cents') }} as dispute_amount,
        d.opened_at,
        d.resolved_at,
        t.merchant_id,
        t.processor_id,
        t.settlement_status,
        t.settled_amount_usd
    from disputes as d
    inner join transactions_settled as t on d.transaction_id = t.transaction_id

)

select * from joined
