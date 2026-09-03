{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        on_schema_change='append_new_columns'
    )
}}

-- The financial ledger, one row per transaction: gross revenue, refunds,
-- FX gain/loss, and bad-debt write-offs all live here as distinct,
-- auditable entries instead of being netted together upstream — so
-- "why did reported revenue move" always has a single-table answer, and
-- Finance can close the books straight off this table. Incremental on
-- transaction/settlement activity so a nightly run only re-derives entries
-- that actually moved (new charge, new settlement, a dispute resolving)
-- rather than recomputing the whole ledger.

with transactions_settled as (

    select * from {{ ref('int_transactions_settled') }}

),

disputes as (

    select * from {{ ref('int_transaction_disputes') }}

    {% if is_incremental() %}
    where coalesce(resolved_at, opened_at) > (select coalesce(max(dispute_resolved_at), '1900-01-01') from {{ this }})  -- noqa: LT05
    {% endif %}

),

-- one dispute row per transaction: a transaction can only be actively
-- disputed once at a time, so the latest-opened dispute wins
latest_dispute as (

    select
        transaction_id,
        dispute_status,
        opened_at as dispute_opened_at,
        resolved_at as dispute_resolved_at
    from disputes
    qualify
        row_number() over (
            partition by transaction_id order by opened_at desc
        ) = 1

),

ledger as (

    select
        t.transaction_id,
        t.merchant_id,
        t.processor_id,
        t.transaction_type,
        t.txn_currency,
        t.created_at as transaction_at,
        t.settlement_status,
        t.settled_at,
        t.payout_id,
        t.net_amount_usd as transaction_amount_usd,
        t.settled_amount_usd,
        ld.dispute_status,
        ld.dispute_resolved_at,

        {{ categorize_revenue(
            't.transaction_type',
            "coalesce(ld.dispute_status, 'none')",
            't.settlement_status'
        ) }} as revenue_category,

        case
            when t.settlement_status = 'settled'
                then {{ fx_gain_loss('t.net_amount_usd', 't.settled_amount_usd') }}  -- noqa: LT05
            else 0
        end as fx_gain_loss_usd

    from transactions_settled as t
    left join latest_dispute as ld on t.transaction_id = ld.transaction_id

    {% if is_incremental() %}
    where coalesce(t.settled_at, t.created_at) > (select coalesce(max(coalesce(settled_at, transaction_at)), '1900-01-01') from {{ this }})  -- noqa: LT05
        or t.transaction_id in (select transaction_id from latest_dispute)
    {% endif %}

),

final as (

    select
        *,
        {{ recognize_bad_debt('revenue_category', 'transaction_amount_usd') }}
            as bad_debt_writeoff_usd
    from ledger

)

select * from final
