-- Grain: one row per transaction, left-joined out to its settlement (if any)
-- and translated to the USD reporting currency at both the transaction date
-- (spot rate when the charge was authorized) and the settlement date (spot
-- rate when cash actually moved) — the gap between the two is FX gain/loss,
-- computed downstream in marts/ledger/fct_ledger_entries.sql.

with transactions as (

    select * from {{ ref('stg_payments__transactions') }}

),

settlements as (

    select * from {{ ref('stg_payments__settlements') }}

),

processors as (

    select * from {{ ref('stg_payments__processors') }}

),

fx_rates as (

    select * from {{ ref('stg_payments__fx_rates') }}

),

txn_fx as (

    select
        t.transaction_id,
        coalesce(fx.usd_rate, 1.0) as txn_usd_rate
    from transactions as t
    left join fx_rates as fx
        on
            t.currency = fx.currency
            and fx.rate_date = date(t.created_at)

),

settled_fx as (

    select
        s.settlement_id,
        coalesce(fx.usd_rate, 1.0) as settlement_usd_rate
    from settlements as s
    left join fx_rates as fx
        on
            s.settlement_currency = fx.currency
            and fx.rate_date = date(s.settled_at)

),

joined as (

    select
        t.transaction_id,
        t.merchant_id,
        t.processor_id,
        p.processor_name,
        t.transaction_type,
        t.status as transaction_status,
        t.currency as txn_currency,
        t.created_at,
        {{ cents_to_amount('t.gross_amount_cents') }} as gross_amount,
        {{ cents_to_amount('t.gross_amount_cents') }}
        * txn_fx.txn_usd_rate as gross_amount_usd,
        {{ cents_to_amount('t.fee_amount_cents') }} as fee_amount,
        {{ cents_to_amount('t.fee_amount_cents') }}
        * txn_fx.txn_usd_rate as fee_amount_usd,
        {{ cents_to_amount('t.net_amount_cents') }} as net_amount,
        {{ cents_to_amount('t.net_amount_cents') }}
        * txn_fx.txn_usd_rate as net_amount_usd,

        s.settlement_id,
        s.settled_at,
        s.settlement_currency,
        s.payout_id,
        case
            when s.settlement_id is not null then 'settled' else 'unsettled'
        end as settlement_status,
        {{ cents_to_amount('s.settled_amount_cents') }} as settled_amount,
        {{ cents_to_amount('s.settled_amount_cents') }}
        * settled_fx.settlement_usd_rate as settled_amount_usd

    from transactions as t
    inner join processors as p on t.processor_id = p.processor_id
    inner join txn_fx on t.transaction_id = txn_fx.transaction_id
    left join settlements as s on t.transaction_id = s.transaction_id
    left join settled_fx on s.settlement_id = settled_fx.settlement_id

)

select * from joined
