-- The financial semantic layer: one row per merchant per month, every
-- metric defined exactly once here so Finance, Product, and GTM all pull
-- the same number for "revenue" or "take rate" out of Metabase instead of
-- three dashboards quietly disagreeing.
--
--   gmv                 sum of captured charge volume (Gross Merchandise Value)
--   refunds_usd         charge volume returned to buyers
--   bad_debt_writeoff_usd  chargebacks lost after payout (see macros/)
--   net_revenue_usd     gmv + refunds_usd - bad_debt_writeoff_usd
--   processor_cost_usd  fees paid to Stripe/PayPal/Adyen/Braintree (COGS)
--   gross_profit_usd    net_revenue_usd - processor_cost_usd
--   take_rate           processor_cost_usd / gmv
--   contribution_margin_pct  gross_profit_usd / net_revenue_usd
--   chargeback_rate     disputed transaction count / total transaction count

with transactions as (

    select * from {{ ref('fct_transactions') }}

),

ledger as (

    select * from {{ ref('fct_ledger_entries') }}

),

disputes as (

    select * from {{ ref('fct_disputes') }}

),

txn_monthly as (

    select
        merchant_id,
        date_trunc('month', created_at) as period_month,
        sum(
            case
                when
                    transaction_type = 'charge'
                    and transaction_status != 'failed'
                    then gross_amount_usd
                else 0
            end
        ) as gmv,
        sum(
            case
                when transaction_type = 'refund' then gross_amount_usd else 0
            end
        ) as refunds_usd,
        sum(
            case
                when
                    transaction_type = 'charge'
                    and transaction_status != 'failed'
                    then fee_amount_usd
                else 0
            end
        ) as processor_cost_usd,
        count(case when transaction_type = 'charge' then 1 end) as charge_count
    from transactions
    group by 1, 2

),

bad_debt_monthly as (

    select
        merchant_id,
        date_trunc('month', transaction_at) as period_month,
        sum(bad_debt_writeoff_usd) as bad_debt_writeoff_usd,
        sum(fx_gain_loss_usd) as fx_gain_loss_usd
    from ledger
    group by 1, 2

),

dispute_monthly as (

    select
        merchant_id,
        date_trunc('month', opened_at) as period_month,
        count(*) as disputed_transaction_count
    from disputes
    group by 1, 2

),

joined as (

    select
        t.merchant_id,
        t.period_month,
        t.gmv,
        t.refunds_usd,
        coalesce(bd.bad_debt_writeoff_usd, 0) as bad_debt_writeoff_usd,
        coalesce(bd.fx_gain_loss_usd, 0) as fx_gain_loss_usd,
        t.processor_cost_usd,
        t.charge_count,
        coalesce(d.disputed_transaction_count, 0) as disputed_transaction_count,

        t.gmv
        + t.refunds_usd
        - coalesce(bd.bad_debt_writeoff_usd, 0) as net_revenue_usd

    from txn_monthly as t
    left join bad_debt_monthly as bd
        on t.merchant_id = bd.merchant_id and t.period_month = bd.period_month
    left join dispute_monthly as d
        on t.merchant_id = d.merchant_id and t.period_month = d.period_month

),

final as (

    select
        *,
        net_revenue_usd - processor_cost_usd as gross_profit_usd,
        case when gmv > 0 then round(processor_cost_usd / gmv, 4) end
            as take_rate,
        case
            when
                net_revenue_usd > 0
                then
                    round(
                        (net_revenue_usd - processor_cost_usd)
                        / net_revenue_usd,
                        4
                    )
        end as contribution_margin_pct,
        case
            when
                charge_count > 0
                then round(disputed_transaction_count::float / charge_count, 4)
        end as chargeback_rate
    from joined

)

select * from final
