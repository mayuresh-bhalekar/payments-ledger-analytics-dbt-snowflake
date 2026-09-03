-- Reconciliation guardrail: the ledger's total settled USD amount must tie
-- back to the raw settlement records (independently converted to USD here,
-- not re-using fct_ledger_entries' own math), within a cent of rounding
-- tolerance. A failure here means a join fanout, a dropped settlement, or a
-- broken FX join is silently misstating reported cash — exactly the class
-- of bug "make the reported numbers reconcile to source" is guarding against.

with settlements_recomputed as (

    select
        s.settlement_id,
        (s.settled_amount_cents / 100.0) * coalesce(fx.usd_rate, 1.0) as settled_amount_usd
    from {{ ref('stg_payments__settlements') }} s
    left join {{ ref('stg_payments__fx_rates') }} fx
        on fx.currency = s.settlement_currency
        and fx.rate_date = date(s.settled_at)

),

source_total as (
    select round(sum(settled_amount_usd), 2) as total_usd
    from settlements_recomputed
),

ledger_total as (
    select round(sum(settled_amount_usd), 2) as total_usd
    from {{ ref('fct_ledger_entries') }}
    where settlement_status = 'settled'
)

select
    source_total.total_usd as source_settled_usd,
    ledger_total.total_usd as ledger_settled_usd,
    abs(source_total.total_usd - ledger_total.total_usd) as diff_usd
from source_total
cross join ledger_total
where abs(source_total.total_usd - ledger_total.total_usd) > 0.01
