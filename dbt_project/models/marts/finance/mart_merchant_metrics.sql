-- Merchant-level cohort/retention and LTV/CAC. Cohort = signup month
-- (dim_merchant.signup_date); a merchant is "active" in a given month if it
-- has at least one captured charge that month (mart_revenue_semantic).
--
-- LTV/CAC note (a deliberate talking point, not an oversight): CAC here is
-- a placeholder constant (var assumed_cac_usd) because this demo has no ad-
-- spend source to attribute real acquisition cost per merchant — cumulative
-- net revenue is a real accounting number, LTV/CAC against a placeholder
-- CAC is not a trustworthy product metric yet. In production this joins to
-- a marketing-spend mart instead of the var.

{% set assumed_cac_usd = var('assumed_cac_usd', 150) %}

with merchants as (

    select * from {{ ref('dim_merchant') }}

),

revenue as (

    select * from {{ ref('mart_revenue_semantic') }}

),

cohort as (

    select
        merchant_id,
        date_trunc('month', signup_date) as cohort_month
    from merchants

),

merchant_activity as (

    select
        r.merchant_id,
        c.cohort_month,
        r.period_month,
        datediff('month', c.cohort_month, r.period_month)
            as months_since_signup,
        r.gmv,
        r.net_revenue_usd
    from revenue as r
    inner join cohort as c on r.merchant_id = c.merchant_id

),

lifetime as (

    select
        merchant_id,
        min(cohort_month) as cohort_month,
        sum(net_revenue_usd) as lifetime_net_revenue_usd,
        count(distinct period_month) as active_months,
        max(period_month) as last_active_month
    from merchant_activity
    group by 1

)

select
    m.merchant_id,
    m.merchant_name,
    m.category,
    m.status,
    l.cohort_month,
    l.active_months,
    l.last_active_month,
    coalesce(l.lifetime_net_revenue_usd, 0) as lifetime_net_revenue_usd,
    {{ assumed_cac_usd }} as assumed_cac_usd,
    case
        when {{ assumed_cac_usd }} > 0
            then
                round(
                    coalesce(l.lifetime_net_revenue_usd, 0)
                    / {{ assumed_cac_usd }},
                    2
                )
    end as ltv_to_cac_ratio_placeholder
from merchants as m
left join lifetime as l on m.merchant_id = l.merchant_id
