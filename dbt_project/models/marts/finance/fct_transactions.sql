-- Transaction-grain fact. This is the GMV/GTV source: sum(gross_amount_usd)
-- where transaction_type = 'charge' is Whop's Gross Merchandise/Transaction
-- Volume definition, and fee_amount_usd is the processor cost line that
-- take-rate and contribution-margin metrics subtract in mart_revenue_semantic.

select
    transaction_id,
    merchant_id,
    processor_id,
    transaction_type,
    transaction_status,
    txn_currency,
    created_at,
    date(created_at) as transaction_date,
    gross_amount_usd,
    fee_amount_usd,
    net_amount_usd,
    settlement_status,
    settled_at,
    settled_amount_usd
from {{ ref('int_transactions_settled') }}
