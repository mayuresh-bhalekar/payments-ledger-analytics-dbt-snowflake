-- Dispute-grain fact backing chargeback/dispute-rate metrics per merchant
-- and per processor (mart_revenue_semantic joins back to this for
-- chargeback_rate).

select
    dispute_id,
    transaction_id,
    merchant_id,
    processor_id,
    reason,
    dispute_status,
    dispute_amount,
    opened_at,
    resolved_at,
    datediff('day', opened_at, resolved_at) as days_to_resolve,
    settlement_status as transaction_settlement_status
from {{ ref('int_transaction_disputes') }}
