select
    processor_id,
    processor_name,
    settlement_currency,
    payout_delay_days
from {{ ref('stg_payments__processors') }}
