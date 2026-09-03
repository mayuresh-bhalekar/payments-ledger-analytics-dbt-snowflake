select
    merchant_id,
    merchant_name,
    merchant_email_masked,
    category,
    country,
    signup_date,
    status
from {{ ref('stg_payments__merchants') }}
