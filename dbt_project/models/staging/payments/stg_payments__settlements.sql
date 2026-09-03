with source as (

    select * from {{ ref('payments_settlements') }}

),

renamed as (

    select
        settlement_id,
        transaction_id,
        settled_amount_cents,
        settlement_currency,
        fx_rate_applied,
        settled_at,
        payout_id

    from source

)

select * from renamed
