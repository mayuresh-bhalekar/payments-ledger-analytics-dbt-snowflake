with source as (

    select * from {{ ref('payments_transactions') }}

),

renamed as (

    select
        transaction_id,
        merchant_id,
        processor_id,
        transaction_type,
        gross_amount_cents,
        currency,
        fee_amount_cents,
        -- Net amount owed to the merchant before settlement/FX: charges are
        -- fee-negative, refunds mirror the sign, so this formula holds for
        -- both.
        gross_amount_cents - fee_amount_cents as net_amount_cents,
        status,
        created_at

    from source

)

select * from renamed
