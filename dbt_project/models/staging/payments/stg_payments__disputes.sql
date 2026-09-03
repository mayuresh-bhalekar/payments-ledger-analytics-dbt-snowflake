with source as (

    select * from {{ ref('payments_disputes') }}

),

renamed as (

    select
        dispute_id,
        transaction_id,
        reason,
        status,
        amount_cents,
        opened_at,
        resolved_at

    from source

)

select * from renamed
