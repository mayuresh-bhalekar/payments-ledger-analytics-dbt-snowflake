with source as (

    select * from {{ ref('payments_processors') }}

),

renamed as (

    select
        processor_id,
        processor_name,
        settlement_currency,
        payout_delay_days

    from source

)

select * from renamed
