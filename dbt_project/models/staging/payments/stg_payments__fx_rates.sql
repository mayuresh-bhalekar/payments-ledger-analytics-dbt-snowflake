with source as (

    select * from {{ ref('payments_fx_rates') }}

),

renamed as (

    select
        currency,
        rate_date,
        usd_rate

    from source

)

select * from renamed
