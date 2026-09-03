-- In production this selects from source('payments', 'payments_merchants'),
-- a RAW table synced by a merchant-service ingestion job (see
-- airflow/dags/payments_ledger_dag.py). The seed stands in as a static
-- replica so the whole project runs end to end without a live source system.
with source as (

    select * from {{ ref('payments_merchants') }}

),

renamed as (

    select
        merchant_id,
        merchant_name,
        {{ mask_pii('merchant_email') }} as merchant_email_masked,
        category,
        country,
        signup_date,
        status,
        source_last_modified

    from source

)

select * from renamed
