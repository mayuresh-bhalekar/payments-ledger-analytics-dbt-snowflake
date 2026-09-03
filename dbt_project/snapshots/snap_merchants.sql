{% snapshot snap_merchants %}

{{
    config(
        target_schema='snapshots',
        unique_key='merchant_id',
        strategy='timestamp',
        updated_at='source_last_modified',
        invalidate_hard_deletes=True,
    )
}}

-- SCD Type 2 on the merchant record: captures status changes over time
-- (active -> suspended -> churned) so historical ledger entries can be
-- joined to "the merchant status that was true as of that transaction
-- date" instead of only the current state — required to correctly explain
-- a past period's chargeback/bad-debt numbers for a merchant that has
-- since been suspended. Run `dbt snapshot` on a schedule immediately after
-- each ingestion cycle.

    select * from {{ ref('stg_payments__merchants') }}

{% endsnapshot %}
