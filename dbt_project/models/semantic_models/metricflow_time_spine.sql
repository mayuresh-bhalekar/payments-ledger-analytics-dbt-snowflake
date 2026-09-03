{{ config(materialized='table') }}

-- Required by the dbt Semantic Layer / MetricFlow: a day-grain calendar
-- spine every time-based metric query joins against. Same bounds and
-- pattern as marts/core/dim_date.sql (which this does not replace — that
-- one is the regular reporting calendar dimension; this one is
-- MetricFlow's own required input, by its documented naming convention).

with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}

)

select cast(date_day as date) as date_day
from spine
