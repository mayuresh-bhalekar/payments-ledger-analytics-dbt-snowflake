{#- Date spine spanning the seed data's activity window with headroom on
    both sides; widen the bounds if the seed data range changes. -#}
with spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2026-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}

)

select
    cast(date_day as date) as date_day,
    year(date_day) as year_number,
    month(date_day) as month_number,
    to_char(date_day, 'YYYY-MM') as year_month,
    quarter(date_day) as quarter_number,
    dayofweek(date_day) as day_of_week_number,
    coalesce(dayofweek(date_day) in (0, 6), false)
        as is_weekend
from spine
