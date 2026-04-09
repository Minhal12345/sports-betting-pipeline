{{ config(materialized="table") }}

with spine as (
    select explode(
        sequence(
            to_date('2025-01-01'),
            to_date('2027-12-31'),
            interval 1 day
        )
    ) as date_day
)

select
    date_day,
    dayofweek(date_day) as day_of_week,
    month(date_day) as month,
    quarter(date_day) as quarter,
    year(date_day) as year
from spine
