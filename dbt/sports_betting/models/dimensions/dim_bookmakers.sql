{{ config(materialized="table") }}

{# SCD Type 1: latest bookmaker_title wins per bookmaker_key. #}
with ranked as (
    select
        bookmaker_key,
        bookmaker_title,
        snapshot_time,
        row_number() over (
            partition by bookmaker_key
            order by snapshot_time desc
        ) as rn
    from {{ ref("stg_odds_snapshots") }}
)

select
    bookmaker_key,
    bookmaker_title,
    true as is_active
from ranked
where rn = 1
