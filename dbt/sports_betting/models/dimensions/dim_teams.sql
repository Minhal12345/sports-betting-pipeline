{{ config(materialized="table") }}

{# SCD Type 2: one row per (sport_key, team_name) with validity columns; expand incremental merge when renames are tracked. #}
with team_lines as (
    select sport_key, home_team as team_name, snapshot_time
    from {{ ref("stg_odds_snapshots") }}
    union all
    select sport_key, away_team as team_name, snapshot_time
    from {{ ref("stg_odds_snapshots") }}
),

versions as (
    select
        sport_key,
        team_name,
        min(snapshot_time) as valid_from
    from team_lines
    group by sport_key, team_name
)

select
    md5(concat_ws('|', coalesce(team_name, ''), coalesce(sport_key, ''))) as team_key,
    team_name,
    sport_key,
    valid_from,
    cast(null as timestamp) as valid_to,
    true as is_current
from versions
