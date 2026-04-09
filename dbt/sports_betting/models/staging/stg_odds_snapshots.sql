{{ config(materialized="view") }}

with source as (
    select * from {{ source("bronze_odds", "odds_snapshots") }}
),

final as (
    select
        game_id,
        sport_key,
        sport_title,
        commence_time,
        home_team,
        away_team,
        bookmaker_key,
        bookmaker_title,
        market_key,
        team,
        cast(odds as double) as odds,
        snapshot_time,
        md5(concat_ws('|', game_id, bookmaker_key, team, snapshot_time, market_key)) as stg_odds_snapshot_id
    from source
)

select * from final
