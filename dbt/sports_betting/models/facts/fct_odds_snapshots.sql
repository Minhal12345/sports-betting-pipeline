{{ config(
    materialized="incremental",
    unique_key="snapshot_id",
    incremental_strategy="merge",
) }}

select
    md5(
        concat_ws(
            '|',
            game_id,
            bookmaker_key,
            team,
            cast(snapshot_time as string),
            market_key
        )
    ) as snapshot_id,
    game_id,
    home_team,
    away_team,
    bookmaker_key,
    team,
    odds,
    snapshot_time,
    commence_time
from {{ ref("stg_odds_snapshots") }}

{% if is_incremental() %}
where snapshot_time > (select coalesce(max(snapshot_time), to_timestamp('1970-01-01')) from {{ this }})
{% endif %}
