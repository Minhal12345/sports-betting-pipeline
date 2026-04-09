{{ config(materialized="view") }}

{# Sample rows until wired to {{ source('bronze_odds', 'odds_snapshots') }} or a seed. #}
with raw as (
    select * from values
        (
            'nba-2025-001',
            'basketball_nba',
            'NBA',
            cast('2025-01-15 19:30:00' as timestamp),
            'Los Angeles Lakers',
            'Boston Celtics',
            'draftkings',
            'DraftKings US',
            'h2h',
            'Los Angeles Lakers',
            1.91,
            cast('2025-01-15 12:00:00' as timestamp)
        ),
        (
            'nba-2025-001',
            'basketball_nba',
            'NBA',
            cast('2025-01-15 19:30:00' as timestamp),
            'Los Angeles Lakers',
            'Boston Celtics',
            'draftkings',
            'DraftKings US',
            'h2h',
            'Boston Celtics',
            2.05,
            cast('2025-01-15 12:00:00' as timestamp)
        ),
        (
            'nba-2025-002',
            'basketball_nba',
            'NBA',
            cast('2025-01-16 20:00:00' as timestamp),
            'Golden State Warriors',
            'Phoenix Suns',
            'fanduel',
            'FanDuel',
            'h2h',
            'Golden State Warriors',
            2.10,
            cast('2025-01-16 08:15:00' as timestamp)
        ),
        (
            'nba-2025-002',
            'basketball_nba',
            'NBA',
            cast('2025-01-16 20:00:00' as timestamp),
            'Golden State Warriors',
            'Phoenix Suns',
            'fanduel',
            'FanDuel',
            'h2h',
            'Phoenix Suns',
            1.87,
            cast('2025-01-16 08:15:00' as timestamp)
        )
        as data(
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
            odds,
            snapshot_time
        )
),

numbered as (
    select
        row_number() over (
            order by game_id, bookmaker_key, team, snapshot_time
        ) as stg_odds_snapshot_id,
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
        odds,
        snapshot_time
    from raw
)

select * from numbered
