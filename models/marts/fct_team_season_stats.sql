with teams as (
    select * from {{ ref('stg_teams_season_summary') }}
),

filtered as (
    select * from teams
    where situation = 'all'
),

with_rates as (
    select
        *,

        -- per-game averages
        case
            when games_played > 0
            then goals_for::float / games_played
            else null
        end                                                 as goals_per_game,

        case
            when games_played > 0
            then goals_against::float / games_played
            else null
        end                                                 as goals_against_per_game,

        case
            when games_played > 0
            then xgoals_for / games_played
            else null
        end                                                 as xgoals_for_per_game,

        case
            when games_played > 0
            then xgoals_against / games_played
            else null
        end                                                 as xgoals_against_per_game,

        -- xgoal differential per 60 minutes
        -- divide total ice time by 2 to approximate even-strength / full-game context
        case
            when ice_time_seconds > 0
            then (xgoals_for - xgoals_against) / (ice_time_seconds / 3600.0 / 2.0)
            else null
        end                                                 as xgoal_diff_per_60

    from filtered
)

select * from with_rates
