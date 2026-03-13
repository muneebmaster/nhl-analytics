with skaters as (
    select * from {{ ref('stg_skaters_game_by_game') }}
),

with_rates as (
    select
        *,

        -- per-60 rate stats
        case
            when ice_time_seconds > 0
            then points / (ice_time_seconds / 3600.0)
            else null
        end                                                 as points_per_60,

        case
            when ice_time_seconds > 0
            then goals / (ice_time_seconds / 3600.0)
            else null
        end                                                 as goals_per_60,

        case
            when ice_time_seconds > 0
            then xgoals / (ice_time_seconds / 3600.0)
            else null
        end                                                 as xgoals_per_60,

        -- game score tier classification
        case
            when game_score >= 3      then 'Elite'
            when game_score >= 1.5    then 'Good'
            when game_score >= 0      then 'Average'
            else                           'Below Average'
        end                                                 as game_score_tier

    from skaters
)

select * from with_rates
