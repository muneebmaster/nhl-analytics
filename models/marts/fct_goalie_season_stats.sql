with goalie_performance as (
    select * from {{ ref('int_goalie_performance') }}
),

with_rates as (
    select
        *,

        -- goals saved above expected per 60 minutes
        case
            when ice_time_seconds > 0
            then goals_saved_above_expected / (ice_time_seconds / 3600.0)
            else null
        end                                                 as goals_saved_above_expected_per_60,

        -- saves per 60
        case
            when ice_time_seconds > 0
            then saves / (ice_time_seconds / 3600.0)
            else null
        end                                                 as saves_per_60,

        -- high danger save pct
        case
            when high_danger_shots_against > 0
            then 1.0 - (high_danger_goals_against::float
                        / high_danger_shots_against::float)
            else null
        end                                                 as high_danger_save_pct

    from goalie_performance
)

select * from with_rates
