with source as (
    select * from {{ source('moneypuck', 'raw_goalies_season_summary') }}
),

renamed as (
    select
        -- identifiers
        playerId::int                                       as player_id,
        season::int                                         as season,
        name                                                as player_name,
        team,
        position,
        situation,

        -- game counts and ice time
        games_played::int                                   as games_played,
        icetime::float                                      as ice_time_seconds,

        -- goals and saves
        xGoals::float                                       as xgoals_against,
        goals::int                                          as goals_against,
        unblocked_shot_attempts::int                        as shots_against,
        ongoal::int                                         as shots_on_goal_against,
        (ongoal::int - goals::int)                          as saves,
        case
            when ongoal::int > 0
            then 1.0 - (goals::float / ongoal::float)
            else null
        end                                                 as save_pct,

        -- quality of shots against
        lowDangerShots::int                                 as low_danger_shots_against,
        mediumDangerShots::int                              as medium_danger_shots_against,
        highDangerShots::int                                as high_danger_shots_against,
        lowDangerGoals::int                                 as low_danger_goals_against,
        mediumDangerGoals::int                              as medium_danger_goals_against,
        highDangerGoals::int                                as high_danger_goals_against,

        -- expected saves
        flurryAdjustedxGoals::float                         as flurry_adjusted_xgoals,
        xRebounds::float                                    as xrebounds,
        rebounds::int                                       as rebounds,
        xFreeze::float                                      as xfreeze,
        freeze::int                                         as freeze,
        xOnGoal::float                                      as x_on_goal,

        -- discipline
        penalityMinutes::int                                as penalty_minutes,
        penalties::int                                      as penalties_taken,
        blocked_shot_attempts::int                          as blocked_shot_attempts

    from source
)

select * from renamed
