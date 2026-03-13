with source as (
    select * from {{ source('moneypuck', 'raw_goalies_game_by_game') }}
),

renamed as (
    select
        -- identifiers
        playerId::int                                       as player_id,
        name                                                as player_name,
        gameId::int                                         as game_id,
        season::int                                         as season,
        playerTeam                                          as team,
        opposingTeam                                        as opposing_team,
        home_or_away,
        TO_DATE(gameDate::varchar, 'YYYYMMDD')              as game_date,
        situation,

        -- ice time
        icetime::float                                      as ice_time_seconds,

        -- goals and saves
        xGoals::float                                       as xgoals_against,
        goals::int                                          as goals_against,
        unblocked_shot_attempts::int                        as shots_against,
        xOnGoal::float                                      as x_on_goal,
        ongoal::int                                         as shots_on_goal_against,
        (ongoal::int - goals::int)                          as saves,
        case
            when ongoal::int > 0
            then 1.0 - (goals::float / ongoal::float)
            else null
        end                                                 as save_pct,

        -- shot danger
        lowDangerGoals::int                                 as low_danger_goals_against,
        mediumDangerGoals::int                              as medium_danger_goals_against,
        highDangerGoals::int                                as high_danger_goals_against

    from source
)

select * from renamed
