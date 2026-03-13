with source as (
    select * from {{ source('moneypuck', 'raw_shots') }}
),

renamed as (
    select
        -- identifiers
        shotID::int                             as shot_id,
        game_id::int                            as game_id,
        season::int                             as season,
        period::int                             as period,
        time::int                               as time_seconds,

        -- shooter info
        shooterPlayerId::int                    as shooter_player_id,
        shooterName                             as shooter_name,
        shooterLeftRight                        as shooter_hand,
        playerPositionThatDidEvent              as shooter_position,

        -- teams
        teamCode                                as team_code,
        homeTeamCode                            as home_team,
        awayTeamCode                            as away_team,
        isHomeTeam::boolean                     as is_home_team,
        isPlayoffGame::boolean                  as is_playoff,

        -- shot outcome
        event,
        goal::boolean                           as is_goal,
        shotType                                as shot_type,
        shotWasOnGoal::boolean                  as was_on_goal,

        -- shot geometry
        shotDistance::float                     as shot_distance,
        shotAngle::float                        as shot_angle,
        arenaAdjustedXCord::float               as x_cord_adjusted,
        arenaAdjustedYCord::float               as y_cord_adjusted,

        -- expected goals
        xGoal::float                            as xgoal,
        shotGoalProbability::float              as shot_goal_probability,

        -- shot context
        shotRebound::boolean                    as is_rebound,
        shotRush::boolean                       as is_rush,

        -- manpower
        homeSkatersOnIce::int                   as home_skaters,
        awaySkatersOnIce::int                   as away_skaters,

        -- game state
        homeTeamGoals::int                      as home_goals,
        awayTeamGoals::int                      as away_goals,

        -- goalie
        goalieIdForShot::int                    as goalie_id,
        goalieNameForShot                       as goalie_name,

        -- win probability
        homeWinProbability::float               as home_win_probability

    from source
)

select * from renamed
