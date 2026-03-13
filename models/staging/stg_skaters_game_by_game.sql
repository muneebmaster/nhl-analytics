with source as (
    select * from {{ source('moneypuck', 'raw_skaters_game_by_game') }}
),

renamed as (
    select
        -- identifiers
        playerId::int                                               as player_id,
        name                                                        as player_name,
        gameId::int                                                 as game_id,
        season::int                                                 as season,
        playerTeam                                                  as team,
        opposingTeam                                                as opposing_team,
        home_or_away,
        TO_DATE(gameDate::varchar, 'YYYYMMDD')                      as game_date,
        position,
        situation,

        -- ice time
        icetime::float                                              as ice_time_seconds,
        shifts::int                                                 as shifts,
        gameScore::float                                            as game_score,

        -- individual scoring
        I_F_goals::int                                              as goals,
        I_F_primaryAssists::int                                     as primary_assists,
        I_F_secondaryAssists::int                                   as secondary_assists,
        (I_F_primaryAssists::int + I_F_secondaryAssists::int)       as assists,
        I_F_points::int                                             as points,

        -- shooting
        I_F_shotsOnGoal::int                                        as shots_on_goal,
        I_F_shotAttempts::int                                       as shot_attempts,
        I_F_xGoals::float                                           as xgoals,

        -- physical and discipline
        I_F_hits::int                                               as hits,
        I_F_takeaways::int                                          as takeaways,
        I_F_giveaways::int                                          as giveaways,
        I_F_faceOffsWon::int                                        as faceoffs_won,
        faceoffsLost::int                                           as faceoffs_lost,
        I_F_penalityMinutes::int                                    as penalty_minutes,
        penalties::int                                              as penalties_taken,

        -- on-ice
        OnIce_F_goals::int                                          as on_ice_goals_for,
        OnIce_F_xGoals::float                                       as on_ice_xgoals_for,
        OnIce_A_goals::int                                          as on_ice_goals_against,
        OnIce_A_xGoals::float                                       as on_ice_xgoals_against

    from source
)

select * from renamed
