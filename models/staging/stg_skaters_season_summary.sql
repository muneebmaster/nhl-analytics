with source as (
    select * from {{ source('moneypuck', 'raw_skaters_season_summary') }}
),

renamed as (
    select
        -- identifiers
        playerId::int                           as player_id,
        season::int                             as season,
        name                                    as player_name,
        team,
        position,
        situation,

        -- game counts and ice time
        games_played::int                       as games_played,
        icetime::float                          as ice_time_seconds,
        shifts::int                             as shifts,
        iceTimeRank::int                        as ice_time_rank,

        -- individual scoring
        I_F_goals::int                          as goals,
        I_F_primaryAssists::int                 as primary_assists,
        I_F_secondaryAssists::int               as secondary_assists,
        (I_F_primaryAssists::int
            + I_F_secondaryAssists::int)        as assists,
        I_F_points::int                         as points,

        -- shooting
        I_F_shotsOnGoal::int                    as shots_on_goal,
        I_F_shotAttempts::int                   as shot_attempts,
        I_F_missedShots::int                    as missed_shots,
        I_F_blockedShotAttempts::int            as blocked_shot_attempts,
        I_F_xGoals::float                       as individual_xgoals,
        gameScore::float                        as game_score,

        -- shot danger zones (individual)
        I_F_lowDangerShots::int                 as low_danger_shots,
        I_F_mediumDangerShots::int              as medium_danger_shots,
        I_F_highDangerShots::int                as high_danger_shots,
        I_F_lowDangerGoals::int                 as low_danger_goals,
        I_F_mediumDangerGoals::int              as medium_danger_goals,
        I_F_highDangerGoals::int                as high_danger_goals,

        -- physical play and discipline
        I_F_hits::int                           as hits,
        I_F_takeaways::int                      as takeaways,
        I_F_giveaways::int                      as giveaways,
        I_F_penalityMinutes::int                as penalty_minutes,
        penalties::int                          as penalties_taken,
        penalityMinutesDrawn::int               as penalty_minutes_drawn,
        penaltiesDrawn::int                     as penalties_drawn,

        -- faceoffs
        I_F_faceOffsWon::int                    as faceoffs_won,
        faceoffsLost::int                       as faceoffs_lost,

        -- zone starts
        I_F_oZoneShiftStarts::int               as o_zone_shift_starts,
        I_F_dZoneShiftStarts::int               as d_zone_shift_starts,
        I_F_neutralZoneShiftStarts::int         as neutral_zone_shift_starts,

        -- on-ice stats
        OnIce_F_goals::int                      as on_ice_goals_for,
        OnIce_F_xGoals::float                   as on_ice_xgoals_for,
        OnIce_F_shotsOnGoal::int                as on_ice_shots_for,
        OnIce_A_goals::int                      as on_ice_goals_against,
        OnIce_A_xGoals::float                   as on_ice_xgoals_against,
        OnIce_A_shotsOnGoal::int                as on_ice_shots_against,

        -- on-ice percentages
        onIce_xGoalsPercentage::float           as on_ice_xgoals_pct,
        offIce_xGoalsPercentage::float          as off_ice_xgoals_pct,
        onIce_corsiPercentage::float            as on_ice_corsi_pct,
        offIce_corsiPercentage::float           as off_ice_corsi_pct,
        onIce_fenwickPercentage::float          as on_ice_fenwick_pct,
        offIce_fenwickPercentage::float         as off_ice_fenwick_pct

    from source
)

select * from renamed
