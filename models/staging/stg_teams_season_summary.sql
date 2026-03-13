with source as (
    -- exclude the duplicate "team.1" column by selecting only what we need
    select
        team,
        season,
        name,
        position,
        situation,
        games_played,
        xGoalsPercentage,
        corsiPercentage,
        fenwickPercentage,
        iceTime,
        xGoalsFor,
        goalsFor,
        shotsOnGoalFor,
        shotAttemptsFor,
        xGoalsAgainst,
        goalsAgainst,
        shotsOnGoalAgainst,
        shotAttemptsAgainst,
        penaltiesFor,
        penaltiesAgainst,
        faceOffsWonFor,
        hitsFor,
        takeawaysFor,
        giveawaysFor
    from {{ source('moneypuck', 'raw_teams_season_summary') }}
),

renamed as (
    select
        -- identifiers
        team,
        season::int                                             as season,
        name                                                    as team_name,
        position,
        situation,

        -- game counts
        games_played::int                                       as games_played,
        iceTime::float                                          as ice_time_seconds,

        -- percentages
        xGoalsPercentage::float                                 as xgoals_pct,
        corsiPercentage::float                                  as corsi_pct,
        fenwickPercentage::float                                as fenwick_pct,

        -- goals
        goalsFor::int                                           as goals_for,
        goalsAgainst::int                                       as goals_against,
        (goalsFor::int - goalsAgainst::int)                     as goal_diff,

        -- expected goals
        xGoalsFor::float                                        as xgoals_for,
        xGoalsAgainst::float                                    as xgoals_against,
        (xGoalsFor::float - xGoalsAgainst::float)               as xgoal_diff,

        -- shots
        shotsOnGoalFor::int                                     as shots_for,
        shotsOnGoalAgainst::int                                 as shots_against,
        shotAttemptsFor::int                                    as shot_attempts_for,
        shotAttemptsAgainst::int                                as shot_attempts_against,

        -- discipline and special teams
        penaltiesFor::int                                       as penalties_for,
        penaltiesAgainst::int                                   as penalties_against,

        -- possession and physical
        faceOffsWonFor::int                                     as face_offs_won,
        hitsFor::int                                            as hits_for,
        takeawaysFor::int                                       as takeaways_for,
        giveawaysFor::int                                       as giveaways_for

    from source
)

select * from renamed
