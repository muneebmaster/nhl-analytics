with source as (
    select * from {{ source('moneypuck', 'raw_lines_season_summary') }}
),

renamed as (
    select
        -- identifiers
        lineId                                          as line_id,
        season::int                                     as season,
        name                                            as line_name,
        team,
        position,
        situation,

        -- ice time
        games_played::int                               as games_played,
        icetime::float                                  as ice_time_seconds,
        iceTimeRank::int                                as ice_time_rank,

        -- percentages
        xGoalsPercentage::float                         as xgoals_pct,
        corsiPercentage::float                          as corsi_pct,
        fenwickPercentage::float                        as fenwick_pct,

        -- goals and expected goals
        xGoalsFor::float                                as xgoals_for,
        goalsFor::int                                   as goals_for,
        xGoalsAgainst::float                            as xgoals_against,
        goalsAgainst::int                               as goals_against,

        -- shots
        shotsOnGoalFor::int                             as shots_for,
        shotsOnGoalAgainst::int                         as shots_against

    from source
)

select * from renamed
