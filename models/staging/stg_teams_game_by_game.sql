with source as (
    select * from {{ source('moneypuck', 'raw_teams_game_by_game') }}
),

renamed as (
    select
        -- identifiers
        team,
        gameId::int                                         as game_id,
        season::int                                         as season,
        playerTeam                                          as player_team,
        opposingTeam                                        as opposing_team,
        home_or_away,
        TO_DATE(gameDate::varchar, 'YYYYMMDD')              as game_date,
        position,
        situation,

        -- goals
        goalsFor::int                                       as goals_for,
        goalsAgainst::int                                   as goals_against,

        -- expected goals
        xGoalsFor::float                                    as xgoals_for,
        xGoalsAgainst::float                                as xgoals_against,

        -- shots
        shotsOnGoalFor::int                                 as shots_for,
        shotsOnGoalAgainst::int                             as shots_against,

        -- percentages (when available in game context)
        corsiPercentage::float                              as corsi_pct,
        fenwickPercentage::float                            as fenwick_pct,
        xGoalsPercentage::float                             as xgoals_pct

    from source
)

select * from renamed
