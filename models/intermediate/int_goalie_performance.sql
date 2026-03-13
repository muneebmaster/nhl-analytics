with goalies as (
    select * from {{ ref('stg_goalies_season_summary') }}
),

filtered as (
    select * from goalies
    where situation = 'all'
),

with_performance_metrics as (
    select
        *,

        -- Goals saved above expected: actual saves vs expected saves
        -- Expected saves = shots_against - xgoals_against
        -- Actual saves = shots_against - goals_against
        -- GSAE = (shots_against - goals_against) - (shots_against - xgoals_against)
        --       = xgoals_against - goals_against
        (xgoals_against - goals_against::float)             as goals_saved_above_expected,

        -- Save percentage above expected
        -- Expected save pct = 1 - (xgoals_against / shots_against)
        -- Actual save pct already computed; difference = actual - expected
        case
            when shots_against > 0
            then save_pct - (1.0 - xgoals_against / shots_against::float)
            else null
        end                                                  as save_pct_vs_expected

    from filtered
)

select * from with_performance_metrics
