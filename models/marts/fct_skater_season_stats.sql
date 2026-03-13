with skaters as (
    select * from {{ ref('int_skaters_with_bios') }}
),

filtered as (
    select * from skaters
    where situation in ('all', '5on5', 'powerPlay', 'other')
),

with_rates as (
    select
        *,

        -- per-60 rate stats (ice_time_seconds / 3600 = hours on ice)
        case
            when ice_time_seconds > 0
            then points / (ice_time_seconds / 3600.0)
            else null
        end                                                 as points_per_60,

        case
            when ice_time_seconds > 0
            then goals / (ice_time_seconds / 3600.0)
            else null
        end                                                 as goals_per_60,

        case
            when ice_time_seconds > 0
            then individual_xgoals / (ice_time_seconds / 3600.0)
            else null
        end                                                 as xgoals_per_60,

        case
            when ice_time_seconds > 0
            then assists / (ice_time_seconds / 3600.0)
            else null
        end                                                 as assists_per_60,

        case
            when ice_time_seconds > 0
            then shots_on_goal / (ice_time_seconds / 3600.0)
            else null
        end                                                 as shots_per_60

    from filtered
)

select * from with_rates
