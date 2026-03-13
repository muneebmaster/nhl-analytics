with shots as (
    select * from {{ ref('stg_shots') }}
),

with_danger_zone as (
    select
        *,

        -- shot danger zone based on arena-adjusted coordinates
        -- High danger: slot area (within ~9 feet of center, deep in the zone)
        -- x >= 69 puts the shot in the offensive zone near the crease
        -- abs(y) <= 9 keeps it in the slot
        case
            when abs(y_cord_adjusted) <= 9
                 and x_cord_adjusted >= 69   then 'High'
            when shot_distance <= 30         then 'Medium'
            else                                  'Low'
        end                                                 as shot_danger_zone

    from shots
)

select * from with_danger_zone
