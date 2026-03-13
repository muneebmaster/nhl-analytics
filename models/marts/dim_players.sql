with bios as (
    select * from {{ ref('stg_player_bios') }}
),

final as (
    select
        -- surrogate key (using player_id directly as natural key is stable)
        player_id,
        player_name,
        position,
        primary_position,
        current_team,
        birth_date,
        datediff('year', birth_date, current_date())        as age,
        height,
        weight_lbs,
        nationality,
        shoots_catches,
        jersey_number

    from bios
)

select * from final
