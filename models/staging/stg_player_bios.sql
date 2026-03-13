with source as (
    select * from {{ source('moneypuck', 'raw_player_bios') }}
),

renamed as (
    select
        playerId::int           as player_id,
        name                    as player_name,
        position,
        team                    as current_team,
        birthDate::date         as birth_date,
        weight::int             as weight_lbs,
        height,
        nationality,
        shootsCatches           as shoots_catches,
        primaryNumber::int      as jersey_number,
        primaryPosition         as primary_position

    from source
)

select * from renamed
