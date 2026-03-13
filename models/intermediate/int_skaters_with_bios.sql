with skaters as (
    select * from {{ ref('stg_skaters_season_summary') }}
),

bios as (
    select * from {{ ref('stg_player_bios') }}
),

joined as (
    select
        -- from skaters (all columns)
        skaters.player_id,
        skaters.season,
        skaters.player_name,
        skaters.team,
        skaters.position,
        skaters.situation,
        skaters.games_played,
        skaters.ice_time_seconds,
        skaters.shifts,
        skaters.ice_time_rank,
        skaters.game_score,
        skaters.goals,
        skaters.primary_assists,
        skaters.secondary_assists,
        skaters.assists,
        skaters.points,
        skaters.shots_on_goal,
        skaters.shot_attempts,
        skaters.missed_shots,
        skaters.blocked_shot_attempts,
        skaters.individual_xgoals,
        skaters.low_danger_shots,
        skaters.medium_danger_shots,
        skaters.high_danger_shots,
        skaters.low_danger_goals,
        skaters.medium_danger_goals,
        skaters.high_danger_goals,
        skaters.hits,
        skaters.takeaways,
        skaters.giveaways,
        skaters.penalty_minutes,
        skaters.penalties_taken,
        skaters.penalty_minutes_drawn,
        skaters.penalties_drawn,
        skaters.faceoffs_won,
        skaters.faceoffs_lost,
        skaters.o_zone_shift_starts,
        skaters.d_zone_shift_starts,
        skaters.neutral_zone_shift_starts,
        skaters.on_ice_goals_for,
        skaters.on_ice_xgoals_for,
        skaters.on_ice_shots_for,
        skaters.on_ice_goals_against,
        skaters.on_ice_xgoals_against,
        skaters.on_ice_shots_against,
        skaters.on_ice_xgoals_pct,
        skaters.off_ice_xgoals_pct,
        skaters.on_ice_corsi_pct,
        skaters.off_ice_corsi_pct,
        skaters.on_ice_fenwick_pct,
        skaters.off_ice_fenwick_pct,

        -- from bios
        bios.birth_date,
        bios.height,
        bios.weight_lbs,
        bios.nationality,
        bios.shoots_catches,
        bios.jersey_number

    from skaters
    left join bios
        on skaters.player_id = bios.player_id
)

select * from joined
