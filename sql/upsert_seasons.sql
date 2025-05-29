INSERT INTO public.seasons (
    season_id,
    season_start_date,
    season_end_date,
    competition_id, 
    season_winner
)
SELECT
    s.season_id,
    s.season_start_date,
    s.season_end_date,
    s.competition_id, 
    s.season_winner
FROM
    staging.seasons s
ON CONFLICT (season_id) DO UPDATE SET
    season_start_date = EXCLUDED.season_start_date,
    season_end_date = EXCLUDED.season_end_date,
    competition_id = EXCLUDED.competition_id, 
    season_winner = EXCLUDED.season_winner;