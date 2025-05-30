INSERT INTO public.matches (
    match_id,
    area_id,
    competition_id,
    season_id,
    date,
    status,
    matchday,
    stage,
    home_team,
    away_team,
    winner,
    home_score,
    away_score,
    home_ht_score,
    away_ht_score,
    referee_name
)
SELECT
    m.match_id,
    m.area_id,
    m.competition_id,
    m.season_id,
    m.date,
    m.status,
    m.matchday,
    m.stage,
    m.home_team,
    m.away_team,
    m.winner,
    m.home_score,
    m.away_score,
    m.home_ht_score,
    m.away_ht_score,
    m.referee_name
FROM
    staging.matches m
ON CONFLICT (match_id) DO UPDATE SET 
    area_id = EXCLUDED.area_id,
    competition_id = EXCLUDED.competition_id,
    season_id = EXCLUDED.season_id,
    date = EXCLUDED.date,
    status = EXCLUDED.status,
    matchday = EXCLUDED.matchday,
    stage = EXCLUDED.stage,
    home_team = EXCLUDED.home_team,
    away_team = EXCLUDED.away_team,
    winner = EXCLUDED.winner,
    home_score = EXCLUDED.home_score,
    away_score = EXCLUDED.away_score,
    home_ht_score = EXCLUDED.home_ht_score,
    away_ht_score = EXCLUDED.away_ht_score,
    referee_name = EXCLUDED.referee_name;