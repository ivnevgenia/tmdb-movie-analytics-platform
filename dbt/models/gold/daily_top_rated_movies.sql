-- dbt/models/gold/daily_top_rated_movies.sql

WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) as max_date FROM {{ ref('core_movies') }} WHERE source = 'api'
)
SELECT 
    movie_id,
    title,
    vote_average,
    vote_count,
    popularity,
    release_date,
    CASE 
        WHEN poster_path IS NOT NULL THEN 'https://image.tmdb.org/t/p/w200' || poster_path
        ELSE NULL 
    END as poster_url
FROM {{ ref('core_movies') }}
WHERE snapshot_date = (SELECT max_date FROM latest_snapshot)
  AND source = 'api'
  AND vote_count > 100
ORDER BY vote_average DESC, vote_count DESC
LIMIT 20
