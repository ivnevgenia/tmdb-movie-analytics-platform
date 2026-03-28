-- dbt/models/gold/daily_most_popular_movies.sql

WITH latest_snapshot AS (
    SELECT MAX(snapshot_date) as max_date FROM {{ ref('core_movies') }} WHERE source = 'api'
)
SELECT 
    movie_id,
    title,
    popularity,
    vote_average,
    vote_count,
    release_date,
    CASE 
        WHEN poster_path IS NOT NULL THEN 'https://image.tmdb.org/t/p/w200' || poster_path
        ELSE NULL 
    END as poster_url
FROM {{ ref('core_movies') }}
WHERE snapshot_date = (SELECT max_date FROM latest_snapshot)
  AND source = 'api'
ORDER BY popularity DESC
LIMIT 20
