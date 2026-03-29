-- dbt/models/gold/historical_genre_distribution.sql

WITH flattened_genre_ids AS (
    SELECT 
        m.movie_id,
        CAST(g.value AS INTEGER) as genre_id
    FROM {{ ref('core_movies') }} m, jsonb_array_elements(raw_genre_ids_json) as g
    WHERE m.raw_genre_ids_json IS NOT NULL
),
flattened_genres AS (
    SELECT 
        f.movie_id,
        g.genre_name
    FROM flattened_genre_ids f
    LEFT JOIN {{ ref('stg_genres') }} g ON f.genre_id = g.genre_id
    WHERE g.genre_name IS NOT NULL
      AND g.snapshot_date = (SELECT MAX(snapshot_date) FROM {{ ref('stg_genres') }})
)
SELECT 
    genre_name,
    COUNT(DISTINCT movie_id) as movie_count
FROM flattened_genres
GROUP BY genre_name
ORDER BY movie_count DESC
