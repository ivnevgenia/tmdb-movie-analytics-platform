-- dbt/models/gold/dim_movies.sql

WITH movie_details AS (
    SELECT * FROM {{ ref('core_movies') }}
),
-- Flatten genres for easier viewing in dim_movies
flattened_genres AS (
    SELECT 
        movie_id,
        snapshot_date,
        string_agg(g->>'name', ', ') as genres
    FROM movie_details, jsonb_array_elements(raw_genres_json) as g
    WHERE raw_genres_json IS NOT NULL
    GROUP BY movie_id, snapshot_date
)
SELECT 
    m.movie_id,
    m.title,
    g.genres,
    m.release_date,
    m.runtime,
    m.snapshot_date
FROM movie_details m
LEFT JOIN flattened_genres g ON m.movie_id = g.movie_id AND m.snapshot_date = g.snapshot_date
WHERE m.snapshot_date = (SELECT MAX(snapshot_date) FROM movie_details)
