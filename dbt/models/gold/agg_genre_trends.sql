-- dbt/models/gold/agg_genre_trends.sql

WITH movies AS (
    SELECT * FROM {{ ref('core_movies') }}
),
discover AS (
    SELECT * FROM {{ ref('stg_discover') }}
),
genres AS (
    SELECT * FROM {{ ref('stg_genres') }}
),
-- Flatten genres from API source
flattened_api_genres AS (
    SELECT 
        movie_id,
        snapshot_date,
        g->>'name' as genre_name,
        popularity
    FROM movies, jsonb_array_elements(raw_genres_json) as g
    WHERE source = 'api' AND raw_genres_json IS NOT NULL
),
-- Flatten and join genres from Discover source
flattened_discover_genres AS (
    SELECT 
        d.movie_id,
        d.snapshot_date,
        g.genre_name,
        d.popularity
    FROM discover d
    CROSS JOIN LATERAL jsonb_array_elements(raw_genre_ids_json) as gid
    JOIN genres g ON (gid::text)::int = g.genre_id AND d.snapshot_date = g.snapshot_date
),
combined_genres AS (
    SELECT * FROM flattened_api_genres
    UNION ALL
    SELECT * FROM flattened_discover_genres
)
SELECT 
    genre_name as genre,
    snapshot_date,
    AVG(popularity) as avg_popularity,
    COUNT(*) as movie_count
FROM combined_genres
GROUP BY genre_name, snapshot_date
ORDER BY snapshot_date DESC, avg_popularity DESC
