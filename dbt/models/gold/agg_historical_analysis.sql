-- dbt/models/gold/agg_historical_analysis.sql

WITH historical_data AS (
    SELECT 
        movie_id,
        title,
        popularity,
        vote_average,
        vote_count,
        release_date,
        EXTRACT(YEAR FROM release_date) as release_year,
        EXTRACT(MONTH FROM release_date) as release_month,
        snapshot_date,
        raw_genre_ids_json,
        source
    FROM {{ ref('core_movies') }}
    WHERE release_date >= '2000-01-01' -- Фильтр по бэкфиллу
),
flattened_genre_ids AS (
    SELECT 
        h.movie_id,
        h.release_year,
        h.release_month,
        h.popularity,
        h.vote_average,
        CAST(g.value AS INTEGER) as genre_id
    FROM historical_data h, jsonb_array_elements(raw_genre_ids_json) as g
    WHERE raw_genre_ids_json IS NOT NULL
),
flattened_genres AS (
    SELECT 
        f.*,
        g.genre_name
    FROM flattened_genre_ids f
    LEFT JOIN {{ ref('stg_genres') }} g ON f.genre_id = g.genre_id
    WHERE g.snapshot_date = (SELECT MAX(snapshot_date) FROM {{ ref('stg_genres') }})
),
yearly_genres AS (
    SELECT 
        release_year,
        genre_name,
        AVG(popularity) as avg_popularity,
        AVG(vote_average) as avg_rating,
        COUNT(DISTINCT movie_id) as movie_count
    FROM flattened_genres
    WHERE genre_name IS NOT NULL
    GROUP BY release_year, genre_name
),
yearly_market_share AS (
    SELECT 
        release_year,
        genre_name,
        movie_count,
        avg_popularity,
        avg_rating,
        SUM(movie_count) OVER (PARTITION BY release_year) as total_movies_per_year,
        ROUND(CAST(movie_count AS NUMERIC) / SUM(movie_count) OVER (PARTITION BY release_year) * 100, 2) as genre_market_share
    FROM yearly_genres
)
SELECT 
    release_year,
    genre_name,
    movie_count,
    avg_popularity,
    CASE WHEN avg_rating IS NULL THEN 0 ELSE avg_rating END as avg_rating,
    total_movies_per_year,
    genre_market_share
FROM yearly_market_share
ORDER BY release_year DESC, genre_market_share DESC
