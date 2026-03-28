-- dbt/models/silver/stg_movie_details.sql

WITH raw_details AS (
    SELECT * FROM {{ source('tmdb_raw', 'movie_details') }}
),
deduplicated AS (
    SELECT 
        id as movie_id,
        title,
        original_title,
        overview,
        status,
        tagline,
        runtime,
        budget,
        revenue,
        popularity,
        vote_average,
        vote_count,
        release_date::DATE as release_date,
        genres as raw_genres_json,
        poster_path,
        snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY id, snapshot_date ORDER BY snapshot_date DESC) as rn
    FROM raw_details
)
SELECT 
    movie_id,
    title,
    original_title,
    overview,
    status,
    tagline,
    runtime,
    budget,
    revenue,
    popularity,
    vote_average,
    vote_count,
    release_date,
    raw_genres_json,
    poster_path,
    snapshot_date
FROM deduplicated
WHERE rn = 1
