-- dbt/models/silver/core_movies.sql

WITH details AS (
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
        (
            SELECT jsonb_agg(g->'id') 
            FROM jsonb_array_elements(raw_genres_json) as g
        ) as raw_genre_ids_json,
        poster_path,
        snapshot_date,
        'api' as source
    FROM {{ ref('stg_movie_details') }}
),
discover AS (
    SELECT 
        movie_id,
        title,
        CAST(NULL AS TEXT) as original_title,
        CAST(NULL AS TEXT) as overview,
        CAST(NULL AS TEXT) as status,
        CAST(NULL AS TEXT) as tagline,
        CAST(NULL AS INTEGER) as runtime,
        CAST(NULL AS BIGINT) as budget,
        CAST(NULL AS BIGINT) as revenue,
        popularity,
        vote_average,
        vote_count,
        release_date,
        CAST(NULL AS JSONB) as raw_genres_json,
        raw_genre_ids_json,
        CAST(NULL AS TEXT) as poster_path,
        snapshot_date,
        'discover' as source
    FROM {{ ref('stg_discover') }}
),
combined AS (
    SELECT * FROM details
    UNION ALL
    SELECT * FROM discover
),
deduplicated AS (
    -- Prefer 'api' over 'discover' if both exist for the same movie and snapshot
    -- Prefer latest snapshot
    SELECT 
        *,
        ROW_NUMBER() OVER (
            PARTITION BY movie_id, snapshot_date 
            ORDER BY 
                CASE WHEN source = 'api' THEN 1 ELSE 2 END,
                snapshot_date DESC
        ) as rn
    FROM combined
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
    raw_genre_ids_json,
    poster_path,
    snapshot_date,
    source
FROM deduplicated
WHERE rn = 1
