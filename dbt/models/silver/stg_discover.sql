-- dbt/models/silver/stg_discover.sql

WITH raw_discover AS (
    SELECT * FROM {{ source('tmdb_raw', 'discover') }}
),
deduplicated AS (
    SELECT 
        id as movie_id,
        title,
        genre_ids as raw_genre_ids_json,
        popularity,
        vote_average,
        vote_count,
        release_date::DATE as release_date,
        release_year,
        snapshot_date,
        data_source,
        ROW_NUMBER() OVER (PARTITION BY id, snapshot_date ORDER BY snapshot_date DESC) as rn
    FROM raw_discover
)
SELECT 
    movie_id,
    title,
    raw_genre_ids_json,
    popularity,
    vote_average,
    vote_count,
    release_date,
    release_year,
    snapshot_date,
    data_source
FROM deduplicated
WHERE rn = 1
