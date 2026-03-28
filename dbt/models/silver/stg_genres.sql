-- dbt/models/silver/stg_genres.sql

WITH raw_genres AS (
    SELECT * FROM {{ source('tmdb_raw', 'genres') }}
),
deduplicated AS (
    SELECT 
        id as genre_id,
        name as genre_name,
        snapshot_date,
        ROW_NUMBER() OVER (PARTITION BY id, snapshot_date ORDER BY snapshot_date DESC) as rn
    FROM raw_genres
)
SELECT 
    genre_id,
    genre_name,
    snapshot_date
FROM deduplicated
WHERE rn = 1
