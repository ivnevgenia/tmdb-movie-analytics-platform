-- dbt/models/gold/fact_movie_popularity.sql

WITH movie_details AS (
    SELECT * FROM {{ ref('core_movies') }}
)
SELECT 
    movie_id,
    snapshot_date,
    popularity,
    vote_average,
    vote_count
FROM movie_details
