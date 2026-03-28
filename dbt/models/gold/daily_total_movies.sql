-- dbt/models/gold/daily_total_movies.sql

SELECT 
    COUNT(DISTINCT movie_id) as total_movies_in_db
FROM {{ ref('core_movies') }}
