-- Initialize Airflow database
CREATE DATABASE airflow;
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;
ALTER DATABASE airflow OWNER TO airflow;

-- Connect to airflow database to grant permissions on public schema
\c airflow
GRANT ALL ON SCHEMA public TO airflow;

-- Back to warehouse database
\c tmdb_warehouse

-- Initialize warehouse schemas
CREATE SCHEMA IF NOT EXISTS raw_tmdb;
CREATE SCHEMA IF NOT EXISTS core_tmdb;
CREATE SCHEMA IF NOT EXISTS gold_tmdb;

-- Ensure the postgres user has access if dbt or airflow use it
GRANT ALL ON SCHEMA raw_tmdb TO postgres;
GRANT ALL ON SCHEMA core_tmdb TO postgres;
GRANT ALL ON SCHEMA gold_tmdb TO postgres;

-- Base Partitioned Tables
-- Trending movies
CREATE TABLE IF NOT EXISTS raw_tmdb.trending (
    id BIGINT,
    title TEXT,
    original_title TEXT,
    overview TEXT,
    poster_path TEXT,
    media_type TEXT,
    adult BOOLEAN,
    original_language TEXT,
    genre_ids JSONB,
    popularity DOUBLE PRECISION,
    release_date TEXT,
    video BOOLEAN,
    vote_average DOUBLE PRECISION,
    vote_count BIGINT,
    snapshot_date DATE,
    data_source TEXT,
    PRIMARY KEY (id, snapshot_date, data_source)
) PARTITION BY RANGE (snapshot_date);

-- Discover movies (historical)
CREATE TABLE IF NOT EXISTS raw_tmdb.discover (
    id BIGINT,
    title TEXT,
    genre_ids JSONB,
    popularity DOUBLE PRECISION,
    vote_average DOUBLE PRECISION,
    vote_count BIGINT,
    release_date TEXT,
    release_year INTEGER,
    snapshot_date DATE,
    data_source TEXT,
    PRIMARY KEY (id, snapshot_date, data_source)
) PARTITION BY RANGE (snapshot_date);

-- Popular movies
CREATE TABLE IF NOT EXISTS raw_tmdb.popular (
    id BIGINT,
    title TEXT,
    original_title TEXT,
    overview TEXT,
    poster_path TEXT,
    adult BOOLEAN,
    original_language TEXT,
    genre_ids JSONB,
    popularity DOUBLE PRECISION,
    release_date TEXT,
    video BOOLEAN,
    vote_average DOUBLE PRECISION,
    vote_count BIGINT,
    snapshot_date DATE,
    data_source TEXT,
    PRIMARY KEY (id, snapshot_date, data_source)
) PARTITION BY RANGE (snapshot_date);

-- Top rated movies
CREATE TABLE IF NOT EXISTS raw_tmdb.top_rated (
    id BIGINT,
    title TEXT,
    original_title TEXT,
    overview TEXT,
    poster_path TEXT,
    adult BOOLEAN,
    original_language TEXT,
    genre_ids JSONB,
    popularity DOUBLE PRECISION,
    release_date TEXT,
    video BOOLEAN,
    vote_average DOUBLE PRECISION,
    vote_count BIGINT,
    snapshot_date DATE,
    data_source TEXT,
    PRIMARY KEY (id, snapshot_date, data_source)
) PARTITION BY RANGE (snapshot_date);

-- Movie details
CREATE TABLE IF NOT EXISTS raw_tmdb.movie_details (
    id BIGINT,
    imdb_id TEXT,
    title TEXT,
    original_title TEXT,
    overview TEXT,
    status TEXT,
    tagline TEXT,
    runtime INTEGER,
    budget BIGINT,
    revenue BIGINT,
    popularity DOUBLE PRECISION,
    vote_average DOUBLE PRECISION,
    vote_count BIGINT,
    release_date TEXT,
    genres JSONB,
    production_companies JSONB,
    production_countries JSONB,
    spoken_languages JSONB,
    homepage TEXT,
    backdrop_path TEXT,
    poster_path TEXT,
    adult BOOLEAN,
    video BOOLEAN,
    original_language TEXT,
    belongs_to_collection JSONB,
    snapshot_date DATE,
    data_source TEXT,
    PRIMARY KEY (id, snapshot_date, data_source)
) PARTITION BY RANGE (snapshot_date);

-- Genres
CREATE TABLE IF NOT EXISTS raw_tmdb.genres (
    id INTEGER,
    name TEXT,
    snapshot_date DATE,
    data_source TEXT,
    PRIMARY KEY (id, snapshot_date, data_source)
) PARTITION BY RANGE (snapshot_date);
