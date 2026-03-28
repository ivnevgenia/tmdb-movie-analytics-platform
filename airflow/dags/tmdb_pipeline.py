from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Add ingestion path to sys.path for importing local scripts
sys.path.append("/opt/airflow/ingestion")

from ingest import IngestionPipeline
from load_to_postgres import PostgresLoader

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'tmdb_data_pipeline',
    default_args=default_args,
    description='A daily batch pipeline to extract TMDb movie data to MinIO and Postgres',
    schedule_interval='@daily',
    catchup=False
) as dag:

    def run_ingest_movies(dataset_type, **kwargs):
        # Use actual current date for snapshots since TMDb API only provides current state
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        pipeline = IngestionPipeline(snapshot_date=snapshot_date)
        df = pipeline.ingest_movies_list(dataset_type)
        return df['id'].tolist() if 'id' in df.columns else []

    def run_ingest_genres(**kwargs):
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        pipeline = IngestionPipeline(snapshot_date=snapshot_date)
        pipeline.ingest_genres()

    def run_ingest_details(**kwargs):
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        # Aggregate movie IDs from upstream tasks
        ti = kwargs['ti']
        trending_ids = ti.xcom_pull(task_ids='extract_trending') or []
        popular_ids = ti.xcom_pull(task_ids='extract_popular') or []
        top_rated_ids = ti.xcom_pull(task_ids='extract_top_rated') or []
        
        # Deduplicate IDs
        movie_ids = list(set(trending_ids + popular_ids + top_rated_ids))
        
        pipeline = IngestionPipeline(snapshot_date=snapshot_date)
        pipeline.ingest_movie_details(movie_ids)

    def run_load_to_postgres(dataset_name, **kwargs):
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        loader = PostgresLoader(snapshot_date=snapshot_date)
        loader.load_dataset(dataset_name)

    # 1. Extraction Tasks
    extract_trending = PythonOperator(
        task_id='extract_trending',
        python_callable=run_ingest_movies,
        op_kwargs={'dataset_type': 'trending'}
    )

    extract_popular = PythonOperator(
        task_id='extract_popular',
        python_callable=run_ingest_movies,
        op_kwargs={'dataset_type': 'popular'}
    )

    extract_top_rated = PythonOperator(
        task_id='extract_top_rated',
        python_callable=run_ingest_movies,
        op_kwargs={'dataset_type': 'top_rated'}
    )

    extract_genres = PythonOperator(
        task_id='extract_genres',
        python_callable=run_ingest_genres
    )

    extract_movie_details = PythonOperator(
        task_id='extract_movie_details',
        python_callable=run_ingest_details
    )

    # 2. Loading Tasks (to Postgres Bronze)
    datasets = ['trending', 'popular', 'top_rated', 'genres', 'movie_details']
    load_tasks = {}
    for ds in datasets:
        load_tasks[ds] = PythonOperator(
            task_id=f'load_{ds}_to_postgres',
            python_callable=run_load_to_postgres,
            op_kwargs={'dataset_name': ds}
        )

    # 3. DBT Tasks
    # dbt project is in /opt/airflow/dbt
    # We run dbt using BashOperator
    run_dbt_models = BashOperator(
        task_id='run_dbt_models',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt run --profiles-dir .',
        env={
            'DBT_USER': os.getenv('WH_USER'),
            'DBT_PASSWORD': os.getenv('WH_PASSWORD'),
            'DBT_HOST': os.getenv('WH_HOST'),
            'DBT_PORT': os.getenv('WH_PORT'),
            'DBT_DB': os.getenv('WH_DB')
        }
    )

    run_dbt_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/dbt && /home/airflow/.local/bin/dbt test --profiles-dir .',
        env={
            'DBT_USER': os.getenv('WH_USER'),
            'DBT_PASSWORD': os.getenv('WH_PASSWORD'),
            'DBT_HOST': os.getenv('WH_HOST'),
            'DBT_PORT': os.getenv('WH_PORT'),
            'DBT_DB': os.getenv('WH_DB')
        }
    )

    # Dependencies
    [extract_trending, extract_popular, extract_top_rated] >> extract_movie_details
    extract_genres >> load_tasks['genres']
    extract_movie_details >> load_tasks['movie_details']
    
    extract_trending >> load_tasks['trending']
    extract_popular >> load_tasks['popular']
    extract_top_rated >> load_tasks['top_rated']

    # Wait for all loading to finish before dbt
    list(load_tasks.values()) >> run_dbt_models >> run_dbt_tests
