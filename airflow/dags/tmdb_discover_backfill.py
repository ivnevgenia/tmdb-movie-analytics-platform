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
    'tmdb_discover_backfill',
    default_args=default_args,
    description='A manual one-time backfill DAG for historical movie data using TMDb Discover API',
    schedule_interval=None,
    catchup=False,
    tags=['backfill', 'tmdb'],
    max_active_tasks=3  # Limit concurrency to avoid OOM
) as dag:

    def run_backfill_year(year, **kwargs):
        # We use current date as snapshot_date for backfill as per requirement
        snapshot_date = datetime.now().strftime("%Y-%m-%d")
        
        # 1. Extraction and Upload to MinIO
        pipeline = IngestionPipeline(snapshot_date=snapshot_date)
        pipeline.ingest_discover(year)
        
        # 2. Load to Postgres Bronze
        loader = PostgresLoader(snapshot_date=snapshot_date)
        custom_path = f"tmdb/discover/year={year}/snapshot_date={snapshot_date}"
        loader.load_dataset("discover", custom_path=custom_path)

    # Loop over years from 2000 to current year
    current_year = datetime.now().year
    years = range(2000, current_year + 1)
    
    backfill_tasks = []
    for year in years:
        task = PythonOperator(
            task_id=f'backfill_{year}',
            python_callable=run_backfill_year,
            op_kwargs={'year': year}
        )
        backfill_tasks.append(task)

    # 3. DBT Tasks (Run after ALL backfill years are complete)
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

    # Dependencies: All backfill tasks >> dbt run >> dbt test
    backfill_tasks >> run_dbt_models >> run_dbt_tests
