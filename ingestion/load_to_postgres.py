import os
import pandas as pd
import boto3
from sqlalchemy import create_engine, text
from io import BytesIO
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PostgresLoader:
    def __init__(self, snapshot_date):
        self.snapshot_date = snapshot_date
        
        # Postgres Config
        self.engine = create_engine(
            f"postgresql://{os.getenv('WH_USER')}:{os.getenv('WH_PASSWORD')}@{os.getenv('WH_HOST')}:{os.getenv('WH_PORT')}/{os.getenv('WH_DB')}"
        )
        
        # MinIO Config
        self.s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio_password"),
            region_name="us-east-1"
        )
        self.bucket = os.getenv("MINIO_BUCKET", "movies-data-lake")

    def load_dataset(self, dataset_name, custom_path=None):
        if custom_path:
            key = f"{custom_path}/{dataset_name}.parquet"
        else:
            key = f"raw/tmdb/{dataset_name}/snapshot_date={self.snapshot_date}/{dataset_name}.parquet"
            
        logger.info(f"Loading {dataset_name} from {key} to Postgres...")
        
        try:
            response = self.s3.get_object(Bucket=self.bucket, Key=key)
            df = pd.read_parquet(BytesIO(response['Body'].read()))
            
            # Ensure JSON columns are handled correctly
            # Convert all non-primitive objects to JSON strings for Postgres JSONB
            import json
            import numpy as np
            
            def to_json(val):
                if isinstance(val, (list, dict, np.ndarray)):
                    if isinstance(val, np.ndarray):
                        return json.dumps(val.tolist())
                    return json.dumps(val)
                return val

            for col in df.columns:
                # Be aggressive: if any element is a list/dict/array, convert the whole column
                if df[col].apply(lambda x: isinstance(x, (list, dict, np.ndarray))).any():
                    df[col] = df[col].apply(to_json)
            
            # Ensure snapshot_date is correct
            df["snapshot_date"] = self.snapshot_date
            
            # Default data_source if not present
            if "data_source" not in df.columns:
                df["data_source"] = "api"
            
            # Idempotency: delete for this snapshot_date AND data_source before append
            # Or use ON CONFLICT DO NOTHING as requested.
            # To use ON CONFLICT with pandas, we can load to a temp table and then insert.
            
            table_name = f"raw_tmdb.{dataset_name}"
            
            # Dynamic partition creation for Postgres
            from datetime import datetime, timedelta
            sd_obj = datetime.strptime(self.snapshot_date, "%Y-%m-%d")
            next_day = (sd_obj + timedelta(days=1)).strftime("%Y-%m-%d")
            partition_name = f"{dataset_name}_{self.snapshot_date.replace('-', '_')}"
            
            with self.engine.begin() as conn:
                # Ensure schema exists
                conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw_tmdb"))
                
                # Check if parent table exists, if not create it (basic version)
                # Note: This is a fallback, ideally init-db.sql handles this
                if dataset_name == "discover":
                    conn.execute(text(f"""
                        CREATE TABLE IF NOT EXISTS raw_tmdb.discover (
                            id BIGINT, title TEXT, genre_ids JSONB, popularity DOUBLE PRECISION,
                            vote_average DOUBLE PRECISION, vote_count BIGINT, release_date TEXT,
                            release_year INTEGER, snapshot_date DATE, data_source TEXT,
                            PRIMARY KEY (id, snapshot_date, data_source)
                        ) PARTITION BY RANGE (snapshot_date);
                    """))

                # Create partition if not exists
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS raw_tmdb.{partition_name} 
                    PARTITION OF raw_tmdb.{dataset_name} 
                    FOR VALUES FROM ('{self.snapshot_date}') TO ('{next_day}')
                """))
                
                # Filter columns to match target table
                from sqlalchemy import inspect
                inspector = inspect(self.engine)
                columns_info = inspector.get_columns(dataset_name, schema='raw_tmdb')
                
                # Create a map of column name to its type
                type_map = {c['name']: str(c['type']).upper() for c in columns_info}
                columns = list(type_map.keys())

                if not columns:
                    # Fallback if inspector fails or table just created
                    columns = df.columns.tolist()
                
                df = df[[c for c in df.columns if c in columns]]
                
                # Load to temporary table in raw_tmdb schema
                temp_table = f"temp_{dataset_name}_{self.snapshot_date.replace('-', '_')}"
                df.to_sql(temp_table, conn, schema='raw_tmdb', if_exists="replace", index=False)
                
                # Insert from temp to main with ON CONFLICT
                # Handle potential type mismatches (e.g. text to date/jsonb) by explicit casting
                target_cols = []
                select_cols = []
                for c in df.columns:
                    target_cols.append(f'"{c}"')
                    col_type = type_map.get(c, "")
                    if "DATE" in col_type:
                        select_cols.append(f'CAST("{c}" AS DATE)')
                    elif "JSON" in col_type:
                        select_cols.append(f'CAST("{c}" AS JSONB)')
                    else:
                        select_cols.append(f'"{c}"')
                
                target_cols_str = ", ".join(target_cols)
                select_cols_str = ", ".join(select_cols)

                conn.execute(text(f"""
                    INSERT INTO {table_name} ({target_cols_str})
                    SELECT {select_cols_str} FROM raw_tmdb.{temp_table}
                    ON CONFLICT (id, snapshot_date, data_source) DO NOTHING
                """))
                
                # Drop temp table
                conn.execute(text(f"DROP TABLE raw_tmdb.{temp_table}"))
                
            logger.info(f"Successfully loaded {dataset_name}.")
            
        except Exception as e:
            logger.error(f"Error loading {dataset_name}: {e}")
            raise

if __name__ == "__main__":
    pass
