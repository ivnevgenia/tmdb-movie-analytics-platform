import os
import json
import time
import pandas as pd
from datetime import datetime
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from tmdb_client import TMDBClient
import logging
from io import BytesIO

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IngestionPipeline:
    def __init__(self, snapshot_date=None):
        self.tmdb = TMDBClient()
        self.snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
        
        # MinIO Config
        self.s3 = boto3.client(
            "s3",
            endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT', 'localhost:9000')}",
            aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
            aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "minio_password"),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )
        self.bucket = os.getenv("MINIO_BUCKET", "movies-data-lake")
        
        # Ensure bucket exists
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                logger.info(f"Creating bucket {self.bucket}...")
                self.s3.create_bucket(Bucket=self.bucket)
            else:
                raise e
        except Exception as e:
            # Handle potential race conditions or other errors
            if "BucketAlreadyOwnedByYou" in str(e) or "BucketAlreadyExists" in str(e):
                logger.info(f"Bucket {self.bucket} already exists.")
            else:
                raise e

    def upload_df_to_minio(self, df, dataset_name, custom_path=None):
        # Add snapshot_date
        df["snapshot_date"] = self.snapshot_date
        
        # Convert to Parquet in-memory
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        
        # Upload to MinIO
        if custom_path:
            key = f"{custom_path}/{dataset_name}.parquet"
        else:
            key = f"raw/tmdb/{dataset_name}/snapshot_date={self.snapshot_date}/{dataset_name}.parquet"
            
        logger.info(f"Uploading {dataset_name} to {key}...")
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=parquet_buffer.getvalue()
        )

    def ingest_genres(self):
        logger.info("Ingesting genres...")
        data = self.tmdb.get_genres()
        df = pd.DataFrame(data["genres"])
        df["data_source"] = "api"
        self.upload_df_to_minio(df, "genres")
        return df

    def ingest_movies_list(self, dataset_type):
        logger.info(f"Ingesting movies list: {dataset_type}...")
        if dataset_type == "trending":
            data = self.tmdb.get_trending_movies()
        elif dataset_type == "popular":
            data = self.tmdb.get_popular_movies()
        elif dataset_type == "top_rated":
            data = self.tmdb.get_top_rated_movies()
        else:
            raise ValueError(f"Unknown dataset type: {dataset_type}")
        
        df = pd.DataFrame(data["results"])
        df["data_source"] = "api"
        self.upload_df_to_minio(df, dataset_type)
        return df

    def ingest_movie_details(self, movie_ids):
        logger.info(f"Ingesting details for {len(movie_ids)} movies...")
        details = []
        for movie_id in movie_ids:
            try:
                detail = self.tmdb.get_movie_details(movie_id)
                detail["genres"] = json.dumps(detail.get("genres", []))
                detail["production_companies"] = json.dumps(detail.get("production_companies", []))
                detail["production_countries"] = json.dumps(detail.get("production_countries", []))
                detail["spoken_languages"] = json.dumps(detail.get("spoken_languages", []))
                detail["data_source"] = "api"
                details.append(detail)
                # Rate limiting sleep
                time.sleep(0.1)
            except Exception as e:
                logger.error(f"Error fetching movie {movie_id}: {e}")
        
        df = pd.DataFrame(details)
        self.upload_df_to_minio(df, "movie_details")
        return df

    def ingest_discover(self, year):
        logger.info(f"Ingesting movies for year: {year}...")
        all_movies = []
        page = 1
        
        while True:
            try:
                data = self.tmdb.discover_movies(year, page=page)
                results = data.get("results", [])
                if not results:
                    break
                
                for movie in results:
                    # Extract required fields
                    movie_data = {
                        "id": movie.get("id"),
                        "title": movie.get("title"),
                        "genre_ids": movie.get("genre_ids"),
                        "popularity": movie.get("popularity"),
                        "vote_average": movie.get("vote_average"),
                        "vote_count": movie.get("vote_count"),
                        "release_date": movie.get("release_date"),
                        "release_year": year,
                        "data_source": "discover"
                    }
                    all_movies.append(movie_data)
                
                logger.info(f"Year {year}: Processed page {page}/{data.get('total_pages')}")
                
                if page >= data.get("total_pages") or page >= 500: # TMDb limit is 500 pages
                    break
                
                page += 1
                time.sleep(0.2) # Respect rate limits
                
            except Exception as e:
                logger.error(f"Error fetching page {page} for year {year}: {e}")
                raise

        if all_movies:
            df = pd.DataFrame(all_movies)
            custom_path = f"tmdb/discover/year={year}/snapshot_date={self.snapshot_date}"
            self.upload_df_to_minio(df, "discover", custom_path=custom_path)
            return df
        return pd.DataFrame()

if __name__ == "__main__":
    # Example standalone run
    # In Airflow, we will call specific methods
    pass
