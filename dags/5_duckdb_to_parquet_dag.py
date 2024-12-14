import os
import duckdb
import boto3
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

# Constants
S3_BUCKET = "warehouse"              
MINIO_ENDPOINT = "http://minio:9000"
DUCKDB_FILE = "/opt/airflow/data/processed/integrated_data.duckdb"

# Airflow default args
default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2024, 1, 1),
}

with DAG(
    "5_duckdb_to_parquet_DAG",
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    def ensure_bucket_exists(bucket_name, endpoint_url):
        """Ensure that the MinIO bucket exists."""
        print(f"Checking if bucket '{bucket_name}' exists...")
        s3 = boto3.client(
            "s3",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            endpoint_url=endpoint_url,
        )
        try:
            s3.head_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' exists.")
        except Exception:
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created.")

    def export_duckdb_to_parquet():
        """Export DuckDB tables as Parquet files to MinIO without 'main_' prefix."""
        try:
            # Ensure MinIO bucket exists
            ensure_bucket_exists(S3_BUCKET, MINIO_ENDPOINT)

            print(f"Connecting to DuckDB file at {DUCKDB_FILE}")
            conn = duckdb.connect(DUCKDB_FILE)
            conn.install_extension("httpfs")
            conn.load_extension("httpfs")

            # Configure DuckDB to use MinIO
            conn.sql("""
            SET s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin';
            SET s3_endpoint='minio:9000';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
            SET s3_region='us-east-1';
            """)

            # Fetch all tables from DuckDB
            print("Fetching tables from DuckDB...")
            tables = conn.execute("""
            SELECT table_schema, table_name 
            FROM information_schema.tables 
            WHERE table_schema NOT IN ('information_schema', 'pg_catalog');
            """).fetchall()

            if not tables:
                print("No tables found in DuckDB!")
                return

            # Export each table to Parquet
            for schema, table_name in tables:
                # Remove 'main_' prefix if present
                cleaned_table_name = table_name.replace("main_", "") if table_name.startswith("main_") else table_name
                parquet_path = f"s3://{S3_BUCKET}/{cleaned_table_name}.parquet"
                print(f"Exporting table '{schema}.{table_name}' to {parquet_path}...")

                conn.sql(f"COPY {schema}.{table_name} TO '{parquet_path}' (FORMAT PARQUET)")
                print(f"Successfully exported: {parquet_path}")

        except Exception as e:
            print(f"Error in export_duckdb_to_parquet: {e}")
            raise
    # Define the Airflow task
    export_to_parquet_task = PythonOperator(
        task_id="export_duckdb_to_parquet",
        python_callable=export_duckdb_to_parquet,
    )

    export_to_parquet_task