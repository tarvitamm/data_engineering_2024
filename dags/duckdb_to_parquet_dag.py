from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import duckdb
from pyiceberg.catalog import load_catalog

# Define AWS credentials for MinIO
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
MINIO_ENDPOINT = "http://minio:9000"
MINIO_BUCKET = "warehouse"

def upload_parquet_to_minio(local_path, s3_path):
    # Connect to MinIO
    s3 = boto3.client(
        "s3",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        endpoint_url=MINIO_ENDPOINT,
    )

    # Upload Parquet file to MinIO
    bucket, key = s3_path.split("/", 1)
    s3.upload_file(local_path, bucket, key)
    print(f"Uploaded {local_path} to s3://{bucket}/{key}")

def extract_from_duckdb(**kwargs):
    db_path = '/opt/airflow/data/processed/integrated_data.duckdb'
    conn = duckdb.connect(db_path)

    # Query data
    df = conn.execute("SELECT * FROM integrated_data").fetchdf()
    conn.close()

    # Save data as Parquet
    parquet_path = '/opt/airflow/data/processed/transformed_data.parquet'
    df.to_parquet(parquet_path, index=False)

    # Upload to MinIO
    s3_path = f"{MINIO_BUCKET}/transformed_data.parquet"
    upload_parquet_to_minio(parquet_path, s3_path)

    return parquet_path  # Pass local Parquet path to next task

def save_to_iceberg(**kwargs):
    # Path to Parquet file in MinIO
    parquet_path = f"s3://{MINIO_BUCKET}/transformed_data.parquet"

    # Configure Iceberg catalog
    iceberg_catalog = load_catalog(
        catalog_name="rest",
        uri="http://iceberg_rest:8181",
        properties={
            "s3.endpoint": MINIO_ENDPOINT,
            "s3.access-key-id": MINIO_ACCESS_KEY,
            "s3.secret-access-key": MINIO_SECRET_KEY,
        },
    )

    # Define Iceberg table name
    table_name = "analytics.transformed_data"

    # Write to Iceberg
    if not iceberg_catalog.table_exists(table_name):
        iceberg_catalog.create_table_from_parquet(
            identifier=table_name,
            parquet_path=parquet_path,
            partition_spec=None,  # Define partitioning if required
        )
    else:
        table = iceberg_catalog.load_table(table_name)
        table.write_parquet(parquet_path)

    print(f"Data saved to Iceberg table: {table_name}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'duckdb_to_iceberg_DAG',
    default_args=default_args,
    description='DAG to transform data from DuckDB to Parquet and save to Iceberg',
    schedule_interval='@once',  # Run on demand
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_duckdb_data = PythonOperator(
        task_id='extract_duckdb_data',
        python_callable=extract_from_duckdb,
        provide_context=True
    )

    save_to_iceberg_task = PythonOperator(
        task_id='save_to_iceberg',
        python_callable=save_to_iceberg,
        provide_context=True
    )

    # Define task dependencies
    extract_duckdb_data >> save_to_iceberg_task
