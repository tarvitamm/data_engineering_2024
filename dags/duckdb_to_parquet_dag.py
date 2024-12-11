import os
import duckdb
import boto3
from pyiceberg.catalog import load_catalog, Catalog
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import pyiceberg

# Constants
S3_BUCKET = "warehouse"              
MINIO_ENDPOINT = "http://minio:9000"
ICEBERG_REST_URI = "http://iceberg_rest:8181/"  
DUCKDB_FILE = "/opt/airflow/data/processed/integrated_data.duckdb"

# Airflow default args
default_args = {
    "owner": "airflow",
    "start_date": datetime.datetime(2024, 1, 1),
}

with DAG(
    "duckdb_iceberg_integration_dag",
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
    def validate_s3_access(bucket_name, endpoint_url):
        """Validate that all necessary S3 operations can be performed."""
        s3 = boto3.client(
            "s3",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            endpoint_url=endpoint_url,
        )
        try:
            print("Validating bucket access...")
            s3.head_bucket(Bucket=bucket_name)
            print(f"HEAD operation successful for bucket '{bucket_name}'.")

            response = s3.list_objects_v2(Bucket=bucket_name)
            print(f"LIST operation successful. Objects in bucket '{bucket_name}':")
            for obj in response.get("Contents", []):
                print(f" - {obj['Key']}")

            test_key = "test_access.txt"
            s3.put_object(Bucket=bucket_name, Key=test_key, Body="test")
            print(f"PUT operation successful. Object '{test_key}' created.")
            s3.get_object(Bucket=bucket_name, Key=test_key)
            print(f"GET operation successful. Object '{test_key}' retrieved.")
            s3.delete_object(Bucket=bucket_name, Key=test_key)
            print(f"DELETE operation successful. Object '{test_key}' deleted.")
        except Exception as e:
            print(f"Access validation failed: {e}")

    def check_bucket_access(bucket_name, endpoint_url):
        """Check if we can list objects in the bucket using the same credentials as Iceberg."""
        print("Verifying bucket accessibility from Python using boto3 and the same credentials as Iceberg...")
        s3 = boto3.client(
            "s3",
            aws_access_key_id="minioadmin",
            aws_secret_access_key="minioadmin",
            endpoint_url=endpoint_url,
        )
        try:
            response = s3.list_objects_v2(Bucket=bucket_name)
            if 'Contents' in response:
                print(f"Bucket '{bucket_name}' is accessible and contains objects.")
            else:
                print(f"Bucket '{bucket_name}' is accessible but empty.")
        except Exception as e:
            print(f"Failed to access bucket '{bucket_name}' using boto3: {e}")

    def debug_iceberg_namespace(catalog):
        """Test if Iceberg can list namespaces."""
        try:
            namespaces = catalog.list_namespaces()
            namespace_names = [ns[0] for ns in namespaces]  # Extract namespace names
            print(f"Namespaces in Iceberg catalog: {namespace_names}")
            if "default" not in namespace_names:
                print("Namespace 'default' does not exist.")
            else:
                print("Namespace 'default' exists.")
        except Exception as e:
            print(f"Iceberg namespace listing failed: {e}")

    def export_and_register_parquet():
        """Export DuckDB tables as Parquet files to MinIO (S3) and register them with Iceberg."""
        try:
            # Ensure MinIO bucket exists
            ensure_bucket_exists(S3_BUCKET, MINIO_ENDPOINT)
            # Check bucket accessibility before Iceberg operations
            check_bucket_access(S3_BUCKET, MINIO_ENDPOINT)

            print(f"Connecting to DuckDB file at {DUCKDB_FILE}")
            conn = duckdb.connect(DUCKDB_FILE)
            conn.install_extension("httpfs")
            conn.load_extension("httpfs")

            # Configure DuckDB to use MinIO
            print("Configuring DuckDB for MinIO...")
            conn.sql(f"""
            SET s3_access_key_id='minioadmin';
            SET s3_secret_access_key='minioadmin';
            SET s3_endpoint='minio:9000';
            SET s3_url_style='path';
            SET s3_use_ssl=false;
            """)
            print("DuckDB S3 configuration complete.")

            # Fetch tables from DuckDB
            print("Fetching tables from DuckDB...")
            tables = conn.execute("SHOW TABLES").fetchall()
            print(f"Tables found in DuckDB: {tables}")
            if not tables:
                print("No tables found in DuckDB!")
                return

            # Print PyIceberg version
            print(f"PyIceberg version: {pyiceberg.__version__}")

            # Configure Iceberg catalog with explicit string properties
            print("Configuring Iceberg catalog with properties:")
            catalog_properties = {
                "warehouse": "warehouse",
                "s3.endpoint": "http://minio:9000",
                "s3.access-key-id": "minioadmin",
                "s3.secret-access-key": "minioadmin",
            }
            for k, v in catalog_properties.items():
                print(f"{k} = {v}")

            try:
                catalog = load_catalog(
                    name="rest",
                    uri=ICEBERG_REST_URI,
                    properties=catalog_properties,
                )
                print("Iceberg catalog initialized successfully.")
            except Exception as e:
                print(f"Error initializing Iceberg catalog: {e}")
                raise

            # Test Iceberg namespace
            debug_iceberg_namespace(catalog)
            validate_s3_access(S3_BUCKET, MINIO_ENDPOINT)
            namespace = "default"
            print(f"Checking if namespace '{namespace}' exists...")
            try:
                namespaces = [ns[0] for ns in catalog.list_namespaces()]
                if namespace not in namespaces:
                    print(f"Namespace '{namespace}' does not exist. Creating it...")
                    catalog.create_namespace(namespace)
                    print(f"Namespace '{namespace}' created.")
                else:
                    print(f"Namespace '{namespace}' already exists.")
            except Exception as e:
                print(f"Failed to check or create namespace '{namespace}': {e}")
                raise

            # Process each table
            for (table_name,) in tables:
                parquet_path = f"s3://{S3_BUCKET}/{table_name}.parquet"
                print(f"Exporting table '{table_name}' to {parquet_path}...")

                try:
                    # Export table to Parquet
                    conn.sql(f"COPY {table_name} TO '{parquet_path}' (FORMAT PARQUET)")
                    print(f"Successfully exported: {parquet_path}")

                    # Load data into Arrow for schema
                    arrow_table = conn.sql(f"SELECT * FROM read_parquet('{parquet_path}')").arrow()

                    # Register table with Iceberg
                    print(f"Registering Parquet with Iceberg as table '{namespace}.{table_name}'...")
                    table_identifier = f"{namespace}.{table_name}"
                    print("CATALOGUE", catalog)
                    print("Identifier", table_identifier)
                    print("Schema", arrow_table.schema)
                    created_table = catalog.create_table(
                            identifier=table_identifier,
                            schema=arrow_table.schema
                        )
                    print("CREATED TABLE", created_table)
                    created_table.append(arrow_table)
                    try:
                        print(f"Attempting to load table '{table_identifier}' from Iceberg...")
                        existing_table = catalog.load_table(table_identifier)
                        print(f"Table '{table_identifier}' already exists. Appending data...")
                        existing_table.append(arrow_table)
                        print(f"Data appended to Iceberg table: {table_identifier}")
                    except Exception as e:
                        print(f"Table '{table_identifier}' does not exist. Creating new Iceberg table.")
                        print(f"Error while loading table: {e}")

                        # Create the table without specifying location explicitly
                        created_table = catalog.create_table(
                            identifier=table_identifier,
                            schema=arrow_table.schema
                        )
                        created_table.append(arrow_table)
                        print(f"Iceberg table created and data appended: {table_identifier}")

                except Exception as e:
                    print(f"Failed to export and register table '{table_name}': {e}")

        except Exception as e:
            print(f"Error in export_and_register_parquet: {e}")
            raise

    # Define the Airflow task
    export_and_register_task = PythonOperator(
        task_id="export_and_register_parquet",
        python_callable=export_and_register_parquet,
    )

    export_and_register_task
