from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time
import duckdb
import psycopg2
from psycopg2 import sql

# PostgreSQL connection details
POSTGRES_CONN_PARAMS = {
    "host": "postgres",  # Matches the service name in docker-compose.yml
    "database": "airflow_db",
    "user": "postgres",
    "password": "postgres",
    "port": 5432,  # Internal port of the PostgreSQL container
}

# DuckDB file path
DUCKDB_FILE = "/opt/airflow/data/processed/integrated_data.duckdb"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

def transfer_duckdb_to_postgres():
    """Transfer all tables from all schemas in DuckDB to PostgreSQL."""
    import duckdb
    import psycopg2
    from psycopg2 import sql

    # Connect to DuckDB
    duckdb_conn = duckdb.connect(DUCKDB_FILE)

    # Connect to PostgreSQL
    pg_conn = psycopg2.connect(**POSTGRES_CONN_PARAMS)
    pg_cursor = pg_conn.cursor()

    # Fetch all tables from all schemas in DuckDB
    tables = duckdb_conn.execute("""
        SELECT table_schema, table_name 
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema');
    """).fetchall()

    for schema_name, table_name in tables:
        print(f"Processing table: {table_name} from schema: {schema_name}")

        # Fetch schema for the table
        schema = duckdb_conn.execute(f"DESCRIBE {schema_name}.{table_name}").fetchall()
        print(f"Schema for table {table_name}: {schema}")

        # Map DuckDB types to PostgreSQL types
        type_mapping = {
            'BIGINT': 'BIGINT',
            'VARCHAR': 'VARCHAR',
            'DOUBLE': 'DOUBLE PRECISION',  # Corrected mapping
        }

        # Create the table in PostgreSQL
        columns = ", ".join([
            f'"{col[0]}" {type_mapping.get(col[1], col[1])}'
            for col in schema
        ])
        create_table_query = f"CREATE TABLE IF NOT EXISTS {schema_name}_{table_name} ({columns});"
        print(f"Creating table with query: {create_table_query}")
        pg_cursor.execute(create_table_query)

        # Fetch data from DuckDB
        data = duckdb_conn.execute(f"SELECT * FROM {schema_name}.{table_name}").fetchall()

        # Insert data into PostgreSQL
        if data:
            placeholders = ", ".join(["%s"] * len(schema))
            insert_query = f"INSERT INTO {schema_name}_{table_name} VALUES ({placeholders});"
            pg_cursor.executemany(insert_query, data)
            print(f"Inserted {len(data)} rows into {schema_name}_{table_name}")

    # Commit changes and close connections
    pg_conn.commit()
    pg_cursor.close()
    pg_conn.close()
    duckdb_conn.close()
    print("Data transfer from DuckDB to PostgreSQL completed successfully.")


with DAG(
    "6_data_to_postgres_DAG",
    default_args=default_args,
    description="Transfer data from DuckDB to PostgreSQL",
    schedule_interval=None,
    catchup=False,
) as dag:

    transfer_data_task = PythonOperator(
        task_id="transfer_duckdb_to_postgres",
        python_callable=transfer_duckdb_to_postgres,
    )

    transfer_data_task