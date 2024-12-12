from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# PostgreSQL container details
POSTGRES_CONTAINER_NAME = "airflow_postgres"
POSTGRES_IMAGE = "postgres:15"
POSTGRES_PORT = 5433  # Ensure this port is available on your host system
POSTGRES_PASSWORD = "postgres"
POSTGRES_DB = "airflow_db"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "start_postgres_dag",
    default_args=default_args,
    description="Start and Stop PostgreSQL container",
    schedule_interval=None,
    catchup=False,
) as dag:

    start_postgres_container = BashOperator(
        task_id="start_postgres_container",
        bash_command=(
            f"docker run --rm --name {POSTGRES_CONTAINER_NAME} "
            f"-e POSTGRES_PASSWORD={POSTGRES_PASSWORD} "
            f"-e POSTGRES_DB={POSTGRES_DB} "
            f"-p {POSTGRES_PORT}:5432 -d {POSTGRES_IMAGE}"
        ),
    )

    # Task dependencies
    start_postgres_container
