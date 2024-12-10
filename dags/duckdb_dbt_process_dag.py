from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow_dbt_python.operators.dbt import DbtRunOperator
from datetime import datetime
import os

def verify_duckdb_file():
    db_path = '/opt/airflow/data/processed/integrated_data.duckdb'
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"DuckDB file not found at {db_path}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    dag_id='duckdb_dbt_process',
    default_args=default_args,
    description='Run dbt transformations',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1), # confirm??
    catchup=False,
) as dag:

    # Task 1: Verify the DuckDB file exists
    verify_file = PythonOperator(
        task_id='verify_duckdb_file',
        python_callable=verify_duckdb_file
    )

    # Task 2: Run dbt
    dbt_run = DbtRunOperator(
        task_id='run_dbt',
        project_dir='/usr/app/dbt_project/',
        profiles_dir='/root/.dbt',
        select='*',
    )

    verify_file >> dbt_run
