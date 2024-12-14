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
    dag_id='4_duckdb_dbt_process_DAG',
    default_args=default_args,
    description='Run dbt transformations in sequence',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),  # confirm start date
    catchup=False,
) as dag:

    # Task 1: Verify the DuckDB file exists
    verify_file = PythonOperator(
        task_id='verify_duckdb_file',
        python_callable=verify_duckdb_file
    )

    # Task 2: Run staging models
    dbt_run_staging = DbtRunOperator(
        task_id='run_dbt_staging',
        project_dir='/usr/app/dbt_project/',
        profiles_dir='/home/airflow/.dbt',
        select=['staging'],  # Ensure models are tagged as 'staging'
    )

    # Task 3: Run dimension models
    dbt_run_dimensions = DbtRunOperator(
        task_id='run_dbt_dimensions',
        project_dir='/usr/app/dbt_project/',
        profiles_dir='/home/airflow/.dbt',
        select=['dimensions'],  # Ensure models are tagged as 'dimensions'
    )

    # Task 4: Run fact models
    dbt_run_fact = DbtRunOperator(
        task_id='run_dbt_fact',
        project_dir='/usr/app/dbt_project/',
        profiles_dir='/home/airflow/.dbt',
        select=['fact'],  # Ensure models are tagged as 'fact'
    )

    # Task 5: Run analysis models
    dbt_run_analysis = DbtRunOperator(
        task_id='run_dbt_analysis',
        project_dir='/usr/app/dbt_project/',
        profiles_dir='/home/airflow/.dbt',
        select=['analysis'],  # Ensure models are tagged as 'analysis'
    )

    # Define task dependencies
    verify_file >> dbt_run_staging >> dbt_run_dimensions >> dbt_run_fact >> dbt_run_analysis