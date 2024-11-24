from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import duckdb

def extract_weather_data(**kwargs):
    # Load processed weather data from JSON
    file_path = '/opt/airflow/data/processed/weather_data.json'
    with open(file_path, 'r') as f:
        weather_data = json.load(f)

    # Convert to DataFrame
    df_weather = pd.DataFrame(weather_data['daily'])

    # Correct handling of 'time' column
    df_weather['date'] = pd.to_datetime(weather_data['daily']['time']).strftime('%Y-%m-%d')  # Convert to string directly
    return df_weather.to_dict(orient='records')  # Return as list of records for XCom

def extract_accident_data(**kwargs):
    # Load cleaned accident data from CSV
    file_path = '/opt/airflow/data/processed/lo_2011_2024_clean.csv'
    df_accidents = pd.read_csv(file_path, sep=';')

    # Convert date to string to ensure JSON serialization
    df_accidents['Toimumisaeg'] = pd.to_datetime(df_accidents['Toimumisaeg']).dt.strftime('%Y-%m-%d')
    return df_accidents.to_dict(orient='records')  # Return as list of records for XCom

def join_datasets(**kwargs):
    ti = kwargs['ti']
    
    # Pull data from XCom
    weather_data = pd.DataFrame(ti.xcom_pull(task_ids='extract_weather_data'))
    accident_data = pd.DataFrame(ti.xcom_pull(task_ids='extract_accident_data'))
    
    # Ensure consistent date formats
    weather_data['date'] = pd.to_datetime(weather_data['date'])
    accident_data['Toimumisaeg'] = pd.to_datetime(accident_data['Toimumisaeg'])

    # Join datasets on date (and optionally location if available)
    joined_data = pd.merge(
        accident_data,
        weather_data,
        left_on='Toimumisaeg',
        right_on='date',
        how='inner'
    )
    
    # Convert all Timestamp objects to string for JSON serialization
    joined_data['Toimumisaeg'] = joined_data['Toimumisaeg'].dt.strftime('%Y-%m-%d')
    joined_data['date'] = joined_data['date'].dt.strftime('%Y-%m-%d')
    
    return joined_data.to_dict(orient='records')  # Return as list of records

def save_combined_data(**kwargs):
    ti = kwargs['ti']
    
    # Pull joined data from XCom
    joined_data = pd.DataFrame(ti.xcom_pull(task_ids='join_datasets'))
    
    # Save to DuckDB
    db_path = '/opt/airflow/data/processed/integrated_data.duckdb'
    conn = duckdb.connect(db_path)
    conn.execute("CREATE OR REPLACE TABLE integrated_data AS SELECT * FROM joined_data")
    conn.close()

    # Optionally save to CSV as well
    csv_path = '/opt/airflow/data/processed/integrated_data.csv'
    joined_data.to_csv(csv_path, index=False)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'combine_datasets_dag',
    default_args=default_args,
    description='DAG to combine weather and accident datasets',
    schedule_interval='@once',  # Run on demand
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    extract_weather = PythonOperator(
        task_id='extract_weather_data',
        python_callable=extract_weather_data,
        provide_context=True
    )

    extract_accidents = PythonOperator(
        task_id='extract_accident_data',
        python_callable=extract_accident_data,
        provide_context=True
    )

    join_data = PythonOperator(
        task_id='join_datasets',
        python_callable=join_datasets,
        provide_context=True
    )

    save_data = PythonOperator(
        task_id='save_combined_data',
        python_callable=save_combined_data,
        provide_context=True
    )

    # Define task dependencies
    [extract_weather, extract_accidents] >> join_data >> save_data
