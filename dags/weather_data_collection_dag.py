from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests

def fetch_weather_data(ti):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": 58.3827,
        "longitude": 26.7158,
        "start_date": "2023-01-01",
        "end_date": "2023-01-10",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum",
        "timezone": "Europe/Tallinn"
    }
    response = requests.get(url, params=params)
    data = response.json()
    
    # Log the JSON response to see it in the Airflow logs
    print("Weather Data:", data)
    
    # Push data to XCom for other tasks to access
    ti.xcom_push(key='weather_data', value=data)

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_data_extraction',
    default_args=default_args,
    description='DAG to fetch weather data from API',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    fetch_weather = PythonOperator(
        task_id='fetch_weather_data',
        python_callable=fetch_weather_data,
    )

    fetch_weather