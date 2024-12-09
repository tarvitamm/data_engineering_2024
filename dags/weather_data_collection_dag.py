from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from pymongo import MongoClient
import json

def fetch_and_save_weather_to_mongodb():
    # Fetch weather data from API
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 58.3827,
        "longitude": 26.7158,
        "start_date": "2011-01-01",
        "end_date": "2024-01-01",
        "daily": "temperature_2m_max,temperature_2m_min,precipitation_sum,snowfall_sum,rain_sum",
        "timezone": "Europe/Tallinn"
    }
    response = requests.get(url, params=params)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")
    
    data = response.json()

    # Connect to MongoDB
    mongo_uri = "mongodb://root:example@mongodb:27017"
    client = MongoClient(mongo_uri)
    db = client["weather_data"]  # Database name
    collection = db["daily_weather"]  # Collection name

    # Transform and insert data into MongoDB
    # Assuming data['daily'] contains the weather records
    if 'daily' in data:
        daily_data = data['daily']
        records = []

        # Transform API response into individual records
        for i in range(len(daily_data['time'])):
            record = {
                "date": daily_data['time'][i],
                "temperature_max": daily_data['temperature_2m_max'][i],
                "temperature_min": daily_data['temperature_2m_min'][i],
                "precipitation_sum": daily_data['precipitation_sum'][i],
                "snowfall_sum": daily_data['snowfall_sum'][i],
                "rain_sum": daily_data['rain_sum'][i]
            }
            records.append(record)

        # Insert records into MongoDB
        collection.insert_many(records)
        print(f"Inserted {len(records)} weather records into MongoDB.")
    else:
        raise Exception("No daily data found in API response.")

    client.close()

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_data_to_mongodb_DAG',
    default_args=default_args,
    description='DAG to fetch weather data from API and save to MongoDB',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    fetch_and_save_weather = PythonOperator(
        task_id='fetch_and_save_weather_to_mongodb',
        python_callable=fetch_and_save_weather_to_mongodb,
    )

    fetch_and_save_weather
