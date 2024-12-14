from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def ingest_data():
    file_path = '/opt/airflow/data/raw/lo_2011_2024.csv'
    df = pd.read_csv(file_path, sep=';')  # Read the raw CSV file
    return df.to_dict(orient='records')  # Convert to JSON-serializable format

def clean_and_transform_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='ingest_data')  # Pull data as a list of dictionaries
    df = pd.DataFrame(data)  # Convert back to a DataFrame

    # List of columns to be kept
    säilitatavad_veerud = [
        "Juhtumi nr", "Toimumisaeg", "Isikuid", "Hukkunuid", "Sõidukeid", "Vigastatuid", "Maakond",
        "Omavalitsus", "Asula", "Liiklusõnnetuse liik", "Liiklusõnnetuse liik (detailne)",
        "Joobes mootorsõidukijuhi osalusel", "Kergliikurijuhi osalusel", "Jalakäija osalusel",
        "Kaassõitja osalusel", "Maastikusõiduki juhi osalusel", "Eaka (65+) mootorsõidukijuhi osalusel",
        "Bussijuhi osalusel", "Veoautojuhi osalusel", "Ühissõidukijuhi osalusel", "Sõiduautojuhi osalusel",
        "Mootorratturi osalusel", "Mopeedijuhi osalusel", "Jalgratturi osalusel", "Alaealise osalusel",
        "Esmase juhiloa omaniku osalusel", "Turvavarustust mitte kasutanud isiku osalusel",
        "Mootorsõidukijuhi osalusel", "X koordinaat", "Y koordinaat"
    ]

    df = df[säilitatavad_veerud].dropna()
    df = df[df['Juhtumi nr'].str.isdigit()]

    # Convert data types
    df['Juhtumi nr'] = df['Juhtumi nr'].astype(int)
    df['Toimumisaeg'] = pd.to_datetime(df['Toimumisaeg'])
    df['Toimumisaeg'] = df['Toimumisaeg'].dt.strftime('%Y-%m-%d %H:%M:%S')  # Convert to string for JSON serialization

    return df.to_dict(orient='records')  # Convert to JSON-serializable format

def validate_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='clean_and_transform_data')  # Pull data as a list of dictionaries
    df = pd.DataFrame(data)  # Convert back to a DataFrame

    # Data validation
    valid_maakonnad = [
        'Harju maakond', 'Hiiu maakond', 'Ida-Viru maakond', 'Jõgeva maakond', 'Järva maakond',
        'Lääne maakond', 'Lääne-Viru maakond', 'Põlva maakond', 'Pärnu maakond', 'Rapla maakond',
        'Saare maakond', 'Tartu maakond', 'Valga maakond', 'Viljandi maakond', 'Võru maakond'
    ]
    df = df[df['Maakond'].isin(valid_maakonnad)]

    valid_asulas = ['JAH', 'EI']
    df = df[df['Asula'].isin(valid_asulas)]

    return df.to_dict(orient='records')  # Convert to JSON-serializable format

def store_data(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='validate_data')  # Pull data as a list of dictionaries
    df = pd.DataFrame(data)  # Convert back to a DataFrame

    # Save the final cleaned data
    output_path = '/opt/airflow/data/processed/lo_2011_2024_clean.csv'
    df.to_csv(output_path, index=False, sep=';')
    print(f"Data stored at {output_path}")

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '2_accident_data_DAG',
    default_args=default_args,
    description='DAG to process input csv file data',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data
    )

    clean_transform_task = PythonOperator(
        task_id='clean_and_transform_data',
        python_callable=clean_and_transform_data,
        provide_context=True
    )

    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True
    )

    store_task = PythonOperator(
        task_id='store_data',
        python_callable=store_data,
        provide_context=True
    )

    # Define dependencies between tasks
    ingest_task >> clean_transform_task >> validate_task >> store_task
