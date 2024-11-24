# Data Engineering Project 2024

This repository contains the data pipeline and transformation process for combining accident data and weather data using Apache Airflow, DuckDB, and dbt. The goal is to create a structured dataset, enabling analysis and insights while adhering to a star schema model.

## Architecture Overview

The project consists of the following components:

### 1. Data Orchestration with Airflow

Three DAGs handle data extraction, processing, and integration:

- **`car_accidents_processing_dag.py`**: Cleans and processes car accident data from a raw CSV file.
- **`weather_data_collection_dag.py`**: Extracts weather data using an API and saves it as JSON.
- **`combine_accident_weather_dag.py`**: Combines processed accident data and weather data, then creates a DuckDB database.

### 2. Data Transformation with dbt

A `dbt_project` is integrated for transforming the data stored in DuckDB.
Includes:

- **`combined_analysis.sql`**: A dbt model to aggregate accident and weather data.
- **`schema.yml`**: Describes the models, columns, and tests for validation.

### 3. Data Modeling

The data will be structured using a **star schema**:

- **Fact Table**: Stores accident and weather data with metrics like accident counts.
- **Dimension Tables**: Contain metadata for locations, accident types, and weather conditions.

---

## Data Pipeline Workflow

### Accident Data Processing

The DAG `car_accidents_processing_dag.py` processes raw accident data, removing null values and cleaning columns.

### Weather Data Extraction

The DAG `weather_data_collection_dag.py` collects weather data using the Open Meteo API, storing it as JSON.

### Combining Data

The DAG `combine_accident_weather_dag.py` joins the processed accident and weather datasets, storing the result in a DuckDB database (`integrated_data.duckdb`).

### Data Transformation

dbt uses SQL models to refine and structure the combined data, creating views for analysis.

---

## Requirements

- **Docker**: Used for containerization and managing Airflow and dbt services.
- **Apache Airflow**: Orchestrates the data pipeline.
- **DuckDB**: A lightweight, fast database to store and query data.
- **dbt**: Transforms and models data in DuckDB.
- **Streamlit (Planned)**: To visualize the results.
