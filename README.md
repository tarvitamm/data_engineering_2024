# Understanding the Link Between Weather Patterns and Traffic Accidents in Estonia

Our project tries to outline the correlations between weather conditions and car accidents by combining weather and car accident data.
The weather data comes from the Open Meteo historical weather API and the accident data comes from the Estonian freely available datasets.

The business questions that we were trying to find answers to:

- What is the relationship between weather conditions and the frequency of traffic accidents?
- How does temperature influence the severity of traffic accidents?
- With what conditions (weather / time of day) do the majority of accidents happen?

## Technical overview

**Key Tools and Frameworks**:

- **Docker & Docker compose**: Containerize the entire environment, allowing easy setup and consistent reproducibility.
- **Apache Airflow**: Orchestrates and schedules the data pipeline steps.
- **DuckDB**: A fast, file-based analytical database used for integrating and querying data.
- **MongoDB**: Stores intermediate weather data.
- **dbt (Data Build Tool)**: Handles SQL-based transformations, ensuring reproducible and tested data models.
- **Redis**: Caches query results and logs query performance for faster data access and monitoring.
- **MinIO & .parquet files**: Local object storage (S3-compatible) for storing Parquet files.
- **Dash & Streamlit**: Provide interactive dashboards and visualizations.
- **Metadata storage layer via Postgres**: Creation of data dictionary and the showing of data lineage.

**Data Flow Summary**:

1. **Weather Data Collection**: Fetch historical weather data from an API and store it in MongoDB.
2. **Car Accident Data Processing**: Clean and validate historical accident data from a CSV file.
3. **Combine Accident & Weather Data**: Integrate both datasets into a unified DuckDB database.
4. **dbt Transformations**: Apply a star schema and run data validations using dbt models.
5. **Export to Parquet**: Store the transformed data in Parquet format on MinIO.
6. **Metadata Storage Layer via Postgres** : Import the DuckDB files into Postgres for the creation of a robust metadata storage layer
7. **Visualizations**: Use Dash and Streamlit apps to explore and visualize the integrated data.

## Pipeline Steps (DAGs)

These are the six DAGs in Airflow. **Run them in the following order**:

1. **`1_weather_data_collection_dag.py`**

   - **What it does**: Fetches weather data from the Open Meteo API and inserts it into MongoDB.
   - **Why needed**: To provide contextual environmental conditions that can be correlated with accident occurrences.

2. **`2_car_accidents_processing_dag.py`**

   - **What it does**: Cleans and validates the raw accident CSV data and stores a refined version.
   - **Why needed**: Ensures consistent, quality data, ready for integration with weather data.

3. **`3_combine_accident_weather_dag.py`**

   - **What it does**: Joins the cleaned accident data with the collected weather data, storing the combined dataset in DuckDB.
   - **Why needed**: Consolidates both datasets into a single source, making advanced analysis possible.

4. **`4_dbt_run_dag.py`**

   - **What it does**: Executes dbt models against the DuckDB database, applying transformations and enforcing a star schema.
   - **Why needed**: Transforms raw integrated data into a well-modeled, analytics-ready format.

5. **`5_duckdb_to_parquet_dag.py`**

   - **What it does**: Exports the transformed DuckDB tables as Parquet files into MinIO.
   - **Why needed**: Provides efficient, scalable columnar storage, easily accessible for downstream systems and queries.

6. **`6_data_to_postgres_dag.py`**
   - **What it does**: Imports DuckDB data into Postgres.
   - **Why needed**: The Postgres database serves as a metadata storage layer, retrieving it's data from DuckDB

## Prerequisites

- **Docker & Docker Compose** must be installed on your system.

## Running the Pipeline

1. **Build and Start the Containers**:

   ```bash
   docker compose build
   docker compose up -d
   ```

   This step:

   - Starts the Airflow webserver and scheduler.
   - Launches MongoDB, Minio, mc, Redis, Postgres and dbt containers.
   - Doesn't launch Dash and Streamlit containers because there is yet no data to visualize.

2. **Access Airflow UI**:

   - When airflow_container is listening to port 8080, then:
   - Open your browser: [http://localhost:8080](http://localhost:8080)
   - Login credentials: `admin / admin`

3. **Execute the DAGs in Order**:
   In the Airflow UI, manually trigger each DAG in the following order, ensuring one finishes before starting the next:

   1. `1_weather_data_to_mongodb_DAG`
   2. `2_accidents_data_DAG`
   3. `3_combine_datasets_DAG`
   4. `4_duckdb_dbt_process_DAG`
   5. `5_duckdb_to_parquet_DAG`
   6. `6_data_to_postgres_DAG`

   Each DAG performs a critical step in the data pipeline.

4. **Check Parquet Files in MinIO**:

   - Open MinIO Console: [http://localhost:9001](http://localhost:9001)
   - Login credentials: `minioadmin / minioadmin`
   - Browse the `warehouse` bucket to confirm the Parquet files have been successfully created.

5. **Start the visualisation containers**
   Make sure that before starting the visualisation containers all of the DAG-s have finished!
   This step:

   - Starts the Dash and Streamlit containers.

   ```bash
   docker network connect data_engineering_network streamlit
   docker network connect data_engineering_network dash_app
   docker compose --profile visualisation up -d
   ```

6. **View Dash Dashboard**:

   - Open Dash: [http://localhost:8050](http://localhost:8050)
   - Explore interactive charts, graphs, and metrics derived from the integrated data.

7. **View Streamlit App**:

   - Open Streamlit: [http://localhost:8501](http://localhost:8501)
   - This is an alternate dashboard view for exploring the processed data.

8. **(optional) View metadata files**
   - Data dictionary made using Open Metadata.
   - Link to metadata files: https://drive.google.com/drive/folders/1VHYvsljIETm5yLpaN0J9oLWXxLifasAn?usp=sharing

---

## Dash visualization information

The Dash application provides the following interactive visualizations and insights:

1. **Crash Density Heatmap**:

   - Displays the geographic density of crashes across Estonia.
   - Uses color intensity to represent the number of persons involved in crashes.

2. **Monthly Aggregation**:

   - Highlights trends in crash casualties, precipitation, and temperature over time.
   - Features bar charts and trendlines for clear trend analysis.

3. **Severity vs. Temperature**:

   - Visualizes how accident severity (injuries and fatalities) varies across different temperature categories.
   - Includes grouped bar charts and trendlines to showcase relationships.

4. **Map Visualization**:

   - A scatter map displaying crash locations with weather conditions (e.g., precipitation levels) and crash severity indicators (e.g., injuries).

5. **Query Logs**:
   - Lists detailed query performance logs, including:
     - Execution times.
     - Rows affected.
     - Timestamps.
   - Useful for monitoring and debugging query performance.

---

## Streamlit visualization information

The Streamlit application provides interactive insights into car crashes and weather data through the following pages:

1. **Monthly Aggregation**:

   - Highlights trends in crashes, precipitation, and temperature on a monthly basis.
   - Visualized with bar charts and trendlines to show:
     - Total persons involved in crashes.
     - Precipitation levels.
     - Average monthly temperatures.

2. **Severity vs Temperature**:

   - Examines how accident severity (injuries and fatalities) correlates with temperature categories.
   - Includes grouped bar charts and trendlines for better understanding of patterns.

3. **Precipitation Analysis**:
   - Displays the relationship between precipitation levels and the number of vehicles and people involved in crashes.
   - Features grouped bar charts to show counts of vehicles and people under various precipitation categories.

---

## Future steps

The inclusion of parquet files using Minio was mainly done with the aim to attach Iceberg to it, for additional metadata information gathering.
Unfortunately due to unforeseen error messages, the integration progress got halted, which resulted in Postgres conversion for metadata generation.
In the repository can be found a file "Iceberg_fix_history.docx", which has the history of a 2 day long debugging session with ChatGPT 4o/o1. All 1090 pages of debugging didn't lead to a working solution since Iceberg refused to find the according Minio bucket where the .parquet files resided.
Therefore the inclusion of Iceberg into this data pipeline will be a future step for our team.
