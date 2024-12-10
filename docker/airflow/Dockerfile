FROM apache/airflow:2.5.1-python3.9

USER root

# Install any additional OS packages if needed
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Install dbt-core and dbt-duckdb adapter
RUN pip install dbt-core dbt-duckdb

# Copy requirements and install Python packages
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
# Install pymongo
RUN pip install pymongo

# No need to copy DAGs, as they are mounted via volumes in docker-compose.yml

# Set the entrypoint (if different from default)
# ENTRYPOINT ["/entrypoint"]

# Expose port (if needed)
EXPOSE 8080
