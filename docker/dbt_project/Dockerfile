FROM python:3.9-slim

# Set the working directory
WORKDIR /usr/app/dbt_project

# Install dependencies for DuckDB CLI
RUN apt-get update && apt-get install -y wget unzip

# Install DuckDB CLI
RUN wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip \
    && unzip duckdb_cli-linux-amd64.zip -d /usr/local/bin/ \
    && chmod +x /usr/local/bin/duckdb

# Install dbt and DuckDB adapter
RUN pip install dbt-core dbt-duckdb

# Keep the container running
CMD ["tail", "-f", "/dev/null"]