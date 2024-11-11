# Use an official Python image as the base
FROM python:3.9-slim

# Set work directory
WORKDIR /usr/app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Set entrypoint to run dbt commands
ENTRYPOINT ["dbt"]
