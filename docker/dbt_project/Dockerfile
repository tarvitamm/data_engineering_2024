FROM python:3.9-slim

# Set the working directory
WORKDIR /usr/app/dbt_project

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DBT project files
COPY . .

# Default command (adjust if needed)
CMD ["bash"]
