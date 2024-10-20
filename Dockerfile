# Use an official Python image as a base
FROM python:3.12-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    git \
    build-essential \
    libpq-dev \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Install the Google Cloud SDK
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH $PATH:/root/google-cloud-sdk/bin

# Set environment variables for Airflow and dbt
ENV AIRFLOW_HOME=/airflow
ENV DBT_HOME=/dbt

# Install Python dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip && pip install -r /app/requirements.txt

# Copy the rest of the application code into the container
COPY . /app
WORKDIR /app

# Set environment variables
ENV PYTHONPATH=/app

# Configure Git credentials and set up GitHub token environment variable
ARG GITHUB_TOKEN
RUN git config --global user.email "sckintas@gmail.com" && \
    git config --global user.name "sckintas" && \
    git config --global credential.helper store && \
    echo "https://${GITHUB_TOKEN}:x-oauth-basic@github.com" > /root/.git-credentials

# Default command to run the Python script for the CI/CD pipelines
CMD ["python", "/app/run_pipeline.py"]
