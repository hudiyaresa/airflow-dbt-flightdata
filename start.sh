#!/bin/bash

# Exit immediately if a command exits with a non-zero status
set -e

# ------------------------------------------
# PacFlights Data Pipeline: Startup Script
# ------------------------------------------

echo "Stopping any existing containers..."
docker compose -f ./setups/airflow/docker-compose.yaml down -v
docker compose -f ./setups/data_sources/docker-compose.yaml down -v
docker compose -f ./setups/data_warehouse/docker-compose.yaml down -v
docker compose -f ./setups/minio/docker-compose.yaml down -v

echo "Starting Airflow services..."
docker compose -f ./setups/airflow/docker-compose.yaml up --build -d

echo "Starting source database services..."
docker compose -f ./setups/data_sources/docker-compose.yaml up --build -d

echo "Starting data warehouse services..."
docker compose -f ./setups/data_warehouse/docker-compose.yaml up --build -d

echo "Starting MinIO services..."
docker compose -f ./setups/minio/docker-compose.yaml up --build -d

echo "Waiting for Airflow and dependencies to fully start (sleeping 50s)..."
sleep 50

echo "Importing Airflow connections from include/connections.yaml..."
docker exec -i airflow-webserver airflow connections import include/connections.yaml

echo "Importing Airflow variables from include/variables.json..."
docker exec -i airflow-webserver airflow variables import -a overwrite include/variables.json

echo "Startup complete. Access the Airflow UI at http://localhost:8080"
