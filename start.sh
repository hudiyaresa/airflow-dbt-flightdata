#!/bin/bash
# PacFlights: start every service, then import Airflow connections and variables.
set -e

if [ ! -f .env ]; then
  echo "Missing .env. Copy .env.example to .env and fill in the values first."
  exit 1
fi

echo "Building and starting all services..."
docker compose up --build -d

echo "Waiting for the Airflow webserver to become healthy..."
until [ "$(docker inspect -f '{{.State.Health.Status}}' airflow-webserver 2>/dev/null)" = "healthy" ]; do
  sleep 5
done

echo "Importing Airflow connections from include/connections.yaml..."
docker exec -i airflow-webserver airflow connections import include/connections.yaml

echo "Importing Airflow variables from include/variables.json..."
docker exec -i airflow-webserver airflow variables import -a overwrite include/variables.json

echo "Startup complete. Airflow UI: http://localhost:8080"
