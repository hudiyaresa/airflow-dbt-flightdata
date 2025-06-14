# Pac-Flights Data Pipeline


## Table of Contents

1. [Overview](#overview)  
2. [Architecture](#architecture)  
3. [Pipeline Flow](#pipeline-flow)  
4. [Project Structure](#project-structure)  
5. [How to Run the Pipeline](#how-to-run-the-pipeline)  
6. [Environment Setup](#environment-setup)  
7. [Screenshots](#screenshots)  



## Overview
This project builds an orchestrated data pipeline for a flight booking system using Apache Airflow. It extracts data from a source PostgreSQL DB, stores it in MinIO, loads it into a warehouse.

This version is improvements that supports incremental data loading, Jinja templating, XComs communication, Slack alerting, dynamic task creation, DBT transformations, modular configuration using CLI and powered by Apache  Airflow using CeleryExecutor.

### Improvement Pipeline Features

| Feature                  | Description                                                        |
| ------------------------ | ------------------------------------------------------------------ |
| Incremental Mode         | Controlled via `load_mode` incremental variable (`true` or `false`)|
| Jinja Templating         | Dynamic query generation using Airflow’s `{{ ds }}` execution date |
| XComs                    | Pass file metadata from Extract → Load                             |
| Pangres Upsert           | UPSERT data using `pangres` with conflict handling                 |
| Skip Exceptions          | Gracefully skip load if CSV file is empty                          |
| Slack Notifier           | Notifies a Slack channel on task failure                           |
| CLI Config for Variables | Load `variables.json` and `connections.yaml` using Airflow CLI     |
| TriggerDagRunOperator    | Used to trigger dependent DAGs (e.g., `flights_warehouse_pipeline`)        |
| DBT Transformation       | Leverages DBT models for transforming staging into dimensional/fact tables |
| Celery Executor          | Scalable parallel task processing                                          |

---


## Architecture

- **Source DB**: PostgreSQL (`bookings` schema)
- **Data Lake**: MinIO (`extracted-data` bucket)
- **Data Warehouse**: PostgreSQL (`staging` schema)
- **Data Transformations**: DBT (`warehouse` schema)
- **Orchestrator**: Apache Airflow
- **Notifier**: Slack Webhook on task failure 
- **Docker**: All services are containerized using Docker Compose

```text
[PostgreSQL (bookings)]
  └→ Extract (Airflow)
      └→ [MinIO: extracted-data/]
          └→ Load (Upsert)
              └→ [PostgreSQL: stg schema]
                  └→ TriggerDagRunOperator
                      └→ [DBT Transformation DAG]
                          └→ [warehouse (dim_*, fct_*)]
````

<!-- ![ELT DAG](docs/elt_dag.png) -->

---

## Pipeline Flow

| Step | Process     | Tool              | Description                                               |
| ---- | ----------- | ----------------- | --------------------------------------------------------- |
| 1    | Extract     | Python + Airflow  | Export from source DB to MinIO as CSV                     |
| 2    | Load        | Airflow + Pangres | Load into staging schema with upsert strategy             |
| 3    | Trigger DAG | Airflow           | Triggers `flights_warehouse_pipeline` using TriggerDagRun |
| 4    | Transform   | DBT + Airflow     | Runs DBT models to build dimensional and fact tables      |

---

## Project Structure

```bash
airflow-dbt-flightdata/
├── dags/
│   ├── flights_staging_pipeline/
│   │   ├── tasks/
│   │   │   ├── components/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── extract.py
│   │   │   │   ├── load.py
│   │   │   │   └── transform.py
│   │   │   ├── __init__.py
│   │   │   ├── main.py
│   │   │   └── run.py
│   ├── flights_warehouse_pipeline/
│   │   ├── flight_dbt/
│   │   │   ├── logs/
│   │   │   │   └── dbt.log
│   │   │   ├── macros/
│   │   │   ├── models/
│   │   │   ├── seeds/
│   │   │   │   ├── dim_date.csv
│   │   │   │   └── dim_time.csv
│   │   │   ├── snapshots/
│   │   │   │   ├── dim_aircrafts.sql
│   │   │   │   ├── dim_airport.sql
│   │   │   │   ├── dim_passenger.sql
│   │   │   │   ├── dim_seat.sql
│   │   │   │   ├── fct_boarding_pass.sql
│   │   │   │   ├── fct_booking_ticket.sql
│   │   │   │   ├── fct_flight_activity.sql
│   │   │   │   └── fct_seat_occupied_daily.sql
│   │   │   └── sources.yml
│   │   ├── dbt_project.yml
│   │   └── __init__.py
├── helper/
│   ├── __pycache__/
│   ├── callback.py
│   ├── __init__.py
│   ├── minio.py
│   └── postgres.py
├── data/
│   ├── source/
│   │   └── init.sql
│   └── warehouse/
│       ├── init.sql
│       └── dtg_schema.sql
├── docs/
├── include/
├── logs/
├── plugins/
├── .env
├── .gitignore
├── .python-version
├── docker-compose.yml
├── Dockerfile
├── fernet.py
├── README.md
└── requirements.txt
```

---


## How to Run the Pipeline

Clone and enter project
```bash
git clone https://github.com/hudiyaresa/airflow-incremental-flightdata.git
```

```bash
cd flights-data-pipeline
```

Start services
```bash
docker-compose up --build
```

### Access UI

| Service       | URL                                            |
| ------------- | ---------------------------------------------- |
| Airflow UI    | [http://localhost:8080](http://localhost:8080) |
| MinIO Browser | [http://localhost:9001](http://localhost:9001) |
| Postgres DB   | localhost:5432 (e.g., via DBeaver or pgAdmin)  |


---

## Environment Setup

Create a `.env` file in the root project with the following keys:

```env
# Airflow config
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=your_fernet_key
AIRFLOW_WEBSERVER_SECRET_KEY=your_webserver_key
AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow-metadata-6/airflow
AIRFLOW_CELERY_RESULT_BACKEND=db+postgresql://airflow:airflow@airflow-metadata-6/airflow
AIRFLOW_CELERY_BROKER_URL=redis://:@redis:6379/0

# Warehouse (staging and dimensional)
WAREHOUSE_DB_USER=...
WAREHOUSE_DB_PASSWORD=...
WAREHOUSE_DB_NAME=warehouse_pacflight

# Source DB (bookings)
PACFLIGHT_DB_USER=...
PACFLIGHT_DB_PASSWORD=...
PACFLIGHT_DB_NAME=pacflight

# MinIO
MINIO_ROOT_USER=...
MINIO_ROOT_PASSWORD=...
```

### Generate Fernet Key for Airflow

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

Then copy it into your `.env`:

```env
AIRFLOW_FERNET_KEY=your_generated_fernet_key
```

## Load Variables and Connections via CLI

```bash
docker exec -it airflow_standalone bash
```

### Import Airflow Variables

```bash
airflow variables import include/variables.json
```

### Import Airflow Connections

```bash
airflow connections import include/connections.yaml
```

make sure to set you credentials in connections (password, secret key, etc)


### Slack Notification Setup

To receive task failure notifications in Slack:

1. Create a Slack webhook via Slack App → Incoming Webhooks
2. Add the webhook URL into your `password` as `HTTP` connections
3. Ensure your DAG uses Slack alert logic in its failure callbacks


---


## Screenshots

### 1. DAG Overview

![DAG Staging](docs/flight_staging_pipeline.png)
![DAG Warehouse](docs/flight_warehouse_pipeline.png)

### 2. Task Groups

| Extract                                  |
| ---------------------------------------- |
| ![Extract Group](docs/extract_group.png) |

| Load                                  |
| ---------------------------------------- |
| ![Load Group](docs/load_group.png) | 

| DBT Transform                                  |
| ---------------------------------------- |
| ![Transform Group](docs/transform_group.png) |

### 3. Querying Final Warehouse

![Query Sample](docs/example_query_from_final_dwh.png)

### 4. Slack Notifications

![Slack Notifications](docs/slack_notifications.png)

### 5. Xcoms

![Xcoms](docs/xcoms.png)
