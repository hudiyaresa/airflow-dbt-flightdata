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
This project builds an orchestrated data pipeline for a flight booking system using Apache Airflow. It extracts data from a source PostgreSQL DB, stores it in MinIO, loads it into a warehouse, and transforms it into analytical tables.

This version is improvements that supports incremental data loading, Jinja templating, XComs communication, Slack alerting, dynamic task creation, and modular configuration using CLI.

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

---


## Architecture

- **Source DB**: PostgreSQL (`bookings` schema)
- **Data Lake**: MinIO (`extracted-data` bucket)
- **Data Warehouse**: PostgreSQL (`warehouse` schema)
- **Orchestrator**: Apache Airflow
- **Notifier**: Slack Webhook on task failure 
- **Docker**: All services are containerized using Docker Compose

```text
[PostgreSQL (bookings)] → Extract → [MinIO: extracted-data/] → Load → [PostgreSQL: stg schema] → Transform → [PostgreSQL: warehouse (dim_*, fct_*)]
````

![ELT DAG](docs/elt_dag.png)

---

## Pipeline Flow

| Step | Process       | Tool             | Description                                                   |
| ---- | ------------- | ---------------- | ------------------------------------------------------------- |
| 1    | **Extract**   | Python + Airflow | Pulls data from PostgreSQL and stores CSV files in MinIO      |
| 2    | **Load**      | SQL + Airflow    | Reads from MinIO and loads into `stg` schema (full load)      |
| 3    | **Transform** | SQL + Airflow    | Converts staging data into warehouse `dim_` and `fct_` tables |

---

## Project Structure

```bash
flights-data-pipeline/
│
├── dags/
│   └── flights_data_pipeline/
│       ├── tasks/
│       │   ├── extract.py
│       │   └── load.py
│       │   └── transform.py
│       └── query/final/            ← transformation SQL for data warehouse
│
├── helper/
│   └── minio.py
│   └── postgres.py
│
├── docs/                     ← Screenshots
│   ├── elt_dag.png
│   ├── extract_group.png
│   ├── load_group.png
│   ├── transform_group.png
│   ├── list_dag_available.png
│   ├── list_task_dag.png
│   ├── example_query_from_final_dwh.png
│   └── slack_notifications.png
│
├── .env                      ← Environment variables
├── docker-compose.yml
└── README.md
```

---


## How to Run the Pipeline

Clone and enter project
```bash
git clone https://github.com/hudiyaresa/flights-data-pipeline.git
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
# Airflow internal config
AIRFLOW_FERNET_KEY=    # Run fernet.py to generate this (see below)
AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow_metadata/airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASSWORD=airflow
AIRFLOW_DB_NAME=airflow

# Warehouse (staging and dimensional)
WAREHOUSE_DB_USER=postgres
WAREHOUSE_DB_PASSWORD=postgres
WAREHOUSE_DB_NAME=warehouse_pacflight

# Source DB (bookings)
PACFLIGHT_DB_USER=postgres
PACFLIGHT_DB_PASSWORD=postgres
PACFLIGHT_DB_NAME=pacflight

# MinIO
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
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

```bash
cd includes
```

### Import Airflow Variables

```bash
airflow variables import includes/variables.json
```

### Import Airflow Connections

```bash
airflow connections import includes/connections.yaml
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

![DAG Screenshot](docs/elt_dag.png)

### 2. Task Groups

| Extract                                  |
| ---------------------------------------- |
| ![Extract Group](docs/extract_group.png) |

| Load                                  |
| ---------------------------------------- |
| ![Load Group](docs/load_group.png) | 

| Transform                                  |
| ---------------------------------------- |
| ![Transform Group](docs/transform_group.png) |

### 3. DAG & Task List

| Available DAGs                           |
| ---------------------------------------- |
| ![List DAG](docs/list_dag_available.png) | 

<!-- | Task List                             |
| ------------------------------------- |
| ![List Tasks](docs/list_task_dag.png) | -->

### 4. Querying Final Warehouse

![Query Sample](docs/example_query_from_final_dwh.png)

### 5. Slack Notifications

![Slack Notifications](docs/slack_notifications.png)

### 6. Xcoms

![Xcoms](docs/xcoms.png)
