# Advanced Pac-Flights Data Pipeline (Airflow + DBT + MinIO + Slack)

A production-grade modular ELT pipeline for airline booking data. The pipeline is powered by Apache Airflow using CeleryExecutor, PostgreSQL, MinIO, DBT, Slack integration, and CLI-driven setup for maximum flexibility.

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Pipeline Flow](#pipeline-flow)
4. [Project Structure](#project-structure)
5. [How to Run the Pipeline](#how-to-run-the-pipeline)
6. [Environment Setup](#environment-setup)
7. [Screenshots](#screenshots)

---

## Overview

This project orchestrates a modular ELT workflow for a flight booking system using Apache Airflow. It supports full and incremental loads, utilizes DBT for warehouse transformations, and integrates Slack notifications for operational monitoring. Airflow is configured using the **Celery Executor** for distributed task execution.

### Key Features

| Feature                  | Description                                                                |
| ------------------------ | -------------------------------------------------------------------------- |
| Incremental & Full Loads | Controlled via Airflow variable `load_mode` (`incremental` or `full`)      |
| Jinja Templating         | Dynamic queries with execution date using `{{ ds }}`                       |
| XComs                    | Data communication between tasks (e.g., extract to load)                   |
| Pangres Upsert           | Upserts into staging tables with primary key conflict handling             |
| Skip Exceptions          | Tasks are skipped gracefully when there's no data                          |
| Slack Notification       | Alerts via Slack webhook on task failures                                  |
| CLI Configuration        | Load Airflow Variables & Connections via CLI (`variables.json`, `yaml`)    |
| TriggerDagRunOperator    | Used to trigger dependent DAGs (e.g., `flights_warehouse_pipeline`)        |
| DBT Transformation       | Leverages DBT models for transforming staging into dimensional/fact tables |
| Celery Executor          | Scalable parallel task processing                                          |

---

## Architecture

```text
[PostgreSQL (bookings)]
  └→ Extract (Airflow)
      └→ [MinIO: extracted-data/]
          └→ Load (Upsert)
              └→ [PostgreSQL: stg schema]
                  └→ TriggerDagRunOperator
                      └→ [DBT Transformation DAG]
                          └→ [warehouse (dim_*, fct_*)]
```

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
flights-data-pipeline/
├── dags/
│   ├── flights_staging_pipeline.py
│   └── flights_warehouse_pipeline.py
│
├── flights_data_pipeline/
│   ├── components/
│   │   ├── extract.py
│   │   ├── load.py
│   │   └── transform.py
│   └── query/
│       ├── stg/         # SQL insert per staging table
│       └── final/       # DBT-compatible transformations
│
├── dbt/
│   └── pacflights_dbt/  # DBT models, seeds, etc.
│
├── include/
│   ├── variables.json
│   └── connections.yaml
│
├── helper/
│   ├── minio.py
│   └── postgres.py
│
├── docs/
├── docker-compose.yml
├── .env
└── README.md
```

---

## How to Run the Pipeline

```bash
git clone https://github.com/your-username/flexible-flights-pipeline.git
cd flights-data-pipeline
docker-compose up --build
```

### Airflow Services

| Service       | URL                                            |
| ------------- | ---------------------------------------------- |
| Airflow UI    | [http://localhost:8080](http://localhost:8080) |
| MinIO Browser | [http://localhost:9001](http://localhost:9001) |
| PostgreSQL    | localhost:5432                                 |

---

## Environment Setup

Create a `.env` file:

```env
AIRFLOW_UID=50000
AIRFLOW_FERNET_KEY=your_fernet_key
AIRFLOW_WEBSERVER_SECRET_KEY=your_webserver_key
AIRFLOW_DB_URI=postgresql+psycopg2://airflow:airflow@airflow-metadata-6/airflow
AIRFLOW_CELERY_RESULT_BACKEND=db+postgresql://airflow:airflow@airflow-metadata-6/airflow
AIRFLOW_CELERY_BROKER_URL=redis://:@redis:6379/0

WAREHOUSE_DB_USER=postgres
WAREHOUSE_DB_PASSWORD=postgres
WAREHOUSE_DB_NAME=warehouse_pacflight

PACFLIGHT_DB_USER=postgres
PACFLIGHT_DB_PASSWORD=postgres
PACFLIGHT_DB_NAME=pacflight

MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123

SLACK_WEBHOOK_URL=https://hooks.slack.com/services/.../...
```

---

## CLI Configuration

```bash
docker exec -it airflow_standalone bash
cd include
```

### Import Variables

```bash
airflow variables import variables.json
```

### Import Connections

```bash
airflow connections import connections.yaml
```

---

## Screenshots

* DAG Graph: ![elt\_dag](docs/elt_dag.png)
* Extract Group: ![extract](docs/extract_group.png)
* Load Group: ![load](docs/load_group.png)
* Transform Group: ![transform](docs/transform_group.png)
* DAG List: ![dags](docs/list_dag_available.png)
* Tasks: ![tasks](docs/list_task_dag.png)
* Final Query Example: ![query](docs/example_query_from_final_dwh.png)
* Slack Notification: ![slack](docs/slack_notifications.png)
* XComs Communication: ![xcom](docs/xcoms.png)

---

## Suggested Repository Names

1. airflow-dbt-flight-pipeline
2. modular-flight-eltsystem
3. flights-pipeline-orchestrator
4. airline-etl-modular
5. dbt-airflow-flightstack
6. airflow-dbt-staging-warehouse
7. flights-datafactory
8. airline-data-lakehouse
9. modular-pipeline-for-flights
10. airflow-celery-flightetl
11. airflow-trigger-dag-flights
12. flight-booking-orchestration
13. flight-db-etl-with-dbt
14. minio-postgres-flightflow
15. airflow-minio-pipeline
16. airflow-flights-celerystack
17. airline-analytics-pipeline
18. airflow-pangres-pipeline
19. flexible-flight-orchestration
20. airflow-pipeline-experiment
21. flights-dbt-airflow
22. airline-eltsuite
23. airflow-trigger-dagstack
24. postgres-to-warehouse-flightetl
25. airflow-pipeline-with-trigger
26. airflow-minio-postgres-dbt
27. pipeline-flightdata-upsert
28. airline-db-orchestration
29. airflow-modular-etl-flights
30. airflow-dbt-triggered-pipeline
