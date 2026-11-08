import os
from datetime import datetime, timedelta
from pathlib import Path

from cosmos import DbtDag
from cosmos.config import ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import TestBehavior
from cosmos.profiles.postgres import PostgresUserPasswordProfileMapping

from helper.callbacks.slack_notifier import slack_notifier
from helper.datasets import STG_DATASET

DBT_PROJECT_PATH = Path(__file__).resolve().parent / "pacflight_dbt"
# dbt runs from its own virtualenv built in the Dockerfile, CI points this somewhere else
DBT_EXECUTABLE_PATH = os.getenv("DBT_EXECUTABLE_PATH", "/opt/airflow/dbt_venv/bin/dbt")

project_config = ProjectConfig(
    dbt_project_path=DBT_PROJECT_PATH,
    project_name="flights_warehouse_pipeline"
)

profile_config = ProfileConfig(
    profile_name="warehouse",
    target_name="warehouse",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id='warehouse_pacflight',
        profile_args={"schema": "final"}
    )
)

dag = DbtDag(
    dag_id="flights_warehouse_pipeline",
    # Runs whenever flights_staging_pipeline finishes loading stg
    schedule=[STG_DATASET],
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2025, 1, 1),
    project_config=project_config,
    profile_config=profile_config,
    execution_config=ExecutionConfig(dbt_executable_path=DBT_EXECUTABLE_PATH),
    render_config=RenderConfig(
        test_behavior=TestBehavior.AFTER_EACH,
        emit_datasets=True
    ),
    operator_args={"install_deps": True},
    default_args={
        'owner': 'hudiyaresa',
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        'on_failure_callback': slack_notifier
    },
    tags=['pacflight', 'dbt']
)
