import logging
from datetime import timedelta

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from pendulum import datetime

from flights_staging_pipeline.tasks.main import extract, load
from helper.callbacks.slack_notifier import slack_notifier
from helper.datasets import STG_DATASET

# Default arguments for the DAG
default_args = {
    'owner': 'hudiyaresa',
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': slack_notifier
}


def is_incremental():
    return bool(Variable.get('incremental', default_var=False, deserialize_json=True))


@dag(
    dag_id='flights_staging_pipeline',
    description='Extract data and load into staging area',
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=['pacflight', 'staging']
)
def flights_staging_pipeline():
    incremental_mode = is_incremental()

    # Marks stg as updated, which schedules flights_warehouse_pipeline through the dataset
    @task(outlets=[STG_DATASET], trigger_rule='none_failed')
    def publish_staging():
        logging.info(f"[Staging] stg is ready, emitting {STG_DATASET.uri}")

    extract(incremental=incremental_mode) >> load(incremental=incremental_mode) >> publish_staging()


# Instantiate the DAG
flights_staging_pipeline()
