from airflow.decorators import dag
from pendulum import datetime
from flights_staging_pipeline.tasks.main import extract, load
from helper.callbacks.slack_notifier import slack_notifier
from airflow.models.variable import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Default arguments for the DAG
default_args = {
    'on_failure_callback': slack_notifier
}

@dag(
    dag_id='flights_staging_pipeline',
    description='Extract data and load into staging area',
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args
)
def flights_staging_pipeline():
    # Get incremental mode from Airflow Variable
    incremental_mode = eval(Variable.get('incremental'))

    trigger_warehouse = TriggerDagRunOperator(
    task_id='trigger_flights_warehouse_pipeline',
    trigger_dag_id='flights_warehouse_pipeline',  # DAG ID to trigger
    wait_for_completion=True
    )

    # Call task groups
    extract(incremental=incremental_mode) >> load(incremental=incremental_mode) >> trigger_warehouse

# Instantiate the DAG
flights_staging_pipeline()