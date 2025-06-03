from airflow.decorators import dag, task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime
from helper.callbacks.slack_notifier import slack_notifier
from pendulum import datetime

from flights_data_pipeline.tasks.extract import Extract
from flights_data_pipeline.tasks.load import Load
from flights_data_pipeline.tasks.transform import Transform

default_args = {
    'on_failure_callback': slack_notifier
}

# ========== Task Groups ==========

@task_group(group_id="extract")
def extract_group(table_list):
    incremental = Variable.get("incremental").lower() == "true"
    for table in table_list:
        PythonOperator(
            task_id=f"{table}",
            python_callable=Extract._pacflight_db,
            op_kwargs={'table_name': table, 'incremental': incremental},
            provide_context=True,            
            do_xcom_push=True
        )        

@task_group(group_id="load")
def load_group(tables_with_pkey):
    incremental = Variable.get("incremental").lower() == "true"
    load_tasks = []

    # Debugging - log tables_with_pkey untuk memastikan format
    # print(f"Tables with Pkey: {tables_with_pkey}")
    
    # tables_with_pkey = eval(tables_with_pkey) if isinstance(tables_with_pkey, str) else tables_with_pkey
    # print(f"Evaluated Tables with Pkey: {tables_with_pkey}")

    for table, pkey in tables_with_pkey.items():
        task = PythonOperator(
            task_id=f"{table}",
            python_callable=Load._pacflight_db,
            op_kwargs={
                'table_name': table,
                'incremental': incremental,
                'table_pkey': tables_with_pkey
            },
            provide_context=True            
        )
        load_tasks.append(task)

    # Set sequential load due to FK dependencies
    for i in range(1, len(load_tasks)):
        load_tasks[i - 1] >> load_tasks[i]

@task_group(group_id="transform")
def transform_group(transform_tables):
    from airflow.models.baseoperator import chain

    previous = None
    for table in transform_tables:
        transform = Transform.build_operator(
            task_id=f"{table}",
            table_name=table,
            sql_dir="flights_data_pipeline/query/final"
        )

        if previous:
            previous >> transform
        previous = transform


# ========== Main DAG ==========

@dag(
    dag_id='flights_data_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=True,
    max_active_runs=1,
    default_args=default_args,    
    tags=['pacflight', 'ETL']
)
def flights_data_pipeline():

    tables_to_extract = eval(Variable.get("tables_to_extract"))
    tables_to_load = eval(Variable.get("tables_to_load"))

    transform_tables = [
        'dim_aircraft', 'dim_airport', 'dim_seat', 'dim_passenger',
        'fct_boarding_pass', 'fct_booking_ticket',
        'fct_seat_occupied_daily', 'fct_flight_activity'
    ]

    # Run task groups
    extract = extract_group(tables_to_extract)
    load = load_group(tables_to_load)
    transform = transform_group(transform_tables)

    # Dependency flow
    extract >> load >> transform

flights_data_pipeline()