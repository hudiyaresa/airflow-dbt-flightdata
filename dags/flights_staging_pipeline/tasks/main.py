from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from flights_staging_pipeline.tasks.components.extract import Extract
from flights_staging_pipeline.tasks.components.load import Load
from airflow.datasets import Dataset

# Task Group for Extract
@task_group
def extract(incremental):            
    tables_to_extract = eval(Variable.get('tables_to_extract'))

    for table_name in tables_to_extract:
        current_task = PythonOperator(
            task_id=f'{table_name}',
            python_callable=Extract._pacflight_db,
            trigger_rule='none_failed',
            op_kwargs={
                'table_name': table_name,
                'incremental': incremental
            }
        )

        current_task

# Task Group for Load
@task_group
def load(incremental):
    tables_load_order = [
        "aircrafts_data",
        "airports_data",
        "bookings",
        "flights",
        "seats",
        "tickets",
        "ticket_flights",
        "boarding_passes"
    ]

    table_pkey = eval(Variable.get("tables_to_load"))
    previous_task = None

    for table_name in tables_load_order:
        current_task = PythonOperator(
            task_id=f"{table_name}",
            python_callable=Load._pacflight_db,
            trigger_rule="none_failed",
                # outlets=[Dataset(f'postgres://warehouse_pacflight:5432/warehouse_pacflight.stg.{table_name}')],
            op_kwargs={
                "table_name": table_name,
                "table_pkey": table_pkey,
                "incremental": incremental
            }
        )

        if previous_task:
            previous_task >> current_task

        previous_task = current_task