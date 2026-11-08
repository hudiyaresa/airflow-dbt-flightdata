from airflow.decorators import task_group
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from flights_staging_pipeline.tasks.components.extract import Extract
from flights_staging_pipeline.tasks.components.load import Load

# Used when the Variables from include/variables.json are not imported yet, so the DAG still parses
DEFAULT_TABLES = [
    "aircrafts_data",
    "airports_data",
    "bookings",
    "tickets",
    "seats",
    "flights",
    "ticket_flights",
    "boarding_passes"
]
DEFAULT_TABLE_PKEYS = {
    "aircrafts_data": "aircraft_code",
    "airports_data": "airport_code",
    "boarding_passes": ["ticket_no", "flight_id"],
    "bookings": "book_ref",
    "flights": "flight_id",
    "seats": ["aircraft_code", "seat_no"],
    "ticket_flights": ["ticket_no", "flight_id"],
    "tickets": "ticket_no"
}

# Parents before children so foreign keys in stg are satisfied
TABLES_LOAD_ORDER = [
    "aircrafts_data",
    "airports_data",
    "bookings",
    "flights",
    "seats",
    "tickets",
    "ticket_flights",
    "boarding_passes"
]


# Task Group for Extract
@task_group
def extract(incremental):
    tables_to_extract = Variable.get("tables_to_extract", default_var=DEFAULT_TABLES, deserialize_json=True)

    for table_name in tables_to_extract:
        PythonOperator(
            task_id=f'{table_name}',
            python_callable=Extract._pacflight_db,
            trigger_rule='none_failed',
            op_kwargs={
                'table_name': table_name,
                'incremental': incremental
            }
        )


# Task Group for Load
@task_group
def load(incremental):
    table_pkey = Variable.get("tables_to_load", default_var=DEFAULT_TABLE_PKEYS, deserialize_json=True)
    previous_task = None

    for table_name in TABLES_LOAD_ORDER:
        current_task = PythonOperator(
            task_id=f"{table_name}",
            python_callable=Load._pacflight_db,
            trigger_rule="none_failed",
            op_kwargs={
                "table_name": table_name,
                "table_pkey": table_pkey,
                "incremental": incremental
            }
        )

        if previous_task:
            previous_task >> current_task

        previous_task = current_task
