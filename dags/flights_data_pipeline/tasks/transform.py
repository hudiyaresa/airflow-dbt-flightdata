import logging
from pathlib import Path
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.exceptions import AirflowException


class Transform:
    @staticmethod
    def build_operator(task_id: str, table_name: str, sql_dir: str = "flights_data_pipeline/query/final"):
        """
        Build a PostgresOperator to transform data for the given table.
        
        Args:
            task_id (str): Task ID for the operator.
            table_name (str): Name of the final table to transform.
            sql_dir (str): Directory path to the SQL query file.

        Returns:
            PostgresOperator: Airflow operator to run the transformation query.
        """
        query_path = f"/opt/airflow/dags/{sql_dir}/{table_name}.sql"

        try:
            sql_content = Path(query_path).read_text()
        except FileNotFoundError:
            raise AirflowException(f"[Transform] SQL file not found for table: {table_name}")

        logging.info(f"[Transform] Building transformation for: {table_name}")
        return PostgresOperator(
            task_id=task_id,
            postgres_conn_id='warehouse_pacflight',
            sql=sql_content
        )
