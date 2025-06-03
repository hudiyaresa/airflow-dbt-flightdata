from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helper.minio import CustomMinio
import logging
import pandas as pd
import json
from airflow.models import Variable
from datetime import timedelta


class Extract:
    @staticmethod
    def _pacflight_db(table_name, incremental, **kwargs):
        """
        Extract all data from Pacflight database (non-incremental).

        Args:
            table_name (str): Name of the table to extract data from.
            **kwargs: Additional keyword arguments.

        Raises:
            AirflowException: If failed to extract data from Pacflight database.
            AirflowSkipException: If no data is found.
        """
        logging.info(f"[Extract] Starting extraction for table: {table_name}")
        try:
            pg_hook = PostgresHook(postgres_conn_id='pacflight_db')
            connection = pg_hook.get_conn()
            cursor = connection.cursor()

            query = ""
            if incremental:
                date = kwargs['ds']
                query = f"""
                    SELECT * FROM bookings.{table_name}
                    WHERE created_at::DATE = '{date}'::DATE - INTERVAL '1 DAY';
                """
                object_name = f'/temp/{table_name}-{(pd.to_datetime(date) - timedelta(days=1)).strftime("%Y-%m-%d")}.csv'
            else:
                query = f"SELECT * FROM bookings.{table_name};"
                object_name = f'/temp/{table_name}.csv'

            logging.info(f"[Extract] Executing query: {query}")
            cursor.execute(query)
            result = cursor.fetchall()

            column_list = [desc[0] for desc in cursor.description]
            cursor.close()
            connection.commit()
            connection.close()

            df = pd.DataFrame(result, columns=column_list)

            if df.empty:
                logging.warning(f"[Extract] Table {table_name} is empty. Skipping...")                
                raise AirflowSkipException(f"{table_name} has no data. Skipped...")

            # === Handle JSON columns that need to be dumped as string ===
            # if table_name == 'aircrafts_data':
            #     df['model'] = df['model'].apply(json.dumps)

            # if table_name == 'airports_data':
            #     df['airport_name'] = df['airport_name'].apply(json.dumps)
            #     df['city'] = df['city'].apply(json.dumps)

            # if table_name == 'tickets':
            #     df['contact_data'] = df['contact_data'].apply(lambda x: json.dumps(x) if x else None)

            # === Replace NaN with None for better compatibility with CSV ===
            if table_name == 'flights':
                df = df.replace({float('nan'): None})

            bucket_name = 'extracted-data'
            logging.info(f"[Extract] Writing data to MinIO bucket: {bucket_name}, object: {object_name}")

            CustomMinio._put_csv(df, bucket_name, object_name)
            logging.info(f"[Extract] Extraction completed for table: {table_name}")
            return {"status": "success", "data_date": str(kwargs['ds'])}            

        except AirflowSkipException as e:
            logging.warning(f"[Extract] Skipped extraction for {table_name}: {str(e)}")
            return {"status": "skipped", "data_date": str(kwargs['ds'])}            
        
        except Exception as e:
            logging.error(f"[Extract] Failed extracting {table_name}: {str(e)}")
            raise AirflowException(f"Error when extracting {table_name} : {str(e)}")
