from airflow.datasets import Dataset

# Updated by flights_staging_pipeline once stg is loaded, consumed by flights_warehouse_pipeline
STG_DATASET = Dataset("postgres://warehouse_pacflight:5432/warehouse_pacflight/stg")
