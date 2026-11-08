FROM apache/airflow:2.10.2

# Airflow side: providers, pangres and cosmos, checked against the Airflow 2.10.2 constraints
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.2/constraints-3.12.txt"

# dbt lives in its own virtualenv so its dependencies never clash with Airflow's
COPY dbt-requirements.txt /dbt-requirements.txt
RUN python -m virtualenv /opt/airflow/dbt_venv && \
    /opt/airflow/dbt_venv/bin/pip install --no-cache-dir -r /dbt-requirements.txt
