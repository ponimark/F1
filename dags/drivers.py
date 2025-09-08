from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.malay.scripts.f1_snowflake_utils import ingest_drivers_to_snowflake  # ✅ keep function name same

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='drivers_incremental',  # ✅ change DAG id so Airflow treats it separately
    default_args=default_args,
    description='Incrementally ingest F1 driver data to Snowflake',
    schedule_interval='@daily',  # ✅ run daily
    start_date=datetime(2025, 8, 7),  # ✅ start today
    catchup=False,
    tags=['f1', 'snowflake', 'drivers'],
) as dag:

    ingest_drivers = PythonOperator(
        task_id='ingest_drivers_incremental_task',  # ✅ task id updated to match
        python_callable=ingest_drivers_to_snowflake,  # ✅ function name stays same
    )

    ingest_drivers
