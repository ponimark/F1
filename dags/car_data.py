from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.malay.scripts.f1_snowflake_utils import ingest_car_data_incremental_to_snowflake


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='car_data_incremental',
    default_args=default_args,
    description='Ingest F1 car telemetry data into Snowflake',
    schedule_interval=None,
    start_date=datetime(2025, 8, 7),
    catchup=False,
    tags=['f1', 'snowflake', 'car_data'],
) as dag:

    ingest_car_data_task = PythonOperator(
        task_id='ingest_car_data_incremental_to_snowflake',
        python_callable=ingest_car_data_incremental_to_snowflake,
    )

    ingest_car_data_task
