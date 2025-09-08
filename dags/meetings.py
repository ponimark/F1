from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.malay.scripts.f1_snowflake_utils import ingest_Meetings_to_snowflake


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='meetings_incremental',
    default_args=default_args,
    description='Ingest F1 car telemetry data into Snowflake',
    schedule_interval=None,
    start_date=datetime(2025, 8, 7),
    catchup=False,
    tags=['f1', 'snowflake', 'meeting'],
) as dag:

    ingest_meeting_task = PythonOperator(
        task_id='ingest_car_data_incremental_to_snowflake',
        python_callable=ingest_Meetings_to_snowflake(),
    )

    ingest_meeting_task
