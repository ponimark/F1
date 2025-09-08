from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from include.malay.scripts.f1_snowflake_utils import ingest_sessions_to_snowflake

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='session_incremental',
    default_args=default_args,
    description='Ingest F1 race control data into Snowflake',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['f1', 'snowflake', 'sessions'],
) as dag:

    ingest_session_task = PythonOperator(
        task_id='ingest_session_to_snowflake',
        python_callable=ingest_sessions_to_snowflake,
    )

    ingest_session_task
