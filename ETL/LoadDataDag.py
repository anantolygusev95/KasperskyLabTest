from datetime import datetime, timedelta

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'angusev',
    'depends_on_past': False,
    'start_date': datetime(year=2020, month=6, day=13, hour=9, minute=41, second=0),
    'email': ['kint38@yandex.ru'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

def start_loading():
    from DatasetLoad import run_load
    return run_load()

dag = DAG(
    'household_service_load', catchup=False, default_args=default_args, schedule_interval='41 9 * * *',
    description='etl'
)

mosdata_api_load = PythonOperator(
    task_id='mosdata_api_load',
    python_callable=start_loading,
    dag=dag
)
