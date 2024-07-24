from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 


from airflow.contrib.sensors.file_sensor import FileSensor

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='sensor',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=7, day=21),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''

    end = EmptyOperator(task_id='end')

    t1 = FileSensor(
        task_id='check_file_exists',
        filepath='/usr/local/airflow/store_files_airflow/raw_store_transactions.csv',
        fs_conn_id='fs_default',
        poke_interval=10,
        timeout=150,
        soft_fail=True
    )

    start >> task_1() >> end