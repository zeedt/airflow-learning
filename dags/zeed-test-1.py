from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='zeed-test-1',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=6, day=6),
        schedule_interval=timedelta(minutes=60),
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        print('Hello somebody from task 1')
        return ''

    end = EmptyOperator(task_id='end')

    start >> task_1() >> end