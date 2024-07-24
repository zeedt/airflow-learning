from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta 

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='latest-only-op',
        default_args=default_args,
        description='',
        start_date=datetime(year=2021, month=7, day=1),
        schedule_interval=timedelta(minutes=60),
        catchup=True,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''

    end = EmptyOperator(task_id='end')

    latest_only = LatestOnlyOperator(task_id='latest-only')

    ignore_task = dummy_task = DummyOperator(
        task_id="ignore_task"
    )

    latest_only >> [start,  task_1(),  end]