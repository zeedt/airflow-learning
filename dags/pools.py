from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='pools',
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

    bash_task_1 = BashOperator(task_id="bash_task_1",
        bash_command='sleep 5', pool='pool_1'
    )
    bash_task_2 = BashOperator(task_id="bash_task_2",
        bash_command='sleep 5', pool='pool_1'
    )
    bash_task_3 = BashOperator(task_id="bash_task_3",
        bash_command='sleep 5', pool='pool_2'
    )
    bash_task_4 = BashOperator(task_id="bash_task_4",
        bash_command='sleep 5', pool='pool_2', priority_weight=2
    )

    # bash_task_1 >> bash_task_2 >> bash_task_3 >> bash_task_4