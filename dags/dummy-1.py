from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 

dag_owner = 'Zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='dummy1',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=7, day=20),
        schedule_interval='',
        catchup=False,
        tags=['']
):

    # start = EmptyOperator(task_id='start')

    task_1 = BashOperator(task_id="task_1" ,bash_command='echo "This is task 1"')
    task_2 = BashOperator(task_id="task_2" ,bash_command='echo "This is task 2"')
    task_3 = BashOperator(task_id="task_3" ,bash_command='echo "This is task 3"')
    task_4 = BashOperator(task_id="task_4" ,bash_command='echo "This is task 4"')
    task_5 = BashOperator(task_id="task_5" ,bash_command='echo "This is task 5"')
    task_6 = BashOperator(task_id="task_6" ,bash_command='echo "This is task 6"')
    task_7 = BashOperator(task_id="task_7" ,bash_command='echo "This is task 7"')
    task_8 = BashOperator(task_id="task_8" ,bash_command='echo "This is task 8"')
    task_9 = BashOperator(task_id="task_9" ,bash_command='echo "This is task 9"')
    task_10 = BashOperator(task_id="task_10" ,bash_command='echo "This is task 10"')

    # @task
    # def task_11():
    #     return ''

    # end = EmptyOperator(task_id='end')

    td = DummyOperator(task_id="dummy_task")

    td >> task_1

    # [task_1, task_2, task_3, task_4, task_5] >> td >> [task_6, task_7, task_8, task_9, task_10]