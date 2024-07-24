
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 

dag_owner = 'Zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='variable-test-dag1',
        default_args=default_args,
        description='Variable testing',
        start_date=datetime(year=2024, month=6, day=6),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''
    
    @task
    def task_2():
        return ''
    
    variable_bash_task = BashOperator(
        task_id="variable_bash_task",
        bash_command='echo "Hi from bash operator. The variable value is " {{var.value.source_path}}',
    )

    end = EmptyOperator(task_id='end')

    start >> task_1() >> task_2() >> variable_bash_task >> end