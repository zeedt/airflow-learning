from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from datetime import datetime, timedelta 

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

def push_function(**kwargs):
    pushed_value = 5
    ti = kwargs['ti']
    ti.xcom_push(key='pushed_value', value=pushed_value)

def branch_function(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='pushed_value', task_ids='push_task')
    if pulled_value % 2 == 0:
        return 'even_task'
    else:
        return 'odd_task'


with DAG(dag_id='branching-1',
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

    push_task = python_task = PythonOperator(
        task_id="push_task",
        python_callable=push_function, provide_context=True
    )

    even_task = BashOperator(task_id="even_task", bash_command='echo "Callng even function"')
    odd_task = BashOperator(task_id="odd_task", bash_command='echo "Callng odd function"')

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=branch_function, provide_context=True
    )
    
    
    # python_task >> first_branch, second_branch

    start >> push_task >> branch_task >> [even_task, odd_task] >> end