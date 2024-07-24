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

with DAG(dag_id='trigger_rule_dag',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=7, day=23),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        raise Exception("Intentional Exception")
    
    @task
    def task_2():
        print('Hello world')

    @task
    def task_3():
        print('Hello from here')

    # end = EmptyOperator(task_id='end', trigger_rule='one_success')
    end = EmptyOperator(task_id='end', trigger_rule='all_success')

    start >> [task_1(), task_2(), task_3()] >> end