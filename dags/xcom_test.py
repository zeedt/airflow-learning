from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 
from airflow.operators.python import PythonOperator

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=1)
        }

with DAG(dag_id='xcom-test',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=6, day=6),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
        tags=['']
):

    start = BashOperator(
        task_id="start_task",
        bash_command='echo "`Start task`"',
    )

    @task()
    def create_message_in_xcom(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='message', value='This is just testing phase')


    @task()
    def create_second_message_in_xcom(**kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='message', value='This is just second testing phase')

    @task()
    def read_message_in_xcom(**kwargs):
        ti = kwargs['ti']
        print('Message saved to xcom is ',ti.xcom_pull(key='message', task_ids='create_message_in_xcom'))

    
    @task()
    def read_second_message_in_xcom(**kwargs):
        ti = kwargs['ti']
        print('Message saved to xcom is ',ti.xcom_pull(key='message'))

    end = bash_task = BashOperator(
        task_id="end_task",
        bash_command='echo "End task"',
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )

    start >> create_message_in_xcom() >> create_second_message_in_xcom() >> read_message_in_xcom() >> read_second_message_in_xcom() >> end