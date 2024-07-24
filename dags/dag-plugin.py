from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from plugins.demo_plugin import DataTransferOperator, FileCountSensor, MysqlToPostgresHook

from datetime import datetime, timedelta 

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

def trigger_data_copy_from_postgres_to_mysql():
    MysqlToPostgresHook().copy_table(mysql_conn_id='mysql', postgres_conn_id='postgres-airflow')
    print('Done...')

with DAG(dag_id='dag-plugin',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=7, day=22),
        schedule_interval=timedelta(minutes=5),
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    data_transfer = DataTransferOperator(
        task_id='data_transfer',
        source_file_path = '/Users/syusuf/Downloads/init_file.sql',
        destination_file_path = '/Users/syusuf/Documents/destination-files/dest_init_file.sql',
        delete_list = ['Airflow', 'is']
    )

    file_sensor = FileCountSensor(
        task_id='file_sensor',
        dir_path='/Users/syusuf/Documents/destination-files/dest_init_file.sql',
        conn_id='fs_default',
        poke_interval=5,
        timeout=100
    )

    mysql_to_postgres = MysqlToPostgresHook(
        task_id='mysql_to_postgres_id',

    )

    copy_data_mysql_to_postgres = python_task = PythonOperator(
        task_id="python_task",
        python_callable= trigger_data_copy_from_postgres_to_mysql
    )

    end = EmptyOperator(task_id='end')

    # start >> end
    start >> data_transfer >> end