from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from psycopg2.extras import execute_values

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

def transfer_function(ds, **kwargs):

    query = "SELECT * FROM source_city_table"

    #source hook
    source_hook = PostgresHook(postgres_conn_id='postgres-airflow', schema='airflow')
    source_conn = source_hook.get_conn()

    #destination hook
    destination_hook = PostgresHook(postgres_conn_id='postgres-airflow', schema='airflow')
    destination_conn = destination_hook.get_conn()

    source_cursor = source_conn.cursor()
    destination_cursor = destination_conn.cursor()

    source_cursor.execute(query)
    records = source_cursor.fetchall()

    if records:
        execute_values(destination_cursor, "INSERT INTO target_city_table VALUES %s", records)
        destination_conn.commit()

    source_cursor.close()
    destination_cursor.close()
    source_conn.close()
    destination_conn.close()
    print("Data transferred successfully!")

with DAG(dag_id='hooks-1',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=7, day=20),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def task_1():
        return ''

    end = EmptyOperator(task_id='end')

    t1 = PythonOperator(task_id='transfer', python_callable=transfer_function, provide_context=True)

    start >> t1 >> end
