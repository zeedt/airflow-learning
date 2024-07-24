from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.providers.mysql.hooks.mysql import MySqlHook

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='student-data-copy',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=7, day=22),
        schedule_interval=timedelta(minutes=5),
        catchup=False,
        tags=['']
):

    start = EmptyOperator(task_id='start')

    @task
    def insert_into_db():
        mysql_hook = MySqlHook(
                mysql_conn_id='mysql', schema='store_database'
        )

        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute("insert into student_data values ('1', 'Olamilekan Olabde'), ('2', 'Similaye Ade')")
        mysql_conn.commit()
        mysql_conn.close()
    
    @task
    def copy_data():
        mysql_hook = MySqlHook(
                mysql_conn_id='mysql', schema='store_database'
        )

        mysql_conn = mysql_hook.get_conn()
        mysql_cursor = mysql_conn.cursor()
        mysql_cursor.execute("insert into student_data_copy select * from student_data ")
        mysql_conn.commit()
        mysql_conn.close()

    end = EmptyOperator(task_id='end')

    start >> insert_into_db() >> copy_data() >> end