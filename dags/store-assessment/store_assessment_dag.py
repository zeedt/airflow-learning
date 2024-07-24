from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta 
import pandas as pd
import re
import mysql.connector
from airflow.providers.mysql.operators.mysql import MySqlOperator
# import datetime

dag_owner = 'zeed'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

with DAG(dag_id='store_assessment',
        default_args=default_args,
        description='',
        start_date=datetime(year=2024, month=6, day=6),
        schedule_interval=timedelta(minutes=1),
        catchup=False,
        tags=[''],
        template_searchpath=['/Users/syusuf/airflow/dags/store-assessment/query']
):

    start  = BashOperator(
        task_id="start",
        bash_command='pwd',
        # env: Optional[Dict[str, str]] = None,
        # output_encoding: str = 'utf-8',
        # skip_exit_code: int = 99,
    )



    @task()
    def process_file():
        df = pd.read_csv("/Users/syusuf/airflow/dags/store-assessment/store_files/raw_store_transactions.csv")
        df['STORE_LOCATION'] = df['STORE_LOCATION'].map(lambda x: clean_store_location(x))
        df['PRODUCT_ID'] = df['PRODUCT_ID'].map(lambda x: clean_product_id(x))

        for to_clean in ['MRP', 'CP', 'DISCOUNT', 'SP']:
            df[to_clean] = df[to_clean].map(lambda x: remove_dollar(x))

        df.to_csv('/Users/syusuf/airflow/dags/store-assessment/store_files/clean_store_transactions.csv', index=False)

    # @task
    # def create_database():
    #     mydb = mysql.connector.connect(
    #         host="localhost",
    #         port=3307,
    #         user="root",
    #         password="password"
    #     )
    #     mycursor = mydb.cursor()

    #     mycursor.execute("CREATE DATABASE IF NOT EXISTS store_database")


    create_database = MySqlOperator(
        task_id='create_database_task',
        mysql_conn_id='mysql',
        database=None,
        sql='CREATE DATABASE IF NOT EXISTS store_database'
    )

    create_table = MySqlOperator(
        task_id='create_table_task',
        mysql_conn_id='mysql',
        database='store_database',
        sql='CREATE TABLE IF NOT EXISTS clean_store_transactions(STORE_ID varchar(50), STORE_LOCATION varchar(50), PRODUCT_CATEGORY varchar(50), PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, DATE date)'
    )

    insert_records = MySqlOperator(
        task_id='insert_records_task',
        mysql_conn_id='mysql',
        database='store_database',
        sql="LOAD DATA INFILE '/store_files_mysql/clean_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS"
    )

    select_records = MySqlOperator(
        task_id='select_records_task',
        mysql_conn_id='mysql',
        database='store_database',
        sql="select_from_table.sql"
    )

    move_store_wise_file = bash_task = BashOperator(
        task_id="move_store_wise_file",
        bash_command='mv /Users/syusuf/airflow/dags/store-assessment/store_files/store_wise_profit.csv /Users/syusuf/airflow/dags/store-assessment/store_files/store_wise_profit%s.csv' % str(datetime.now()).replace(' ', '')
    )

    move_location_wise_file = bash_task = BashOperator(
        task_id="move_location_wise_file",
        bash_command='mv /Users/syusuf/airflow/dags/store-assessment/store_files/location_wise_profit.csv /Users/syusuf/airflow/dags/store-assessment/store_files/location_wise_profit%s.csv' % str(datetime.now()).replace(' ', '')
    )


    # @task
    # def create_table():
    #     mydb = mysql.connector.connect(
    #         host="localhost",
    #         port=3307,
    #         database='store_database',
    #         user="root",
    #         password="password"
    #     )
    #     mycursor = mydb.cursor()
    #     mycursor.execute("CREATE TABLE IF NOT EXISTS clean_store_transactions(STORE_ID varchar(50), STORE_LOCATION varchar(50), PRODUCT_CATEGORY varchar(50), PRODUCT_ID int, MRP float, CP float, DISCOUNT float, SP float, DATE date)")

    # @task
    # def insert_records():
    #     mydb = mysql.connector.connect(
    #         host="localhost",
    #         port=3307,
    #         database='store_database',
    #         user="root",
    #         password="password"
    #     )
    #     mycursor = mydb.cursor()
    #     mycursor.execute("LOAD DATA INFILE '/Users/syusuf/airflow/dags/store-assessment/store_files/clean_store_transactions.csv' INTO TABLE clean_store_transactions FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' IGNORE 1 ROWS")
        

    def clean_store_location(st_loc):
        return re.sub(r'[^\w\s]', '', st_loc).strip()

    def clean_product_id(pd_id):
        matches = re.findall(r'\d+', pd_id)
        if matches:
            return matches[0]
        return pd_id

    def remove_dollar(amount):
        return float(amount.replace('$', ''))
    
    end = EmptyOperator(task_id='end')

    start >> process_file() >> create_database >> create_table >> insert_records >> select_records >> move_store_wise_file >> move_location_wise_file >> end