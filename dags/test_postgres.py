from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime,timedelta


import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.python_operator import PythonOperator




def query_postgres_and_print_to_pandas():
    
    postgres_hook = PostgresHook(postgres_conn_id='postgre') 
   
    df = postgres_hook.get_pandas_df("SELECT * FROM public.users") 
    print(df)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}


dag = DAG(
    'print_postgres_table',
    default_args=default_args,
    description='mencetak isi tabel PostgreSQL',
    schedule_interval=None,
)
print_table_task = PythonOperator(
    task_id='print_table_to_pandas_task',
    python_callable=query_postgres_and_print_to_pandas,
    dag=dag,
)

print_table_task