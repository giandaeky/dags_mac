from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from datetime import datetime,timedelta

# Tentukan argumen default untuk DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Buat DAG
dag = DAG(
    'print_postgres_table',
    default_args=default_args,
    description='mencetak isi tabel PostgreSQL',
    schedule_interval=None,
)

print_table_task = PostgresOperator(
    task_id='print_table_task',
    postgres_conn_id='postgre', 
    sql="SELECT * FROM public.users",  
    dag=dag,
)

print_table_task
