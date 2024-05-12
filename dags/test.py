from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello world!'

dag = DAG('hello_world_dag_new', description='Simple DAG for testing Airflow',
          schedule_interval='@daily',
          start_date=datetime(2022, 1, 1), catchup=False)

print_hello_task = PythonOperator(
    task_id='print_hello_task1',
    python_callable=print_hello,
    dag=dag,
)

print_hello_task
