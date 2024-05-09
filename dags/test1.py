from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

# Fungsi yang akan dieksekusi oleh operator Python
def print_hello():
    return 'Hello world!'

# Buat objek DAG dengan nama 'hello_world_dag' dan jadwalnya diatur menjadi sekali sehari
dag = DAG('hello_world_dag', description='Simple DAG for testing Airflow',
          schedule_interval='@daily',
          start_date=datetime(2022, 1, 1), catchup=False)

# Buat operator Python dengan tugas print_hello
print_hello_task = PythonOperator(
    task_id='print_hello_ta1234',
    python_callable=print_hello,
    dag=dag,
)

# Menetapkan dependensi antara tugas
print_hello_task