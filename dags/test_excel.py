from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 10),
}

def read_excel_task():
    excel_file_path = 'https://github.com/giandaeky/dags_mac/blob/7aabecd40382f271e040245cf28412fbfae0531f/external_code/file_example_XLS_10.xls'  
    df = pd.read_excel(excel_file_path)
    print(df.head()) 


with DAG(
    'example_dag',
    default_args=default_args,
    description='Contoh DAG untuk membaca file Excel dalam Airflow di Kubernetes dengan GitSync',
    schedule_interval=None,
) as dag:

    read_excel_operator = PythonOperator(
        task_id='read_excel_task',
        python_callable=read_excel_task,
    )