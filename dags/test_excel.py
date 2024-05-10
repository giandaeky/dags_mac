from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 10),
}
def read_excel_task():
    excel_file_path = 'https://github.com/giandaeky/dags_mac/blob/2caf89a012fcda22c6b606a24f73c111fbb37194/external_code/file_example_XLSX_10.xlsx'
    df = pd.read_excel(excel_file_path)
    print(df.head()) 


# https://github.com/giandaeky/dags_mac/blob/2caf89a012fcda22c6b606a24f73c111fbb37194/external_code/file_example_XLSX_10.xlsx
# def read_excel_task():
#     excel_file_path = 'https://raw.githubusercontent.com/giandaeky/dags_mac/7d94db234f60972e50e16e9374ee7cca4fa963e2/external_code/file_example_XLSX_10.xlsx'
#     df = pd.read_excel(excel_file_path)
#     print(df.head()) 




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



    # https://github.com/giandaeky/dags_mac/blob/2caf89a012fcda22c6b606a24f73c111fbb37194/external_code/file_example_XLSX_10.xlsx
