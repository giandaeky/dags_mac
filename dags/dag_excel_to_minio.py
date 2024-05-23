from airflow import DAG
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
from minio import Minio
import pandas as pd
from airflow.hooks.base_hook import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import io

# from airflow.providers.amazon.aws.operators.s3 import (
#     S3PutBucketTaggingOperator,
# )

def read_file_from_minio():
    conn = BaseHook.get_connection('minio')  
    client = Minio(
        "10.111.24.253:9000",
        access_key=conn.login, 
        secret_key=conn.password,
        secure=False
    )
    bucket_name = "bucketrumah"
    file_name = "file_example_XLSX_10.xlsx"

    response = client.get_object(bucket_name, file_name)
    data = response.read()

    df = pd.read_excel(io.BytesIO(data))
    print(df.head())
    return df


def insert_to_postgres(df):
    records = df.to_records(index=False).tolist()
    hook = PostgresHook(postgres_conn_id='postgre')
    connection = hook.get_conn()
    cursor = connection.cursor()
    insert_query = "INSERT INTO master.test (id,Frist_name, Last_name,Gender, Country,Age,Date) VALUES (%s, %s,%s,%s,%s,%s,%s)"
    cursor.executemany(insert_query, records)
    connection.commit()
    cursor.close()
    connection.close()
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'catchup': False,
}

dag = DAG(
    'dag_excel_to_minio',
    default_args=default_args,
    description='step_1',
    schedule_interval='@once',
    catchup=False
)


read_task = PythonOperator(
    task_id='read_file',
    python_callable=read_file_from_minio,
    dag=dag,
)



exec_postgres= PythonOperator(
    task_id='insert',
    python_callable=insert_to_postgres,
    dag=dag,
)

read_task>>exec_postgres
# run_external_script = BashOperator(
#     task_id='running_script',
#     bash_command='python3 /Users/giandaeky/Gianda/Deployment/projects/minio/send_bucket.py',
#     dag=dag,
# )

# read_?
# run_external_script
