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
    # return df


def insert_to_postgres():
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
    a=["First Name"]
    df = pd.read_excel(io.BytesIO(data),usecols=a)
    print(df.head())
   
   
    records = [tuple(x) for x in df.to_numpy()]
    hook = PostgresHook(postgres_conn_id='postgre')
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute("""
   
CREATE TABLE IF NOT EXISTS master.test1
(
    id integer NOT NULL,
    "Frist_name" character varying(10) COLLATE pg_catalog."default",
    "Last_name" character varying(10) COLLATE pg_catalog."default",
    "Gender" character varying(6) COLLATE pg_catalog."default",
    "Country" character varying(50) COLLATE pg_catalog."default",
    "Age" integer,
    "Date" date,
    CONSTRAINT test_pkey1 PRIMARY KEY (id)
)

    """)



    insert_query = """INSERT INTO master.test1 ("Frist_name") VALUES (%s)"""
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
