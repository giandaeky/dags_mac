from airflow import DAG
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
from minio import Minio
import pandas as pd
from airflow.hooks.base_hook import BaseHook
import io

# from airflow.providers.amazon.aws.operators.s3 import (
#     S3PutBucketTaggingOperator,
# )

def read_file_from_minio():
    conn = BaseHook.get_connection('minio_3')  
    client = Minio(
        conn.host,
        access_key=conn.login, 
        secret_key=conn.password,
        secure=False  # Set to True if using HTTPS
    )
    bucket_name = "bucketrumah"
    file_name = "file_example_XLSX_10.xlsx"

    # Get the object from MinIO
    response = client.get_object(bucket_name, file_name)
    data = response.read()

    # Load data into a DataFrame
    df = pd.read_excel(io.BytesIO(data))
    print(df.head())

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
# run_external_script = BashOperator(
#     task_id='running_script',
#     bash_command='python3 /Users/giandaeky/Gianda/Deployment/projects/minio/send_bucket.py',
#     dag=dag,
# )

read_task
# run_external_script
