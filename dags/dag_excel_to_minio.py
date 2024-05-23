from airflow import DAG
from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.utils.dates import days_ago
from minio import Minio
import pandas as pd
# from airflow.hooks.base_hook import BaseHook
# from airflow.providers.amazon.aws.operators.s3 import (
#     S3PutBucketTaggingOperator,
# )


def read_file_from_minio():
    # conn = BaseHook.get_connection('minio_default')
    client = Minio(
        "gianda-minio.com",
        access_key="dirumah",
        secret_key="dirumah123",
        secure=False  
    )
    bucket_name = "bucketrumah"
    file_name = "bucketrumah/file_example_XLSX_10.xlsx"
    data = client.get_object(bucket_name, file_name)
    df = pd.read_csv(data)
    print(df.head())

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    # 'retries': 1,
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


# run_external_script
