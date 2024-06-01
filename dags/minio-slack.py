from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
from minio import Minio
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import io

from airflow.operators.dummy import DummyOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5)
    

}



dag = DAG(
    'upload_files_slack',
    default_args=default_args,
    description='Upload files from Minio to Slack',
    schedule_interval=timedelta(days=1),
)



start_task = DummyOperator(task_id='start_task', dag=dag)

end_task = DummyOperator(task_id='end_task', dag=dag)
 

def get_minio_client():
    conn = BaseHook.get_connection('minio')
    
    client = Minio(
         "10.111.24.253:9000",
        access_key=conn.login, 
        secret_key=conn.password,
        secure=False
    )
    return client

def get_slack_client():
    slack_conn = BaseHook.get_connection('slack-conn')
    extra = slack_conn.extra_dejson

    slack_client = WebClient(token=extra.get('token'))
    return slack_client

def upload_files_to_slack():
    client = get_minio_client()
    slack_client = get_slack_client()

    bucket_name = "bahan-baca-data"
    folder_path = "Data-Enginer/"
    objects = client.list_objects(bucket_name, folder_path)

    def get_file_stream_from_minio(client, bucket_name, object_name):
        # try:
            response = client.get_object(bucket_name, object_name)
            file_data = response.read()
            response.close()
            response.release_conn()
            return io.BytesIO(file_data)
        # except Exception as e:
        #     print(f"Error: {str(e)}")
        #     return None

    def upload_file_to_slack(slack_client, slack_channel, filesend, filename):
        
            response = slack_client.files_upload(
                channel=slack_channel,
                file=filesend,
                title=filename,
                initial_comment=f"test{filename}",
                filename=filename
            )
            print(f"File uploaded to Slack successfully: {response['file']['id']}")
        # except SlackApiError as e:
        #     print(f"Slack API Error: {e.response['error']}")
        # except Exception as e:
        #     print(f"Error: {str(e)}")



    slack_channel = '#data-adalah-data'

    
    for obj in objects:
        file_stream = get_file_stream_from_minio(client, obj.bucket_name, obj.object_name)
        if file_stream:
            upload_file_to_slack(slack_client, slack_channel, file_stream, obj.object_name)

upload_task = PythonOperator(
    task_id='upload_files_to_slack_task',
    python_callable=upload_files_to_slack,
    dag=dag,
)

start_task >> upload_task >> end_task