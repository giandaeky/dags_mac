from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

from airflow.providers.git.operators.git_sync import GitSyncOperator


dag = DAG('hello_world_dag', description='Simple DAG for testing Airflow',
          schedule_interval='@daily',
          start_date=datetime(2022, 1, 1), catchup=False)



git_sync_task = GitSyncOperator(
    task_id="git_sync_task",
    repo="https://github.com/yourusername/yourrepository.git",
    branch="main",
    dag=dag,
)


read_excel_task = PythonOperator(
    task_id='read_excel_task',
    python_callable=read_excel_file,
    dag=dag,
)

