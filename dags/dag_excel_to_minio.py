from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator


# def print_hello():
#     return ' world!'

dag = DAG('dag_excel_to_minio', 
          description='step1',
          schedule_interval='@once',
        #   start_date=datetime(2022, 1, 1), catchup=False
          )


run_external_script = BashOperator(
    task_id='running script',
    bash_command='python3 /Users/giandaeky/Gianda/Deployment/projects/minio/send_bucket.py',
    dag=dag,
)


run_external_script
