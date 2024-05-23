from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator




default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'dag_excel_to_minio',
    default_args=default_args,
    description='Run external Python script once',
    schedule_interval='@once',
    catchup=False
)



run_external_script = BashOperator(
    task_id='running script',
    bash_command='python3 /Users/giandaeky/Gianda/Deployment/projects/minio/send_bucket.py',
    dag=dag,
)


run_external_script
