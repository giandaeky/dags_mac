from airflow import DAG
from datetime import datetime
from airflow.operators.bash_operator import BashOperator




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



run_external_script = BashOperator(
    task_id='running_script',
    bash_command='python3 /Users/giandaeky/Gianda/Deployment/projects/minio/send_bucket.py',
    dag=dag,
)


run_external_script
