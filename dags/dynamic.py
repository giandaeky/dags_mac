from airflow.models import DAG
from airflow import XComArg
from airflow.operators.empty import EmptyOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


def get_dags_ids_to_monitor():
    postgres_hook = PostgresHook()
    dags_ids = postgres_hook.get_records(
        sql="SELECT dag_id FROM dag_tag"
    )
    # convert list of tuples of one string to a list of strings
    return [dag_id[0] for dag_id in dags_ids]


with DAG(
    dag_id="monitor_dag",
    start_date=datetime(2022, 8, 27)
) as dag:

    dags_to_monitor = get_dags_ids_to_monitor()

    sensor_tasks = [
        ExternalTaskSensor(
            task_id=f"{external_dag_id}_external_tasks_sensor",
            external_task_id="empty_task",
            external_dag_id=external_dag_id
        )
        for external_dag_id in dags_to_monitor
    ]
    notify_task = EmptyOperator(task_id="send_notification")
    sensor_tasks >> notify_task