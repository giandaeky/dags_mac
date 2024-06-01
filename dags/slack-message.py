from airflow import DAG
from airflow.providers.slack.operators.slack import SlackAPIPostOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'slack_notification_dag',
    default_args=default_args,
    schedule_interval=None,
)

send_slack_message = SlackAPIPostOperator(
    task_id='send_message',
    slack_conn_id='slack-conn',  
    # token='YOUR_SLACK_OAUTH_TOKEN', 
    channel='#general', 
    text='Hello from Airflow!',
    dag=dag,
)

send_slack_message