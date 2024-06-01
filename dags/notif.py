# Import this operator to use it for creating slack task
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator

### Slack Alert Function #####
def slack_alert(context):
    
    # we are retrieving our Slack Variable
    SLACK_CONN_ID = Variable.get('slack_conn')

    '''Owner - Rajesh 
    '''
    # we are fetching Slack connection Password 
    slack_webhook_token = BaseHook.get_connection(SLACK_CONN_ID).password

    # We are getting the object after making connection to Slack using Basehook
    channel = BaseHook.get_connection(SLACK_CONN_ID).login
    
    # Here you can customize the message as per your need.

    slack_msg = f"""
        :x: Task Failed.
        *Task*: {context.get('task_instance').task_id}
        *Dag*: {context.get('task_instance').dag_id}
        *Execution Time*: {convert_datetime(context.get('execution_date'))}
        <{context.get('task_instance').log_url}|*Logs*>
    """

    # its a sub task which will be executed whenever the function is called 

    slack_alert = SlackWebhookOperator(
        task_id='slack_fail',
        webhook_token=slack_webhook_token,
        message=slack_msg,
        channel=channel,
        username='airflow',
        http_conn_id=SLACK_CONN_ID
    )

    # execute method to run this task
    slack_alert.execute(context=context)