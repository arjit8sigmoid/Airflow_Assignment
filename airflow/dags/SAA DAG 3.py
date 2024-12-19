import random

from airflow import DAG
from airflow.providers.slack.notifications.slack_webhook import (
    send_slack_webhook_notification,
)
from airflow.operators.python import PythonOperator


def random_func():
    if not random.choice([True, False]):
        raise ValueError("Random failure occurred.")


success_alert = send_slack_webhook_notification(
    slack_webhook_conn_id="slack_webhook", text="Task successful"
)
failure_alert = send_slack_webhook_notification(
    slack_webhook_conn_id="slack_webhook", text="Task failed"
)

with DAG(
    dag_id="DEMO_1",
    on_success_callback= success_alert,
    on_failure_callback= failure_alert
) as dag:

    dummy_task = PythonOperator(
        task_id="dummy_task",
        python_callable=random_func,
    )

    dummy_task