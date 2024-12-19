from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from json import dumps
from httplib2 import Http

with DAG (
    dag_id="google_chat_alert",
    dag_display_name ='Google Chat Alert',
    default_args={
        'owner': "Arjit Srivastava"
    },
    description='A simple DAG to send a message to Google Chat',
    tags=['alert']
) as dag:
    def send_alert():
        url = Variable.get('airflow_webhook_url')
        app_message = {"text": Variable.get('demo_var')}
        message_headers = {"Content-Type": "application/json; charset=UTF-8"}
        http_obj = Http()
        response = http_obj.request(
            uri = url,
            method = 'POST',
            headers = message_headers,
            body = dumps(app_message)
        )
        # if response.status == 200:
        #     print("Message successfully sent to Google Chat!")
        # else:
        #     print(f"Failed to send message. Status code: {response.status_code}, Response: {response.text}")
        # print(response['status'])

    demo_alert_task = PythonOperator(
        task_id = 'demo_alert_task',
        python_callable= send_alert
    )

    demo_alert_task