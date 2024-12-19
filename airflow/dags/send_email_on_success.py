from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator  # Correct import for PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email_smtp
from datetime import datetime

def send_email(context, email='arjit.s@sigmoidanalytics.com'):
	subject = "[Airflow] DAG {0} - Task {1}: Success".format(
		context['task_instance_key_str'].split('__')[0],
		context['task_instance_key_str'].split('__')[1]
		)
	html_content = """
	DAG: {0}<br>
	Task: {1}<br>
	Succeeded on: {2}
	""".format(
		context['task_instance_key_str'].split('__')[0],
		context['task_instance_key_str'].split('__')[1],
		datetime.now()
		)
	send_email_smtp(email, subject, html_content)
      
with DAG(
    dag_id='send_email_on_success',
    dag_display_name='Send Email On Success',
    default_args={
        'owner': 'Arjit Srivastava',
        'email': ['iamtheimpossible1209@gmail.com'],
        'on_success_callback': send_email,
    },
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False
) as dag:
    
    empty_task = EmptyOperator(
        task_id = 'always_successful'
    )


    empty_task