from airflow import DAG
from airflow.operators.empty import EmptyOperator  # Correct import for EmptyOperator in Airflow 2.x
from airflow.operators.python import PythonOperator  # Correct import for PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email

# Define the DAG and its default arguments
dag = DAG(
    'email_notification_example',
    default_args={
        'owner': 'airflow',
        'retries': 2,
        'retry_delay': 30,
        'email_on_failure': True,  # Send an email if the task fails
        'email_on_retry': True,    # Send an email on retry
        'email': ['iamtheimpossible1209@gmail.com'],  # Global email for notifications
    },
    description='DAG with email notifications on failure or retries',
    schedule_interval='@daily',
    start_date=days_ago(1),
)

# Dummy task to simulate task failure
def fail_task():
    raise Exception('This is a failed task')

# Define the EmptyOperator task (it doesn't do anything but serves as a placeholder)
empty_task = EmptyOperator(
    task_id='empty_task',  # This is the task name
    dag=dag,
)

# Define the Python task that will fail
fail_task_operator = PythonOperator(
    task_id='fail_task_operator',  # Unique task ID to avoid conflict with function name
    python_callable=fail_task,
    dag=dag,
)

# Send a custom email when the task fails (per-task notification)
def failure_callback(context):
    try:
        send_email(
            to='prak13580@gmail.com',  # Corrected email format
            subject=f"Airflow Task Failed: {context['task_instance'].task_id}",
            html_content=f"Task {context['task_instance'].task_id} failed with error: {context['exception']}"
        )
    except Exception as e:
        print(f"Error while sending email: {e}")

# Attach the failure callback to the task
fail_task_operator.on_failure_callback = failure_callback

# Set the task dependencies
empty_task >> fail_task_operator