from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

def print_hello_airflow():
    print("Hello Airflow")

dag = DAG(
    dag_id = 'Hello_Airflow',
    dag_display_name ='Hello Printer!',
    default_args={
        'owner': 'Arjit Srivastava'
    }
)

print_hello_airflow_task = PythonOperator(
    task_id='print_hello_airflow',
    python_callable=print_hello_airflow,
    dag=dag
)

end_task = EmptyOperator(
    task_id = 'end_task',
    dag = dag
)

print_hello_airflow_task >> end_task

