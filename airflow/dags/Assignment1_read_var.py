from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

def var_reader():
    print(Variable.get('demo_var'))

dag = DAG(
    dag_id = 'Read_Var',
    dag_display_name ='var reader',
    default_args={
        'owner': 'Arjit Srivastava'
    }
)

python_var_reader_task = PythonOperator(
    task_id="python_var_reader_task",
    python_callable=var_reader,
    dag=dag
)

bash_var_reader_task = BashOperator(
    task_id='bash_var_reader_task',
    bash_command='echo {{var.value.demo_var}}',
    dag=dag
)

end_task = EmptyOperator(
    task_id = 'end_task',
    dag = dag
)

python_var_reader_task >> bash_var_reader_task >>end_task