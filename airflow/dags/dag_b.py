from airflow import DAG
from airflow.decorators import task


with DAG(
        dag_id = 'DAG_B',
        dag_display_name = 'Read Letter To Supe',
        description='DAG that reads fan letters from DAG_A and prints the content.'
) as dag_b:

    @task
    def read_from_file(**context):
        task_b_supe = context['ti'].xcom_pull(dag_id = 'DAG_A', task_ids = 'task_b', key = 'supe', include_prior_dates = True)
        task_b_file_path = context['ti'].xcom_pull(dag_id = 'DAG_A', task_ids = 'task_b', key = 'file_path', include_prior_dates = True)
        task_c_supe = context['ti'].xcom_pull(dag_id = 'DAG_A', task_ids = 'task_c', key = 'supe', include_prior_dates = True)
        task_c_file_path = context['ti'].xcom_pull(dag_id = 'DAG_A', task_ids = 'task_c', key = 'file_path', include_prior_dates = True)

        print(task_b_supe, task_b_file_path, task_c_supe, task_c_file_path)
        # with open(file_path, "r") as f:
        # letter_content = f.read()

        # for x in list(supe):
        #     print(x)

        # for x in list(file_path):
        #     print(x)

        # print(f"Fan letter content:\n{letter_content}")


    read_from_file()