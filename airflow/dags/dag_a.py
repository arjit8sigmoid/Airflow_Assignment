import random
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.decorators import task

with DAG(
    dag_id = 'DAG_A',
    dag_display_name='Letter to Supe',
    default_args={
        'owner': 'Arjit Srivastava'
    },
    schedule_interval = '@daily',
    start_date = days_ago(1),
    catchup = False
):
    @task.branch
    def task_a():
        luck_score = random.random()
        print(f"LUCK_SCORE generated: {luck_score}")

        if luck_score > 0.5:
            return 'task_b'
        else:
            return 'task_c'

    @task
    def task_b(ti):
        """Writing a letter to Supehero A in fan_letter_supe_a.txt"""
        letter_content = "Dear SUPE_A,\n\nYou are my most favorite superhero! Keep saving the world!\nSincerely, A Fan."
        with open("/Users/arjitsrivastava/Downloads/fan_letter_supe_a.txt", "w") as f:
            f.write(letter_content)
        print("Fan letter to SUPE_A written.")
        f.close()

        ti.xcom_push(key='supe', value='SUPE_A')
        ti.xcom_push(key='file_path', value='/Users/arjitsrivastava/downloads/fan_letter_supe_a.txt')

    @task
    def task_c(ti):
        """Writing a letter to Superhero B in fan_letter_supe_b.txt"""
        letter_content = "Dear SUPE_B,\n\nYou are my second favorite superhero! Keep up the great work!\nSincerely, A Fan."
        with open("/Users/arjitsrivastava/Downloads/fan_letter_supe_b.txt", "w") as f:
            f.write(letter_content)
        print("Fan letter to SUPE_B written.")
        f.close()

        ti.xcom_push(key='supe', value='SUPE_B')
        ti.xcom_push(key='file_path', value='/Users/arjitsrivastava/downloads/fan_letter_supe_b.txt')

    task_d = TriggerDagRunOperator(
        task_id = 'task_d',
        trigger_dag_id = 'DAG_B',
        trigger_rule = 'none_failed',
        # conf = {
        #         'supe': "{{ti.xcom_pull(task_ids=['task_b', 'task_c'], key='supe')}}",
        #         'file_path': "{{ti.xcom_pull(task_ids=['task_b', 'task_c'], key='file_path')}}"
        #         }
    )

    task_a() >> [task_b(), task_c()] >> task_d