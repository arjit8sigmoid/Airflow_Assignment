from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with DAG(dag_id="DEMO") as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="postgres_connection",
        sql="CREATE TABLE people (name VARCHAR(255),city VARCHAR(255));",
        show_return_value_in_logs=True,
    )

    insert_values = SQLExecuteQueryOperator(
        task_id="insert_values",
        conn_id="postgres_connection",
        sql="INSERT INTO people (name, city) VALUES ( 'arjit', 'bengaluru'), ('prakhar', 'pune'), ('parth', 'noida'), ('sanidhya', 'guwahati');",
        show_return_value_in_logs=True,
    )

    select_values = SQLExecuteQueryOperator(
        task_id="select_values",
        conn_id="postgres_connection",
        sql="SELECT * FROM people;",
        show_return_value_in_logs=True,
    )

    create_table >> insert_values >> select_values
