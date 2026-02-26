from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# --------------------------------
# Python function (task logic)
# --------------------------------
def hello_task():
    print("Hello! This is my first DAG.")

# --------------------------------
# DAG definition
# --------------------------------
with DAG(
    dag_id="simple_hello_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    task_hello = PythonOperator(
        task_id="hello_task",
        python_callable=hello_task
    )

    task_hello1