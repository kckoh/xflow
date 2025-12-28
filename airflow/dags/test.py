from datetime import datetime

from airflow.operators.python import PythonOperator

from airflow import DAG

# Define the DAG
with DAG(
    dag_id="my_dag_name",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Manual trigger only
    catchup=False,
) as dag:
    # Define tasks
    def my_task():
        print("Hello from Airflow!")

    task1 = PythonOperator(task_id="my_task", python_callable=my_task)
