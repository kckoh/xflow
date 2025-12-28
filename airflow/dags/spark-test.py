from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="spark_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    spark_job = BashOperator(
        task_id="run_spark_job",
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --name airflow-spark-test \
                /opt/spark/jobs/test_spark.py
        """,
    )
