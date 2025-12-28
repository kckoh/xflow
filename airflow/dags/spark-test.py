from datetime import datetime

from airflow.operators.bash import BashOperator

from airflow import DAG

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
                --jars /opt/spark/jars/extra/postgresql-42.7.4.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar \
                /opt/spark/jobs/postgres_etl.py
        """,
    )
