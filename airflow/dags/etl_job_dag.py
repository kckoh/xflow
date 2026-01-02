"""
ETL Job DAG (Local) - For local development with Docker

This DAG runs Spark jobs via docker exec on the local Spark container.
For production (EKS), use etl_job_dag_k8s.py instead.

Trigger via Airflow API:
POST /api/v1/dags/etl_job_dag/dagRuns
{
    "conf": {"job_id": "your_job_id_here"},
    "dag_run_id": "unique_run_id"
}
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from etl_common import (
    fetch_job_config,
    finalize_import,
    on_success_callback,
    on_failure_callback,
)


with DAG(
    dag_id="etl_job_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=5,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["etl", "spark", "local"],
) as dag:
    # Task 1: Fetch job config from MongoDB
    fetch_config = PythonOperator(
        task_id="fetch_job_config",
        python_callable=fetch_job_config,
    )

    # Task 2: Run Spark ETL via Docker
    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
                --master 'local[2]' \
                --driver-memory 4g \
                --conf 'spark.sql.shuffle.partitions=100' \
                --conf 'spark.memory.fraction=0.6' \
                --name "ETL-{{ dag_run.conf.get('job_id', 'unknown') }}" \
                --jars /opt/spark/jars/extra/postgresql-42.7.4.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/extra/mongo-spark-connector_2.12-10.3.0.jar,/opt/spark/jars/extra/bson-4.11.1.jar,/opt/spark/jars/extra/mongodb-driver-core-4.11.1.jar,/opt/spark/jars/extra/mongodb-driver-sync-4.11.1.jar \
                /opt/spark/jobs/etl_runner.py '{{ ti.xcom_pull(task_ids="fetch_job_config") }}'
        """,
    )

    # Task 3: Finalize import
    finalize = PythonOperator(
        task_id="finalize_import",
        python_callable=finalize_import,
    )

    fetch_config >> run_spark_etl >> finalize
