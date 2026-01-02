"""
ETL Job DAG (Kubernetes) - For production on EKS with Spot instances

This DAG runs Spark jobs via SparkKubernetesOperator on EKS.
For local development, use etl_job_dag.py instead.

Trigger via Airflow API:
POST /api/v1/dags/etl_job_dag_k8s/dagRuns
{
    "conf": {"job_id": "your_job_id_here"},
    "dag_run_id": "unique_run_id"
}
"""

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator

from etl_common import (
    fetch_job_config,
    finalize_import,
    on_success_callback,
    on_failure_callback,
)


def generate_spark_application(**context):
    """Generate SparkApplication YAML from job config"""
    config_json = context["ti"].xcom_pull(task_ids="fetch_job_config")
    job_id = context["dag_run"].conf.get("job_id", "unknown")

    # Extract run_id from dag_run_id for unique naming
    # dag_run_id format: job_{job_id}_{run_id}
    dag_run_id = context["dag_run"].run_id
    run_id = dag_run_id.split("_")[-1][:8] if "_" in dag_run_id else dag_run_id[:8]

    # Escape the config JSON for YAML
    escaped_config = json.dumps(config_json)

    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": f"etl-{job_id[:8]}-{run_id}",
            "namespace": "spark-jobs",
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest",
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/spark/jobs/etl_runner.py",
            "arguments": [config_json],
            "sparkVersion": "3.5.0",
            "sparkConf": {
                "spark.sql.shuffle.partitions": "24",
                "spark.memory.fraction": "0.6",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
            },
            "driver": {
                "cores": 1,
                "memory": "2g",
                "serviceAccount": "spark-sa",
                "nodeSelector": {
                    "node-type": "spark",
                    "lifecycle": "spot",
                },
                "tolerations": [{
                    "key": "spark-only",
                    "operator": "Equal",
                    "value": "true",
                    "effect": "NoSchedule",
                }],
                "env": [{"name": "AWS_REGION", "value": "ap-northeast-2"}],
            },
            "executor": {
                "cores": 4,
                "instances": 1,
                "memory": "20g",
                "nodeSelector": {
                    "node-type": "spark",
                    "lifecycle": "spot",
                },
                "tolerations": [{
                    "key": "spark-only",
                    "operator": "Equal",
                    "value": "true",
                    "effect": "NoSchedule",
                }],
                "env": [{"name": "AWS_REGION", "value": "ap-northeast-2"}],
            },
            "restartPolicy": {
                "type": "OnFailure",
                "onFailureRetries": 3,
                "onFailureRetryInterval": 10,
                "onSubmissionFailureRetries": 3,
            },
            "timeToLiveSeconds": 60,  # 완료 후 1분 뒤 자동 삭제
        },
    }

    return spark_app


with DAG(
    dag_id="etl_job_dag_k8s",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=5,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["etl", "spark", "kubernetes", "production"],
) as dag:
    # Task 1: Fetch job config from MongoDB
    fetch_config = PythonOperator(
        task_id="fetch_job_config",
        python_callable=fetch_job_config,
    )

    # Task 2: Generate SparkApplication spec
    generate_spark_spec = PythonOperator(
        task_id="generate_spark_spec",
        python_callable=generate_spark_application,
    )

    # Task 3: Run Spark ETL on Kubernetes
    run_spark_etl = SparkKubernetesOperator(
        task_id="run_spark_etl",
        namespace="spark-jobs",
        application_file="{{ ti.xcom_pull(task_ids='generate_spark_spec') }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        watch=True,  # Wait for Spark job to complete
    )

    # Task 4: Finalize import
    finalize = PythonOperator(
        task_id="finalize_import",
        python_callable=finalize_import,
    )

    fetch_config >> generate_spark_spec >> run_spark_etl >> finalize
