"""
Dataset DAG (Kubernetes) - For production on EKS with Spot instances

This DAG runs Spark jobs via SparkKubernetesOperator on EKS.
Uses Sensor for reliable completion detection (polling instead of watch).

Trigger via Airflow API:
POST /api/v1/dags/dataset_dag_k8s/dagRuns
{
    "conf": {"dataset_id": "your_dataset_id_here"},
    "dag_run_id": "unique_run_id"
}
"""

import json
import json
import base64
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor

from etl_common import (
    fetch_dataset_config,
    finalize_import,
    run_quality_check,
    on_success_callback,
    on_failure_callback,
)


def get_executor_count(size_gb: float) -> int:
    """Determine executor count based on data size"""
    if size_gb <= 1:
        return 1
    elif size_gb <= 10:
        return 2
    elif size_gb <= 30:
        return 3
    elif size_gb <= 50:
        return 4
    else:
        return 5


def generate_spark_application(**context):
    """Generate SparkApplication YAML from dataset config"""
    config_json = context["ti"].xcom_pull(task_ids="fetch_dataset_config")
    # Support both dataset_id (direct run) and job_id (scheduled run)
    dataset_id = context["dag_run"].conf.get("dataset_id") or context["dag_run"].conf.get("job_id", "unknown")

    # Extract run_id from dag_run_id for unique naming
    dag_run_id = context["dag_run"].run_id
    # Use last segment after underscore for uniqueness (e.g., dataset_xxx_yyy -> yyy)
    import re
    parts = dag_run_id.split('_')
    if len(parts) >= 2:
        # Take last part (most unique) and extract alphanumeric chars
        clean_run_id = re.sub(r'[^a-z0-9]', '', parts[-1].lower())[:8]
    else:
        clean_run_id = re.sub(r'[^a-z0-9]', '', dag_run_id.lower())[-8:]
    if not clean_run_id:
        clean_run_id = f"{int(datetime.now().timestamp()) % 100000000:08d}"

    # Parse config to get estimated size for auto-scaling
    config = json.loads(config_json)
    estimated_size_gb = config.get("estimated_size_gb", 1)
    executor_instances = get_executor_count(estimated_size_gb)
    print(f"Auto-scaling: {estimated_size_gb:.2f} GB -> {executor_instances} executor(s)")

    # Encode config to base64 for safe passing
    encoded_config = base64.b64encode(config_json.encode("utf-8")).decode("utf-8")

    spark_app_name = f"etl-{dataset_id[:8]}-{clean_run_id}"

    spark_app = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": spark_app_name,
            "namespace": "spark-jobs",
        },
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": "134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest",
            "imagePullPolicy": "Always",
            "mainApplicationFile": "local:///opt/spark/jobs/etl_runner.py",
            "arguments": ["--base64", encoded_config],
            "sparkVersion": "3.5.0",
            "sparkConf": {
                "spark.sql.shuffle.partitions": "24",
                "spark.memory.fraction": "0.6",
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
                "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            },
            # Delta Lake JARs are pre-installed in the image, no need for packages
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
                "instances": executor_instances,
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
            "timeToLiveSeconds": 600,  # 완료 후 10분 뒤 자동 삭제
        },
    }

    # Store app name for sensor
    context["ti"].xcom_push(key="spark_app_name", value=spark_app_name)

    return spark_app


with DAG(
    dag_id="dataset_dag_k8s",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=5,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["etl", "spark", "kubernetes", "production"],
) as dag:
    # Task 1: Fetch dataset config from MongoDB
    fetch_config = PythonOperator(
        task_id="fetch_dataset_config",
        python_callable=fetch_dataset_config,
    )

    # Task 2: Generate SparkApplication spec
    generate_spark_spec = PythonOperator(
        task_id="generate_spark_spec",
        python_callable=generate_spark_application,
    )

    # Task 3: Submit Spark job (no watch - just submit)
    submit_spark_job = SparkKubernetesOperator(
        task_id="submit_spark_job",
        namespace="spark-jobs",
        application_file="{{ ti.xcom_pull(task_ids='generate_spark_spec') }}",
        kubernetes_conn_id="kubernetes_default",
        do_xcom_push=True,
        watch=False,  # Don't watch - use sensor instead
    )

    # Task 4: Wait for Spark job completion (polling)
    wait_for_spark = SparkKubernetesSensor(
        task_id="wait_for_spark",
        namespace="spark-jobs",
        application_name="{{ ti.xcom_pull(task_ids='generate_spark_spec', key='spark_app_name') }}",
        kubernetes_conn_id="kubernetes_default",
        poke_interval=30,  # Check every 30 seconds
        timeout=3600,  # 1 hour timeout
    )

    # Task 5: Run Quality Check
    quality_check = PythonOperator(
        task_id="run_quality_check",
        python_callable=run_quality_check,
    )

    # Task 6: Finalize import
    finalize = PythonOperator(
        task_id="finalize_import",
        python_callable=finalize_import,
    )

    fetch_config >> generate_spark_spec >> submit_spark_job >> wait_for_spark >> quality_check >> finalize

