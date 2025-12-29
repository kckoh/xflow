"""
ETL Job DAG - Parameterized DAG for executing ETL jobs

This DAG is triggered with a job_id parameter and:
1. Fetches the job configuration from MongoDB
2. Fetches the source connection details
3. Executes the Spark ETL runner with the configuration
4. Updates the job run status on completion

Trigger via Airflow API:
POST /api/v1/dags/etl_job_dag/dagRuns
{
    "conf": {"job_id": "your_job_id_here"},
    "dag_run_id": "unique_run_id"
}
"""

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable


def fetch_job_config(**context):
    """Fetch job configuration from MongoDB"""
    import pymongo
    from bson import ObjectId

    job_id = context["dag_run"].conf.get("job_id")
    if not job_id:
        raise ValueError("job_id is required in dag_run.conf")

    # Connect to MongoDB
    mongo_url = Variable.get("MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017")
    mongo_db = Variable.get("MONGODB_DATABASE", default_var="mydb")

    client = pymongo.MongoClient(mongo_url)
    db = client[mongo_db]

    # Fetch ETL job
    job = db.etl_jobs.find_one({"_id": ObjectId(job_id)})
    if not job:
        raise ValueError(f"ETL job not found: {job_id}")

    # Fetch source connection if RDB
    source = job.get("source", {})
    if source.get("type") == "rdb" and source.get("connection_id"):
        connection = db.rdb_sources.find_one({"_id": ObjectId(source["connection_id"])})
        if connection:
            # Remove MongoDB-specific fields and add connection details
            source["connection"] = {
                "type": connection.get("type"),
                "host": connection.get("host"),
                "port": connection.get("port"),
                "database_name": connection.get("database_name"),
                "user_name": connection.get("user_name"),
                "password": connection.get("password"),
            }

    # Build complete config for Spark
    config = {
        "name": job.get("name"),
        "source": source,
        "transforms": job.get("transforms", []),
        "destination": job.get("destination", {}),
    }

    # Add S3 config for LocalStack
    if config["destination"].get("type") == "s3":
        config["destination"]["s3_config"] = {
            "endpoint": Variable.get("S3_ENDPOINT", default_var="http://localstack-main:4566"),
            "access_key": Variable.get("S3_ACCESS_KEY", default_var="test"),
            "secret_key": Variable.get("S3_SECRET_KEY", default_var="test"),
        }

    client.close()

    # Store config in XCom for next task
    return json.dumps(config)


def update_job_run_status(status: str, error_message: str = None, **context):
    """Update job run status in MongoDB"""
    import pymongo
    from bson import ObjectId

    dag_run = context["dag_run"]
    job_id = dag_run.conf.get("job_id")

    # Extract run_id from dag_run_id (format: job_{job_id}_{run_id})
    dag_run_id = dag_run.run_id
    parts = dag_run_id.split("_")
    run_id = parts[-1] if len(parts) >= 3 else None

    if not run_id:
        print(f"Could not extract run_id from dag_run_id: {dag_run_id}")
        return

    mongo_url = Variable.get("MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017")
    mongo_db = Variable.get("MONGODB_DATABASE", default_var="mydb")

    client = pymongo.MongoClient(mongo_url)
    db = client[mongo_db]

    update_data = {
        "status": status,
        "finished_at": datetime.utcnow(),
    }
    if error_message:
        update_data["error_message"] = error_message

    try:
        db.job_runs.update_one(
            {"_id": ObjectId(run_id)},
            {"$set": update_data}
        )
        print(f"Updated job run {run_id} to status: {status}")
    except Exception as e:
        print(f"Failed to update job run status: {e}")
    finally:
        client.close()


def on_success_callback(context):
    """Callback when DAG succeeds"""
    update_job_run_status("success", **context)


def on_failure_callback(context):
    """Callback when DAG fails"""
    error = str(context.get("exception", "Unknown error"))
    update_job_run_status("failed", error_message=error, **context)


with DAG(
    dag_id="etl_job_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,  # Only triggered via API
    catchup=False,
    max_active_runs=5,
    on_success_callback=on_success_callback,
    on_failure_callback=on_failure_callback,
    tags=["etl", "spark"],
) as dag:

    # Task 1: Fetch job config from MongoDB
    fetch_config = PythonOperator(
        task_id="fetch_job_config",
        python_callable=fetch_job_config,
    )

    # Task 2: Run Spark ETL
    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
                --master 'local[*]' \
                --driver-memory 2g \
                --name "ETL-{{ dag_run.conf.get('job_id', 'unknown') }}" \
                --jars /opt/spark/jars/extra/postgresql-42.7.4.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar \
                /opt/spark/jobs/etl_runner.py '{{ ti.xcom_pull(task_ids="fetch_job_config") }}'
        """,
    )

    fetch_config >> run_spark_etl
