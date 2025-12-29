"""
ETL Job DAG - Parameterized DAG for executing ETL jobs

This DAG is triggered with a job_id parameter and:
1. Fetches the job configuration from MongoDB
2. Fetches the source connection details
3. Executes the Spark ETL runner with the configuration
4. Runs Glue Crawler to register data in Glue Catalog
5. Updates the job run status on completion

Trigger via Airflow API:
POST /api/v1/dags/etl_job_dag/dagRuns
{
    "conf": {"job_id": "your_job_id_here"},
    "dag_run_id": "unique_run_id"
}
"""

import json
from datetime import datetime

from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow import DAG


def fetch_job_config(**context):
    """Fetch job configuration from MongoDB"""
    import pymongo
    from bson import ObjectId

    job_id = context["dag_run"].conf.get("job_id")
    if not job_id:
        raise ValueError("job_id is required in dag_run.conf")

    # Connect to MongoDB
    mongo_url = Variable.get(
        "MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017"
    )
    mongo_db = Variable.get("MONGODB_DATABASE", default_var="mydb")

    client = pymongo.MongoClient(mongo_url)
    db = client[mongo_db]

    # Fetch ETL job
    job = db.etl_jobs.find_one({"_id": ObjectId(job_id)})
    if not job:
        raise ValueError(f"ETL job not found: {job_id}")

    # Handle multiple sources (new) or single source (legacy)
    sources = job.get("sources", [])
    if not sources and job.get("source"):
        # Legacy single source - wrap in list
        sources = [job.get("source")]

    # Enrich each source with connection details
    enriched_sources = []
    for source in sources:
        if source.get("type") == "rdb" and source.get("connection_id"):
            connection = db.rdb_sources.find_one({"_id": ObjectId(source["connection_id"])})
            if connection:
                source["connection"] = {
                    "type": connection.get("type"),
                    "host": connection.get("host"),
                    "port": connection.get("port"),
                    "database_name": connection.get("database_name"),
                    "user_name": connection.get("user_name"),
                    "password": connection.get("password"),
                }
        enriched_sources.append(source)

    # Build complete config for Spark
    config = {
        "name": job.get("name"),
        "sources": enriched_sources,  # Multiple sources
        "transforms": job.get("transforms", []),
        "destination": job.get("destination", {}),
        "nodes": job.get("nodes", []),  # Include nodes for schema info
    }

    # Add S3 config for LocalStack
    if config["destination"].get("type") == "s3":
        config["destination"]["s3_config"] = {
            "endpoint": Variable.get(
                "S3_ENDPOINT", default_var="http://localstack-main:4566"
            ),
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

    mongo_url = Variable.get(
        "MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017"
    )
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
        db.job_runs.update_one({"_id": ObjectId(run_id)}, {"$set": update_data})
        print(f"Updated job run {run_id} to status: {status}")
    except Exception as e:
        print(f"Failed to update job run status: {e}")
    finally:
        client.close()


def run_glue_crawler(**context):
    """Register table in Glue Catalog directly (no Crawler = no Trino)"""
    import boto3
    from botocore.exceptions import ClientError

    # Get config from previous task
    config = json.loads(context["ti"].xcom_pull(task_ids="fetch_job_config"))
    job_name = config["name"]
    destination = config["destination"]

    # Glue settings
    database_name = "xflow_db"
    table_name = job_name.replace("-", "_").replace(" ", "_").lower()
    s3_path = destination["path"].replace("s3a://", "s3://")
    if not s3_path.endswith("/"):
        s3_path += "/"
    s3_path += job_name  # Add job name as folder

    # Create Glue client
    glue = boto3.client(
        "glue",
        endpoint_url=Variable.get(
            "AWS_ENDPOINT", default_var="http://localstack-main:4566"
        ),
        region_name=Variable.get("AWS_REGION", default_var="ap-northeast-2"),
        aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID", default_var="test"),
        aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY", default_var="test"),
    )

    # 1. Create database if not exists
    try:
        glue.create_database(DatabaseInput={"Name": database_name})
        print(f"Created database: {database_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f"Database already exists: {database_name}")
        else:
            raise

    # 2. Build columns from schema (from nodes data)
    columns = []
    nodes = config.get("nodes", [])
    transforms = config.get("transforms", [])

    # Find schema from nodes - priority: last transform node > source nodes
    schema_source = None

    # First try: get schema from the last transform node
    if transforms:
        last_transform_id = transforms[-1].get("nodeId")
        for node in nodes:
            if node.get("id") == last_transform_id:
                schema_source = node.get("data", {}).get("schema")
                break

    # Fallback: get schema from first source node
    if not schema_source:
        for node in nodes:
            if node.get("type") == "input":
                schema_source = node.get("data", {}).get("schema")
                if schema_source:
                    break

    # Convert to Glue column format
    if schema_source:
        type_mapping = {
            "integer": "int", "int4": "int", "int8": "bigint",
            "float8": "double", "float4": "float", "numeric": "decimal",
            "varchar": "string", "text": "string",
            "boolean": "boolean", "bool": "boolean",
            "timestamp": "timestamp", "date": "date",
        }
        for col in schema_source:
            col_name = col.get("key", col.get("name", "unknown"))
            col_type = col.get("type", "string").lower()
            glue_type = type_mapping.get(col_type, "string")
            columns.append({"Name": col_name, "Type": glue_type})
    else:
        columns = [{"Name": "data", "Type": "string"}]

    # 3. Create or update table directly (no Crawler = no Trino!)
    table_input = {
        "Name": table_name,
        "StorageDescriptor": {
            "Columns": columns,
            "Location": s3_path,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Compressed": True,
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification": "parquet",
            "compressionType": "snappy",
        },
    }

    try:
        glue.create_table(DatabaseName=database_name, TableInput=table_input)
        print(f"Created table: {database_name}.{table_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            glue.update_table(DatabaseName=database_name, TableInput=table_input)
            print(f"Updated table: {database_name}.{table_name}")
        else:
            raise

    print(f"✅ Table registered at: {s3_path}")

    return {
        "database": database_name,
        "table": table_name,
        "location": s3_path,
    }


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
    # Use local[2] to limit parallelism and avoid OOM on large datasets
    run_spark_etl = BashOperator(
        task_id="run_spark_etl",
        bash_command="""
            docker exec spark-master /opt/spark/bin/spark-submit \
                --master 'local[2]' \
                --driver-memory 4g \
                --conf 'spark.sql.shuffle.partitions=16' \
                --conf 'spark.memory.fraction=0.6' \
                --name "ETL-{{ dag_run.conf.get('job_id', 'unknown') }}" \
                --jars /opt/spark/jars/extra/postgresql-42.7.4.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar \
                /opt/spark/jobs/etl_runner.py '{{ ti.xcom_pull(task_ids="fetch_job_config") }}'
        """,
    )

    # Task 3: Run Glue Crawler to register data in Glue Catalog
    run_crawler = PythonOperator(
        task_id="run_glue_crawler",
        python_callable=run_glue_crawler,
    )

    fetch_config >> run_spark_etl >> run_crawler
