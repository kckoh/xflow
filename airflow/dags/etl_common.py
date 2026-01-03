"""
ETL Common - Shared functions for ETL DAGs

This module contains common functions used by both local and production ETL DAGs.
"""

import json
from datetime import datetime

from airflow.models import Variable




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
            connection = db.connections.find_one(
                {"_id": ObjectId(source["connection_id"])}
            )
            if connection:
                config = connection.get("config", {})
                source["connection"] = {
                    "type": connection.get("type"),
                    "host": config.get("host"),
                    "port": int(config.get("port", 5432)),
                    "database_name": config.get("database_name"),
                    "user_name": config.get("user_name"),
                    "password": config.get("password"),
                }
        if source.get("type") == "s3" and source.get("connection_id"):
            connection = db.connections.find_one(
                {"_id": ObjectId(source["connection_id"])}
            )
            if connection:
                source["connection"] = connection.get("config", {})
        enriched_sources.append(source)

    # Get estimated size from job document (calculated at job creation time)
    estimated_size_gb = job.get("estimated_size_gb", 1.0)
    print(f"Estimated source size from job config: {estimated_size_gb:.2f} GB")

    # Build complete config for Spark
    # Build complete config for Spark
    config = {
        "job_id": job_id,
        "name": job.get("name"),
        "sources": enriched_sources,
        "transforms": job.get("transforms", []),
        "destination": job.get("destination", {}),
        "nodes": job.get("nodes", []),
        "estimated_size_gb": estimated_size_gb,
    }
    
    # Inject incremental config into sources and destination if present
    incremental_config = job.get("incremental_config")
    if incremental_config:
        # Merge the top-level last_sync_timestamp into the config dict for Spark
        last_sync = job.get("last_sync_timestamp")
        if last_sync:
            incremental_config["last_sync_timestamp"] = last_sync.isoformat()
            
        for source in config["sources"]:
            source["incremental_config"] = incremental_config
        config["destination"]["incremental_config"] = incremental_config

    # Add S3 config (different for local vs production)
    if config["destination"].get("type") == "s3":
        env = Variable.get("ENVIRONMENT", default_var="local")
        if env == "production":
            # Production: Use IAM role, no explicit credentials needed
            config["destination"]["s3_config"] = {
                "use_iam_role": True,
                "region": Variable.get("AWS_REGION", default_var="ap-northeast-2"),
            }
        else:
            # Local: Use LocalStack with explicit credentials
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


def finalize_import(**context):
    """Final task: Set import_ready flag to True and update last_sync_timestamp for incremental loads"""
    import pymongo
    from bson import ObjectId

    job_id = context["dag_run"].conf.get("job_id")

    mongo_url = Variable.get(
        "MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017"
    )
    mongo_db = Variable.get("MONGODB_DATABASE", default_var="mydb")

    client = pymongo.MongoClient(mongo_url)
    db = client[mongo_db]

    try:
        # Fetch job to check if incremental is enabled
        job = db.etl_jobs.find_one({"_id": ObjectId(job_id)})
        if not job:
            print(f"ETL Job {job_id} not found")
            return

        update_fields = {"import_ready": True}

        # Update last_sync_timestamp if incremental is enabled
        incremental_config = job.get("incremental_config") or {}
        if incremental_config.get("enabled"):
            current_time = datetime.utcnow()
            update_fields["last_sync_timestamp"] = current_time
            print(f"[Incremental] Updated last_sync_timestamp: {current_time.isoformat()}")

        result = db.etl_jobs.update_one(
            {"_id": ObjectId(job_id)},
            {"$set": update_fields}
        )

        if result.modified_count > 0:
            print(f"ETL Job {job_id}: import_ready = True")
        else:
            print(f"ETL Job {job_id} not found or already marked")

    except Exception as e:
        print(f"Failed to finalize import: {e}")
        raise
    finally:
        client.close()


def on_success_callback(context):
    """Callback when DAG succeeds"""
    update_job_run_status("success", **context)


def on_failure_callback(context):
    """Callback when DAG fails"""
    error = str(context.get("exception", "Unknown error"))
    update_job_run_status("failed", error_message=error, **context)
