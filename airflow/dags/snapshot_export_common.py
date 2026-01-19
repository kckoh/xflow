"""
Snapshot Export Common - RDS Snapshot Export functions for Airflow DAGs

This module contains functions for managing RDS Snapshot Export workflow
to support efficient initial load of large RDB datasets.

Usage:
    - Initial load (no last_sync_timestamp): Use Snapshot Export → S3 Parquet → Spark
    - Incremental load (has last_sync_timestamp): Use JDBC with WHERE clause
"""

import json
import time
import logging
from datetime import datetime

import boto3
from botocore.exceptions import ClientError
from airflow.models import Variable

logger = logging.getLogger(__name__)


def should_use_snapshot_export(**context) -> str:
    """
    Determine whether to use RDS Snapshot Export or JDBC for data loading.

    Decision Logic:
    1. Check if sources contain RDB type
    2. Check if incremental_config.initial_load_mode == "snapshot"
    3. Check if last_sync_timestamp is None (initial load)

    Returns:
        "snapshot_export" - Use RDS Snapshot Export for initial load
        "jdbc" - Use direct JDBC read (incremental or full)
    """
    config_json = context["ti"].xcom_pull(task_ids="fetch_dataset_config")
    config = json.loads(config_json)

    sources = config.get("sources", [])
    incremental_config = config.get("destination", {}).get("incremental_config", {})

    # Check if any source is RDB type
    has_rdb_source = any(
        s.get("type") in ["rdb", "postgres", "mysql", "postgresql"]
        for s in sources
    )

    if not has_rdb_source:
        # MongoDB, API, S3, Kafka sources → use existing logic
        print("[Mode Decision] No RDB source found, using JDBC path")
        return "jdbc"

    # Check initial_load_mode configuration
    initial_load_mode = incremental_config.get("initial_load_mode", "jdbc")
    has_last_sync = incremental_config.get("last_sync_timestamp") is not None

    print(f"[Mode Decision] RDB source detected")
    print(f"   initial_load_mode: {initial_load_mode}")
    print(f"   has_last_sync_timestamp: {has_last_sync}")

    # Use snapshot export only for initial load with snapshot mode configured
    if not has_last_sync and initial_load_mode == "snapshot":
        # Check if snapshot_export_config is available
        snapshot_config = _get_snapshot_export_config(config)
        if snapshot_config:
            print("[Mode Decision] → Using SNAPSHOT_EXPORT for initial load")
            return "snapshot_export"
        else:
            print("[Mode Decision] → snapshot_export_config not found, falling back to JDBC")
            return "jdbc"

    print("[Mode Decision] → Using JDBC (incremental or default)")
    return "jdbc"


def _get_snapshot_export_config(config: dict) -> dict:
    """Get snapshot export configuration from dataset config or Airflow Variables"""
    # First check dataset-level config
    snapshot_config = config.get("snapshot_export_config")
    if snapshot_config:
        return snapshot_config

    # Fall back to Airflow Variables (global defaults)
    try:
        return {
            "s3_bucket": Variable.get("RDS_EXPORT_S3_BUCKET"),
            "s3_prefix": Variable.get("RDS_EXPORT_S3_PREFIX", default_var="raw/snapshot-exports"),
            "iam_role_arn": Variable.get("RDS_EXPORT_IAM_ROLE_ARN"),
            "auto_cleanup_snapshot": Variable.get("RDS_EXPORT_AUTO_CLEANUP", default_var="true").lower() == "true",
        }
    except Exception:
        return None


def _get_rds_instance_identifier(config: dict) -> str:
    """Extract RDS instance identifier from source connection config"""
    sources = config.get("sources", [])

    for source in sources:
        if source.get("type") in ["rdb", "postgres", "mysql", "postgresql"]:
            connection = source.get("connection", {})
            # RDS instance identifier can be stored in connection config
            # or derived from host (for RDS endpoints)
            db_instance_id = connection.get("db_instance_identifier")
            if db_instance_id:
                return db_instance_id

            # Try to extract from host (e.g., mydb-prod.xxx.ap-northeast-2.rds.amazonaws.com)
            host = connection.get("host", "")
            if ".rds.amazonaws.com" in host:
                return host.split(".")[0]

    # Check snapshot_export_config
    snapshot_config = _get_snapshot_export_config(config)
    if snapshot_config:
        return snapshot_config.get("db_instance_identifier")

    return None


def _get_export_tables(config: dict) -> list:
    """Get list of tables to export in RDS export format (database.schema.table)"""
    sources = config.get("sources", [])
    export_tables = []

    for source in sources:
        if source.get("type") in ["rdb", "postgres", "mysql", "postgresql"]:
            connection = source.get("connection", {})
            database = connection.get("database_name", "")
            table = source.get("table", "")

            if database and table:
                # PostgreSQL: database.public.table
                # MySQL: database.table
                db_type = connection.get("type", "postgres")
                if db_type == "postgres":
                    schema = source.get("schema", "public")
                    export_tables.append(f"{database}.{schema}.{table}")
                else:
                    export_tables.append(f"{database}.{table}")

    return export_tables if export_tables else None  # None means export all


def start_snapshot_export(**context) -> dict:
    """
    Start RDS Snapshot Export workflow.

    Steps:
    1. Create RDS snapshot
    2. Return snapshot info (async - wait is separate task)

    Returns:
        dict with snapshot_id and export configuration
    """
    config_json = context["ti"].xcom_pull(task_ids="fetch_dataset_config")
    config = json.loads(config_json)

    snapshot_config = _get_snapshot_export_config(config)
    if not snapshot_config:
        raise ValueError("snapshot_export_config not found in dataset or Airflow Variables")

    db_instance_id = _get_rds_instance_identifier(config)
    if not db_instance_id:
        raise ValueError("Could not determine RDS instance identifier")

    dataset_id = config.get("dataset_id", "unknown")
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    snapshot_id = f"xflow-{dataset_id[:8]}-{timestamp}"

    region = Variable.get("AWS_REGION", default_var="ap-northeast-2")
    rds_client = boto3.client("rds", region_name=region)

    print(f"[Snapshot Export] Creating snapshot: {snapshot_id}")
    print(f"   DB Instance: {db_instance_id}")
    print(f"   S3 Bucket: {snapshot_config.get('s3_bucket')}")

    try:
        response = rds_client.create_db_snapshot(
            DBInstanceIdentifier=db_instance_id,
            DBSnapshotIdentifier=snapshot_id,
            Tags=[
                {"Key": "xflow:dataset_id", "Value": dataset_id},
                {"Key": "xflow:purpose", "Value": "initial-load"},
            ],
        )

        result = {
            "snapshot_id": snapshot_id,
            "snapshot_arn": response["DBSnapshot"]["DBSnapshotArn"],
            "db_instance_id": db_instance_id,
            "status": "creating",
            "s3_bucket": snapshot_config.get("s3_bucket"),
            "s3_prefix": snapshot_config.get("s3_prefix"),
            "iam_role_arn": snapshot_config.get("iam_role_arn"),
            "kms_key_id": snapshot_config.get("kms_key_id"),
            "auto_cleanup_snapshot": snapshot_config.get("auto_cleanup_snapshot", True),
            "export_tables": _get_export_tables(config),
        }

        print(f"[Snapshot Export] Snapshot creation initiated: {snapshot_id}")
        return result

    except ClientError as e:
        print(f"[Snapshot Export] Failed to create snapshot: {e}")
        raise


def wait_for_export_complete(**context) -> dict:
    """
    Wait for RDS Snapshot Export to complete.

    Steps:
    1. Wait for snapshot to become available
    2. Start export task
    3. Wait for export to complete

    Returns:
        dict with export results including S3 paths
    """
    snapshot_info = context["ti"].xcom_pull(task_ids="start_snapshot_export")
    if not snapshot_info:
        raise ValueError("No snapshot info found from start_snapshot_export task")

    snapshot_id = snapshot_info["snapshot_id"]
    s3_bucket = snapshot_info["s3_bucket"]
    s3_prefix = snapshot_info["s3_prefix"]
    iam_role_arn = snapshot_info["iam_role_arn"]
    kms_key_id = snapshot_info.get("kms_key_id")
    export_tables = snapshot_info.get("export_tables")

    region = Variable.get("AWS_REGION", default_var="ap-northeast-2")
    rds_client = boto3.client("rds", region_name=region)

    # Step 1: Wait for snapshot to be available
    print(f"[Snapshot Export] Waiting for snapshot {snapshot_id} to become available...")
    max_wait_minutes = 60
    poll_interval = 30
    start_time = time.time()

    while time.time() - start_time < max_wait_minutes * 60:
        response = rds_client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id)
        status = response["DBSnapshots"][0]["Status"]
        print(f"   Snapshot status: {status}")

        if status == "available":
            break
        elif status in ["failed", "deleted"]:
            raise RuntimeError(f"Snapshot {snapshot_id} failed with status: {status}")

        time.sleep(poll_interval)
    else:
        raise TimeoutError(f"Snapshot {snapshot_id} timed out after {max_wait_minutes} minutes")

    # Step 2: Start export task
    snapshot_arn = response["DBSnapshots"][0]["DBSnapshotArn"]
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    export_task_id = f"export-{snapshot_id}-{timestamp}"

    print(f"[Snapshot Export] Starting export task: {export_task_id}")

    export_params = {
        "ExportTaskIdentifier": export_task_id,
        "SourceArn": snapshot_arn,
        "S3BucketName": s3_bucket,
        "S3Prefix": s3_prefix,
        "IamRoleArn": iam_role_arn,
    }

    if kms_key_id:
        export_params["KmsKeyId"] = kms_key_id

    if export_tables:
        export_params["ExportOnly"] = export_tables

    try:
        export_response = rds_client.start_export_task(**export_params)
    except ClientError as e:
        print(f"[Snapshot Export] Failed to start export: {e}")
        raise

    # Step 3: Wait for export to complete
    print(f"[Snapshot Export] Waiting for export task {export_task_id} to complete...")
    max_export_wait_minutes = 180  # 3 hours for large datasets
    start_time = time.time()

    while time.time() - start_time < max_export_wait_minutes * 60:
        response = rds_client.describe_export_tasks(ExportTaskIdentifier=export_task_id)
        task = response["ExportTasks"][0]
        status = task["Status"]
        progress = task.get("PercentProgress", 0)

        print(f"   Export status: {status} ({progress}%)")

        if status == "COMPLETE":
            result = {
                "export_task_id": export_task_id,
                "snapshot_id": snapshot_id,
                "status": "COMPLETE",
                "s3_bucket": s3_bucket,
                "s3_prefix": f"{s3_prefix}/{export_task_id}",
                "total_extracted_data_in_gb": task.get("TotalExtractedDataInGB", 0),
                "auto_cleanup_snapshot": snapshot_info.get("auto_cleanup_snapshot", True),
            }
            print(f"[Snapshot Export] Export completed: {result['total_extracted_data_in_gb']} GB")
            return result

        elif status in ["FAILED", "CANCELED"]:
            failure_cause = task.get("FailureCause", "Unknown")
            raise RuntimeError(f"Export task failed: {failure_cause}")

        time.sleep(60)  # Check every minute

    raise TimeoutError(f"Export task {export_task_id} timed out after {max_export_wait_minutes} minutes")


def cleanup_snapshot(**context) -> bool:
    """
    Clean up snapshot after export (if auto_cleanup_snapshot is enabled).

    Returns:
        True if cleanup succeeded or was skipped
    """
    export_info = context["ti"].xcom_pull(task_ids="wait_for_export")
    if not export_info:
        print("[Snapshot Export] No export info found, skipping cleanup")
        return True

    if not export_info.get("auto_cleanup_snapshot", True):
        print("[Snapshot Export] auto_cleanup_snapshot is disabled, skipping cleanup")
        return True

    snapshot_id = export_info.get("snapshot_id")
    if not snapshot_id:
        print("[Snapshot Export] No snapshot_id found, skipping cleanup")
        return True

    region = Variable.get("AWS_REGION", default_var="ap-northeast-2")
    rds_client = boto3.client("rds", region_name=region)

    print(f"[Snapshot Export] Cleaning up snapshot: {snapshot_id}")

    try:
        rds_client.delete_db_snapshot(DBSnapshotIdentifier=snapshot_id)
        print(f"[Snapshot Export] Snapshot {snapshot_id} deleted successfully")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "DBSnapshotNotFound":
            print(f"[Snapshot Export] Snapshot {snapshot_id} already deleted")
            return True
        print(f"[Snapshot Export] Failed to delete snapshot: {e}")
        return False


def build_snapshot_path_for_source(export_info: dict, source_config: dict) -> str:
    """
    Build S3 path for a specific table from snapshot export.

    RDS Export creates files in format:
    s3://{bucket}/{prefix}/{export_task_id}/{database}.{schema}.{table}/*.parquet

    Args:
        export_info: Export task completion info
        source_config: Source configuration with connection and table info

    Returns:
        S3A path to the exported Parquet files
    """
    if not export_info:
        return None

    s3_bucket = export_info.get("s3_bucket")
    s3_prefix = export_info.get("s3_prefix")

    if not s3_bucket or not s3_prefix:
        return None

    connection = source_config.get("connection", {})
    database = connection.get("database_name", "")
    table = source_config.get("table", "")

    if not database or not table:
        return None

    # Build table path based on database type
    db_type = connection.get("type", "postgres")
    if db_type == "postgres":
        schema = source_config.get("schema", "public")
        table_path = f"{database}.{schema}.{table}"
    else:
        table_path = f"{database}.{table}"

    return f"s3a://{s3_bucket}/{s3_prefix}/{table_path}/"
