"""
Dataset Common - Shared functions for Dataset DAGs

This module contains common functions used by both local and production Dataset DAGs.
"""

import json
import base64
from datetime import datetime

from airflow.models import Variable



def convert_nodes_to_sources(nodes, edges, db):
    """Convert Target Wizard nodes/edges format to sources/transforms format"""
    from bson import ObjectId

    sources = []
    transforms = []

    # Build edge map (target -> source)
    edge_map = {}
    for edge in edges:
        target_id = edge.get("target")
        source_id = edge.get("source")
        if target_id not in edge_map:
            edge_map[target_id] = []
        edge_map[target_id].append(source_id)

    # Process nodes
    for node in nodes:
        node_id = node.get("id")
        node_data = node.get("data", {})

        # Determine node category from multiple possible sources
        node_category = node_data.get("nodeCategory")

        # If nodeCategory not set, infer from node id or other fields
        if not node_category:
            if node_id.startswith("source-"):
                node_category = "source"
            elif node_id.startswith("target-"):
                node_category = "target"
            elif node_id.startswith("transform-"):
                node_category = "transform"
            elif node_data.get("sourceJobId") or node_data.get("sourceDatasetId"):
                node_category = "source"
            elif node_data.get("transformType"):
                node_category = "transform"
            else:
                # Check if this node has jobs array (lineage node from import)
                if node_data.get("jobs"):
                    node_category = "source"
                else:
                    node_category = "source"  # Default to source

        if node_category == "source":
            # This is a source node - need to fetch the source dataset details
            source_dataset_id = node_data.get("sourceDatasetId")
            source_job_id = node_data.get("sourceJobId")

            # Determine which ID to use for fetching source info
            fetch_id = source_dataset_id or source_job_id
            print(f"   Processing source node: {node_id}, sourceDatasetId={source_dataset_id}, sourceJobId={source_job_id}, fetch_id={fetch_id}")

            if fetch_id:
                # Try to fetch from source_datasets collection first
                source_dataset = None
                try:
                    source_dataset = db.source_datasets.find_one({"_id": ObjectId(fetch_id)})
                except:
                    pass

                # If not found in source_datasets, try etl_jobs collection
                if not source_dataset:
                    try:
                        source_dataset = db.etl_jobs.find_one({"_id": ObjectId(fetch_id)})
                    except:
                        pass

                print(f"   Found source_dataset: {source_dataset is not None}")

                if source_dataset:
                    # Check if this is from source_datasets collection (has source_type field)
                    if "source_type" in source_dataset:
                        # Source dataset format
                        source_type = source_dataset.get("source_type", "postgres")
                        connection_id = source_dataset.get("connection_id")
                        table_name = source_dataset.get("table")
                        collection_name = source_dataset.get("collection")

                        # Map source_type to type
                        if source_type in ["postgres", "mysql"]:
                            mapped_type = "rdb"
                        else:
                            mapped_type = source_type

                        source_config = {
                            "nodeId": node_id,
                            "type": mapped_type,
                            "connection_id": str(connection_id) if connection_id else None,
                            "table": table_name,
                            "collection": collection_name,
                        }

                        # Add S3-specific fields
                        if source_type == "s3":
                            source_config["bucket"] = source_dataset.get("bucket")
                            source_config["path"] = source_dataset.get("path")
                            source_config["format"] = source_dataset.get("format", "parquet")
                            # Get customRegex from node_data (set in Target Wizard)
                            source_config["customRegex"] = node_data.get("customRegex") or source_dataset.get("customRegex")

                        # Add API-specific fields
                        if source_type == "api" or mapped_type == "api":
                            api_config = source_dataset.get("api", {})
                            source_config["endpoint"] = api_config.get("endpoint", "")
                            source_config["method"] = api_config.get("method", "GET")
                            source_config["query_params"] = api_config.get("query_params", {})
                            source_config["pagination"] = api_config.get("pagination", {"type": "none", "config": {}})
                            source_config["response_path"] = api_config.get("response_path", "")

                            # API incremental config (from source_dataset.api.incremental_config)
                            api_incremental = api_config.get("incremental_config") or api_config.get("incremental") or {}
                            if api_incremental.get("enabled"):
                                source_config["incremental_config"] = {
                                    "enabled": True,
                                    "timestamp_param": api_incremental.get("timestamp_param", "since"),
                                    "start_from": api_incremental.get("start_from", "")
                                }
                                print(f"   [API] Incremental load enabled: timestamp_param={api_incremental.get('timestamp_param')}, start_from={api_incremental.get('start_from', 'not set')}")

                            print(f"   [API] Added API config: endpoint={source_config['endpoint']}, pagination={source_config['pagination'].get('type')}")

                        # Add incremental load config from node_data (for RDB/S3/MongoDB)
                        incremental_config = node_data.get("incrementalConfig")
                        if incremental_config and incremental_config.get("enabled") and source_type != "api":
                            source_config["incremental_config"] = incremental_config
                            print(f"   [Incremental Load] Enabled for {node_id}: timestamp_column={incremental_config.get('timestamp_column')}")

                        sources.append(source_config)
                        if source_type == "s3":
                            print(f"   Converted S3 source node: {node_id} -> bucket={source_config.get('bucket')}, format={source_config.get('format')}, customRegex={bool(source_config.get('customRegex'))}")
                        else:
                            print(f"   Converted source node: {node_id} -> {source_type} (table: {table_name or collection_name or source_config.get('bucket')})")
                    else:
                        # ETL Job format - get source info from the ETL job's source config
                        job_source = source_dataset.get("source", {})
                        job_sources = source_dataset.get("sources", [])

                        if job_sources:
                            job_source = job_sources[0]

                        # Check if we found source config, otherwise look in the job's nodes
                        if not job_source.get("type") and not job_source.get("table"):
                            # Job doesn't have sources, check if it has nodes with source info
                            job_nodes = source_dataset.get("nodes", [])
                            print(f"   Job has no sources field, checking {len(job_nodes)} nodes")
                            for jn in job_nodes:
                                jn_data = jn.get("data", {})
                                jn_category = jn_data.get("nodeCategory")
                                jn_id = jn.get("id", "")

                                # Check if this is a source node
                                if jn_category == "source" or jn_id.startswith("source-"):
                                    # Found a source node in the referenced job
                                    # Try to get its sourceDatasetId
                                    nested_source_id = jn_data.get("sourceDatasetId")
                                    if nested_source_id:
                                        print(f"   Found nested sourceDatasetId: {nested_source_id}")
                                        try:
                                            nested_source = db.source_datasets.find_one({"_id": ObjectId(nested_source_id)})
                                            if nested_source and "source_type" in nested_source:
                                                source_type = nested_source.get("source_type", "postgres")
                                                if source_type in ["postgres", "mysql"]:
                                                    mapped_type = "rdb"
                                                else:
                                                    mapped_type = source_type

                                                source_config = {
                                                    "nodeId": node_id,
                                                    "type": mapped_type,
                                                    "connection_id": str(nested_source.get("connection_id")) if nested_source.get("connection_id") else None,
                                                    "table": nested_source.get("table"),
                                                    "collection": nested_source.get("collection"),
                                                }
                                                if source_type == "s3":
                                                    source_config["bucket"] = nested_source.get("bucket")
                                                    source_config["path"] = nested_source.get("path")

                                                # Add incremental load config from node_data
                                                incremental_config = jn_data.get("incrementalConfig")
                                                if incremental_config and incremental_config.get("enabled"):
                                                    source_config["incremental_config"] = incremental_config
                                                    print(f"   [Incremental Load] Enabled for nested {node_id}: timestamp_column={incremental_config.get('timestamp_column')}")

                                                sources.append(source_config)
                                                print(f"   Converted nested source: {node_id} -> {mapped_type} (table: {source_config.get('table') or source_config.get('bucket')})")
                                                break
                                        except Exception as e:
                                            print(f"   Failed to fetch nested source: {e}")
                            # If we added a source from nested lookup, continue to next node
                            if sources and sources[-1].get("nodeId") == node_id:
                                continue

                        source_type = job_source.get("type", "rdb")
                        connection_id = job_source.get("connection_id")
                        table_name = job_source.get("table")

                        source_config = {
                            "nodeId": node_id,
                            "type": source_type,
                            "connection_id": str(connection_id) if connection_id else None,
                            "table": table_name,
                            "collection": job_source.get("collection"),
                        }

                        # Add S3-specific fields
                        if source_type == "s3":
                            source_config["bucket"] = job_source.get("bucket")
                            source_config["path"] = job_source.get("path")

                        sources.append(source_config)
                        print(f"   Converted source node: {node_id} -> {source_type} (table: {table_name or source_config.get('bucket')})")
                else:
                    # Fallback: use data from the node itself
                    print(f"   source_dataset not found, using node data fallback")
                    print(f"   Node data keys: {list(node_data.keys())}")

                    # Try to get source info from node's config or nested data
                    config_data = node_data.get("config", {})
                    source_info = config_data.get("source", {}) if config_data else {}

                    platform = node_data.get("platform") or source_info.get("type") or "postgres"
                    source_type = "rdb" if platform.lower() in ["postgres", "mysql", "postgresql", "rdb"] else platform.lower()

                    # Get table from various possible locations
                    table_name = (
                        node_data.get("table") or
                        node_data.get("name") or
                        source_info.get("table") or
                        config_data.get("table")
                    )

                    source_config = {
                        "nodeId": node_id,
                        "type": source_type,
                        "table": table_name,
                    }

                    # Check if node has connection_id directly
                    connection_id = node_data.get("connection_id") or source_info.get("connection_id") or config_data.get("connection_id")
                    if connection_id:
                        source_config["connection_id"] = str(connection_id)

                    # Add collection for MongoDB
                    collection = node_data.get("collection") or source_info.get("collection")
                    if collection:
                        source_config["collection"] = collection

                    # Add S3-specific fields
                    if source_type == "s3":
                        source_config["bucket"] = node_data.get("bucket") or source_info.get("bucket")
                        source_config["path"] = node_data.get("path") or source_info.get("path")

                    sources.append(source_config)
                    print(f"   Converted source node (fallback): {node_id} -> {source_type} (table: {table_name})")
            else:
                # No fetch_id, try to extract source info directly from node data
                print(f"   No fetch_id for source node {node_id}, checking node data")
                print(f"   Node data keys: {list(node_data.keys())}")

                # Try to get source info from node's config or nested data
                config_data = node_data.get("config", {})
                source_info = config_data.get("source", {}) if config_data else {}

                # Check if this node has source configuration in data
                platform = node_data.get("platform") or node_data.get("type") or source_info.get("type") or "postgres"
                source_type = "rdb" if platform.lower() in ["postgres", "mysql", "postgresql", "rdb"] else platform.lower()

                # Get table from various possible locations
                table_name = (
                    node_data.get("table") or
                    node_data.get("name") or
                    source_info.get("table") or
                    config_data.get("table")
                )

                source_config = {
                    "nodeId": node_id,
                    "type": source_type,
                    "table": table_name,
                    "collection": node_data.get("collection") or source_info.get("collection"),
                }

                connection_id = node_data.get("connection_id") or source_info.get("connection_id") or config_data.get("connection_id")
                if connection_id:
                    source_config["connection_id"] = str(connection_id)

                # Add S3-specific fields
                if source_type == "s3":
                    source_config["bucket"] = node_data.get("bucket") or source_info.get("bucket")
                    source_config["path"] = node_data.get("path") or source_info.get("path")

                sources.append(source_config)
                print(f"   Converted source node (from node data): {node_id} -> {source_type} (table: {table_name})")

        elif node_category == "transform":
            # This is a transform node
            transform_type = node_data.get("transformType", "select-fields")
            transform_config = node_data.get("transformConfig", {}) or {}
            
            # Special handling for SQL transform to ensure query is passed
            if transform_type == 'sql' and not transform_config.get('sql'):
                query = node_data.get('query')
                if query:
                    transform_config['sql'] = query
                    
            input_node_ids = edge_map.get(node_id, [])

            transform = {
                "nodeId": node_id,
                "type": transform_type,
                "config": transform_config,
                "inputNodeIds": input_node_ids,
            }
            transforms.append(transform)
            print(f"   Converted transform node: {node_id} -> {transform_type}")

        elif node_category == "target":
            # This is a target node - we'll use the destination config from the job
            print(f"   Target node found: {node_id}")

    print(f"   Summary: Extracted {len(sources)} source(s) and {len(transforms)} transform(s)")
    return sources, transforms


def fetch_dataset_config(as_base64=False, **context):
    """Fetch dataset configuration from MongoDB"""
    import pymongo
    from bson import ObjectId

    # Support both dataset_id (direct run) and job_id (scheduled run)
    dataset_id = context["dag_run"].conf.get("dataset_id") or context["dag_run"].conf.get("job_id")
    if not dataset_id:
        raise ValueError("dataset_id or job_id is required in dag_run.conf")

    # Connect to MongoDB
    mongo_url = Variable.get(
        "MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017"
    )
    mongo_db = Variable.get("MONGODB_DATABASE", default_var="mydb")

    client = pymongo.MongoClient(mongo_url)
    db = client[mongo_db]

    # Fetch Dataset (pipeline configuration)
    dataset = db.datasets.find_one({"_id": ObjectId(dataset_id)})
    if not dataset:
        raise ValueError(f"Dataset not found: {dataset_id}")

    # Check if this is a Target Wizard dataset (has nodes/edges but no sources)
    nodes = dataset.get("nodes", [])
    edges = dataset.get("edges", [])

    # Handle multiple sources (new) or single source (legacy) or nodes (Target Wizard)
    sources = dataset.get("sources", [])
    transforms = dataset.get("transforms", [])

    if not sources and nodes:
        # This is a Target Wizard dataset - convert nodes/edges to sources/transforms
        print(f"Converting Target Wizard nodes/edges to sources/transforms...")
        sources, transforms = convert_nodes_to_sources(nodes, edges, db)
    elif not sources and dataset.get("source"):
        # Legacy single source - wrap in list
        sources = [dataset.get("source")]

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
        elif source.get("type") == "mongodb" and source.get("connection_id"):
            connection = db.connections.find_one(
                {"_id": ObjectId(source["connection_id"])}
            )
            if connection:
                config = connection.get("config", {})
                source["connection"] = {
                    "type": connection.get("type"),
                    "uri": config.get("uri"),
                    "database": config.get("database"),
                }
        elif source.get("type") == "s3" and source.get("connection_id"):
            connection = db.connections.find_one(
                {"_id": ObjectId(source["connection_id"])}
            )
            if connection:
                source["connection"] = connection.get("config", {})
        elif source.get("type") == "api" and source.get("connection_id"):
            connection = db.connections.find_one(
                {"_id": ObjectId(source["connection_id"])}
            )
            if connection:
                source["connection"] = {
                    "type": connection.get("type"),
                    "config": connection.get("config", {}),
                }
        enriched_sources.append(source)

    # Get estimated size from dataset document (calculated at dataset creation time)
    estimated_size_gb = dataset.get("estimated_size_gb", 1.0)
    print(f"Estimated source size from dataset config: {estimated_size_gb:.2f} GB")

    # Build complete config for Spark
    config = {
        "dataset_id": dataset_id,
        "name": dataset.get("name"),
        "sources": enriched_sources,
        "transforms": transforms,  # Use converted transforms (from nodes or original)
        "destination": dataset.get("destination", {}),
        "nodes": nodes,
        "estimated_size_gb": estimated_size_gb,
    }

    # Inject incremental config into sources and destination if present
    incremental_config = dataset.get("incremental_config")
    if incremental_config:
        # Merge the top-level last_sync_timestamp into the config dict for Spark
        last_sync = dataset.get("last_sync_timestamp")
        if last_sync:
            incremental_config["last_sync_timestamp"] = last_sync.isoformat()

        for source in config["sources"]:
            if source.get("incremental_config"):
                # Source already has its own incremental config (API, RDB, S3, etc.)
                # Only inject last_sync_timestamp, don't overwrite the config
                if last_sync:
                    source["incremental_config"]["last_sync_timestamp"] = incremental_config["last_sync_timestamp"]
            else:
                # Source doesn't have incremental config, use dataset-level (backwards compatibility)
                source["incremental_config"] = incremental_config
        config["destination"]["incremental_config"] = incremental_config

    # Add S3 config (different for local vs production)
    if config["destination"].get("type") == "s3":
        # Auto-generate glue_table_name if not provided
        if not config["destination"].get("glue_table_name"):
            # Use dataset name, sanitized for Glue (lowercase, underscores only)
            import re
            safe_name = re.sub(r'[^a-z0-9_]', '_', dataset.get("name", "unknown").lower())
            config["destination"]["glue_table_name"] = f"tgt_{safe_name}"
            print(f"   Auto-generated glue_table_name: {config['destination']['glue_table_name']}")

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
    json_config = json.dumps(config)
    
    if as_base64:
        # Return base64 encoded string for safe CLI usage
        return base64.b64encode(json_config.encode("utf-8")).decode("utf-8")
        
    return json_config


# Backward compatibility alias
fetch_job_config = fetch_dataset_config


def update_job_run_status(status: str, error_message: str = None, **context):
    """Update job run status in MongoDB"""
    import pymongo
    from bson import ObjectId

    dag_run = context["dag_run"]
    dataset_id = dag_run.conf.get("dataset_id")

    # Extract run_id from dag_run_id (format: dataset_{dataset_id}_{run_id})
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


def calculate_and_update_dataset_size(db, dataset_id, s3_client):
    """Helper function to calculate and update S3 file size for a single dataset

    Searches in both datasets and source_datasets collections, but only updates
    datasets that have destination paths (target datasets).
    """
    import re
    from bson import ObjectId

    try:
        # Try to find in datasets collection first (target datasets)
        dataset = db.datasets.find_one({"_id": ObjectId(dataset_id)})

        # If not found, try source_datasets collection (source-only datasets)
        if not dataset:
            dataset = db.source_datasets.find_one({"_id": ObjectId(dataset_id)})
            if dataset:
                print(f"   ℹ️ Dataset {dataset.get('name', dataset_id)} is a source dataset (no destination path), skipping size calculation")
                return

        if not dataset:
            print(f"   ⚠️ Dataset {dataset_id} not found in datasets or source_datasets, skipping")
            return

        destination = dataset.get("destination", {})
        s3_path = destination.get("path")

        if not s3_path:
            print(f"   ℹ️ Dataset {dataset.get('name', dataset_id)} has no S3 destination path, skipping")
            return

        dataset_name = dataset.get("name", "")

        print(f"   Calculating size for {dataset_name}...")

        # Parse S3 path (s3://bucket/path or s3a://bucket/path)
        path = re.sub(r'^s3a?://', '', s3_path)
        parts = path.split('/', 1)
        bucket = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''
        # Remove trailing slash for consistency (matches Backend approach)
        prefix = prefix.rstrip('/')

        # Calculate total size with pagination and retry logic
        total_size = 0
        file_count = 0

        # Retry logic for eventual consistency issues
        max_retries = 3
        retry_delay = 2  # seconds

        for attempt in range(max_retries):
            if attempt > 0:
                import time
                time.sleep(retry_delay)

            continuation_token = None
            temp_total_size = 0
            temp_file_count = 0

            while True:
                list_params = {'Bucket': bucket, 'Prefix': prefix}
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token

                try:
                    response = s3_client.list_objects_v2(**list_params)
                except Exception as list_err:
                    print(f"   ⚠️ Error listing S3 objects: {list_err}")
                    raise

                if 'Contents' in response:
                    for obj in response['Contents']:
                        temp_total_size += obj['Size']
                        temp_file_count += 1

                if response.get('IsTruncated'):
                    continuation_token = response.get('NextContinuationToken')
                else:
                    break

            # If we found files, use them and break
            if temp_file_count > 0:
                total_size = temp_total_size
                file_count = temp_file_count
                break

        # Update dataset with calculated size (only in datasets collection)
        db.datasets.update_one(
            {"_id": ObjectId(dataset_id)},
            {"$set": {"actual_size_bytes": total_size}}
        )
        print(f"   ✅ Updated {dataset_name}: {file_count} files, {total_size} bytes ({total_size / (1024**3):.2f} GB)")

    except Exception as e:
        print(f"   ⚠️ Error calculating S3 size for dataset {dataset_id}: {e}")


def finalize_import(**context):
    """Final task: Set import_ready flag to True and update last_sync_timestamp for incremental loads
    Also updates actual_size_bytes for current dataset and all source datasets (recursive)"""
    import pymongo
    from bson import ObjectId

    # Support both dataset_id (direct run) and job_id (scheduled run)
    dataset_id = context["dag_run"].conf.get("dataset_id") or context["dag_run"].conf.get("job_id")

    mongo_url = Variable.get(
        "MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017"
    )
    mongo_db = Variable.get("MONGODB_DATABASE", default_var="mydb")

    client = pymongo.MongoClient(mongo_url)
    db = client[mongo_db]

    try:
        # Fetch dataset to check if incremental is enabled
        dataset = db.datasets.find_one({"_id": ObjectId(dataset_id)})
        if not dataset:
            print(f"Dataset {dataset_id} not found")
            return

        update_fields = {"import_ready": True}

        # Update last_sync_timestamp if incremental is enabled
        incremental_config = dataset.get("incremental_config") or {}
        if incremental_config.get("enabled"):
            current_time = datetime.utcnow()
            update_fields["last_sync_timestamp"] = current_time
            print(f"[Incremental] Updated last_sync_timestamp: {current_time.isoformat()}")

        # Get S3 client (supports both LocalStack and AWS)
        import boto3
        aws_endpoint = Variable.get("AWS_ENDPOINT", default_var=None) or Variable.get("AWS_ENDPOINT_URL", default_var=None)

        if aws_endpoint:
            # LocalStack configuration
            s3_client = boto3.client(
                's3',
                endpoint_url=aws_endpoint,
                aws_access_key_id=Variable.get("AWS_ACCESS_KEY_ID", default_var="test"),
                aws_secret_access_key=Variable.get("AWS_SECRET_ACCESS_KEY", default_var="test"),
                region_name=Variable.get("AWS_REGION", default_var="ap-northeast-2")
            )
        else:
            # AWS configuration (IRSA)
            s3_client = boto3.client(
                's3',
                region_name=Variable.get("AWS_REGION", default_var="ap-northeast-2")
            )

        # Calculate S3 file size for current dataset
        destination = dataset.get("destination", {})
        s3_path = destination.get("path")

        if s3_path:
            dataset_name = dataset.get("name", "")

            print(f"📊 Calculating S3 file size for dataset {dataset_name}...")
            try:
                import re

                # Parse S3 path (s3://bucket/path or s3a://bucket/path)
                path = re.sub(r'^s3a?://', '', s3_path)
                parts = path.split('/', 1)
                bucket = parts[0]
                prefix = parts[1] if len(parts) > 1 else ''
                # Remove trailing slash for consistency (matches Backend approach)
                prefix = prefix.rstrip('/')

                # Calculate total size with pagination and retry logic
                total_size = 0
                file_count = 0

                # Retry logic for eventual consistency issues
                max_retries = 3
                retry_delay = 2  # seconds

                for attempt in range(max_retries):
                    if attempt > 0:
                        import time
                        time.sleep(retry_delay)

                    continuation_token = None
                    temp_total_size = 0
                    temp_file_count = 0

                    while True:
                        list_params = {'Bucket': bucket, 'Prefix': prefix}
                        if continuation_token:
                            list_params['ContinuationToken'] = continuation_token

                        try:
                            response = s3_client.list_objects_v2(**list_params)
                        except Exception as list_err:
                            print(f"⚠️ Error listing S3 objects: {list_err}")
                            raise

                        if 'Contents' in response:
                            for obj in response['Contents']:
                                temp_total_size += obj['Size']
                                temp_file_count += 1

                        if response.get('IsTruncated'):
                            continuation_token = response.get('NextContinuationToken')
                        else:
                            break

                    # If we found files, use them and break
                    if temp_file_count > 0:
                        total_size = temp_total_size
                        file_count = temp_file_count
                        break

                # Update dataset with calculated size
                update_fields["actual_size_bytes"] = total_size
                print(f"✅ Calculated size: {file_count} files, {total_size} bytes ({total_size / (1024**3):.2f} GB)")
            except Exception as e:
                print(f"⚠️ Error calculating S3 size: {e}")
                # Continue even if size calculation fails

        # Calculate row count using Trino (after S3 size calculation)
        # Get glue_table_name from dataset.destination (already loaded above)
        glue_table_name = destination.get("glue_table_name") if destination else None
        
        if glue_table_name:
            print(f"📊 Calculating row count for table {glue_table_name}...")
            
            try:
                import trino
                
                # Connect to Trino (same as register_trino_table)
                trino_host = Variable.get("TRINO_HOST", default_var="trino")
                trino_port = int(Variable.get("TRINO_PORT", default_var="8080"))
                
                conn = trino.dbapi.connect(
                    host=trino_host,
                    port=trino_port,
                    user="airflow",
                    catalog="lakehouse",
                    schema="default",
                )
                
                cursor = conn.cursor()
                
                # Count rows from Trino table
                count_query = f"SELECT COUNT(*) as row_count FROM lakehouse.default.{glue_table_name}"
                print(f"[Trino] Executing: {count_query}")
                cursor.execute(count_query)
                row_count_result = cursor.fetchone()
                row_count = row_count_result[0] if row_count_result else 0
                
                # Update dataset with row count
                update_fields["row_count"] = row_count
                print(f"✅ Calculated row count: {row_count:,} rows")
                
                cursor.close()
                conn.close()
                
            except Exception as e:
                print(f"⚠️ Error calculating row count: {e}")
                import traceback
                traceback.print_exc()
                # Continue even if row count fails
        else:
            # Skip silently in local environment (no Glue catalog)
            pass


        # Update current dataset with all fields
        result = db.datasets.update_one(
            {"_id": ObjectId(dataset_id)},
            {"$set": update_fields}
        )

        if result.modified_count > 0:
            print(f"Dataset {dataset_id}: import_ready = True")
        else:
            print(f"Dataset {dataset_id} not found or already marked")

        # Update sizes for all source datasets recursively
        print("\n📊 Updating sizes for source datasets...")
        processed_ids = set()  # Track processed datasets to avoid infinite loops

        def update_source_datasets(current_dataset_id):
            """Recursively update sizes for source datasets"""
            if current_dataset_id in processed_ids:
                return
            processed_ids.add(current_dataset_id)

            try:
                current_dataset = db.datasets.find_one({"_id": ObjectId(current_dataset_id)})
                if not current_dataset:
                    return

                # Get sources from the dataset
                sources = current_dataset.get("sources", [])
                nodes = current_dataset.get("nodes", [])

                # Extract source dataset/job IDs from sources array
                for source in sources:
                    source_dataset_id = source.get("sourceDatasetId")
                    source_job_id = source.get("sourceJobId")

                    # Check sourceDatasetId first (direct dataset reference)
                    if source_dataset_id:
                        print(f"   Found source dataset: {source_dataset_id}")
                        calculate_and_update_dataset_size(db, source_dataset_id, s3_client)
                        update_source_datasets(source_dataset_id)  # Recursive call

                    # Check sourceJobId (reference to another job/dataset)
                    elif source_job_id:
                        print(f"   Found source job: {source_job_id}")
                        calculate_and_update_dataset_size(db, source_job_id, s3_client)
                        update_source_datasets(source_job_id)  # Recursive call

                # Extract source IDs from nodes (for Target Wizard datasets)
                for node in nodes:
                    node_data = node.get("data", {})
                    node_category = node_data.get("nodeCategory")

                    if node_category == "source":
                        source_dataset_id = node_data.get("sourceDatasetId")
                        source_job_id = node_data.get("sourceJobId")

                        if source_dataset_id:
                            print(f"   Found source dataset in node: {source_dataset_id}")
                            calculate_and_update_dataset_size(db, source_dataset_id, s3_client)
                            update_source_datasets(source_dataset_id)  # Recursive call

                        elif source_job_id:
                            print(f"   Found source job in node: {source_job_id}")
                            calculate_and_update_dataset_size(db, source_job_id, s3_client)
                            update_source_datasets(source_job_id)  # Recursive call

            except Exception as e:
                print(f"   ⚠️ Error processing source datasets for {current_dataset_id}: {e}")

        # Start recursive update from current dataset
        update_source_datasets(dataset_id)
        print("✅ Finished updating all source dataset sizes")

    except Exception as e:
        print(f"Failed to finalize import: {e}")
        raise
    finally:
        client.close()


def on_success_callback(context):
    """Callback when DAG succeeds"""
    update_job_run_status("success", **context)


def on_failure_callback(context):
    """Callback when DAG fails - also fetches Spark logs"""
    from kubernetes import client, config

    error = str(context.get("exception", "Unknown error"))

    spark_logs = ""

    # Try to get Spark driver logs
    try:
        # Get spark app name from XCom
        ti = context.get("ti") or context.get("task_instance")
        if ti:
            spark_app_name = ti.xcom_pull(task_ids="generate_spark_spec", key="spark_app_name")
            if spark_app_name:
                config.load_incluster_config()
                v1 = client.CoreV1Api()

                # Find driver pod
                driver_pod_name = f"{spark_app_name}-driver"
                try:
                    logs = v1.read_namespaced_pod_log(
                        name=driver_pod_name,
                        namespace="spark-jobs",
                    )
                    spark_logs = f"\n\n=== Spark Driver Logs ===\n{logs}"
                    print(spark_logs)
                except Exception as log_err:
                    spark_logs = f"\n\nFailed to fetch Spark logs: {log_err}"
                    print(spark_logs)
    except Exception as e:
        print(f"Error fetching Spark logs: {e}")

    full_error = error + spark_logs
    # Store up to 50KB of error message
    update_job_run_status("failed", error_message=full_error[:50000], **context)


def register_trino_table(**context):
    """
    Register Delta Lake table in Trino using register_table procedure.

    Trino's register_table reads the _delta_log and creates proper metadata
    in Glue catalog that Trino can query.
    """
    import trino

    # Get config from previous task
    config_json = context["ti"].xcom_pull(task_ids="fetch_dataset_config")
    config = json.loads(config_json)

    destination = config.get("destination", {})
    glue_table_name = destination.get("glue_table_name")
    s3_path = destination.get("path")

    if not glue_table_name or not s3_path:
        print("[Trino] No glue_table_name or s3_path, skipping Trino registration")
        return

    # Build S3 location (Trino expects s3:// not s3a://)
    s3_location = s3_path.replace("s3a://", "s3://")
    # Ensure path ends with table name
    if not s3_location.endswith(glue_table_name):
        s3_location = s3_location.rstrip("/") + "/" + glue_table_name

    print(f"[Trino] Registering table: lakehouse.default.{glue_table_name}")
    print(f"[Trino] S3 location: {s3_location}")

    # Connect to Trino (service in default namespace)
    trino_host = Variable.get("TRINO_HOST", default_var="trino-cluster-trino.default.svc.cluster.local")
    trino_port = int(Variable.get("TRINO_PORT", default_var="8080"))

    conn = trino.dbapi.connect(
        host=trino_host,
        port=trino_port,
        user="airflow",
        catalog="lakehouse",
        schema="default",
    )

    try:
        cursor = conn.cursor()

        # First, try to unregister existing table (for tables created by boto3 or previous runs)
        try:
            unregister_sql = f"""
                CALL lakehouse.system.unregister_table(
                    schema_name => 'default',
                    table_name => '{glue_table_name}'
                )
            """
            print(f"[Trino] Executing: unregister_table for {glue_table_name}")
            cursor.execute(unregister_sql)
            cursor.fetchall()
            print(f"[Trino] Unregistered existing table")
        except Exception as e:
            print(f"[Trino] Unregister table (ignore): {e}")

        # Register table using register_table procedure
        register_sql = f"""
            CALL lakehouse.system.register_table(
                schema_name => 'default',
                table_name => '{glue_table_name}',
                table_location => '{s3_location}'
            )
        """
        print(f"[Trino] Executing: {register_sql.strip()}")
        cursor.execute(register_sql)
        result = cursor.fetchall()
        print(f"[Trino] Result: {result}")

        # Verify table exists
        verify_sql = f"SHOW TABLES LIKE '{glue_table_name}'"
        cursor.execute(verify_sql)
        tables = cursor.fetchall()

        if tables:
            print(f"[Trino] ✅ Table registered successfully: lakehouse.default.{glue_table_name}")
        else:
            print(f"[Trino] ⚠️ Table registration may have failed - table not found")

    except Exception as e:
        print(f"[Trino] ❌ Error registering table: {e}")
        raise
    finally:
        conn.close()


def run_quality_check(**context):
    """
    Run quality check on the Dataset output data.
    Calls the Quality Check API with the ETLJob ID and S3 path.
    """
    import requests
    import pymongo
    from bson import ObjectId

    # Support both dataset_id (direct run) and job_id (scheduled run)
    dataset_id = context["dag_run"].conf.get("dataset_id") or context["dag_run"].conf.get("job_id")
    if not dataset_id:
        print("[Quality] No dataset_id or job_id found, skipping quality check")
        return

    # Get MongoDB connection
    mongo_url = Variable.get(
        "MONGODB_URL", default_var="mongodb://mongo:mongo@mongodb:27017"
    )
    mongo_db = Variable.get("MONGODB_DATABASE", default_var="mydb")

    client = None
    try:
        client = pymongo.MongoClient(mongo_url)
        db = client[mongo_db]

        # Fetch Dataset (pipeline) to get destination info
        dataset = db.datasets.find_one({"_id": ObjectId(dataset_id)})
        if not dataset:
            print(f"[Quality] Dataset {dataset_id} not found")
            return

        destination = dataset.get("destination", {})
        s3_path = destination.get("s3_path") or destination.get("path")

        if not s3_path:
            print("[Quality] No S3 path found in dataset destination")
            return

        # Call Quality Check API with dataset_id
        backend_url = Variable.get(
            "BACKEND_URL", default_var="http://backend:8000"
        )

        api_url = f"{backend_url}/api/quality/{dataset_id}/run"
        payload = {"s3_path": s3_path}

        print(f"[Quality] Calling API: {api_url}")
        print(f"[Quality] Payload: {payload}")

        response = requests.post(api_url, json=payload, timeout=300)

        if response.status_code == 200:
            result = response.json()
            score = result.get("overall_score", 0)
            print(f"[Quality] Check completed! Score: {score}/100")
        else:
            print(f"[Quality] API call failed: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"[Quality] Error running quality check: {e}")
        # Don't raise - quality check failure should not fail the DAG
    finally:
        if client:
            client.close()
