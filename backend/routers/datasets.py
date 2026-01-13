import os
from datetime import datetime
from typing import List

import httpx
from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status

from models import Dataset, JobRun, Connection, ETLJob, User, Role
from schemas.dataset import DatasetCreate, DatasetUpdate, DatasetResponse
from services.lineage_service import sync_pipeline_to_etljob
# OpenSearch Dual Write
from utils.indexers import index_single_dataset, delete_dataset_from_index
from utils.schedule_converter import generate_schedule
from dependencies import sessions, get_user_session
from utils.permissions import get_user_permissions
from typing import Optional, Dict, Any
from fastapi import Depends

router = APIRouter()


def validate_incremental_config_for_schedule(schedule_frequency: str, incremental_config: dict, sources: list):
    """
    Validate that scheduled datasets have proper incremental configuration

    Args:
        schedule_frequency: Schedule frequency string
        incremental_config: Dataset-level incremental config
        sources: List of source configurations

    Raises:
        HTTPException: If validation fails
    """
    if not schedule_frequency or schedule_frequency == "None":
        return  # No schedule, no validation needed

    # Check if incremental config exists and is enabled
    if not incremental_config or not incremental_config.get("enabled"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Scheduled datasets require incremental load to be enabled. Please ensure your source has a timestamp column (updated_at, created_at, etc.)"
        )

    # Validate timestamp column for RDB/MongoDB sources
    for source_item in sources:
        source_type = source_item.get("type") or source_item.get("source_type")
        source_name = source_item.get("name", "Unknown")

        if source_type in ["rdb", "mongodb"]:
            # Check source-level incremental_config first, then dataset-level
            inc_cfg = source_item.get("incremental_config") or incremental_config
            if not inc_cfg or not inc_cfg.get("timestamp_column"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Source '{source_name}' requires a timestamp column for incremental load. No suitable column (updated_at, created_at, etc.) was found."
                )
        elif source_type == "api":
            # API sources need timestamp_param in incremental_config
            api_inc_cfg = source_item.get("api", {}).get("incremental_config")
            if api_inc_cfg and api_inc_cfg.get("enabled") and not api_inc_cfg.get("timestamp_param"):
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"API source '{source_name}' requires a timestamp parameter (e.g., 'since', 'updated_at') for incremental load."
                )


async def get_table_size_gb(connection: Connection, table_name: str) -> float:
    """Calculate table size in GB from database connection"""
    config = connection.config
    db_type = connection.type

    try:
        if db_type == "postgres":
            import psycopg2
            conn = psycopg2.connect(
                host=config.get("host"),
                port=config.get("port", 5432),
                database=config.get("database_name"),
                user=config.get("user_name"),
                password=config.get("password"),
            )
            cursor = conn.cursor()
            cursor.execute(
                "SELECT pg_total_relation_size(%s) / (1024.0 * 1024.0 * 1024.0)",
                (table_name,)
            )
            result = cursor.fetchone()
            size_gb = float(result[0]) if result and result[0] else 0.0
            cursor.close()
            conn.close()

        elif db_type == "mysql":
            import pymysql
            conn = pymysql.connect(
                host=config.get("host"),
                port=config.get("port", 3306),
                database=config.get("database_name"),
                user=config.get("user_name"),
                password=config.get("password"),
            )
            cursor = conn.cursor()
            cursor.execute("""
                SELECT (data_length + index_length) / (1024 * 1024 * 1024)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """, (config.get("database_name"), table_name))
            result = cursor.fetchone()
            size_gb = float(result[0]) if result and result[0] else 0.0
            cursor.close()
            conn.close()

        elif db_type == "mongodb":
            from pymongo import MongoClient
            host = config.get("host")
            port = config.get("port", 27017)
            user = config.get("user_name")
            password = config.get("password")
            db_name = config.get("database_name")

            if user and password:
                mongo_url = f"mongodb://{user}:{password}@{host}:{port}"
            else:
                mongo_url = f"mongodb://{host}:{port}"

            client = MongoClient(mongo_url)
            db = client[db_name]
            stats = db.command("collStats", table_name)
            size_bytes = stats.get("size", 0) + stats.get("totalIndexSize", 0)
            size_gb = size_bytes / (1024 * 1024 * 1024)
            client.close()
        else:
            return 1.0

        print(f"Table {table_name} ({db_type}) size: {size_gb:.2f} GB")
        return max(size_gb, 0.1)  # Minimum 0.1 GB

    except Exception as e:
        print(f"Failed to get table size for {table_name}: {e}")
        return 1.0  # Default 1GB on error

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://airflow-webserver.airflow:8080/api/v1")
AIRFLOW_AUTH = ("admin", "admin")
# DAG ID: "dataset_dag" for local, "dataset_dag_k8s" for production (EKS)
AIRFLOW_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "dataset_dag")
print(f"[STARTUP] datasets.py loaded - AIRFLOW_DAG_ID={AIRFLOW_DAG_ID}")


@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
async def create_dataset(dataset: DatasetCreate, session_id: str = None):
    """Create a new Dataset configuration with auto-permission grant"""
    # Check if dataset name exists
    existing_dataset = await Dataset.find_one(Dataset.name == dataset.name)
    if existing_dataset:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Dataset name already exists"
        )

    # Handle both multiple sources (new) and single source (legacy)
    sources_data = []
    if dataset.sources:
        sources_data = [s.model_dump() for s in dataset.sources]
    elif dataset.source:
        sources_data = [dataset.source.model_dump()]

    # Validate all source connections and calculate table sizes
    total_size_gb = 0.0
    for source_item in sources_data:
        if source_item.get("connection_id"):
            try:
                connection = await Connection.get(PydanticObjectId(source_item["connection_id"]))
                if not connection:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail=f"Source connection not found: {source_item['connection_id']}"
                    )
                # Calculate table size if table_name exists
                table_name = source_item.get("table_name")
                if table_name and connection.type in ["postgres", "mysql", "mongodb"]:
                    size_gb = await get_table_size_gb(connection, table_name)
                    source_item["size_gb"] = size_gb
                    total_size_gb += size_gb
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid source connection ID: {source_item.get('connection_id')}"
                )

    # Validate incremental config if schedule is set
    validate_incremental_config_for_schedule(
        dataset.schedule_frequency,
        dataset.incremental_config,
        sources_data
    )

    # Create new Dataset with estimated size for Spark auto-scaling
    schedule = None
    if dataset.schedule_frequency:
        schedule = generate_schedule(dataset.schedule_frequency, dataset.ui_params)

    new_dataset = Dataset(
        name=dataset.name,
        description=dataset.description,
        dataset_type=dataset.dataset_type,
        job_type=dataset.job_type,
        sources=sources_data,
        source=sources_data[0] if sources_data else {},  # Legacy compatibility
        transforms=[t.model_dump() for t in dataset.transforms],
        targets=dataset.targets or [],
        destination=dataset.destination.model_dump(),

        # Schedule info
        schedule=schedule,
        schedule_frequency=dataset.schedule_frequency,
        ui_params=dataset.ui_params,
        incremental_config=dataset.incremental_config,

        status="draft",
        nodes=dataset.nodes or [],
        edges=dataset.edges or [],
        estimated_size_gb=total_size_gb if total_size_gb > 0 else 1.0,
        
        # Owner field - get from session
        owner=sessions[session_id].get("name") or sessions[session_id].get("email") or "" if session_id and session_id in sessions else None,
        
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    await new_dataset.insert()

    # Auto-grant permissions to creator's roles and shared roles
    from bson import ObjectId
    
    # 1. Grant access to creator's roles (if session exists)
    if session_id and session_id in sessions:
        user_session = sessions[session_id]
        user_id = user_session.get("user_id")
        
        if user_id:
            try:
                creator = await User.get(ObjectId(user_id))
                if creator and creator.role_ids:
                    # Update all roles belonging to the creator
                    for role_id in creator.role_ids:
                        try:
                            role = await Role.get(PydanticObjectId(role_id))
                            if role and str(new_dataset.id) not in role.dataset_permissions:
                                role.dataset_permissions.append(str(new_dataset.id))
                                await role.save()
                                print(f"✅ Auto-granted access to creator role: {role.name} -> {new_dataset.name}")
                        except Exception as inner_e:
                            print(f"⚠️ Failed to update creator role {role_id}: {inner_e}")
            except Exception as e:
                print(f"⚠️ Failed to grant creator role access: {e}")
    
    # 2. Grant access to shared roles
    if dataset.shared_role_ids:
        for shared_role_id in dataset.shared_role_ids:
            try:
                role = await Role.get(PydanticObjectId(shared_role_id))
                if role and str(new_dataset.id) not in role.dataset_permissions:
                    role.dataset_permissions.append(str(new_dataset.id))
                    await role.save()
                    print(f"✅ Shared dataset with role: {role.name} -> {new_dataset.name}")
            except Exception as e:
                print(f"⚠️ Failed to share with role {shared_role_id}: {e}")

    # Dual Write: OpenSearch에 인덱싱
    await index_single_dataset(new_dataset)

    # Sync to ETLJob (Lineage)
    await sync_pipeline_to_etljob(new_dataset)

    return DatasetResponse(
        id=str(new_dataset.id),
        name=new_dataset.name,
        description=new_dataset.description,
        owner=new_dataset.owner,  # Add owner field
        dataset_type=new_dataset.dataset_type,
        job_type=new_dataset.job_type,
        sources=new_dataset.sources,
        source=new_dataset.source,
        transforms=new_dataset.transforms,
        targets=new_dataset.targets,
        destination=new_dataset.destination,
        schedule=new_dataset.schedule,
        schedule_frequency=new_dataset.schedule_frequency,
        ui_params=new_dataset.ui_params,
        incremental_config=new_dataset.incremental_config,
        status=new_dataset.status,
        nodes=new_dataset.nodes,
        edges=new_dataset.edges,
        created_at=new_dataset.created_at,
        updated_at=new_dataset.updated_at,
        import_ready=False,
    )


@router.get("", response_model=List[DatasetResponse])
async def list_datasets(
    import_ready: bool = None,
    user_session: Optional[Dict[str, Any]] = Depends(get_user_session)
):
    """Get all Datasets with their active status, filtered by RBAC permissions"""
    
    # Build query filter
    query = {}
    if import_ready is not None:
        query["import_ready"] = import_ready

    # RBAC Permission Filter
    if user_session:
        user_id = user_session.get("user_id")
        if user_id:
            from bson import ObjectId
            user = await User.get(ObjectId(user_id))
            if user:
                perms = await get_user_permissions(user)
                
                # If not admin and not full access, filter by ID list
                if not perms["is_admin"] and not perms["all_datasets"]:
                    accessible_ids = []
                    for did in perms["accessible_datasets"]:
                        try:
                            accessible_ids.append(PydanticObjectId(did))
                        except:
                            pass
                    
                    # Add ID filter to existing query
                    query["_id"] = {"$in": accessible_ids}

    datasets = await Dataset.find(query).to_list()

    # Pre-fetch ETLJobs to map is_active status
    etl_jobs = await ETLJob.find_all().to_list()
    status_map = {d.dataset_id: d.is_active for d in etl_jobs if d.dataset_id}

    return [
        DatasetResponse(
            id=str(dataset.id),
            name=dataset.name,
            description=dataset.description,
            owner=getattr(dataset, 'owner', None),  # Add owner field
            dataset_type=getattr(dataset, 'dataset_type', 'source'),
            job_type=getattr(dataset, 'job_type', 'batch'),
            sources=dataset.sources,
            source=dataset.source,
            transforms=dataset.transforms,
            targets=getattr(dataset, 'targets', []),
            destination=dataset.destination,
            schedule=dataset.schedule,
            schedule_frequency=dataset.schedule_frequency,
            ui_params=dataset.ui_params,
            incremental_config=dataset.incremental_config,
            status=dataset.status,
            nodes=dataset.nodes,
            edges=dataset.edges,
            created_at=dataset.created_at,
            updated_at=dataset.updated_at,
            is_active=dataset.status == "active",  # Derive from status for consistency
            import_ready=getattr(dataset, 'import_ready', False)
        )
        for dataset in datasets
    ]


@router.get("/{dataset_id}", response_model=DatasetResponse)
async def get_dataset(dataset_id: str):
    """Get a specific Dataset by ID"""
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    return DatasetResponse(
        id=str(dataset.id),
        name=dataset.name,
        description=dataset.description,
        owner=getattr(dataset, 'owner', None),  # Add owner field
        dataset_type=getattr(dataset, 'dataset_type', 'source'),
        job_type=dataset.job_type,
        sources=dataset.sources,
        source=dataset.source,
        transforms=dataset.transforms,
        targets=getattr(dataset, 'targets', []),
        destination=dataset.destination,
        schedule=dataset.schedule,
        schedule_frequency=dataset.schedule_frequency,
        ui_params=dataset.ui_params,
        incremental_config=dataset.incremental_config,
        status=dataset.status,
        nodes=dataset.nodes,
        edges=dataset.edges,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at,
        is_active=dataset.status == "active",  # Derive from status
        import_ready=getattr(dataset, 'import_ready', False),
    )


@router.put("/{dataset_id}", response_model=DatasetResponse)
async def update_dataset(dataset_id: str, dataset_update: DatasetUpdate):
    """Update a Dataset configuration"""
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Update fields if provided
    if dataset_update.name is not None:
        dataset.name = dataset_update.name
    if dataset_update.description is not None:
        dataset.description = dataset_update.description

    # Handle both multiple sources (new) and single source (legacy)
    if dataset_update.sources is not None:
        dataset.sources = [s.model_dump() for s in dataset_update.sources]
        dataset.source = dataset.sources[0] if dataset.sources else {}  # Legacy compatibility
    elif dataset_update.source is not None:
        dataset.source = dataset_update.source.model_dump()
        dataset.sources = [dataset.source]  # Also update sources array

    if dataset_update.transforms is not None:
        dataset.transforms = [t.model_dump() for t in dataset_update.transforms]
    if dataset_update.targets is not None:
        dataset.targets = dataset_update.targets
    if dataset_update.destination is not None:
        dataset.destination = dataset_update.destination.model_dump()

    # Handle schedule updates
    if dataset_update.schedule_frequency == "":
        # Explicitly clear schedule if frequency is empty string
        dataset.schedule_frequency = None
        dataset.ui_params = None
        dataset.schedule = None
    elif dataset_update.schedule_frequency is not None or dataset_update.ui_params is not None:
        # Use existing values if not provided in update
        freq = dataset_update.schedule_frequency if dataset_update.schedule_frequency is not None else dataset.schedule_frequency
        params = dataset_update.ui_params if dataset_update.ui_params is not None else dataset.ui_params

        dataset.schedule_frequency = freq
        dataset.ui_params = params
        dataset.schedule = generate_schedule(freq, params)

    if dataset_update.status is not None:
        dataset.status = dataset_update.status

    if dataset_update.incremental_config is not None:
        dataset.incremental_config = dataset_update.incremental_config
    if dataset_update.import_ready is not None:
        dataset.import_ready = dataset_update.import_ready
    if dataset_update.nodes is not None:
        dataset.nodes = dataset_update.nodes
    if dataset_update.edges is not None:
        dataset.edges = dataset_update.edges
    if dataset_update.dataset_type is not None:
        dataset.dataset_type = dataset_update.dataset_type
    if dataset_update.job_type is not None:
        dataset.job_type = dataset_update.job_type

    # Validate incremental config if schedule is set
    validate_incremental_config_for_schedule(
        dataset.schedule_frequency,
        dataset.incremental_config,
        dataset.sources
    )

    dataset.updated_at = datetime.utcnow()
    await dataset.save()

    # Dual Write: OpenSearch 업데이트
    await index_single_dataset(dataset)

    # Sync to ETLJob (Lineage)
    await sync_pipeline_to_etljob(dataset)

    return DatasetResponse(
        id=str(dataset.id),
        name=dataset.name,
        description=dataset.description,
        dataset_type=getattr(dataset, 'dataset_type', 'source'),
        job_type=dataset.job_type,
        sources=dataset.sources,
        source=dataset.source,
        transforms=dataset.transforms,
        targets=dataset.targets,
        destination=dataset.destination,
        schedule=dataset.schedule,
        schedule_frequency=dataset.schedule_frequency,
        ui_params=dataset.ui_params,
        incremental_config=dataset.incremental_config,
        status=dataset.status,
        nodes=dataset.nodes,
        edges=dataset.edges,
        created_at=dataset.created_at,
        updated_at=dataset.updated_at,
        import_ready=getattr(dataset, 'import_ready', False),
    )



@router.patch("/{dataset_id}/nodes/{node_id}/metadata")
async def update_node_metadata(dataset_id: str, node_id: str, metadata: dict):
    """Update metadata for a specific node in a Dataset.

    Used by Domain to sync description/tags back to Dataset.

    Args:
        dataset_id: Dataset ID
        node_id: Node ID within the dataset
        metadata: { table: { description, tags }, columns: { colName: { description, tags } } }
    """
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Find and update the specific node
    node_found = False
    for i, node in enumerate(dataset.nodes or []):
        if node.get("id") == node_id:
            # Merge metadata
            if "data" not in dataset.nodes[i]:
                dataset.nodes[i]["data"] = {}
            if "metadata" not in dataset.nodes[i]["data"]:
                dataset.nodes[i]["data"]["metadata"] = {}

            # Update table metadata
            if "table" in metadata:
                if "table" not in dataset.nodes[i]["data"]["metadata"]:
                    dataset.nodes[i]["data"]["metadata"]["table"] = {}
                dataset.nodes[i]["data"]["metadata"]["table"].update(metadata["table"])

            # Update column metadata
            if "columns" in metadata:
                if "columns" not in dataset.nodes[i]["data"]["metadata"]:
                    dataset.nodes[i]["data"]["metadata"]["columns"] = {}
                dataset.nodes[i]["data"]["metadata"]["columns"].update(metadata["columns"])

            node_found = True
            break

    if not node_found:
        raise HTTPException(status_code=404, detail=f"Node {node_id} not found in dataset")

    dataset.updated_at = datetime.utcnow()
    await dataset.save()

    return {"message": "Metadata updated successfully", "dataset_id": dataset_id, "node_id": node_id}


@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_dataset(dataset_id: str):
    """Delete a Dataset"""
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    await dataset.delete()

    # Dual Write: OpenSearch에서 삭제
    await delete_dataset_from_index(dataset_id)

    # Cascading Delete: Delete associated ETLJob
    etl_job = await ETLJob.find_one(ETLJob.dataset_id == dataset_id)
    if etl_job:
        await etl_job.delete()

    return None


@router.post("/{dataset_id}/activate")
async def activate_dataset(dataset_id: str):
    """Activate schedule for a Dataset"""
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset.schedule:
        raise HTTPException(status_code=400, detail="Cannot activate dataset without a schedule")

    dataset.status = "active"
    dataset.updated_at = datetime.utcnow()
    await dataset.save()

    # Sync to ETLJob to update is_active
    await sync_pipeline_to_etljob(dataset)

    return {"message": "Dataset schedule activated", "status": "active"}


@router.post("/{dataset_id}/deactivate")
async def deactivate_dataset(dataset_id: str):
    """Pause schedule for a Dataset"""
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    dataset.status = "paused"
    dataset.updated_at = datetime.utcnow()
    await dataset.save()

    # Sync to ETLJob to update is_active
    await sync_pipeline_to_etljob(dataset)

    return {"message": "Dataset schedule paused", "status": "paused"}



@router.post("/{dataset_id}/run")
async def run_dataset(dataset_id: str):
    """Trigger immediate execution of a dataset (manual run)"""
    print(f"[DEBUG] run_dataset called - dataset_id={dataset_id}, AIRFLOW_DAG_ID={AIRFLOW_DAG_ID}")
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Always trigger immediate execution (manual run)
    # Schedule state is managed separately by activate/deactivate endpoints

    # Create job run record
    job_run = JobRun(
        dataset_id=dataset_id,
        status="pending",
        started_at=datetime.utcnow(),
    )
    await job_run.insert()

    # Trigger Airflow DAG
    airflow_url = f"{AIRFLOW_BASE_URL}/dags/{AIRFLOW_DAG_ID}/dagRuns"
    print(f"[DEBUG] Triggering Airflow DAG: URL={airflow_url}, DAG_ID={AIRFLOW_DAG_ID}")
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                airflow_url,
                json={
                    "conf": {"dataset_id": dataset_id},
                    "dag_run_id": f"dataset_{dataset_id}_{job_run.id}",
                },
                auth=AIRFLOW_AUTH,
                timeout=10.0,
            )

            if response.status_code in [200, 201]:
                airflow_data = response.json()
                job_run.airflow_run_id = airflow_data.get("dag_run_id")
                job_run.status = "running"
                await job_run.save()
            else:
                job_run.status = "failed"
                job_run.error_message = f"Airflow API error: {response.text}"
                await job_run.save()
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail=f"Failed to trigger Airflow DAG: {response.text}"
                )

    except httpx.RequestError as e:
        job_run.status = "failed"
        job_run.error_message = f"Connection error: {str(e)}"
        await job_run.save()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to connect to Airflow: {str(e)}"
        )

    return {
        "message": "Job triggered successfully (manual run)",
        "dataset_id": dataset_id,
        "run_id": str(job_run.id),
        "airflow_run_id": job_run.airflow_run_id,
    }


@router.post("/{dataset_id}/calculate-size")
async def calculate_dataset_size(dataset_id: str):
    """Calculate and update S3 file size for a dataset"""
    from utils.s3_utils import calculate_s3_size_bytes
    
    try:
        dataset = await Dataset.get(PydanticObjectId(dataset_id))
    except Exception:
        raise HTTPException(status_code=404, detail="Dataset not found")

    if not dataset:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Get S3 path
    s3_path = dataset.destination.get("path") if dataset.destination else None
    if not s3_path:
        raise HTTPException(status_code=400, detail="Dataset has no S3 destination path")

    try:
        # Calculate S3 file size
        file_size_bytes = await calculate_s3_size_bytes(s3_path)
        
        # Update Dataset
        dataset.actual_size_bytes = file_size_bytes
        dataset.updated_at = datetime.utcnow()
        await dataset.save()
        
        print(f"✅ Updated Dataset {dataset.name} with actual size: {file_size_bytes} bytes")
        
        return {
            "message": "S3 file size calculated successfully",
            "dataset_id": dataset_id,
            "size_bytes": file_size_bytes,
            "path": s3_path
        }
    except Exception as e:
        print(f"❌ Failed to calculate S3 size: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to calculate S3 size: {str(e)}"
        )
