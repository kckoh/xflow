from fastapi import APIRouter, HTTPException, Query, Body, status, Depends
from typing import List, Optional, Dict, Any
from bson import ObjectId
import database
from schemas.catalog import CatalogItem, DatasetDetail, DatasetUpdate, DatasetCreate, LineageCreate
from services import catalog_service, lineage_service
from dependencies import sessions, get_user_session

router = APIRouter()

@router.post("", status_code=status.HTTP_201_CREATED)
async def create_new_dataset(dataset_data: DatasetCreate):
    """
    Register a new dataset in the catalog.
    """

    return await catalog_service.create_dataset(dataset_data)


@router.get("")
async def get_catalog(
    type: Optional[str] = Query(None, description="Filter by layer (e.g. RAW, MART)"),
    platform: Optional[str] = Query(None, description="Filter by platform"),
    search: Optional[str] = Query(None, description="Search by name or description"),
    user_session: Optional[Dict[str, Any]] = Depends(get_user_session)
):
    """
    Fetch list of datasets with optional filtering.
    If authenticated, filters based on user's dataset_access permissions.
    """
    db = database.mongodb_client[database.DATABASE_NAME]
    # Build Search/Filter Query
    query = {}
    
    # Only show datasets with successful ETL execution
    query["import_ready"] = True
    
    if type:
        query["properties.layer"] = type.upper() # Mock data stores layer in properties
    if platform:
        query["platform"] = platform
    if search:
        # Basic MongoDB Text/Regex Search
        query["$or"] = [
            {"name": {"$regex": search, "$options": "i"}},
            {"description": {"$regex": search, "$options": "i"}}
        ]
    
    # Default Filter: Hide "external" sources from main list
    if "tags" not in query:
        query["tags"] = {"$ne": "external"}

    cursor = db.datasets.find(query).limit(100)
    items = []
    async for doc in cursor:
        # Map properties.layer to top-level layer field for response
        doc["id"] = str(doc["_id"])
        del doc["_id"] # Remove ObjectId to make it JSON serializable
        
        if "properties" in doc and "layer" in doc["properties"]:
            doc["layer"] = doc["properties"]["layer"]
            
        # Use stored schema from MongoDB (fast!)
        # Schema is already saved when ETL job runs
        schema = None
        if "schema" in doc and doc["schema"]:
            schema = doc["schema"]
        elif "targets" in doc and len(doc["targets"]) > 0 and "schema" in doc["targets"][0]:
            schema = doc["targets"][0]["schema"]
        elif "nodes" in doc:
            transform_node = next((n for n in doc["nodes"] if n.get("data", {}).get("nodeCategory") == "transform" or n.get("data", {}).get("transformType")), None)
            if transform_node and "outputSchema" in transform_node.get("data", {}):
                schema = transform_node["data"]["outputSchema"]
        
        doc["columns"] = schema if schema else []
             
        # Add is_active flag (based on import_ready)
        doc["is_active"] = doc.get("import_ready", False)
        
        # Add actual file size from S3 (bytes)
        doc["size_bytes"] = doc.get("actual_size_bytes")
        
        # Add row_count (from quality check results or placeholder)
        doc["row_count"] = doc.get("row_count")
        
        # Add format from destination
        destination = doc.get("destination", {})
        doc["format"] = destination.get("format", "parquet") if destination else "parquet"
        
        # ✅ Ensure targets and destination are present for CatalogDatasetSelector
        if "targets" not in doc:
            doc["targets"] = []
        if "destination" not in doc:
            doc["destination"] = {}
             
        items.append(doc)
    
    # Check etl_access if authenticated
    if user_session:
        etl_access = user_session.get("etl_access", False)
        is_admin = user_session.get("is_admin", False)
        
        # Admin or etl_access = true can see catalog
        if not is_admin and not etl_access:
            # No access to catalog
            return []
        
    return items

############ Mockdata 추후에 Get hive/s3로 변경###########
@router.get("/mock-sources")
async def get_mock_sources():
    """
    Return a list of Mock Hive/S3 Tables (Source Candidates).
    Used for 'Unreal-style' source addition in Lineage UI.
    """
    return [
        {
            "name": "raw_user_logs",
            "platform": "S3",
            "schema": [
                {"name": "user_id", "type": "string"},
                {"name": "event_type", "type": "string"},
                {"name": "timestamp", "type": "timestamp"},
                {"name": "device_id", "type": "string"}
            ]
        },
        {
            "name": "raw_transactions",
            "platform": "S3",
            "schema": [
                {"name": "tx_id", "type": "string"},
                {"name": "amount", "type": "double"},
                {"name": "currency", "type": "string"},
                {"name": "user_id", "type": "string"},
                {"name": "tx_date", "type": "date"}
            ]
        },
        {
            "name": "dim_products",
            "platform": "S3",
            "schema": [
                {"name": "product_id", "type": "int"},
                {"name": "product_name", "type": "string"},
                {"name": "category", "type": "string"},
                {"name": "price", "type": "double"}
            ]
        }
    ]
############Mockdata###########

@router.get("/{id}")
async def get_dataset_detail(id: str):
    """
    Fetch full details of a dataset.
    """
    db = database.mongodb_client[database.DATABASE_NAME]
    
    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    doc = await db.datasets.find_one({"_id": obj_id})
    if not doc:
        raise HTTPException(status_code=404, detail="Dataset not found")
        
    doc["id"] = str(doc["_id"])
    del doc["_id"] # Remove ObjectId
    
    if "properties" in doc and "layer" in doc["properties"]:
        doc["layer"] = doc["properties"]["layer"]
        
    # 'schema' field in DB maps to 'columns' for frontend
    if "schema" in doc:
        doc["columns"] = doc["schema"]
        # del doc["schema"] # Optional: remove original key or keep it
    
    # Add actual file size from S3 (bytes)
    doc["size_bytes"] = doc.get("actual_size_bytes")
    
    # Add row_count
    doc["row_count"] = doc.get("row_count")
    
    # Add format from destination
    destination = doc.get("destination", {})
    doc["format"] = destination.get("format", "parquet") if destination else "parquet"
        
    return doc


@router.patch("/{id}")
async def update_dataset(id: str, update_data: DatasetUpdate):
    """
    Update business metadata (Description, Owner, Tags).
    Uses catalog_service for Dual-Write (MongoDB + Neo4j) and rollback.
    """

    
    # Delegate to service
    doc = await catalog_service.update_dataset_metadata(id, update_data)
    
    return doc

@router.delete("/{id}")
async def delete_dataset(id: str):
    """
    Delete a dataset and its lineage.
    """

    return await catalog_service.delete_dataset(id)


@router.get("/{id}/lineage")
async def get_dataset_lineage(id: str):
    """
    Fetch lineage graph for a dataset.
    """

    
    # Verify ID format
    try:
        ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # lineage_service.get_lineage is already compatible with Neo4j logic
    # using 'mongo_id' property on nodes which matches our setup
    result = await lineage_service.get_lineage(id)
    return result





@router.post("/{id}/lineage", status_code=status.HTTP_201_CREATED)
async def create_lineage(id: str, lineage_data: LineageCreate):
    """
    Create a lineage relationship: {id} -> {target_id}
    """

    return await catalog_service.add_lineage(
        id, 
        lineage_data.target_id, 
        lineage_data.type,
        lineage_data.source_col,
        lineage_data.target_col
    )

@router.delete("/{id}/lineage/{target_id}")
async def delete_lineage(
    id: str, 
    target_id: str,
    source_col: Optional[str] = Query(None, description="Source column name for column-level deletion"),
    target_col: Optional[str] = Query(None, description="Target column name for column-level deletion")
):
    """
    Remove a lineage relationship between two datasets.
    
    - For table-level: DELETE /catalog/{id}/lineage/{target_id}
    - For column-level: DELETE /catalog/{id}/lineage/{target_id}?source_col=col1&target_col=col2
    """

    result = await catalog_service.remove_lineage(id, target_id, source_col, target_col)
    return result





