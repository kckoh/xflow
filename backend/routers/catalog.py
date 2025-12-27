from fastapi import APIRouter, HTTPException, Query, Body
from typing import List, Optional
from bson import ObjectId
import database
from schemas.catalog import CatalogItem, DatasetDetail, DatasetUpdate

router = APIRouter()


@router.get("")
async def get_catalog(
    type: Optional[str] = Query(None, description="Filter by layer (e.g. RAW, MART)"),
    platform: Optional[str] = Query(None, description="Filter by platform"),
    search: Optional[str] = Query(None, description="Search by name or description")
):
    """
    Fetch list of datasets with optional filtering.
    """
    db = database.mongodb_client[database.DATABASE_NAME]
    
    # Build Search/Filter Query
    query = {}
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

    cursor = db.datasets.find(query).limit(100)
    items = []
    async for doc in cursor:
        # Map properties.layer to top-level layer field for response
        doc["id"] = str(doc["_id"])
        del doc["_id"] # Remove ObjectId to make it JSON serializable
        
        if "properties" in doc and "layer" in doc["properties"]:
            doc["layer"] = doc["properties"]["layer"]
            
        # Map schema -> columns if needed for list view (though usually not needed for list)
        if "schema" in doc:
             doc["columns"] = doc["schema"]
             
        items.append(doc)
        
    return items


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
        
    return doc


@router.patch("/{id}")
async def update_dataset(id: str, update_data: DatasetUpdate):
    """
    Update business metadata (Description, Owner, Tags).
    """
    db = database.mongodb_client[database.DATABASE_NAME]
    
    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # Filter out None values
    update_dict = {k: v for k, v in update_data.dict().items() if v is not None}
    
    if not update_dict:
        raise HTTPException(status_code=400, detail="No valid fields to update")

    result = await db.datasets.update_one(
        {"_id": obj_id},
        {"$set": update_dict}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Dataset not found")
        
    # Return updated document
    doc = await db.datasets.find_one({"_id": obj_id})
    doc["id"] = str(doc["_id"])
    del doc["_id"]
    
    if "properties" in doc and "layer" in doc["properties"]:
        doc["layer"] = doc["properties"]["layer"]
        
    if "schema" in doc:
        doc["columns"] = doc["schema"]
        
    return doc


@router.get("/{id}/lineage")
async def get_dataset_lineage(id: str):
    """
    Fetch lineage graph for a dataset.
    """
    from services import lineage_service
    
    # Verify ID format
    try:
        ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # lineage_service.get_lineage is already compatible with Neo4j logic
    # using 'mongo_id' property on nodes which matches our setup
    result = await lineage_service.get_lineage(id)
    return result
