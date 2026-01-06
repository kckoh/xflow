from typing import Dict, Any, Optional
from bson import ObjectId
from fastapi import HTTPException
from datetime import datetime 
import database
from schemas.catalog import DatasetUpdate, DatasetCreate

# Common DB Client
def get_db():
    return database.mongodb_client[database.DATABASE_NAME]

async def update_dataset_metadata(id: str, update_data: DatasetUpdate) -> Dict[str, Any]:
    """
    Update dataset metadata in MongoDB.
    Removed Neo4j Dual-Write logic.
    """
    
    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # Filter out None values from update data
    update_dict = {k: v for k, v in update_data.dict().items() if v is not None}
    
    if not update_dict:
        # Nothing to update, just return the document
        doc = await get_db().datasets.find_one({"_id": obj_id})
        if not doc:
             raise HTTPException(status_code=404, detail="Dataset not found")
        doc["id"] = str(doc["_id"])
        del doc["_id"]
        return doc

    # Update MongoDB
    result = await get_db().datasets.update_one(
        {"_id": obj_id},
        {"$set": update_dict}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Return updated document
    updated_doc = await get_db().datasets.find_one({"_id": obj_id})
    updated_doc["id"] = str(updated_doc["_id"])
    del updated_doc["_id"]
    
    # Normalize schema/layer like in router
    if "properties" in updated_doc and "layer" in updated_doc["properties"]:
        updated_doc["layer"] = updated_doc["properties"]["layer"]
    if "schema" in updated_doc:
        updated_doc["columns"] = updated_doc["schema"]

    return updated_doc

async def add_lineage(source_id: str, target_id: str, relationship_type: str = "FLOWS_TO", source_col: Optional[str] = None, target_col: Optional[str] = None):
    """
    Mock/Log implementation of add_lineage.
    In the new Dataset model, lineage is derived from 'inputNodeIds' and 'job_id'.
    Manual lineage additions are currently not fully supported in the new model 
    unless we add a specific 'manual_lineage' field.
    """
    print(f"ℹ️ add_lineage called: {source_id} -> {target_id}. This is currently a no-op in MongoDB migration.")
    return {"status": "success", "source": source_id, "target": target_id, "note": "Lineage is now managed via Dataset definitions."}

async def remove_lineage(source_id: str, target_id: str, source_col: str = None, target_col: str = None):
    """
    Mock implementation of remove_lineage.
    """
    print(f"ℹ️ remove_lineage called: {source_id} -> {target_id}")
    return {"status": "success"}

async def create_dataset(create_data: DatasetCreate) -> Dict[str, Any]:
    """
    Register a new dataset in the catalog (MongoDB).
    """
    
    # Check if name exists (simple unique check)
    existing = await get_db().datasets.find_one({"name": create_data.name})
    if existing:
        existing["id"] = str(existing["_id"])
        del existing["_id"]
        if "schema" not in existing:
             existing["schema"] = []
        return existing

    # Prepare MongoDB Document
    new_dataset = create_data.dict()
    
    # Initialize schema if not present
    if "schema" not in new_dataset or new_dataset["schema"] is None:
        new_dataset["schema"] = []
        
    # Set Metadata
    new_dataset["properties"] = {
        "domain": create_data.domain,
        "created_at": datetime.utcnow()
    }
    
    # Generate URN
    new_dataset["urn"] = f"urn:li:dataset:(urn:li:dataPlatform:{create_data.platform},{create_data.name},PROD)"
    new_dataset["created_at"] = datetime.utcnow()
    
    # Insert into MongoDB
    try:
        result = await get_db().datasets.insert_one(new_dataset)
        new_id = str(result.inserted_id)
        print(f"✅ Created Dataset in MongoDB: {new_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save to MongoDB: {e}")

    # Return Result
    new_dataset["id"] = new_id
    del new_dataset["_id"]
    return new_dataset

async def delete_dataset(id: str) -> Dict[str, Any]:
    """
    Delete dataset from MongoDB.
    """
    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # Delete from MongoDB
    result = await get_db().datasets.delete_one({"_id": obj_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Dataset not found")

    print(f"✅ Deleted Dataset from MongoDB: {id}")
    return {"status": "success", "id": id}
