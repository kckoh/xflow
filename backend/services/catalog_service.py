from typing import Dict, Any, Optional
from bson import ObjectId
from fastapi import HTTPException
import database
from schemas.catalog import DatasetUpdate

async def update_dataset_metadata(id: str, update_data: DatasetUpdate) -> Dict[str, Any]:
    """
    Update dataset metadata in both MongoDB and Neo4j (Dual-Write).
    Implements rollback for MongoDB if Neo4j update fails.
    """
    db = database.mongodb_client[database.DATABASE_NAME]
    
    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # Fetch original document (for rollback and verification)
    original_doc = await db.datasets.find_one({"_id": obj_id})
    if not original_doc:
        raise HTTPException(status_code=404, detail="Dataset not found")

    # Filter out None values from update data
    update_dict = {k: v for k, v in update_data.dict().items() if v is not None}
    
    if not update_dict:
        # Nothing to update, just return the original
        original_doc["id"] = str(original_doc["_id"])
        del original_doc["_id"]
        return original_doc

    # Update MongoDB (Primary Source of Truth)
    await db.datasets.update_one(
        {"_id": obj_id},
        {"$set": update_dict}
    )

    # Update Neo4j (Graph Index)
    driver = database.neo4j_driver
    if driver:
        try:
            # Map specific text fields to Neo4j properties
            # Note: We don't store everything in Neo4j, just search/display relevant props
            neo4j_props = {}
            if "description" in update_dict:
                neo4j_props["description"] = update_dict["description"]
            
            # TODO: Handle tags if needed (tags usually array, might need UNWIND or specific handling)
            # For simplicity, let's update scalar props for now. 
            # If tags are updated, we might want to manage Tag nodes, but for now let's just update property if it exists.
            
            if neo4j_props:
                query = """
                MATCH (t:Table {mongo_id: $id})
                SET t += $props
                RETURN t
                """
                with driver.session() as session:
                    session.run(query, id=id, props=neo4j_props)
                    print(f"✅ Synced update to Neo4j for dataset {id}")
            
        except Exception as e:
            print(f"❌ Neo4j Update Failed: {e}. Rolling back MongoDB...")
            
            # Rollback MongoDB
            # Restore original fields that were modified
            rollback_dict = {k: original_doc.get(k) for k in update_dict.keys()}
            await db.datasets.update_one(
                {"_id": obj_id},
                {"$set": rollback_dict}
            )
            raise HTTPException(status_code=500, detail="Failed to sync metadata to Graph DB. Changes reverted.")

    # Return updated document
    updated_doc = await db.datasets.find_one({"_id": obj_id})
    updated_doc["id"] = str(updated_doc["_id"])
    del updated_doc["_id"]
    
    # Normalize schema/layer like in router
    if "properties" in updated_doc and "layer" in updated_doc["properties"]:
        updated_doc["layer"] = updated_doc["properties"]["layer"]
    if "schema" in updated_doc:
        updated_doc["columns"] = updated_doc["schema"]

    return updated_doc

async def add_lineage(source_id: str, target_id: str, relationship_type: str = "FLOWS_TO"):
    """
    Create a relationship between two datasets in Neo4j.
    Default is 'FLOWS_TO' (source -> target).
    """
    driver = database.neo4j_driver
    if not driver:
        raise HTTPException(status_code=503, detail="Graph DB is not available")

    query = """
    MATCH (s:Table {mongo_id: $source_id})
    MATCH (t:Table {mongo_id: $target_id})
    MERGE (s)-[r:FLOWS_TO]->(t)
    RETURN r
    """
    
    # specialized relationship types can be added here if needed
    
    try:
        with driver.session() as session:
            result = session.run(query, source_id=source_id, target_id=target_id)
            record = result.single()
            if not record:
                # Could mean nodes don't exist
                raise HTTPException(status_code=404, detail="One or both datasets not found in Graph")
            print(f"✅ Created lineage: {source_id} -> {target_id}")
            return {"status": "success", "source": source_id, "target": target_id}
            
    except Exception as e:
        print(f"❌ Failed to add lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def remove_lineage(source_id: str, target_id: str):
    """
    Remove the relationship between two datasets in Neo4j.
    """
    driver = database.neo4j_driver
    if not driver:
        raise HTTPException(status_code=503, detail="Graph DB is not available")

    query = """
    MATCH (s:Table {mongo_id: $source_id})-[r:FLOWS_TO]->(t:Table {mongo_id: $target_id})
    DELETE r
    RETURN count(r) as deleted_count
    """
    
    try:
        with driver.session() as session:
            result = session.run(query, source_id=source_id, target_id=target_id)
            record = result.single()
            if record["deleted_count"] == 0:
                 raise HTTPException(status_code=404, detail="Relationship not found")
            
            print(f"✅ Removed lineage: {source_id} -> {target_id}")
            return {"status": "success", "deleted_count": record["deleted_count"]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Failed to remove lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))
