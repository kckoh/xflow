from typing import Dict, Any, Optional
from bson import ObjectId
from fastapi import HTTPException
from datetime import datetime 
import database
from schemas.catalog import DatasetUpdate, DatasetCreate


# Common DB Client
db = database.mongodb_client[database.DATABASE_NAME]



async def update_dataset_metadata(id: str, update_data: DatasetUpdate) -> Dict[str, Any]:
    """
    Update dataset metadata in both MongoDB and Neo4j (Dual-Write).
    Implements rollback for MongoDB if Neo4j update fails.
    """

    
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
            
            with driver.session() as session:
                if neo4j_props:
                    query = """
                    MATCH (t:Table {mongo_id: $id})
                    SET t += $props
                    RETURN t
                    """
                    session.run(query, id=id, props=neo4j_props)
                
                # Sync Schema Changes if present
                if "schema" in update_dict:
                    columns_data = []
                    for col in update_dict["schema"]:
                        # col might be dict or object depending on Pydantic
                        # If dict:
                        if isinstance(col, dict):
                             columns_data.append({
                                "name": col.get("name"),
                                "type": col.get("type", "string"),
                            })
                        else:
                             columns_data.append({
                                "name": col.name,
                                "type": col.type,
                            })

                    schema_query = """
                    MATCH (t:Table {mongo_id: $id})
                    WITH t
                    UNWIND $columns as col
                    MERGE (c:Column {id: $id + '_' + col.name})
                    SET c.name = col.name,
                        c.type = col.type
                    MERGE (t)-[:HAS_COLUMN]->(c)
                    """
                    session.run(schema_query, id=id, columns=columns_data)
                    print(f"✅ Synced schema to Neo4j for dataset {id}")

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

async def add_lineage(source_id: str, target_id: str, relationship_type: str = "FLOWS_TO", source_col: Optional[str] = None, target_col: Optional[str] = None):
    """
    Create a relationship between two datasets (or their columns) in Neo4j.
    If columns are specified, links Column nodes.
    Otherwise, links Table nodes (Fallback).
    """
    driver = database.neo4j_driver
    if not driver:
        raise HTTPException(status_code=503, detail="Graph DB is not available")

    # Column Level Lineage
    if source_col and target_col:
        query = """
        MATCH (s:Table {mongo_id: $source_id})-[:HAS_COLUMN]->(sc:Column {name: $source_col})
        MATCH (t:Table {mongo_id: $target_id})-[:HAS_COLUMN]->(tc:Column {name: $target_col})
        MERGE (sc)-[r:FLOWS_TO]->(tc)
        RETURN r
        """
        params = {
            "source_id": source_id, "target_id": target_id,
            "source_col": source_col, "target_col": target_col
        }
    else:
        # Table Level Lineage (Legacy/Fallback)
        query = """
        MATCH (s:Table {mongo_id: $source_id})
        MATCH (t:Table {mongo_id: $target_id})
        MERGE (s)-[r:FLOWS_TO]->(t)
        RETURN r
        """
        params = {"source_id": source_id, "target_id": target_id}

    
    # specialized relationship types can be added here if needed
    
    try:
        with driver.session() as session:
            result = session.run(query, **params)
            record = result.single()
            if not record:
                # Could mean nodes don't exist
                raise HTTPException(status_code=404, detail="One or both datasets not found in Graph")
            print(f"✅ Created lineage: {source_id} -> {target_id}")

            # ---------------------------------------------------------
            # Schema Inheritance Logic (MongoDB)
            # Only copy the CONNECTED column, not all columns
            # ---------------------------------------------------------
            try:

                s_obj_id = ObjectId(source_id)
                t_obj_id = ObjectId(target_id)

                # Fetch Target
                target_doc = await db.datasets.find_one({"_id": t_obj_id})
                
                if target_doc:
                    target_schema = target_doc.get("schema", [])
                    existing_names = {c.get("name") for c in target_schema if "name" in c}
                    
                    # Fetch Source
                    source_doc = await db.datasets.find_one({"_id": s_obj_id})
                    
                    if source_doc and "schema" in source_doc and source_doc["schema"]:
                        source_schema = source_doc["schema"]
                        
                        columns_to_add = []
                        
                        # Column-level lineage: Only copy the connected column
                        if source_col and target_col:
                            # Check if target column already exists
                            if target_col not in existing_names:
                                # Find the source column definition
                                source_col_def = next(
                                    (col for col in source_schema if col.get("name") == source_col),
                                    None
                                )
                                
                                if source_col_def:
                                    # Create new column with target name but source properties
                                    new_col = {
                                        "name": target_col,
                                        "type": source_col_def.get("type", "string"),
                                        "description": f"Inherited from {source_doc.get('name', 'source')}.{source_col}",
                                        "is_partition": False,
                                        "tags": []
                                    }
                                    columns_to_add.append(new_col)
                        
                        # Table-level lineage: Copy all missing columns (legacy behavior)
                        else:
                            for col in source_schema:
                                if col.get("name") not in existing_names:
                                    columns_to_add.append(col)
                                    existing_names.add(col.get("name"))
                        
                        if columns_to_add:
                            # Merge and Update
                            target_schema.extend(columns_to_add)
                            
                            await db.datasets.update_one(
                                {"_id": t_obj_id},
                                {"$set": {"schema": target_schema}}
                            )
                            print(f"✅ Schema Merge: Added {len(columns_to_add)} column(s) to Target.")
                        else:
                            print("ℹ️ Schema Merge: Column already exists in target.")
            except Exception as e:
                print(f"⚠️ Schema Merge Failed (Non-critical): {e}")
            # ---------------------------------------------------------

            return {"status": "success", "source": source_id, "target": target_id}
            
    except Exception as e:
        print(f"❌ Failed to add lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def remove_lineage(source_id: str, target_id: str, source_col: str = None, target_col: str = None):
    """
    Remove lineage relationship and clean up orphaned columns in MongoDB.
    
    Args:
        source_id: Source dataset MongoDB ID
        target_id: Target dataset MongoDB ID
        source_col: Source column name (optional, for column-level lineage)
        target_col: Target column name (optional, for column-level lineage)
    """
    driver = database.neo4j_driver
    if not driver:
        raise HTTPException(status_code=503, detail="Graph DB is not available")

    try:
        with driver.session() as session:
            # Column-level lineage deletion
            if source_col and target_col:
                query = """
                MATCH (s:Table {mongo_id: $source_id})-[:HAS_COLUMN]->(sc:Column {name: $source_col})
                MATCH (t:Table {mongo_id: $target_id})-[:HAS_COLUMN]->(tc:Column {name: $target_col})
                MATCH (sc)-[r:FLOWS_TO]->(tc)
                DELETE r
                RETURN sc, tc
                """
                result = session.run(query, 
                    source_id=source_id, 
                    target_id=target_id,
                    source_col=source_col, 
                    target_col=target_col
                )
                record = result.single()
                
                if not record:
                    raise HTTPException(status_code=404, detail="Column relationship not found")
                
                print(f"✅ Removed column lineage: {source_id}.{source_col} -> {target_id}.{target_col}")
                return {"status": "success", "type": "column", "orphaned": False}
                

            
            # Table-level lineage deletion (legacy)
            else:
                query = """
                MATCH (s:Table {mongo_id: $source_id})-[r:FLOWS_TO]->(t:Table {mongo_id: $target_id})
                DELETE r
                RETURN count(r) as deleted_count
                """
                
                result = session.run(query, source_id=source_id, target_id=target_id)
                record = result.single()
                
                if record["deleted_count"] == 0:
                    raise HTTPException(status_code=404, detail="Table relationship not found")
                
                print(f"✅ Removed table lineage: {source_id} -> {target_id}")
                return {"status": "success", "type": "table", "deleted_count": record["deleted_count"]}

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Failed to remove lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def create_dataset(create_data: DatasetCreate) -> Dict[str, Any]:
    """
    Create a new dataset in MongoDB and Neo4j.
    """

    
    # Check if name exists (simple unique check)
    existing = await db.datasets.find_one({"name": create_data.name})
    if existing:
        # Get-or-Create behavior: Return existing if found
        existing["id"] = str(existing["_id"])
        del existing["_id"]
        # Ensure schema is present in return
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
    # tags are already in create_data.dict()
    
    # Insert into MongoDB
    try:
        result = await db.datasets.insert_one(new_dataset)
        new_id = str(result.inserted_id)
        print(f"✅ Created Dataset in MongoDB: {new_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save to MongoDB: {e}")

    driver = database.neo4j_driver
    if driver:
        try:
            # Prepare columns for Cypher UNWIND
            columns_data = []
            if create_data.schema:
                for col in create_data.schema:
                    columns_data.append({
                        "name": col.name,
                        "type": col.type,
                        "description": col.description or ""
                    })

            query = """
            MERGE (t:Table {mongo_id: $id})
            SET t.name = $name,
                t.platform = $platform,
                t.domain = $domain,
                t.description = $description,
                t.urn = $urn
            
            WITH t
            UNWIND $columns as col
            MERGE (c:Column {id: $id + '_' + col.name})
            SET c.name = col.name,
                c.type = col.type
            MERGE (t)-[:HAS_COLUMN]->(c)
            """
            with driver.session() as session:
                session.run(query, 
                            id=new_id, 
                            name=create_data.name, 
                            platform=create_data.platform, 
                            domain=create_data.domain,
                            description=create_data.description or "",
                            urn=new_dataset["urn"],
                            columns=columns_data)
                print(f"✅ Created Neo4j node for dataset {new_id} with {len(columns_data)} columns")
        except Exception as e:
            print(f"❌ Neo4j Creation Failed: {e}. Rolling back MongoDB...")
            await db.datasets.delete_one({"_id": result.inserted_id})
            raise HTTPException(status_code=500, detail=f"Failed to create graph node: {e}")

    # Return Result
    new_dataset["id"] = new_id
    del new_dataset["_id"]
    return new_dataset



async def delete_dataset(id: str) -> Dict[str, Any]:
    """
    Delete a dataset from MongoDB and Neo4j.
    """

    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # 1. Delete from Neo4j (Relationships will be deleted automatically if we delete the node, 
    # but strictly speaking we should detach delete)
    driver = database.neo4j_driver
    if driver:
        try:
            query = """
            MATCH (t:Table {mongo_id: $id})
            OPTIONAL MATCH (t)-[:HAS_COLUMN]->(c:Column)
            DETACH DELETE t, c
            RETURN count(t) as deleted_count
            """
            with driver.session() as session:
                result = session.run(query, id=id)
                record = result.single()
                print(f"✅ Deleted Neo4j node for dataset {id}. Count: {record['deleted_count']}")
        except Exception as e:
            # We log but continue to delete from Mongo - logical deletion priority
            print(f"⚠️ Neo4j Deletion Warning: {e}")

    # 2. Delete from MongoDB
    result = await db.datasets.delete_one({"_id": obj_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Dataset not found")

    print(f"✅ Deleted Dataset from MongoDB: {id}")
    return {"status": "success", "id": id}
