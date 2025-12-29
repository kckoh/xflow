from typing import Dict, Any, Optional
from bson import ObjectId
from fastapi import HTTPException
from datetime import datetime 
import database
from schemas.catalog import DatasetUpdate, DatasetCreate
from models import Table, Column
from neomodel import db as neomodel_db

# Common DB Client
# db = database.mongodb_client[database.DATABASE_NAME]  <-- This was causing import error because database.mongodb_client is None at startup
def get_db():
    return database.mongodb_client[database.DATABASE_NAME]

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
    original_doc = await get_db().datasets.find_one({"_id": obj_id})
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
    await get_db().datasets.update_one(
        {"_id": obj_id},
        {"$set": update_dict}
    )

    # Update Neo4j (OGM)
    try:
        table_node = Table.nodes.get_or_none(mongo_id=id)
        if table_node:
            if update_data.description is not None:
                table_node.description = update_data.description
            
            # Update other fields if they exist in the update
            # (Note: Pydantic model DatasetUpdate might not have all Table fields)
            
            table_node.save()

            # Sync Schema
            if update_data.schema:
                 with neomodel_db.transaction:
                    for col_def in update_data.schema:
                        col_name = col_def.name if not isinstance(col_def, dict) else col_def.get("name")
                        col_type = col_def.type if not isinstance(col_def, dict) else col_def.get("type", "string")
                        
                        # Define ID for Column
                        col_uid = f"{id}_{col_name}"
                        
                        # Merge Column Node
                        col_node = Column.nodes.get_or_none(uid=col_uid)
                        if not col_node:
                            col_node = Column(uid=col_uid, name=col_name, type=col_type).save()
                        else:
                            col_node.name = col_name # update props
                            col_node.type = col_type
                            col_node.save()
                        
                        # Ensure Relationship
                        if not table_node.has_columns.is_connected(col_node):
                            table_node.has_columns.connect(col_node)

            print(f"✅ Synced update to Neo4j (OGM) for dataset {id}")

    except Exception as e:
        print(f"❌ Neo4j Update Failed: {e}. Rolling back MongoDB...")
        
        # Rollback MongoDB
        # Restore original fields that were modified
        rollback_dict = {k: original_doc.get(k) for k in update_dict.keys()}
        await get_db().datasets.update_one(
            {"_id": obj_id},
            {"$set": rollback_dict}
        )
        raise HTTPException(status_code=500, detail="Failed to sync metadata to Graph DB. Changes reverted.")

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
    OGM Implementation of add_lineage
    """
    try:
        # Find Source and Target Tables
        source_table = Table.nodes.get_or_none(mongo_id=source_id)
        target_table = Table.nodes.get_or_none(mongo_id=target_id)
        
        if not source_table or not target_table:
             raise HTTPException(status_code=404, detail="One or both datasets not found in Graph")

        if source_col and target_col:
            # Column Lineage
            s_col_uid = f"{source_id}_{source_col}"
            t_col_uid = f"{target_id}_{target_col}"
            
            s_col = Column.nodes.get_or_none(uid=s_col_uid)
            t_col = Column.nodes.get_or_none(uid=t_col_uid)
            
            # 1. Sync Source if missing
            if not s_col:
                print(f"ℹ️ Source Column {s_col_uid} missing in Graph. Checking MongoDB...")
                # Fetch confirm existence in Mongo
                s_obj_id = ObjectId(source_id)
                source_doc = await get_db().datasets.find_one({"_id": s_obj_id})
                
                if source_doc and "schema" in source_doc:
                     col_def = next((c for c in source_doc["schema"] if c.get("name") == source_col), None)
                     if col_def:
                         print(f"✅ Found column in MongoDB. Syncing to Graph...")
                         # Ensure Table exists
                         if not source_table:
                             source_table = Table(
                                 mongo_id=source_id,
                                 name=source_doc.get("name"),
                                 platform=source_doc.get("platform", "hive"),
                                 urn=source_doc.get("urn", ""),
                                 description=source_doc.get("description", "")
                             ).save()
                         
                         # Create Column
                         s_col = Column(
                             uid=s_col_uid,
                             name=source_col,
                             type=col_def.get("type", "string"),
                             description=col_def.get("description", "")
                         ).save()
                         
                         source_table.has_columns.connect(s_col)
                     else:
                         raise HTTPException(status_code=404, detail=f"Source Column {source_col} not found in Dataset Schema")
                else:
                    raise HTTPException(status_code=404, detail="Source Dataset not found")

            # 2. Auto-create Target Column if missing (Schema Inheritance)
            if not t_col:
                print(f"ℹ️ Target Column {t_col_uid} not found. Creating it...")
                t_col = Column(
                    uid=t_col_uid, 
                    name=target_col, 
                    type=s_col.type, # Inherit type from source
                    description=f"Inherited from {source_col}"
                ).save()
                
                # Ensure Target Table connect to this new column
                if not target_table:
                     # Fetch target doc to allow lazy creation of target table too?
                     # For now assuming target table might exist or we just need the column node for logic,
                     # but we should connect it to a table.
                     # Let's try to fetch target doc too if table missing.
                     t_obj_id = ObjectId(target_id)
                     target_doc = await get_db().datasets.find_one({"_id": t_obj_id})
                     if target_doc:
                         target_table = Table(
                                 mongo_id=target_id,
                                 name=target_doc.get("name"),
                                 platform=target_doc.get("platform", "hive"),
                                 urn=target_doc.get("urn", ""),
                                 description=target_doc.get("description", "")
                             ).save()

                if target_table:
                    target_table.has_columns.connect(t_col)

            # Check link
            if not s_col.flows_to.is_connected(t_col):
                s_col.flows_to.connect(t_col)
                print(f"✅ Created Column Lineage (OGM): {source_col} -> {target_col}")
            else:
                print(f"ℹ️ Column Lineage already exists: {source_col} -> {target_col}")
            
            # (End OGM Logic, continue to Mongo Schema Merge below)

        else:
            # Table Lineage
            print(f"🔗 Attempting Table Lineage: {source_table.name} -> {target_table.name}")
            if not source_table.flows_to.is_connected(target_table):
                source_table.flows_to.connect(target_table)
                print(f"✅ Created Table Lineage (OGM): {source_id} -> {target_id}")
            else:
                print(f"ℹ️ Table Lineage already exists: {source_id} -> {target_id}")

        # ---------------------------------------------------------
        # Schema Inheritance Logic (MongoDB)
        # Only copy the CONNECTED column, not all columns
        # ---------------------------------------------------------
        try:
            s_obj_id = ObjectId(source_id)
            t_obj_id = ObjectId(target_id)

            # Fetch Target
            target_doc = await get_db().datasets.find_one({"_id": t_obj_id})
            
            if target_doc:
                target_schema = target_doc.get("schema", [])
                existing_names = {c.get("name") for c in target_schema if "name" in c}
                
                # Fetch Source
                source_doc = await get_db().datasets.find_one({"_id": s_obj_id})
                
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
                        
                        await get_db().datasets.update_one(
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
    OGM Implementation of remove_lineage
    """
    try:
        if source_col and target_col:
            s_col_uid = f"{source_id}_{source_col}"
            t_col_uid = f"{target_id}_{target_col}"
            
            s_col = Column.nodes.get_or_none(uid=s_col_uid)
            t_col = Column.nodes.get_or_none(uid=t_col_uid)
            
            if s_col and t_col and s_col.flows_to.is_connected(t_col):
                s_col.flows_to.disconnect(t_col)
                print(f"✅ Removed Column Lineage (OGM)")
                return {"status": "success", "type": "column", "orphaned": False}
            else:
                 raise HTTPException(status_code=404, detail="Column relationship not found")
        else:
            source_table = Table.nodes.get_or_none(mongo_id=source_id)
            target_table = Table.nodes.get_or_none(mongo_id=target_id)
            
            if source_table and target_table and source_table.flows_to.is_connected(target_table):
                source_table.flows_to.disconnect(target_table)
                print(f"✅ Removed Table Lineage (OGM)")
                return {"status": "success", "type": "table"}
            else:
                raise HTTPException(status_code=404, detail="Table relationship not found")

    except Exception as e:
        print(f"❌ Failed to remove lineage: {e}")
        raise HTTPException(status_code=500, detail=str(e))

async def create_dataset(create_data: DatasetCreate) -> Dict[str, Any]:
    """
    OGM Implementation of create_dataset
    """
    
    # Check if name exists (simple unique check)
    existing = await get_db().datasets.find_one({"name": create_data.name})
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
    
    # Insert into MongoDB
    try:
        result = await get_db().datasets.insert_one(new_dataset)
        new_id = str(result.inserted_id)
        print(f"✅ Created Dataset in MongoDB: {new_id}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to save to MongoDB: {e}")

    # Neo4j Creation via OGM
    try:
        # Create Table Node
        table_params = {
            "mongo_id": new_id,
            "name": create_data.name,
            "platform": create_data.platform,
            "domain": create_data.domain or "General",
            "description": create_data.description or "",
            "urn": new_dataset["urn"]
        }
        
        table = Table(**table_params).save()
        
        # Create Columns
        if create_data.schema:
            for col in create_data.schema:
                col_uid = f"{new_id}_{col.name}"
                col_node = Column(
                    uid=col_uid,
                    name=col.name,
                    type=col.type,
                    description=col.description or ""
                ).save()
                
                table.has_columns.connect(col_node)
        
        print(f"✅ Created Neo4j node (OGM) for {new_id}")

    except Exception as e:
        print(f"❌ Neo4j Creation Failed: {e}. Rolling back MongoDB...")
        await get_db().datasets.delete_one({"_id": result.inserted_id})
        raise HTTPException(status_code=500, detail=f"Failed to create graph node: {e}")

    # Return Result
    new_dataset["id"] = new_id
    del new_dataset["_id"]
    return new_dataset

async def delete_dataset(id: str) -> Dict[str, Any]:
    """
    OGM Implementation of delete_dataset
    """
    try:
        obj_id = ObjectId(id)
    except:
        raise HTTPException(status_code=400, detail="Invalid ID format")

    # 1. Delete from Neo4j OGM
    try:
        table_node = Table.nodes.get_or_none(mongo_id=id)
        if table_node:
             # Delete connected columns to prevent orphans 
             for col in table_node.has_columns:
                 col.delete()
             
             table_node.delete()
             print(f"✅ Deleted Neo4j node (OGM) for {id}")
    except Exception as e:
        print(f"⚠️ Neo4j Deletion Warning: {e}")

    # 2. Delete from MongoDB
    result = await get_db().datasets.delete_one({"_id": obj_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Dataset not found")

    print(f"✅ Deleted Dataset from MongoDB: {id}")
    return {"status": "success", "id": id}
