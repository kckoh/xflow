import sys
import os
import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
from neomodel import config, db as neomodel_db, install_all_labels

# Add backend root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from models import Table, Column
import database

async def sync_all():
    print("🚀 Starting Full Sync from MongoDB to Neo4j...")
    
    # 1. Setup Connections from Environment
    # Note: When running inside Docker, these are set.
    # We fallback to database.py defaults if missing, but careful with hostnames.
    
    mongo_url = os.getenv("MONGODB_URL", database.MONGODB_URL)
    neo4j_uri = os.getenv("NEO4J_URI", database.NEO4J_URI)
    neo4j_user = os.getenv("NEO4J_USER", database.NEO4J_USER)
    neo4j_pass = os.getenv("NEO4J_PASSWORD", database.NEO4J_PASSWORD)

    # Clean URI for Neomodel
    config.DATABASE_URL = f"bolt://{neo4j_user}:{neo4j_pass}@{neo4j_uri.replace('bolt://', '')}"
    print(f"🔌 Neo4j Config: {config.DATABASE_URL}")
    
    try:
        install_all_labels()
        print("✅ Labels Installed")
    except Exception as e:
        print(f"⚠️ Label Install Warning: {e}")

    # Connect Mongo
    mongo_client = AsyncIOMotorClient(mongo_url)
    db = mongo_client[database.DATABASE_NAME]
    
    # 2. Fetch Datasets
    cursor = db.datasets.find({})
    datasets = await cursor.to_list(length=None)
    print(f"📊 Found {len(datasets)} datasets in MongoDB.")
    
    count = 0
    for doc in datasets:
        try:
            mongo_id = str(doc["_id"])
            name = doc.get("name")
            print(f"🔄 Syncing [{count+1}/{len(datasets)}] {name} ({mongo_id})...")
            
            # --- Sync Table Node ---
            table = Table.nodes.get_or_none(mongo_id=mongo_id)
            if not table:
                table = Table(
                    mongo_id=mongo_id,
                    name=name,
                    platform=doc.get("platform", "hive"),
                    urn=doc.get("urn", ""),
                    domain=doc.get("properties", {}).get("domain", ""),
                    description=doc.get("description", "")
                ).save()
                print(f"   ➕ Created Table Node")
            else:
                table.name = name
                # Update other fields if needed
                table.save()
                print(f"   ✔️ Updated Table Node")

            # --- Sync Columns ---
            if "schema" in doc and doc["schema"]:
                for col_def in doc["schema"]:
                    c_name = col_def.get("name")
                    if not c_name: continue
                    
                    c_type = col_def.get("type", "string")
                    c_desc = col_def.get("description", "")
                    c_uid = f"{mongo_id}_{c_name}"
                    
                    col = Column.nodes.get_or_none(uid=c_uid)
                    if not col:
                        col = Column(uid=c_uid, name=c_name, type=c_type, description=c_desc).save()
                    else:
                        col.name = c_name
                        col.type = c_type
                        col.save()
                    
                    # Ensure Relationship
                    if not table.has_columns.is_connected(col):
                        table.has_columns.connect(col)
                        
                print(f"   ✅ Synced {len(doc['schema'])} columns")
            
            count += 1
            
        except Exception as e:
            print(f"❌ Error syncing {doc.get('name')}: {e}")

    print("------------------------------------------------")
    print("✅ Full Synchronization Complete.")

if __name__ == "__main__":
    asyncio.run(sync_all())
