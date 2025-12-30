import asyncio
import os
import json
from motor.motor_asyncio import AsyncIOMotorClient
from bson import ObjectId
from datetime import datetime

class JSONEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return str(o)

async def main():
    # Helper to print latest dataset
    url = os.getenv("MONGODB_URL", "mongodb://mongo:mongo@mongodb:27017")
    print(f"Connecting to {url}...")
    client = AsyncIOMotorClient(url)
    db = client["mydb"] # backend uses 'mydb' by default
    
    # print("Dropping datasets collection...")
    # await db.datasets.drop()
    # print("✅ Datasets collection dropped.")
    
    print("Fetching latest dataset...")
    doc = await db.datasets.find_one(sort=[('_id', -1)])
    
    if doc:
        print("\n=== LATEST DATASET DOCUMENT ===\n")
        print(json.dumps(doc, indent=2, cls=JSONEncoder))
        print("\n===============================\n")
    else:
        print("No datasets found.")

    print("\nFetching latest ETLJob...")
    job = await db.etl_jobs.find_one(sort=[('_id', -1)])
    if job:
        print("\n=== LATEST ETLJOB DOCUMENT ===\n")
        print(json.dumps(job, indent=2, cls=JSONEncoder))
        print("\n==============================\n")
    else:
        print("No ETL Jobs found.")

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())
