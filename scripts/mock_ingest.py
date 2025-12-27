import os
import asyncio
import random
from datetime import datetime
from typing import List, Dict

# Requirements: pip install motor neo4j faker
from faker import Faker
from motor.motor_asyncio import AsyncIOMotorClient
from neo4j import GraphDatabase

# --- Configuration ---
MONGODB_URL = os.getenv("MONGODB_URL", "mongodb://mongo:mongo@localhost:27017")
DATABASE_NAME = os.getenv("MONGODB_DATABASE", "mydb")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")

fake = Faker()

# --- Defines ---
DATA_TYPES = ["STRING", "INT", "BIGINT", "FLOAT", "BOOLEAN", "TIMESTAMP", "DATE", "ARRAY<STRING>", "STRUCT<a:INT,b:STRING>"]
LAYERS = ["RAW", "STAGING", "MART", "SERVING"]
DOMAINS = ["Sales", "Marketing", "Finance", "Logistics", "HR"]

class MockIngester:
    def __init__(self):
        self.mongo_client = AsyncIOMotorClient(MONGODB_URL)
        self.db = self.mongo_client[DATABASE_NAME]
        self.neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

    async def clean_slate(self):
        """Delete all data from MongoDB and Neo4j."""
        print("🧹 Cleaning existing data...")
        
        # 1. MongoDB Clean
        await self.db["datasets"].delete_many({})
        print("   - MongoDB: 'datasets' collection cleared.")

        # 2. Neo4j Clean
        with self.neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
        print("   - Neo4j: All nodes and relationships deleted.")

    def generate_schema(self) -> List[Dict]:
        """Generate random columns."""
        num_cols = random.randint(3, 15)
        columns = []
        for _ in range(num_cols):
            col_name = fake.word().lower()
            if random.random() < 0.3:
                col_name += "_id"
            
            columns.append({
                "name": col_name,
                "type": random.choice(DATA_TYPES),
                "description": fake.sentence(nb_words=5),
                "is_partition": random.choice([True, False]) if random.random() < 0.1 else False
            })
        return columns

    async def ingest_mock_data(self, num_datasets=50):
        print(f"🚀 Generating {num_datasets} mock datasets...")
        
        datasets = []
        # Create Datasets
        for _ in range(num_datasets):
            layer = random.choice(LAYERS)
            domain = random.choice(DOMAINS)
            table_name = f"{layer.lower()}_{domain.lower()}_{fake.word()}"
            
            dataset = {
                "name": table_name,
                "urn": f"urn:li:dataset:(hive,{table_name},PROD)",
                "platform": "postgresql",
                "description": fake.catch_phrase(),
                "schema": self.generate_schema(),
                "owner": fake.name(), # Governance
                "tags": [fake.word() for _ in range(random.randint(0, 3))], # Governance
                "properties": {
                    "layer": layer,
                    "domain": domain,
                    "created_at": datetime.now().isoformat(),
                    "updated_at": datetime.now().isoformat(), # Added modification time
                    "retention_days": random.choice([30, 90, 365])
                }
            }
            datasets.append(dataset)

        # 1. Load to MongoDB
        if datasets:
            result = await self.db["datasets"].insert_many(datasets)
            inserted_ids = result.inserted_ids
            print(f"   - MongoDB: Inserted {len(inserted_ids)} documents.")
            
            # Map Mongo ID back to dataset object for Neo4j linking
            for i, ds in enumerate(datasets):
                ds["_id"] = str(inserted_ids[i])

        # 2. Load to Neo4j (Nodes + Lineage)
        with self.neo4j_driver.session() as session:
            # Create Nodes
            for ds in datasets:
                query = """
                MERGE (t:Table {urn: $urn})
                SET t.name = $name,
                    t.mongo_id = $mongo_id,
                    t.layer = $layer,
                    t.domain = $domain
                """
                session.run(query, 
                            urn=ds["urn"], 
                            name=ds["name"], 
                            mongo_id=ds["_id"],
                            layer=ds["properties"]["layer"],
                            domain=ds["properties"]["domain"])
            
            print("   - Neo4j: Nodes created.")

            # Create Random Lineage (Relationships)
            # Strategy: Connect lower layers to upper layers
            # RAW -> STAGING -> MART -> SERVING
            layer_order = {"RAW": 0, "STAGING": 1, "MART": 2, "SERVING": 3}
            
            count_edges = 0
            for source in datasets:
                source_layer_idx = layer_order.get(source["properties"]["layer"], 0)
                
                # Chance to flow to next layer
                if source_layer_idx < 3 and random.random() > 0.3:
                    # Find potential targets in next layers
                    potential_targets = [
                        d for d in datasets 
                        if layer_order.get(d["properties"]["layer"], 0) > source_layer_idx
                    ]
                    
                    if potential_targets:
                        # Pick 1-2 targets
                        targets = random.sample(potential_targets, k=random.randint(1, min(2, len(potential_targets))))
                        
                        for target in targets:
                            query = """
                            MATCH (s:Table {urn: $source_urn})
                            MATCH (t:Table {urn: $target_urn})
                            MERGE (s)-[:FLOWS_TO]->(t)
                            """
                            session.run(query, source_urn=source["urn"], target_urn=target["urn"])
                            count_edges += 1

            print(f"   - Neo4j: {count_edges} lineage relationships created.")

    def close(self):
        self.neo4j_driver.close()
        self.mongo_client.close()

if __name__ == "__main__":
    ingester = MockIngester()
    
    loop = asyncio.get_event_loop()
    try:
        # Run clean slate
        loop.run_until_complete(ingester.clean_slate())
        # Run ingestion
        loop.run_until_complete(ingester.ingest_mock_data(num_datasets=50))
    finally:
        ingester.close()
        print("✨ Done!")
