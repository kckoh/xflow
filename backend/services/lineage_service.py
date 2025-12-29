from typing import List, Dict, Any
from bson import ObjectId
import database
from models import Table, Column
from neomodel import db as neomodel_db

def get_db():
    return database.mongodb_client[database.DATABASE_NAME]

async def get_lineage(dataset_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch lineage data (Column Level) from Neo4j OGM.
    Returns nodes (Tables) and edges (Column-to-Column links).
    """
    
    driver = database.neo4j_driver
    if not driver:
        return {"nodes": [], "edges": []}

    try:
        with driver.session() as session:
            # Get center node
            center_query = "MATCH (center:Table {mongo_id: $id}) RETURN center"
            center_result = session.run(center_query, id=dataset_id)
            center_record = center_result.single()
            
            if not center_record:
                return {"nodes": [], "edges": []}
            
            center_node = center_record["center"]
            all_nodes = [center_node]
            raw_edges = []
            
            # Get upstream relationships
            upstream_query = """
            MATCH (upstream:Table)-[:HAS_COLUMN]->(uc:Column)-[r:FLOWS_TO]->(cc:Column)<-[:HAS_COLUMN]-(center:Table {mongo_id: $id})
            RETURN upstream, r, uc.name as source_col, cc.name as target_col, center
            """
            upstream_result = session.run(upstream_query, id=dataset_id)
            for record in upstream_result:
                upstream_node = record["upstream"]
                rel = record["r"]
                source_col = record["source_col"]
                target_col = record["target_col"]
                target_node = record["center"]
                
                all_nodes.append(upstream_node)
                raw_edges.append({
                    "rel": rel,
                    "source": upstream_node,
                    "target": target_node,
                    "source_col": source_col,
                    "target_col": target_col
                })
            
            # Get downstream relationships
            downstream_query = """
            MATCH (center:Table {mongo_id: $id})-[:HAS_COLUMN]->(cc:Column)-[r:FLOWS_TO]->(dc:Column)<-[:HAS_COLUMN]-(downstream:Table)
            RETURN downstream, r, cc.name as source_col, dc.name as target_col, center
            """
            downstream_result = session.run(downstream_query, id=dataset_id)
            for record in downstream_result:
                downstream_node = record["downstream"]
                rel = record["r"]
                source_col = record["source_col"]
                target_col = record["target_col"]
                source_node = record["center"]
                
                all_nodes.append(downstream_node)
                raw_edges.append({
                    "rel": rel,
                    "source": source_node,
                    "target": downstream_node,
                    "source_col": source_col,
                    "target_col": target_col
                })
            
            # Deduplicate nodes by ID
            unique_nodes = {}
            for node in all_nodes:
                if not node: continue
                n_id = node.element_id if hasattr(node, "element_id") else str(node.id)
                if n_id not in unique_nodes:
                    unique_nodes[n_id] = node
    
            # Build Frontend Nodes
            nodes = []
            for n_id, node_obj in unique_nodes.items():
                # node_obj is a Neo4j Node object
                props = dict(node_obj.items()) 
                
                react_node = {
                    "id": n_id, 
                    "type": "custom", 
                    "data": { 
                        "label": props.get("name", "Unnamed"),
                        "type": "Table",
                        "mongoId": props.get("mongo_id"),
                        **props
                    },
                    "position": {"x": 0, "y": 0} 
                }
                nodes.append(react_node)
    
            # Build Frontend Edges
            edges = []
            seen_edges = set()
            
            for e_obj in raw_edges:
                rel = e_obj.get("rel")
                if rel is None:
                    continue
                
                rel_id = rel.element_id if hasattr(rel, "element_id") else str(rel.id)
                
                if rel_id in seen_edges:
                    continue
                seen_edges.add(rel_id)
    
                src_node = e_obj.get("source")
                tgt_node = e_obj.get("target")
                
                if src_node is None or tgt_node is None:
                    continue
                
                source_id = src_node.element_id if hasattr(src_node, "element_id") else str(src_node.id)
                target_id = tgt_node.element_id if hasattr(tgt_node, "element_id") else str(tgt_node.id)
    
                edge_data = {
                    "id": rel_id,
                    "source": source_id,
                    "target": target_id,
                    "sourceHandle": e_obj.get("source_col"),
                    "targetHandle": e_obj.get("target_col"),
                    "label": rel.type,
                    "animated": True
                }
                edges.append(edge_data)
    
            # --- Enrichment: Fetch Schema from MongoDB ---
            # (Same logic as before to populate columns for UI)
            mongo_ids = [n["data"]["mongoId"] for n in nodes if n["data"].get("mongoId")]
            if mongo_ids:
                try:
                    db = get_db()
                    if db is not None:
                        obj_ids = [ObjectId(mid) for mid in mongo_ids if mid and ObjectId.is_valid(mid)]
                        cursor = db.datasets.find({"_id": {"$in": obj_ids}}, {"_id": 1, "schema": 1})
                        mongo_docs = await cursor.to_list(length=len(obj_ids))
                        schema_map = {str(doc["_id"]): doc.get("schema", []) for doc in mongo_docs}
                        
                        for node in nodes:
                            mid = node["data"].get("mongoId")
                            if mid in schema_map:
                                node["data"]["columns"] = [col["name"] for col in schema_map[mid]]
                                node["data"]["rawSchema"] = schema_map[mid]
                except Exception as e:
                    print(f"⚠️ Enrichment Error: {e}")
    
            return {"nodes": nodes, "edges": edges}

    except Exception as e:
        print(f"❌ Lineage Fetch Error (OGM/Driver): {e}")
        return {"nodes": [], "edges": []}
