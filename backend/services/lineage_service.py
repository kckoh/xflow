from typing import List, Dict, Any
from bson import ObjectId
import database

async def get_lineage(dataset_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """
    Fetch lineage data from Neo4j for a specific dataset (Table).
    Returns nodes and edges formatted for React Flow.
    """
    driver = database.neo4j_driver

    if not driver:
        # Fallback or error if Neo4j is not connected
        return {"nodes": [], "edges": []}

    # Simplified Cypher query for basic Table-to-Table lineage
    # Fix: Use direct collects to handle isolated nodes (no relationships) correctly
    # Update: Returns Map Projection with degree info
    query = """
    MATCH (t:Table {mongo_id: $id})
    
    // Upstream
    OPTIONAL MATCH (upstream:Table)-[r_up:FLOWS_TO]->(t)
    
    // Downstream
    OPTIONAL MATCH (t)-[r_down:FLOWS_TO]->(downstream:Table)
    
    WITH collect(DISTINCT t) + collect(DISTINCT upstream) + collect(DISTINCT downstream) as nodeList,
           collect(DISTINCT r_up) + collect(DISTINCT r_down) as relList
    
    UNWIND nodeList as n
    // Calculate degrees using subqueries (or size of pattern match)
    WITH n, relList, 
         COUNT { (n)<-[:FLOWS_TO]-() } as inD, 
         COUNT { (n)-[:FLOWS_TO]->() } as outD
         
    RETURN collect({
        elementId: elementId(n),
        labels: labels(n),
        properties: properties(n),
        inDegree: inD,
        outDegree: outD
    }) as nodes, relList as rels
    """
    
    with driver.session() as session:
        result = session.run(query, id=dataset_id)
        record = result.single()
        
        # Safe default if query returns nothing
        if not record:
            # Fallback checks (omitted for brevity, main query handles isolated nodes)
             return {"nodes": [], "edges": []}

        # Check for None (Neo4j driver might return None for empty collections in some versions)
        neo4j_nodes = record.get("nodes") or []
        neo4j_rels = record.get("rels") or []
        
        nodes = []
        edges = []
        
        for node_data in neo4j_nodes:
            # node_data is a Dict due to Map Projection in Cypher
            props = node_data["properties"]
            str_id = node_data["elementId"]
            labels = node_data["labels"]
            node_type = labels[0] if labels else "Table"
            
            in_degree = node_data["inDegree"]
            out_degree = node_data["outDegree"]
            
            react_node = {
                "id": str_id, 
                "type": "custom", 
                "data": { 
                    "label": props.get("name", "Unnamed"),
                    "type": node_type,
                    "mongoId": props.get("mongo_id"),
                    "inDegree": in_degree,
                    "outDegree": out_degree,
                    **props
                },
                "position": {"x": 0, "y": 0} 
            }
            nodes.append(react_node)
            
        for rel in neo4j_rels:
            str_rel_id = rel.element_id if hasattr(rel, "element_id") else str(rel.id)
            start_node_id = rel.start_node.element_id if hasattr(rel.start_node, "element_id") else str(rel.start_node.id)
            end_node_id = rel.end_node.element_id if hasattr(rel.end_node, "element_id") else str(rel.end_node.id)
            
            react_edge = {
                "id": str_rel_id,
                "source": start_node_id,
                "target": end_node_id,
                "label": type(rel).__name__, 
                "animated": True
            }
            edges.append(react_edge)
            

        # --- Enrichment: Fetch Schema from MongoDB ---
        mongo_ids = [n["data"]["mongoId"] for n in nodes if n["data"].get("mongoId")]
        
        if mongo_ids and database.mongodb_client:
            try:
                db = database.mongodb_client[database.DATABASE_NAME]
                
                # Convert strings to ObjectIds
                obj_ids = [ObjectId(mid) for mid in mongo_ids if mid and ObjectId.is_valid(mid)]
                
                # Fetch only necessary fields
                cursor = db.datasets.find({"_id": {"$in": obj_ids}}, {"_id": 1, "schema": 1})
                mongo_docs = await cursor.to_list(length=len(obj_ids))
                
                # Create map: str(id) -> schema
                schema_map = {str(doc["_id"]): doc.get("schema", []) for doc in mongo_docs}
                
                # Update nodes
                for node in nodes:
                    mid = node["data"].get("mongoId")
                    if mid in schema_map:
                        node["data"]["columns"] = [col["name"] for col in schema_map[mid]]
                        # Also keep raw schema if needed by UI
                        node["data"]["rawSchema"] = schema_map[mid]
            except Exception as e:
                print(f"⚠️ Error enriching lineage with MongoDB data: {e}")

        return {"nodes": nodes, "edges": edges}
