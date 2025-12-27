from typing import List, Dict, Any
import database

def get_lineage(dataset_id: str) -> Dict[str, List[Dict[str, Any]]]:
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
    query = """
    MATCH (t:Table {mongo_id: $id})
    
    // Upstream
    OPTIONAL MATCH (upstream:Table)-[r_up:FLOWS_TO]->(t)
    
    // Downstream
    OPTIONAL MATCH (t)-[r_down:FLOWS_TO]->(downstream:Table)
    
    // Return all distinct nodes and relationships
    // collect(node) ignores nulls, so if upstream/downstream are null, they won't be in the list
    RETURN collect(DISTINCT t) + collect(DISTINCT upstream) + collect(DISTINCT downstream) as nodes,
           collect(DISTINCT r_up) + collect(DISTINCT r_down) as rels
    """
    
    with driver.session() as session:
        result = session.run(query, id=dataset_id)
        record = result.single()
        
        # Safe default if query returns nothing
        if not record:
            # If no paths found, at least return the node itself if it exists
            # (Separate check to avoid complexity, or just return empty)
            # Let's try to fetch just the single node if paths are empty
            with driver.session() as session2:
                 single_res = session2.run("MATCH (t:Table {mongo_id: $id}) RETURN t", id=dataset_id).single()
                 if single_res:
                     record = {"nodes": [single_res["t"]], "rels": []}
                 else:
                     return {"nodes": [], "edges": []}

        # Check for None (Neo4j driver might return None for empty collections in some versions)
        neo4j_nodes = record.get("nodes") or []
        neo4j_rels = record.get("rels") or []
        
        nodes = []
        edges = []
        
        for node in neo4j_nodes:
            # Extract basic props
            props = dict(node)
            # Safe ID access
            str_id = node.element_id if hasattr(node, "element_id") else str(node.id) 
            
            # Determine type/label
            labels = list(node.labels)
            node_type = labels[0] if labels else "Table"
            
            react_node = {
                "id": str_id, 
                "type": "custom", # or 'default' if no custom node
                "data": { 
                    "label": props.get("name", "Unnamed"),
                    "type": node_type,
                    "mongoId": props.get("mongo_id"),
                    **props
                },
                "position": {"x": 0, "y": 0} # Layout happens in frontend
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
            
        return {"nodes": nodes, "edges": edges}
