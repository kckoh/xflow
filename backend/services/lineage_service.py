from typing import List, Dict, Set
from models import ETLJob, Dataset
from datetime import datetime

async def sync_pipeline_to_dataset(job: ETLJob):
    """
    Syncs ETLJob configuration to Dataset model for lineage tracking.
    Parses nodes and edges to build detailed source/transform/target specs.
    Also updates the is_active status of the dataset.
    """
    if not job.nodes:
        return

    # 1. Parse Nodes and Build Maps
    nodes_map = {node['id']: node for node in job.nodes}
    
    # 2. Build Input Map (Target Node ID -> List of Source Node IDs)
    input_map: Dict[str, List[str]] = {}
    if job.edges:
        for edge in job.edges:
            target_id = edge['target']
            source_id = edge['source']
            if target_id not in input_map:
                input_map[target_id] = []
            input_map[target_id].append(source_id)

    # 3. Categorize Nodes
    sources = []
    transforms = []
    targets = []

    for node in job.nodes:
        node_id = node['id']
        data = node.get('data', {})
        category = data.get('nodeCategory')
        
        # Base item structure
        item = {
            "nodeId": node_id,
            "type": data.get('type') or data.get('transformType') or "unknown",
            "schema": data.get('schema', []),
            "inputNodeIds": input_map.get(node_id, []),
            "config": _extract_config(data)
        }

        # Add connection info for sources/targets if available
        if data.get('connectionId'):
            item['connection_id'] = data.get('connectionId')
        
        # URN Generation (Standardized)
        urn = f"urn:unknown:{node_id}"
        config = item['config']
        
        if category == 'source':
            conn_id = config.get('connection_id') or config.get('sourceId') or 'unknown'
            table_name = config.get('table') or config.get('tableName') or 'unknown'
            schema_name = "public"
            urn = f"urn:rdb:{conn_id}:{schema_name}.{table_name}"
            
        elif category == 'target':
            s3_path = config.get('path') or config.get('s3Location') or ''
            clean_path = s3_path.replace("s3://", "").replace("s3a://", "")
            if "/" in clean_path:
                bucket, key = clean_path.split("/", 1)
                urn = f"urn:s3:{bucket}:{key}"
            else:
                urn = f"urn:s3:{clean_path}"
                
        elif category == 'transform':
            urn = f"urn:job:{job.id}:{node_id}"
            
        item['urn'] = urn

        if category == 'source':
            sources.append(item)
        elif category == 'transform':
            transforms.append(item)
        elif category == 'target':
            targets.append(item)

    # 4. Check Connectivity for Active Status
    is_active = _check_connectivity(sources, targets, input_map)

    # 5. Upsert Dataset
    dataset = await Dataset.find_one(Dataset.job_id == str(job.id))
    if not dataset:
        dataset = Dataset(
            name=job.name,
            job_id=str(job.id),
            created_at=datetime.utcnow()
        )
    
    # Update fields
    dataset.name = job.name
    dataset.description = job.description
    dataset.sources = sources
    dataset.transforms = transforms
    dataset.targets = targets
    dataset.is_active = is_active
    dataset.updated_at = datetime.utcnow()
    
    await dataset.save()
    
    # Optional: Update Job status immediately if needed
    if is_active and job.status == 'draft':
        # job.status = 'active' # Decide if we want to auto-activate
        pass 


def _extract_config(data: dict) -> dict:
    """Extract relevant configuration from node data"""
    config = {}
    # Copy all fields except schema and UI specific ones
    exclude_keys = ['schema', 'nodeCategory', 'label', 'icon', 'onColumnClick']
    for k, v in data.items():
        if k not in exclude_keys:
            config[k] = v
    return config


def _check_connectivity(sources: List[dict], targets: List[dict], input_map: Dict[str, List[str]]) -> bool:
    """
    Check if there is at least one complete path from a Source to a Target.
    Simple BFS/DFS from Targets backwards to Sources.
    """
    if not sources or not targets:
        return False
        
    source_ids = {s['nodeId'] for s in sources}
    
    # Check each target if it connects to ANY source
    for target in targets:
        # Trace back from this target
        queue = [target['nodeId']]
        visited = set()
        
        while queue:
            curr = queue.pop(0)
            if curr in visited:
                continue
            visited.add(curr)
            
            # If current node is a source, we found a path!
            if curr in source_ids:
                return True
            
            # Add parents to queue
            parents = input_map.get(curr, [])
            queue.extend(parents)
            
    return False
