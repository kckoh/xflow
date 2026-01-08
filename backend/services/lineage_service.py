from typing import List, Dict, Set, Optional
from models import Dataset, ETLJob
from datetime import datetime
from bson import ObjectId
import database
import os


async def get_lineage(dataset_id: str) -> Dict:
    """
    Dataset의 lineage 정보 반환 (재귀적)
    - Recursion: Source가 다른 Dataset의 Output인 경우 상위 리니지 병합
    - Layout: 
      - X: Level based (Right to Left)
      - Y: Dynamic Smart Layout (Leaf Stacking + Centering)
    - Deduplication: Intermediate Dataset인 경우 중복 노드 제거
    """
    full_graph = {
        "sources": [],
        "targets": [],
        "nodes": [],
        "edges": []
    }
    
    visited_datasets = set()
    
    # y_cursor: [next_available_y] - Mutable container to track global vertical usage
    y_cursor = [-150]
    
    # level: 0(Current), -1(Parent), -2(Grandparent) ...
    await _fetch_recursive(dataset_id, full_graph, visited_datasets, level=0, y_cursor=y_cursor)
    
    return full_graph


async def _fetch_recursive(dataset_id: str, graph: Dict, visited: Set[str], level: int, y_cursor: List[int]) -> float:
    """
    재귀적으로 리니지 정보를 가져와서 graph에 병합
    Returns: The Y-coordinate of the primary 'Target Node' for this dataset (for centering)
    """
    # Circular dependency check: return a placeholder Y if already visited? 
    # But current logic needs to traverse to draw. 
    # If visited, we assume the node exists. Find its Y? 
    # For now, simplistic handle: just return current cursor if visited to avoid crash, 
    # but ideally we should lookup the existing node's Y. 
    # Given the strict DAG nature usually, we'll proceed.
    prefix = f"ds_{dataset_id}_"
    target_unique_id = f"{prefix}target_0"
    
    # 이미 존재하는 노드라면 그 위치 반환 (Duplicate Visit)
    existing_node = next((n for n in graph["nodes"] if n["id"] == target_unique_id), None)
    if existing_node:
        return existing_node["position"]["y"]
        
    if dataset_id in visited:
        # Should catch above, but safety
        return y_cursor[0]
        
    visited.add(dataset_id)
    
    db = database.mongodb_client[database.DATABASE_NAME]
    
    try:
        obj_id = ObjectId(dataset_id)
    except:
        return y_cursor[0]

    doc = await db.datasets.find_one({"_id": obj_id})
    if not doc:
        return y_cursor[0]

    # === 1. Prepare Data ===
    current_sources = doc.get("sources", [])
    if not current_sources and doc.get("nodes"):
        for node in doc.get("nodes", []):
            node_data = node.get("data", {})
            if node_data.get("nodeCategory") == "source":
                current_sources.append({
                    "nodeId": node.get("id"),
                    "type": node_data.get("platform", "rdb"),
                    "schema": node_data.get("columns", []),
                    "config": {
                        "name": node_data.get("name") or node_data.get("label"),
                        "table": node_data.get("tableName"),
                        "path": node_data.get("path") or node_data.get("s3Location"),
                        "catalogDatasetId": node_data.get("catalogDatasetId"),
                        "platform": node_data.get("platform")
                    }
                })

    current_targets = []
    destination = doc.get("destination", {})
    
    # Fallback schema logic
    fallback_target_schema = []
    last_transform_schema = []
    for node in doc.get("nodes", []):
        node_data = node.get("data", {})
        if node_data.get("nodeCategory") == "target":
            fallback_target_schema = node_data.get("columns", []) or node_data.get("schema", [])
        elif node_data.get("nodeCategory") == "transform":
            output_schema = node_data.get("outputSchema", [])
            if output_schema:
                last_transform_schema = output_schema
                
    if not fallback_target_schema and last_transform_schema:
        fallback_target_schema = last_transform_schema

    if destination.get("type") == "s3":
        current_targets.append({
            "nodeId": "target_0",
            "type": "s3",
            "schema": fallback_target_schema,
            "config": {
                "path": destination.get("path", ""),
                "name": doc.get("name", ""),
                "platform": "S3"
            }
        })

    # === 2. Calculate Layout & Build Graph ===
    # Special handling for Level 0 (Final Target) to push it to the right
    if level == 0:
        base_x = 600
    else:
        # Move upstream closer to 600
        # Level -1 will be at 600 + (-1 * 300) = 300. (Gap 300)
        base_x = 600 + (level * 300)
    
    # 2-1. Process Sources to determine Y position
    source_y_positions = []
    
    for idx, source in enumerate(current_sources):
        upstream_dataset_id = source.get("config", {}).get("catalogDatasetId")
        
        if upstream_dataset_id:
            # Case A: Recursive Upstream
            # Recurse FIRST to layout upstream nodes. 
            # It returns the Y of the upstream target.
            upstream_y = await _fetch_recursive(upstream_dataset_id, graph, visited, level - 1, y_cursor)
            source_y_positions.append(upstream_y)
            
            # Connect Upstream -> Current
            upstream_target_id = f"ds_{upstream_dataset_id}_target_0"
            link_id = f"link_{upstream_target_id}_{target_unique_id}"
            if not any(e["id"] == link_id for e in graph["edges"]):
                graph["edges"].append({
                    "id": link_id,
                    "source": upstream_target_id,
                    "target": target_unique_id,
                    "type": "default",
                    "animated": True,
                    "style": { "strokeDasharray": "5 5", "stroke": "#f97316", "strokeWidth": 2 }
                    # Removed label
                })
        else:
            # Case B: Root Source (Leaf)
            # Allocate new Y slot
            current_y = y_cursor[0]
            y_cursor[0] += 300 # Increment for next slot
            
            source_y_positions.append(current_y)
            
            unique_source_id = f"{prefix}{source.get('nodeId', f'source_{idx}')}"
            if not any(n["id"] == unique_source_id for n in graph["nodes"]):
                graph["nodes"].append({
                    "id": unique_source_id,
                    "type": "custom",
                    "position": {"x": base_x - 300, "y": current_y},
                    "data": {
                        "label": source.get("config", {}).get("name") or unique_source_id,
                        "name": source.get("config", {}).get("name") or unique_source_id,
                        "platform": source.get("type", "rdb"),
                        "columns": source.get("schema", []),
                        "nodeCategory": "source",
                        "expanded": True
                    }
                })
                
            # Connect Root Source -> Current
            edge_id = f"edge-{unique_source_id}-{target_unique_id}"
            graph["edges"].append({
                "id": edge_id,
                "source": unique_source_id,
                "target": target_unique_id,
                "type": "default",
                "animated": True
            })

    # 2-2. Determine Target Node Y (Centering)
    if source_y_positions:
        my_y = sum(source_y_positions) / len(source_y_positions)
    else:
        # No sources? Just take next slot
        my_y = y_cursor[0]
        y_cursor[0] += 250

    # 2-3. Create Target Node
    if current_targets and not any(n["id"] == target_unique_id for n in graph["nodes"]):
        target_info = current_targets[0]
        graph["nodes"].append({
            "id": target_unique_id,
            "type": "custom",
            "position": {"x": base_x, "y": my_y},
            "data": {
                "label": target_info.get("config", {}).get("name") or doc.get("name", "Target"),
                "name": target_info.get("config", {}).get("name") or doc.get("name", "Target"),
                "platform": "S3",
                "columns": target_info.get("schema", []),
                "nodeCategory": "target",
                "expanded": True
            }
        })
        
    return my_y


async def _get_s3_schema(destination: Dict, dataset_name: str) -> List[Dict]:
    """
    DuckDB를 사용하여 S3 Parquet 파일의 실제 스키마 조회
    """
    try:
        import duckdb
        import boto3

        # S3 경로 구성
        base_path = destination.get("path", "")
        if not base_path:
            return []

        # dataset_name 추가
        s3_path = base_path
        if dataset_name and not base_path.endswith(dataset_name):
            if not base_path.endswith('/'):
                s3_path += '/'
            s3_path += dataset_name

        # s3a:// → s3:// 변환
        s3_path = s3_path.replace("s3a://", "s3://")

        # DuckDB 설정
        con = duckdb.connect()
        con.execute("INSTALL httpfs; LOAD httpfs;")

        # AWS 자격 증명 설정
        session = boto3.Session()
        creds = session.get_credentials()

        if creds:
            frozen = creds.get_frozen_credentials()
            con.execute(f"SET s3_access_key_id='{frozen.access_key}';")
            con.execute(f"SET s3_secret_access_key='{frozen.secret_key}';")
            if frozen.token:
                con.execute(f"SET s3_session_token='{frozen.token}';")

        region = session.region_name or os.getenv("AWS_REGION", "ap-northeast-2")
        con.execute(f"SET s3_region='{region}';")

        # LocalStack 엔드포인트 처리
        endpoint = os.getenv("AWS_ENDPOINT") or os.getenv("S3_ENDPOINT_URL")
        if endpoint:
            endpoint_url = endpoint.replace("http://", "").replace("https://", "")
            con.execute(f"SET s3_endpoint='{endpoint_url}';")
            if "http://" in endpoint:
                con.execute("SET s3_use_ssl=false;")
                con.execute("SET s3_url_style='path';")

        # 스키마만 조회 (LIMIT 0)
        query = f"SELECT * FROM read_parquet('{s3_path}/*.parquet') LIMIT 0"
        result = con.execute(query)

        # 컬럼 정보 추출 - description 사용
        schema = []
        for col_info in result.description:
            schema.append({
                "name": col_info[0],
                "key": col_info[0],
                "type": col_info[1]
            })

        con.close()
        return schema

    except Exception as e:
        print(f"Failed to get S3 schema: {e}")
        return []

async def sync_pipeline_to_etljob(dataset: Dataset):
    """
    Syncs Dataset configuration to ETLJob model for lineage tracking.
    Parses nodes and edges to build detailed source/transform/target specs.
    Also updates the is_active status of the ETLJob.
    """
    if not dataset.nodes:
        return

    # 1. Parse Nodes and Build Maps
    nodes_map = {node['id']: node for node in dataset.nodes}

    # 2. Build Input Map (Target Node ID -> List of Source Node IDs)
    input_map: Dict[str, List[str]] = {}
    if dataset.edges:
        for edge in dataset.edges:
            target_id = edge['target']
            source_id = edge['source']
            if target_id not in input_map:
                input_map[target_id] = []
            input_map[target_id].append(source_id)

    # 3. Categorize Nodes
    sources = []
    transforms = []
    targets = []

    for node in dataset.nodes:
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
            urn = f"urn:dataset:{dataset.id}:{node_id}"

        item['urn'] = urn

        if category == 'source':
            sources.append(item)
        elif category == 'transform':
            transforms.append(item)
        elif category == 'target':
            targets.append(item)

    # 4. Check Connectivity for Active Status
    is_active = _check_connectivity(sources, targets, input_map)

    # 5. Upsert ETLJob
    etl_job = await ETLJob.find_one(ETLJob.dataset_id == str(dataset.id))
    if not etl_job:
        etl_job = ETLJob(
            name=dataset.name,
            dataset_id=str(dataset.id),
            created_at=datetime.utcnow()
        )

    # Update fields
    etl_job.name = dataset.name
    etl_job.description = dataset.description
    etl_job.sources = sources
    etl_job.transforms = transforms
    etl_job.targets = targets
    etl_job.is_active = is_active
    etl_job.updated_at = datetime.utcnow()

    await etl_job.save()
    
    # ✅ Also update Dataset.targets for catalog display
    dataset.targets = targets
    await dataset.save()


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
