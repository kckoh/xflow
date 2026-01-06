from typing import List, Dict, Set, Optional
from models import Dataset, ETLJob
from datetime import datetime
from bson import ObjectId
import database
import os


async def get_lineage(dataset_id: str) -> Dict:
    """
    Dataset의 lineage 정보 반환
    - Source: nodes에서 source 카테고리 추출
    - Target: S3에서 DuckDB로 실제 스키마 조회 (실패 시 nodes에서 fallback)
    - Transform: 제외 (Source → Target 직접 연결)
    """
    db = database.mongodb_client[database.DATABASE_NAME]

    try:
        obj_id = ObjectId(dataset_id)
    except:
        return {"sources": [], "transforms": [], "targets": [], "nodes": [], "edges": []}

    doc = await db.datasets.find_one({"_id": obj_id})
    if not doc:
        return {"sources": [], "transforms": [], "targets": [], "nodes": [], "edges": []}

    # 1. Source 노드 추출 - sources 필드 또는 nodes에서 추출
    sources = doc.get("sources", [])

    # sources가 비어있으면 nodes에서 source 카테고리 추출
    if not sources and doc.get("nodes"):
        for node in doc.get("nodes", []):
            node_data = node.get("data", {})
            if node_data.get("nodeCategory") == "source":
                sources.append({
                    "nodeId": node.get("id"),
                    "type": node_data.get("platform", "rdb"),
                    "schema": node_data.get("columns", []),
                    "config": {
                        "name": node_data.get("name") or node_data.get("label"),
                        "table": node_data.get("tableName"),
                        "platform": node_data.get("platform")
                    }
                })

    # 2. Target 노드 - S3에서 실제 스키마 조회
    targets = []
    destination = doc.get("destination", {})

    # nodes에서 target 스키마 가져오기 (fallback용)
    # 우선순위: target 노드 > 마지막 transform의 outputSchema
    fallback_target_schema = []
    last_transform_schema = []

    for node in doc.get("nodes", []):
        node_data = node.get("data", {})
        if node_data.get("nodeCategory") == "target":
            fallback_target_schema = node_data.get("columns", []) or node_data.get("schema", [])
        elif node_data.get("nodeCategory") == "transform":
            # Transform의 outputSchema 저장 (마지막 transform 사용)
            output_schema = node_data.get("outputSchema", [])
            if output_schema:
                last_transform_schema = output_schema

    # target 노드에 스키마가 없으면 transform의 outputSchema 사용
    if not fallback_target_schema and last_transform_schema:
        fallback_target_schema = last_transform_schema

    if destination.get("type") == "s3":
        # DuckDB로 S3 실제 스키마 조회 시도
        target_schema = await _get_s3_schema(destination, doc.get("name", ""))

        # S3 스키마 조회 실패 시 fallback 사용
        if not target_schema:
            target_schema = fallback_target_schema
            print(f"Using fallback schema for {doc.get('name')}")

        targets.append({
            "nodeId": "target_0",
            "type": "s3",
            "schema": target_schema,
            "config": {
                "path": destination.get("path", ""),
                "name": doc.get("name", ""),
                "platform": "S3"
            }
        })

    # 3. 노드와 엣지 생성 (ReactFlow 형식)
    nodes = []
    edges = []

    # Source 노드들 추가
    for idx, source in enumerate(sources):
        node_id = source.get("nodeId", f"source_{idx}")
        nodes.append({
            "id": node_id,
            "type": "custom",
            "position": {"x": 100, "y": 100 + idx * 150},
            "data": {
                "label": source.get("config", {}).get("name") or source.get("config", {}).get("table") or node_id,
                "name": source.get("config", {}).get("name") or source.get("config", {}).get("table") or node_id,
                "platform": source.get("type", "rdb"),
                "columns": source.get("schema", []),
                "nodeCategory": "source"
            }
        })

        # Source → Target 엣지
        if targets:
            edges.append({
                "id": f"edge-{node_id}-target_0",
                "source": node_id,
                "target": "target_0",
                "type": "smoothstep",
                "animated": True
            })

    # Target 노드 추가
    for idx, target in enumerate(targets):
        node_id = target.get("nodeId", f"target_{idx}")
        nodes.append({
            "id": node_id,
            "type": "custom",
            "position": {"x": 500, "y": 100 + idx * 150},
            "data": {
                "label": target.get("config", {}).get("name") or doc.get("name", "Target"),
                "name": target.get("config", {}).get("name") or doc.get("name", "Target"),
                "platform": "S3",
                "columns": target.get("schema", []),
                "nodeCategory": "target"
            }
        })

    return {
        "sources": sources,
        "transforms": [],  # Transform 제외
        "targets": targets,
        "nodes": nodes,
        "edges": edges
    }


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
