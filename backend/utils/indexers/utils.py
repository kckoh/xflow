"""
공통 유틸리티 함수
노드/컬럼 메타데이터 추출 등
"""
from typing import List, Dict, Any


def extract_node_metadata(nodes: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    nodes 배열에서 검색 가능한 메타데이터 추출
    
    ETL Job 노드 구조:
    {
        "id": "node_...",
        "data": {
            "description": "노드 설명",
            "label": "노드 이름",
            "schema": [...],
            "metadata": {
                "table": {"description": "...", "tags": [...]},
                "columns": {
                    "column_name": {"description": "...", "tags": [...]}
                }
            }
        }
    }
    
    Returns:
        {
            'node_descriptions': List[str],
            'node_tags': List[str],
            'column_names': List[str],
            'column_descriptions': List[str],
            'column_tags': List[str]
        }
    """
    node_descriptions = []
    node_tags = []
    column_names = []
    column_descriptions = []
    column_tags = []
    
    for node in nodes:
        if not isinstance(node, dict):
            continue
        
        # React Flow 노드 구조: data 필드 안에 실제 데이터
        data = node.get('data', node)
        
        # 1. 노드 레벨 description
        if desc := data.get('description'):
            node_descriptions.append(desc)
        
        # 노드 label도 검색 대상
        if label := data.get('label'):
            node_descriptions.append(label)
        
        # 노드 name (테이블명 등)
        if name := data.get('tableName'):
            node_descriptions.append(name)
        
        # 2. metadata 영역 처리
        metadata = data.get('metadata', {})
        
        # 테이블 레벨 메타데이터
        table_meta = metadata.get('table', {})
        if table_desc := table_meta.get('description'):
            node_descriptions.append(table_desc)
        if table_tags := table_meta.get('tags'):
            if isinstance(table_tags, list):
                node_tags.extend(table_tags)
        
        # 컬럼 레벨 메타데이터 (dict 형태)
        columns_meta = metadata.get('columns', {})
        if isinstance(columns_meta, dict):
            for col_name, col_meta in columns_meta.items():
                column_names.append(col_name)
                if isinstance(col_meta, dict):
                    if col_desc := col_meta.get('description'):
                        column_descriptions.append(col_desc)
                    if col_tags := col_meta.get('tags'):
                        if isinstance(col_tags, list):
                            column_tags.extend(col_tags)
        
        # 3. schema 배열에서 컬럼 정보 추출
        schema = data.get('schema') or data.get('columns') or []
        if isinstance(schema, list):
            for col in schema:
                if isinstance(col, str):
                    column_names.append(col)
                elif isinstance(col, dict):
                    col_name = col.get('name') or col.get('key') or col.get('field')
                    if col_name:
                        column_names.append(col_name)
                    if col_desc := col.get('description'):
                        column_descriptions.append(col_desc)
                    if col_tags := col.get('tags'):
                        if isinstance(col_tags, list):
                            column_tags.extend(col_tags)
    
    return {
        'node_descriptions': list(set(filter(None, node_descriptions))),
        'node_tags': list(set(filter(None, node_tags))),
        'column_names': list(set(filter(None, column_names))),
        'column_descriptions': list(set(filter(None, column_descriptions))),
        'column_tags': list(set(filter(None, column_tags)))
    }
