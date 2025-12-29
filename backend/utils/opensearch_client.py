"""
OpenSearch Client 설정 및 연결 관리
"""
import os
import json
from pathlib import Path
from opensearchpy import OpenSearch
from typing import Optional


# 환경 설정
OPENSEARCH_HOST = os.getenv('OPENSEARCH_HOST', 'localhost')
OPENSEARCH_PORT = int(os.getenv('OPENSEARCH_PORT', '9200'))
OPENSEARCH_USE_SSL = os.getenv('OPENSEARCH_USE_SSL', 'false').lower() == 'true'
OPENSEARCH_VERIFY_CERTS = os.getenv('OPENSEARCH_VERIFY_CERTS', 'false').lower() == 'true'

# OpenSearch 클라이언트 싱글톤
_opensearch_client: Optional[OpenSearch] = None

# 매핑 설정 로드
MAPPINGS_FILE = Path(__file__).parent.parent / 'config' / 'opensearch_mappings.json'

def load_catalog_mapping():
    """
    JSON 파일에서 catalog 인덱스 매핑 로드
    """
    with open(MAPPINGS_FILE, 'r', encoding='utf-8') as f:
        mappings = json.load(f)
    return mappings['catalog_index']

# 매핑 로드
_catalog_config = load_catalog_mapping()
CATALOG_INDEX = _catalog_config['index_name']
CATALOG_INDEX_MAPPING = {
    'settings': _catalog_config['settings'],
    'mappings': _catalog_config['mappings']
}


def get_opensearch_client() -> OpenSearch:
    """
    OpenSearch 클라이언트 반환 (싱글톤 패턴)
    """
    global _opensearch_client

    if _opensearch_client is None:
        _opensearch_client = OpenSearch(
            hosts=[{
                'host': OPENSEARCH_HOST,
                'port': OPENSEARCH_PORT
            }],
            http_auth=None,  # 인증 없음 (개발 환경)
            use_ssl=OPENSEARCH_USE_SSL,
            verify_certs=OPENSEARCH_VERIFY_CERTS,
            ssl_show_warn=False,
            timeout=30,
            max_retries=3,
            retry_on_timeout=True
        )

    return _opensearch_client


def create_catalog_index():
    """
    Data Catalog 인덱스 생성 (이미 존재하면 스킵)
    """
    client = get_opensearch_client()

    # 인덱스 존재 확인
    if client.indices.exists(index=CATALOG_INDEX):
        print(f"Index '{CATALOG_INDEX}' already exists")
        return

    # 인덱스 생성 (매핑은 JSON 파일에서 로드)
    response = client.indices.create(
        index=CATALOG_INDEX,
        body=CATALOG_INDEX_MAPPING
    )

    print(f"Index '{CATALOG_INDEX}' created successfully")
    return response


def delete_catalog_index():
    """
    Data Catalog 인덱스 삭제 (테스트/재생성용)
    """
    client = get_opensearch_client()

    if client.indices.exists(index=CATALOG_INDEX):
        client.indices.delete(index=CATALOG_INDEX)
        print(f"Index '{CATALOG_INDEX}' deleted")
    else:
        print(f"Index '{CATALOG_INDEX}' does not exist")


def check_opensearch_connection() -> bool:
    """
    OpenSearch 연결 확인
    """
    try:
        client = get_opensearch_client()
        info = client.info()
        print(f"Connected to OpenSearch: {info['version']['number']}")
        return True
    except Exception as e:
        print(f"Failed to connect to OpenSearch: {e}")
        return False


def initialize_opensearch():
    """
    OpenSearch 초기화 (연결 확인 + 인덱스 생성)
    FastAPI 시작 시 호출
    """
    if check_opensearch_connection():
        create_catalog_index()
        return True
    return False
