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


def load_mappings():
    """
    JSON 파일에서 모든 인덱스 매핑 로드
    """
    with open(MAPPINGS_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)


# 매핑 로드
_mappings = load_mappings()

# Domain Index 설정
DOMAIN_INDEX = _mappings['domain_index']['index_name']
DOMAIN_INDEX_MAPPING = {
    'settings': _mappings['domain_index']['settings'],
    'mappings': _mappings['domain_index']['mappings']
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


def create_index(index_name: str, mapping: dict):
    """
    인덱스 생성 (이미 존재하면 스킵)
    """
    client = get_opensearch_client()

    if client.indices.exists(index=index_name):
        print(f"Index '{index_name}' already exists")
        return

    response = client.indices.create(
        index=index_name,
        body=mapping
    )

    print(f"Index '{index_name}' created successfully")
    return response


def delete_index(index_name: str):
    """
    인덱스 삭제
    """
    client = get_opensearch_client()

    if client.indices.exists(index=index_name):
        client.indices.delete(index=index_name)
        print(f"Index '{index_name}' deleted")
    else:
        print(f"Index '{index_name}' does not exist")


def create_domain_index():
    """
    Domain 인덱스 생성 (이미 존재하면 스킵)
    """
    return create_index(DOMAIN_INDEX, DOMAIN_INDEX_MAPPING)


def delete_domain_index():
    """
    Domain 인덱스 삭제
    """
    return delete_index(DOMAIN_INDEX)


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
        create_domain_index()
        return True
    return False
