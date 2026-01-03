"""
Domain 인덱서
MongoDB의 domains 컬렉션을 OpenSearch에 인덱싱
"""
from datetime import datetime
from opensearchpy import helpers
from utils.opensearch_client import get_opensearch_client, DOMAIN_INDEX
from utils.indexers.utils import extract_node_metadata
from models import Domain


def build_domain_document(domain: Domain) -> dict:
    """
    Domain 객체를 OpenSearch 문서로 변환
    """
    node_meta = extract_node_metadata(domain.nodes or [])
    
    return {
        'doc_id': str(domain.id),
        'doc_type': 'domain',
        'name': domain.name,
        'description': domain.description,
        'type': domain.type,
        'owner': domain.owner,
        'tags': domain.tags,
        'node_descriptions': node_meta['node_descriptions'],
        'node_tags': node_meta['node_tags'],
        'column_names': node_meta['column_names'],
        'column_descriptions': node_meta['column_descriptions'],
        'column_tags': node_meta['column_tags'],
        'created_at': domain.created_at.isoformat() if domain.created_at else None,
        'updated_at': domain.updated_at.isoformat() if domain.updated_at else None,
        'last_indexed': datetime.utcnow().isoformat()
    }


async def index_single_domain(domain: Domain) -> bool:
    """
    단일 Domain을 OpenSearch에 인덱싱 (Dual Write용)
    
    Args:
        domain: Domain 객체
    
    Returns:
        성공 여부
    """
    try:
        opensearch = get_opensearch_client()
        doc = build_domain_document(domain)
        
        opensearch.index(
            index=DOMAIN_INDEX,
            id=f"domain_{doc['doc_id']}",
            body=doc
        )
        print(f"Domain indexed: {domain.name}")
        return True
        
    except Exception as e:
        print(f"Domain indexing error: {e}")
        return False


async def delete_domain_from_index(domain_id: str) -> bool:
    """
    OpenSearch에서 Domain 삭제 (Dual Write용)
    
    Args:
        domain_id: Domain ID
    
    Returns:
        성공 여부
    """
    try:
        opensearch = get_opensearch_client()
        opensearch.delete(
            index=DOMAIN_INDEX,
            id=f"domain_{domain_id}",
            ignore=[404]  # 없어도 에러 안 냄
        )
        print(f"Domain deleted from index: {domain_id}")
        return True
        
    except Exception as e:
        print(f"Domain delete error: {e}")
        return False


async def index_domains() -> int:
    """
    MongoDB domains 컬렉션 전체를 OpenSearch에 인덱싱
    
    Returns:
        인덱싱된 문서 수
    """
    try:
        domains = await Domain.find_all().to_list()
        
        if not domains:
            print("No domains found to index")
            return 0
        
        documents = [build_domain_document(domain) for domain in domains]
        
        opensearch = get_opensearch_client()
        actions = [
            {
                '_index': DOMAIN_INDEX,
                '_id': f"domain_{doc['doc_id']}",
                '_source': doc
            }
            for doc in documents
        ]
        helpers.bulk(opensearch, actions)
        print(f"Domains: {len(documents)} documents indexed")
        
        return len(documents)
        
    except Exception as e:
        print(f"Domain indexing error: {e}")
        return 0
