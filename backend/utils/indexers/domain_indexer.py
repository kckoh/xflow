"""
Domain/ETL Job 인덱서
MongoDB의 domains, etl_jobs 컬렉션을 OpenSearch에 인덱싱
"""
from datetime import datetime
from typing import List
from opensearchpy import helpers
from utils.opensearch_client import get_opensearch_client, DOMAIN_INDEX
from models import Domain, ETLJob


async def index_domains() -> int:
    """
    MongoDB domains 컬렉션을 OpenSearch에 인덱싱
    
    Returns:
        인덱싱된 문서 수
    """
    try:
        # MongoDB에서 모든 도메인 조회
        domains = await Domain.find_all().to_list()
        
        if not domains:
            print("No domains found to index")
            return 0
        
        # OpenSearch 문서 생성
        documents = []
        for domain in domains:
            doc = {
                'doc_id': str(domain.id),
                'doc_type': 'domain',
                'name': domain.name,
                'description': domain.description,
                'type': domain.type,
                'owner': domain.owner,
                'tags': domain.tags,
                'created_at': domain.created_at.isoformat() if domain.created_at else None,
                'updated_at': domain.updated_at.isoformat() if domain.updated_at else None,
                'last_indexed': datetime.utcnow().isoformat()
            }
            documents.append(doc)
        
        # Bulk 인덱싱
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


async def index_etl_jobs() -> int:
    """
    MongoDB etl_jobs 컬렉션을 OpenSearch에 인덱싱
    
    Returns:
        인덱싱된 문서 수
    """
    try:
        # MongoDB에서 모든 ETL Jobs 조회
        jobs = await ETLJob.find_all().to_list()
        
        if not jobs:
            print("No ETL jobs found to index")
            return 0
        
        # OpenSearch 문서 생성
        documents = []
        for job in jobs:
            doc = {
                'doc_id': str(job.id),
                'doc_type': 'etl_job',
                'name': job.name,
                'description': job.description,
                'status': job.status,
                'created_at': job.created_at.isoformat() if job.created_at else None,
                'updated_at': job.updated_at.isoformat() if job.updated_at else None,
                'last_indexed': datetime.utcnow().isoformat()
            }
            documents.append(doc)
        
        # Bulk 인덱싱
        opensearch = get_opensearch_client()
        actions = [
            {
                '_index': DOMAIN_INDEX,
                '_id': f"etl_job_{doc['doc_id']}",
                '_source': doc
            }
            for doc in documents
        ]
        helpers.bulk(opensearch, actions)
        print(f"ETL Jobs: {len(documents)} documents indexed")
        
        return len(documents)
        
    except Exception as e:
        print(f"ETL job indexing error: {e}")
        return 0


async def index_all_domains_and_jobs() -> dict:
    """
    Domain과 ETL Job 모두 인덱싱
    
    Returns:
        인덱싱 결과 dict
    """
    domains_count = await index_domains()
    jobs_count = await index_etl_jobs()
    
    return {
        'domains': domains_count,
        'etl_jobs': jobs_count,
        'total': domains_count + jobs_count
    }
