"""
ETL Job 인덱서
MongoDB의 etl_jobs 컬렉션을 OpenSearch에 인덱싱
"""
from datetime import datetime
from opensearchpy import helpers
from utils.opensearch_client import get_opensearch_client, DOMAIN_INDEX
from utils.indexers.utils import extract_node_metadata
from models import ETLJob


def build_etl_job_document(job: ETLJob) -> dict:
    """
    ETL Job 객체를 OpenSearch 문서로 변환
    """
    node_meta = extract_node_metadata(job.nodes or [])
    
    return {
        'doc_id': str(job.id),
        'doc_type': 'etl_job',
        'name': job.name,
        'description': job.description,
        'status': job.status,
        'node_descriptions': node_meta['node_descriptions'],
        'node_tags': node_meta['node_tags'],
        'column_names': node_meta['column_names'],
        'column_descriptions': node_meta['column_descriptions'],
        'column_tags': node_meta['column_tags'],
        'created_at': job.created_at.isoformat() if job.created_at else None,
        'updated_at': job.updated_at.isoformat() if job.updated_at else None,
        'last_indexed': datetime.utcnow().isoformat()
    }


async def index_single_etl_job(job: ETLJob) -> bool:
    """
    단일 ETL Job을 OpenSearch에 인덱싱 (Dual Write용)
    
    Args:
        job: ETL Job 객체
    
    Returns:
        성공 여부
    """
    try:
        opensearch = get_opensearch_client()
        doc = build_etl_job_document(job)
        
        opensearch.index(
            index=DOMAIN_INDEX,
            id=f"etl_job_{doc['doc_id']}",
            body=doc
        )
        print(f"ETL Job indexed: {job.name}")
        return True
        
    except Exception as e:
        print(f"ETL Job indexing error: {e}")
        return False


async def delete_etl_job_from_index(job_id: str) -> bool:
    """
    OpenSearch에서 ETL Job 삭제 (Dual Write용)
    
    Args:
        job_id: ETL Job ID
    
    Returns:
        성공 여부
    """
    try:
        opensearch = get_opensearch_client()
        opensearch.delete(
            index=DOMAIN_INDEX,
            id=f"etl_job_{job_id}",
            ignore=[404]
        )
        print(f"ETL Job deleted from index: {job_id}")
        return True
        
    except Exception as e:
        print(f"ETL Job delete error: {e}")
        return False


async def index_etl_jobs() -> int:
    """
    MongoDB etl_jobs 컬렉션 전체를 OpenSearch에 인덱싱
    
    Returns:
        인덱싱된 문서 수
    """
    try:
        jobs = await ETLJob.find_all().to_list()
        
        if not jobs:
            print("No ETL jobs found to index")
            return 0
        
        documents = [build_etl_job_document(job) for job in jobs]
        
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
