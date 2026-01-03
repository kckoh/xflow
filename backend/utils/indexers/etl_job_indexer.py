"""
ETL Job 인덱서
MongoDB의 etl_jobs 컬렉션을 OpenSearch에 인덱싱
"""
from datetime import datetime
from opensearchpy import helpers
from utils.opensearch_client import get_opensearch_client, DOMAIN_INDEX
from utils.indexers.utils import extract_node_metadata
from models import ETLJob


async def index_etl_jobs() -> int:
    """
    MongoDB etl_jobs 컬렉션을 OpenSearch에 인덱싱
    노드/컬럼 단위 메타데이터 포함
    
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
            # 노드/컬럼 메타데이터 추출 (nodes 배열에서)
            node_meta = extract_node_metadata(job.nodes or [])
            
            doc = {
                'doc_id': str(job.id),
                'doc_type': 'etl_job',
                'name': job.name,
                'description': job.description,
                'status': job.status,
                # 노드/컬럼 메타데이터
                'node_descriptions': node_meta['node_descriptions'],
                'node_tags': node_meta['node_tags'],
                'column_names': node_meta['column_names'],
                'column_descriptions': node_meta['column_descriptions'],
                'column_tags': node_meta['column_tags'],
                # 타임스탬프
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
