"""
Data Indexers
OpenSearch 인덱싱 모듈
"""
from .domain_indexer import (
    index_domains,
    index_single_domain,
    delete_domain_from_index
)
from .etl_job_indexer import (
    index_etl_jobs,
    index_single_etl_job,
    delete_etl_job_from_index
)
from .utils import extract_node_metadata


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


__all__ = [
    'index_domains',
    'index_single_domain',
    'delete_domain_from_index',
    'index_etl_jobs',
    'index_single_etl_job',
    'delete_etl_job_from_index',
    'index_all_domains_and_jobs',
    'extract_node_metadata'
]
