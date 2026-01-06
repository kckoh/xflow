"""
Data Indexers
OpenSearch 인덱싱 모듈
"""
from .dataset_indexer import (
    index_datasets,
    index_single_dataset,
    delete_dataset_from_index
)
from .utils import extract_node_metadata


async def index_all_datasets() -> dict:
    """
    Dataset 전체 인덱싱

    Returns:
        인덱싱 결과 dict
    """
    datasets_count = await index_datasets()

    return {
        'datasets': datasets_count,
        'total': datasets_count
    }


__all__ = [
    'index_datasets',
    'index_single_dataset',
    'delete_dataset_from_index',
    'index_all_datasets',
    'extract_node_metadata'
]
