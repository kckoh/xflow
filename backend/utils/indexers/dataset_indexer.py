"""
Dataset 인덱서
MongoDB의 datasets 컬렉션을 OpenSearch에 인덱싱
"""
from datetime import datetime
from opensearchpy import helpers
from utils.opensearch_client import get_opensearch_client, DOMAIN_INDEX
from utils.indexers.utils import extract_node_metadata
from models import Dataset


def build_dataset_document(dataset: Dataset) -> dict:
    """
    Dataset 객체를 OpenSearch 문서로 변환
    """
    node_meta = extract_node_metadata(dataset.nodes or [])

    # Extract S3 path from destination and append dataset name for full path
    s3_path = None
    if dataset.destination:
        dest = dataset.destination
        base_path = None
        if isinstance(dest, dict):
            base_path = dest.get('path')
        elif hasattr(dest, 'path'):
            base_path = dest.path

        if base_path:
            # Construct full path: base_path + dataset_name/*.parquet
            # e.g., s3://xflow-benchmark/ + 1gb -> s3://xflow-benchmark/1gb/*.parquet
            base_path = base_path.rstrip('/')
            s3_path = f"{base_path}/{dataset.name}/*.parquet"

    return {
        'doc_id': str(dataset.id),
        'doc_type': 'dataset',
        'name': dataset.name,
        'description': dataset.description,
        'status': dataset.status,
        's3_path': s3_path,  # S3 Parquet path for DuckDB queries
        'node_descriptions': node_meta['node_descriptions'],
        'node_tags': node_meta['node_tags'],
        'column_names': node_meta['column_names'],
        'column_descriptions': node_meta['column_descriptions'],
        'column_tags': node_meta['column_tags'],
        'created_at': dataset.created_at.isoformat() if dataset.created_at else None,
        'updated_at': dataset.updated_at.isoformat() if dataset.updated_at else None,
        'last_indexed': datetime.utcnow().isoformat()
    }


async def index_single_dataset(dataset: Dataset) -> bool:
    """
    단일 Dataset을 OpenSearch에 인덱싱 (Dual Write용)

    Args:
        dataset: Dataset 객체

    Returns:
        성공 여부
    """
    try:
        opensearch = get_opensearch_client()
        doc = build_dataset_document(dataset)

        opensearch.index(
            index=DOMAIN_INDEX,
            id=f"dataset_{doc['doc_id']}",
            body=doc
        )
        print(f"Dataset indexed: {dataset.name}")
        return True

    except Exception as e:
        print(f"Dataset indexing error: {e}")
        return False


async def delete_dataset_from_index(dataset_id: str) -> bool:
    """
    OpenSearch에서 Dataset 삭제 (Dual Write용)

    Args:
        dataset_id: Dataset ID

    Returns:
        성공 여부
    """
    try:
        opensearch = get_opensearch_client()
        opensearch.delete(
            index=DOMAIN_INDEX,
            id=f"dataset_{dataset_id}",
            ignore=[404]
        )
        print(f"Dataset deleted from index: {dataset_id}")
        return True

    except Exception as e:
        print(f"Dataset delete error: {e}")
        return False


async def index_datasets() -> int:
    """
    MongoDB datasets 컬렉션 전체를 OpenSearch에 인덱싱

    Returns:
        인덱싱된 문서 수
    """
    try:
        datasets = await Dataset.find_all().to_list()

        if not datasets:
            print("No datasets found to index")
            return 0

        documents = [build_dataset_document(dataset) for dataset in datasets]

        opensearch = get_opensearch_client()
        actions = [
            {
                '_index': DOMAIN_INDEX,
                '_id': f"dataset_{doc['doc_id']}",
                '_source': doc
            }
            for doc in documents
        ]
        helpers.bulk(opensearch, actions)
        print(f"Datasets: {len(documents)} documents indexed")

        return len(documents)

    except Exception as e:
        print(f"Dataset indexing error: {e}")
        return 0
