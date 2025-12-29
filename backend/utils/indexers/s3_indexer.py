"""
S3 (Glue Catalog) 메타데이터 인덱서
"""
import asyncio
from datetime import datetime
from typing import List
from opensearchpy import helpers
from utils.opensearch_client import get_opensearch_client, CATALOG_INDEX
from utils.aws_client import get_aws_client
from schemas.opensearch import CatalogDocument


async def index_s3_metadata() -> int:
    """
    S3 (Glue Catalog) 메타데이터 인덱싱
    데이터베이스 → 테이블 → 컬럼 순회

    Returns:
        인덱싱된 문서 수
    """
    def _fetch_and_index():
        glue = get_aws_client('glue')
        opensearch = get_opensearch_client()
        documents: List[CatalogDocument] = []

        try:
            # 1. 모든 데이터베이스 조회
            databases_response = glue.get_databases()
            databases = databases_response.get('DatabaseList', [])

            for db in databases:
                db_name = db['Name']

                # 2. 각 데이터베이스의 테이블 조회
                tables_response = glue.get_tables(DatabaseName=db_name)
                tables = tables_response.get('TableList', [])

                for table in tables:
                    table_name = table['Name']
                    storage_desc = table.get('StorageDescriptor', {})
                    columns = storage_desc.get('Columns', [])
                    location = storage_desc.get('Location', '')

                    # 테이블 메타데이터 추출 (Glue Parameters)
                    table_params = table.get('Parameters', {})
                    owner = table_params.get('owner', None)
                    domain = table_params.get('domain', db_name)  # Fallback: DB 이름
                    tags_str = table_params.get('tags', '')
                    tags = [t.strip() for t in tags_str.split(',') if t.strip()] if tags_str else None

                    # 3. 각 컬럼을 Pydantic 스키마로 변환
                    for column in columns:
                        doc = CatalogDocument(
                            source='s3',
                            source_type='table',
                            database=db_name,
                            resource_name=table_name,
                            field_name=column['Name'],
                            field_type=column['Type'],
                            description=column.get('Comment', None),
                            location=location,
                            tag=tags,
                            owner=owner,
                            domain=domain,
                            last_indexed=datetime.utcnow()
                        )
                        documents.append(doc)

            # 4. Bulk 인덱싱
            if documents:
                actions = [
                    {
                        '_index': CATALOG_INDEX,
                        '_source': doc.model_dump(mode='json')
                    }
                    for doc in documents
                ]
                helpers.bulk(opensearch, actions)
                print(f"S3 (Glue): {len(documents)} documents indexed")

            return len(documents)

        except Exception as e:
            print(f"S3 indexing error: {e}")
            return 0

    # 동기 함수를 비동기로 실행
    return await asyncio.to_thread(_fetch_and_index)
