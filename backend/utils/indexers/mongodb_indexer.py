"""
MongoDB 메타데이터 인덱서
"""
import asyncio
from datetime import datetime
from typing import List
from opensearchpy import helpers
from utils.opensearch_client import get_opensearch_client, CATALOG_INDEX
import database
from schemas.opensearch import CatalogDocument


async def index_mongodb_metadata() -> int:
    """
    MongoDB 메타데이터 인덱싱
    데이터베이스 → 컬렉션 → 필드 순회 (샘플링으로 스키마 추론)

    Returns:
        인덱싱된 문서 수
    """
    documents: List[CatalogDocument] = []

    try:
        # 기존 글로벌 MongoDB 클라이언트 재사용
        if not database.mongodb_client:
            print("MongoDB client not initialized")
            return 0

        # 시스템 DB 제외
        db_names = await database.mongodb_client.list_database_names()
        db_names = [name for name in db_names if name not in ['admin', 'config', 'local']]

        for db_name in db_names:
            db = database.mongodb_client[db_name]
            collection_names = await db.list_collection_names()

            for collection_name in collection_names:
                collection = db[collection_name]

                # 샘플 문서로 스키마 추론 (최대 100개)
                sample_docs = await collection.find().limit(100).to_list(length=100)

                if not sample_docs:
                    continue

                # 모든 필드 수집
                all_fields = set()
                for doc in sample_docs:
                    all_fields.update(doc.keys())

                # 각 필드를 Pydantic 스키마로 변환
                for field_name in all_fields:
                    # 필드 타입 추론 (첫 번째 샘플에서)
                    field_value = None
                    for doc in sample_docs:
                        if field_name in doc:
                            field_value = doc[field_name]
                            break

                    field_type = type(field_value).__name__ if field_value is not None else 'unknown'

                    catalog_doc = CatalogDocument(
                        source='mongodb',
                        source_type='collection',
                        database=db_name,
                        resource_name=collection_name,
                        field_name=field_name,
                        field_type=field_type,
                        last_indexed=datetime.utcnow()
                    )
                    documents.append(catalog_doc)

        # Bulk 인덱싱 (동기 함수를 비동기로 실행)
        if documents:
            def _bulk_index():
                opensearch = get_opensearch_client()
                actions = [
                    {
                        '_index': CATALOG_INDEX,
                        '_source': doc.model_dump(mode='json')
                    }
                    for doc in documents
                ]
                helpers.bulk(opensearch, actions)

            await asyncio.to_thread(_bulk_index)
            print(f"MongoDB: {len(documents)} documents indexed")

        return len(documents)

    except Exception as e:
        print(f"MongoDB indexing error: {e}")
        return 0
