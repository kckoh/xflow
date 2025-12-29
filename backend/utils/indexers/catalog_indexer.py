"""
통합 Catalog 인덱서
모든 데이터 소스 (S3, MongoDB) 인덱싱
"""
import asyncio
from .s3_indexer import index_s3_metadata
from .mongodb_indexer import index_mongodb_metadata
from schemas.opensearch import IndexingResult


async def index_all_sources() -> IndexingResult:
    """
    모든 데이터 소스 병렬 인덱싱
    S3 + MongoDB

    Returns:
        인덱싱 결과 (Pydantic 스키마)
    """
    print("🔄 Starting catalog indexing...")

    # 병렬 실행
    results = await asyncio.gather(
        index_s3_metadata(),
        index_mongodb_metadata(),
        return_exceptions=True
    )

    # 결과 정리
    s3_count = results[0] if not isinstance(results[0], Exception) else 0
    mongodb_count = results[1] if not isinstance(results[1], Exception) else 0

    result = IndexingResult(
        s3=s3_count,
        mongodb=mongodb_count,
        total=s3_count + mongodb_count
    )

    print(f"Total indexed: {result.total} documents")
    print(f"   - S3: {result.s3}")
    print(f"   - MongoDB: {result.mongodb}")

    return result
