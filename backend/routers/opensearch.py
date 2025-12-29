"""
OpenSearch 관련 API 엔드포인트
- 수동 인덱싱 트리거
- 검색 쿼리 처리
- 상태 확인
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Literal
from schemas.opensearch import IndexingResult, SearchQuery, SearchResult, CatalogDocument, StatusResponse
from utils.indexers import index_all_sources
from utils.opensearch_client import get_opensearch_client, CATALOG_INDEX

router = APIRouter()


@router.get("/status", response_model=StatusResponse)
async def get_status():
    """
    OpenSearch 상태 확인
    연결 상태, 인덱스 존재 여부, 문서 수 확인

    Returns:
        StatusResponse: OpenSearch 및 인덱스 상태 정보
    """
    try:
        opensearch = get_opensearch_client()

        # OpenSearch 연결 확인
        opensearch_connected = False
        index_exists = False
        total_documents = 0
        s3_documents = 0
        mongodb_documents = 0

        try:
            # Cluster health 확인
            opensearch.cluster.health()
            opensearch_connected = True

            # 인덱스 존재 확인
            index_exists = opensearch.indices.exists(index=CATALOG_INDEX)

            if index_exists:
                # 총 문서 수
                count_response = opensearch.count(index=CATALOG_INDEX)
                total_documents = count_response['count']

                # S3 문서 수
                s3_response = opensearch.count(
                    index=CATALOG_INDEX,
                    body={"query": {"term": {"source": "s3"}}}
                )
                s3_documents = s3_response['count']

                # MongoDB 문서 수
                mongodb_response = opensearch.count(
                    index=CATALOG_INDEX,
                    body={"query": {"term": {"source": "mongodb"}}}
                )
                mongodb_documents = mongodb_response['count']

        except Exception as e:
            print(f"OpenSearch status check error: {e}")

        # 전체 상태 판단
        if opensearch_connected and index_exists and total_documents > 0:
            status = "healthy"
        elif opensearch_connected and index_exists:
            status = "degraded"
        else:
            status = "unhealthy"

        return StatusResponse(
            status=status,
            opensearch_connected=opensearch_connected,
            index_exists=index_exists,
            total_documents=total_documents,
            s3_documents=s3_documents,
            mongodb_documents=mongodb_documents
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Status check failed: {str(e)}"
        )


@router.post("/index", response_model=IndexingResult)
async def trigger_indexing():
    """
    수동 인덱싱 트리거
    모든 데이터 소스 (S3, MongoDB) 메타데이터를 OpenSearch에 인덱싱

    Returns:
        IndexingResult: 인덱싱된 문서 수 (소스별 + 총합)
    """
    try:
        result = await index_all_sources()
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Indexing failed: {str(e)}"
        )


@router.get("/search", response_model=SearchResult)
async def search_catalog(
    q: str = Query(..., min_length=1, description="검색어"),
    source: Optional[Literal['s3', 'mongodb']] = Query(None, description="특정 소스만 검색"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한")
):
    """
    데이터 카탈로그 검색
    데이터베이스, 테이블/컬렉션, 필드 이름 전문 검색

    Args:
        q: 검색어
        source: 특정 소스만 검색 (선택)
        limit: 결과 개수 제한 (1-100)

    Returns:
        SearchResult: 검색 결과 (총 개수 + 문서 리스트)
    """
    try:
        opensearch = get_opensearch_client()

        # OpenSearch 쿼리 구성
        query = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["database^3", "resource_name^2", "field_name^1"],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    }
                ]
            }
        }

        # 소스 필터 추가 (옵션)
        if source:
            query["bool"]["filter"] = [
                {"term": {"source": source}}
            ]

        # 검색 실행
        response = opensearch.search(
            index=CATALOG_INDEX,
            body={
                "query": query,
                "size": limit,
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"last_indexed": {"order": "desc"}}
                ]
            }
        )

        # 결과 파싱
        total = response['hits']['total']['value']
        hits = response['hits']['hits']

        results = [
            CatalogDocument(**hit['_source'])
            for hit in hits
        ]

        return SearchResult(
            total=total,
            results=results
        )

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Search failed: {str(e)}"
        )
