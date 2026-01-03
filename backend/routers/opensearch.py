"""
OpenSearch 관련 API 엔드포인트
- 수동 인덱싱 트리거
- 검색 쿼리 처리
- 상태 확인
- Bulk Reindex
"""
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, Literal, List
from schemas.opensearch import (
    IndexingResult, DomainDocument, DomainSearchResult,
    ReindexRequest, ReindexResult, StatusResponse
)
from utils.indexers import index_all_domains_and_jobs
from utils.opensearch_client import (
    get_opensearch_client, DOMAIN_INDEX,
    delete_domain_index, create_domain_index
)

router = APIRouter()


@router.get("/status", response_model=StatusResponse)
async def get_status():
    """
    OpenSearch 상태 확인
    연결 상태, 인덱스 존재 여부, 문서 수 확인
    """
    try:
        opensearch = get_opensearch_client()

        opensearch_connected = False
        domain_index_exists = False
        total_documents = 0
        domain_documents = 0
        etl_job_documents = 0

        try:
            opensearch.cluster.health()
            opensearch_connected = True

            # Domain 인덱스 확인
            domain_index_exists = opensearch.indices.exists(index=DOMAIN_INDEX)
            if domain_index_exists:
                # 총 문서 수
                count_response = opensearch.count(index=DOMAIN_INDEX)
                total_documents = count_response['count']

                # Domain 문서 수
                domain_response = opensearch.count(
                    index=DOMAIN_INDEX,
                    body={"query": {"term": {"doc_type": "domain"}}}
                )
                domain_documents = domain_response['count']

                # ETL Job 문서 수
                etl_response = opensearch.count(
                    index=DOMAIN_INDEX,
                    body={"query": {"term": {"doc_type": "etl_job"}}}
                )
                etl_job_documents = etl_response['count']

        except Exception as e:
            print(f"OpenSearch status check error: {e}")

        if opensearch_connected and domain_index_exists and total_documents > 0:
            status = "healthy"
        elif opensearch_connected and domain_index_exists:
            status = "degraded"
        else:
            status = "unhealthy"

        return StatusResponse(
            status=status,
            opensearch_connected=opensearch_connected,
            domain_index_exists=domain_index_exists,
            total_documents=total_documents,
            domain_documents=domain_documents,
            etl_job_documents=etl_job_documents
        )

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Status check failed: {str(e)}")


@router.post("/index", response_model=IndexingResult, status_code=201)
async def trigger_indexing():
    """
    수동 인덱싱 트리거
    Domain과 ETL Job을 OpenSearch에 인덱싱
    """
    try:
        result = await index_all_domains_and_jobs()
        
        return IndexingResult(
            domains=result['domains'],
            etl_jobs=result['etl_jobs'],
            total=result['total']
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Indexing failed: {str(e)}")


@router.post("/reindex", response_model=ReindexResult)
async def reindex(request: ReindexRequest = None):
    """
    인덱스 재생성 (삭제 후 재인덱싱)
    스키마 변경 후 사용
    """
    try:
        delete_existing = request.delete_existing if request else True
        
        if delete_existing:
            delete_domain_index()
            create_domain_index()
        
        result = await index_all_domains_and_jobs()
        
        return ReindexResult(
            success=True,
            domains_indexed=result['domains'],
            etl_jobs_indexed=result['etl_jobs'],
            total=result['total'],
            message="Reindexing completed successfully"
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Reindexing failed: {str(e)}")


@router.get("/search", response_model=DomainSearchResult)
async def search(
    q: str = Query(..., min_length=1, description="검색어"),
    doc_type: Optional[Literal['domain', 'etl_job']] = Query(None, description="문서 타입 필터"),
    tags: Optional[List[str]] = Query(None, description="태그 필터"),
    limit: int = Query(20, ge=1, le=100, description="결과 개수 제한"),
    offset: int = Query(0, ge=0, description="페이지네이션 오프셋")
):
    """
    Domain/ETL Job 통합 검색
    이름, 설명으로 검색
    """
    try:
        opensearch = get_opensearch_client()

        query = {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": q,
                            "fields": ["name^3", "description^2"],
                            "type": "best_fields",
                            "fuzziness": "AUTO"
                        }
                    }
                ],
                "filter": []
            }
        }

        if doc_type:
            query["bool"]["filter"].append({"term": {"doc_type": doc_type}})

        if tags:
            query["bool"]["filter"].append({"terms": {"tags": tags}})

        response = opensearch.search(
            index=DOMAIN_INDEX,
            body={
                "query": query,
                "size": limit,
                "from": offset,
                "sort": [
                    {"_score": {"order": "desc"}},
                    {"updated_at": {"order": "desc"}}
                ]
            }
        )

        total = response['hits']['total']['value']
        hits = response['hits']['hits']

        results = [DomainDocument(**hit['_source']) for hit in hits]

        return DomainSearchResult(total=total, results=results)

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")
