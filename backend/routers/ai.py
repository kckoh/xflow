"""
AI Query Assistant API 엔드포인트
자연어 → SQL 변환 (Text-to-SQL)
"""
from fastapi import APIRouter, HTTPException, Query, Request
from schemas.ai import (
    GenerateSQLRequest, GenerateSQLResponse,
    SchemaSearchResult, SchemaSearchResponse
)
from services.opensearch_rag_service import get_rag_service
from services.bedrock_service import get_bedrock_service
from utils.limiter import limiter

router = APIRouter()


@router.post("/generate-sql", response_model=GenerateSQLResponse)
@limiter.limit("100/minute")
async def generate_sql(request: Request, body: GenerateSQLRequest):
    """
    자연어 질문을 SQL로 변환
    
    Supports two modes:
    1. Legacy: OpenSearch RAG + Bedrock (prompt_type='general')
    2. New: Direct prompt templates (field_transform, sql_transform, sql_lab, partition)
    """
    try:
        bedrock_service = get_bedrock_service()
        
        # New mode: Use prompt templates
        if body.prompt_type != 'general':
            sql = bedrock_service.generate_sql(
                question=body.question,
                prompt_type=body.prompt_type,
                metadata=body.metadata or {}
            )
            return GenerateSQLResponse(
                sql=sql,
                schema_context=f"Prompt type: {body.prompt_type}"
            )
        
        # Legacy mode: OpenSearch RAG
        rag_service = get_rag_service()
        results, schema_context = rag_service.search_schema(body.question)
        
        # 검색 결과 없으면 빈 컨텍스트로 진행
        if not results:
            schema_context = "No matching schemas found."
        
        sql = bedrock_service.generate_sql(
            question=body.question,
            schema_context=schema_context,
            additional_context=body.context
        )
        
        return GenerateSQLResponse(
            sql=sql,
            schema_context=schema_context
        )
        
    except Exception as e:
        print(f"Generate SQL error: {e}")
        raise HTTPException(
            status_code=500,
            detail="SQL generation failed. Please try again."
        )


@router.get("/search-schema", response_model=SchemaSearchResponse)
async def search_schema(
    q: str = Query(..., min_length=1, description="검색어"),
    limit: int = Query(5, ge=1, le=20, description="결과 개수")
):
    """
    OpenSearch에서 스키마 검색

    질문이나 키워드로 관련 테이블/컬럼 정보 검색
    """
    try:
        rag_service = get_rag_service()
        results, context = rag_service.search_schema(q, limit)

        search_results = [
            SchemaSearchResult(
                name=r['name'],
                doc_type=r['doc_type'],
                description=r.get('description'),
                columns=r.get('columns', []),
                score=r['score']
            )
            for r in results
        ]

        return SchemaSearchResponse(
            total=len(search_results),
            results=search_results,
            context=context
        )

    except Exception as e:
        print(f"Search schema error: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Schema search failed: {str(e)}"
        )


@router.get("/health")
async def health_check():
    """
    AI 서비스 헬스 체크
    OpenSearch 및 Bedrock 연결 상태 확인
    """
    status = {
        "opensearch": False,
        "bedrock": "unknown"  # Bedrock는 실제 호출 시에만 확인 가능
    }

    try:
        # OpenSearch 연결 확인
        rag_service = get_rag_service()
        rag_service.client.cluster.health()
        status["opensearch"] = True
    except Exception as e:
        status["opensearch_error"] = str(e)

    # Bedrock 초기화 확인
    try:
        bedrock_service = get_bedrock_service()
        status["bedrock"] = "initialized"
        status["bedrock_model"] = bedrock_service.model_id
        status["bedrock_region"] = bedrock_service.region
    except Exception as e:
        status["bedrock"] = "error"
        status["bedrock_error"] = str(e)

    return status
