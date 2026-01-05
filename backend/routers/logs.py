from fastapi import APIRouter, HTTPException, Body
from schemas.log_schema import (
    ApacheLogSchema,
    LogFormatGuide,
    TestConnectionRequest,
    PreviewLogsRequest,
    TransformLogsRequest
)
from utils.log_utils import (
    check_connection,
    read_apache_logs,
    extract_schema_from_apache_log,
    transform_apache_logs
)
import os
import re

router = APIRouter(prefix="/api/logs", tags=["logs"])

# 환경 변수
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"


def parse_s3_uri(uri: str):
    match = re.match(r"^s3://([^/]+)/(.+)$", uri or "")
    if not match:
        return None, None
    return match.group(1), match.group(2)


@router.get("/schema")
async def get_log_schema():
    """
    Apache Combined Log Format 스키마 정보 반환

    유저가 S3에 업로드할 로그의 권장 형식 제공
    """
    guide = LogFormatGuide()

    return {
        "format_name": guide.format_name,
        "format_pattern": guide.format_pattern,
        "example": guide.example,
        "description": guide.description,
        "fields": guide.fields,
        "schema": ApacheLogSchema.model_json_schema(),
        "schema_example": ApacheLogSchema.Config.json_schema_extra.get("example")
    }


@router.get("/health")
async def health_check():
    """로그 시스템 헬스 체크"""
    return {
        "status": "healthy",
        "storage_type": "s3" if USE_S3 else "local",
        "s3_configured": USE_S3,
        "log_format": "Apache Combined Log Format"
    }


@router.post("/test-connection")
async def test_connection(request: TestConnectionRequest):
    """
    S3/로컬 연결 테스트 (연결 확인만)

    ETL Visual > Source > S3 > "Create Connection" 시 사용
    유저의 S3 버킷에 Apache 로그 파일이 있는지 확인

    Args:
        request: 연결 테스트 요청 (JSON body)
            - bucket: 유저의 S3 버킷명 (필수!)
            - path: S3 경로 (필수!, 예: logs/)
            - region, access_key_id, secret_access_key: AWS 자격 증명 (선택)

    Returns:
        {
            "connection_valid": bool,
            "message": str,
            "storage_type": "s3" | "local"
        }
    """
    try:
        # AWS config 구성 (제공된 경우)
        aws_config = None
        if request.access_key_id and request.secret_access_key:
            aws_config = {
                'region': request.region,
                'access_key_id': request.access_key_id,
                'secret_access_key': request.secret_access_key
            }

        result = check_connection(request.bucket, request.path, aws_config)
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Connection test failed: {str(e)}"
        )


@router.post("/preview")
async def preview_logs_data(request: PreviewLogsRequest):
    """
    S3/로컬 Apache 로그 미리보기 (스키마 + 샘플 데이터)

    ETL Visual > Transform Panel > "Preview Input" 시 사용
    유저의 S3에서 Apache 로그를 읽어서 스키마와 샘플 데이터 반환

    Args:
        request: 미리보기 요청 (JSON body)
            - bucket: 유저의 S3 버킷명 (필수!)
            - path: S3 경로 (필수!)
            - limit: 미리보기 할 로그 개수 (기본값: 5)
            - region, access_key_id, secret_access_key: AWS 자격 증명 (선택)

    Returns:
        {
            "schema": [{"key": "client_ip", "type": "string"}, ...],
            "sample_data": [...],
            "total_files": int,
            "total_logs_previewed": int
        }
    """
    try:
        # AWS config 구성 (제공된 경우)
        aws_config = None
        if request.access_key_id and request.secret_access_key:
            aws_config = {
                'region': request.region,
                'access_key_id': request.access_key_id,
                'secret_access_key': request.secret_access_key
            }

        # 1. 유저 S3에서 Apache 로그 읽기
        result = read_apache_logs(request.bucket, request.path, request.limit, aws_config)
        logs_data = result["logs_data"]
        total_files = result["total_files"]

        if not logs_data:
            raise HTTPException(
                status_code=404,
                detail="No Apache log data found"
            )

        # 2. 스키마 반환
        schema = extract_schema_from_apache_log()

        return {
            "schema": schema,
            "sample_data": logs_data[:request.limit],
            "total_files": total_files,
            "total_logs_previewed": len(logs_data)
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Preview failed: {str(e)}"
        )


@router.post("/transform")
async def transform_logs_api(request: TransformLogsRequest):
    """
    Apache 로그 Transform 실행

    유저 S3 Apache 로그 → Parquet 변환 → 타겟 S3 저장

    Args:
        request: Transform 요청 (JSON body)
            - source_bucket: 소스 S3 버킷 (로그 원본, 필수!)
            - source_path: 소스 S3 경로 (필수!)
            - target_bucket: 타겟 S3 버킷 (저장 위치, 선택)
            - target_path: 타겟 S3 경로 (필수! s3://bucket/path 형식도 가능)
            - selected_fields: 선택할 필드 리스트 (선택)
            - filters: 필터 조건 dict (선택)

    Returns:
        {
            "status": "success" | "failed",
            "records_processed": int,
            "output_path": str,
            "source_info": {...},
            "transform_info": {...},
            "target_info": {...}
        }
    """
    source_bucket = request.source_bucket
    source_path = request.source_path
    target_bucket = request.target_bucket
    target_path = request.target_path
    selected_fields = request.selected_fields
    filters = request.filters

    # Debug logging
    print(f"[DEBUG] Received transform request:")
    print(f"  source_bucket: {source_bucket}")
    print(f"  source_path: {source_path}")
    print(f"  target_bucket: {target_bucket}")
    print(f"  target_path: {target_path}")

    # target_path가 s3:// URI 형식이면 파싱
    if not target_bucket and target_path:
        print(f"[DEBUG] Attempting to parse S3 URI: {target_path}")
        parsed_bucket, parsed_path = parse_s3_uri(target_path)
        print(f"[DEBUG] Parsed result: bucket={parsed_bucket}, path={parsed_path}")
        if parsed_bucket and parsed_path:
            target_bucket = parsed_bucket
            target_path = parsed_path
            print(f"[DEBUG] Updated: target_bucket={target_bucket}, target_path={target_path}")

    # 모든 파라미터 필수 체크
    if not source_bucket:
        raise HTTPException(
            status_code=400,
            detail="source_bucket is required. Please specify where to read the logs from."
        )

    if not source_path:
        raise HTTPException(
            status_code=400,
            detail="source_path is required. Please specify which path to read from."
        )

    if not target_bucket:
        raise HTTPException(
            status_code=400,
            detail="target_bucket is required. Please specify where to save the transformed data."
        )

    if not target_path:
        raise HTTPException(
            status_code=400,
            detail="target_path is required. Please specify which path to save to."
        )

    try:
        # AWS config 구성 (제공된 경우)
        aws_config = None
        if request.access_key_id and request.secret_access_key:
            aws_config = {
                'region': request.region,
                'access_key_id': request.access_key_id,
                'secret_access_key': request.secret_access_key
            }

        result = transform_apache_logs(
            source_bucket=source_bucket,
            source_path=source_path,
            target_bucket=target_bucket,
            target_path=target_path,
            selected_fields=selected_fields,
            filters=filters,
            aws_config=aws_config
        )

        if result["status"] == "failed":
            raise HTTPException(
                status_code=500,
                detail=result.get("error", "Transform failed")
            )

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Transform failed: {str(e)}"
        )
