"""
Log Utility Functions

Apache Combined Log Format 파싱 및 Transform
"""

import re
from pathlib import Path
from typing import List, Dict, Any, Tuple
import os
import pandas as pd


# ============================================================
# 환경 변수 설정
# ============================================================
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"
LOCAL_LOG_DIR = os.getenv("LOCAL_LOG_DIR", "./data/logs")

# AWS 설정 (LocalStack 로컬 개발용 / 실제 AWS 배포용)
AWS_ENDPOINT = os.getenv("AWS_ENDPOINT", None)  # LocalStack: http://localstack-main:4566 / 실제 AWS: None
AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")


# ============================================================
# S3 클라이언트 초기화
# ============================================================
s3_client = None
if USE_S3:
    try:
        import boto3

        # LocalStack (로컬 개발 환경)용 설정
        if AWS_ENDPOINT:
            s3_client = boto3.client(
                's3',
                endpoint_url=AWS_ENDPOINT,  # LocalStack 엔드포인트
                region_name=AWS_REGION,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                use_ssl=False,  # LocalStack은 SSL 미사용
                verify=False    # 인증서 검증 비활성화
            )
            print(f"✅ S3 client initialized with LocalStack: {AWS_ENDPOINT}")

        # 실제 AWS S3 (프로덕션 배포 환경)용 설정
        else:
            # 배포 시: AWS_ENDPOINT 환경 변수를 제거하거나 빈 문자열로 설정
            # IAM Role 또는 ~/.aws/credentials 사용
            s3_client = boto3.client(
                's3',
                region_name=AWS_REGION
                # aws_access_key_id, aws_secret_access_key는 IAM Role에서 자동 로드
            )
            print(f"✅ S3 client initialized with AWS: {AWS_REGION}")

    except ImportError:
        print("⚠️  boto3 not installed. Using local file system.")
        USE_S3 = False
    except Exception as e:
        print(f"⚠️  Failed to initialize S3 client: {e}")
        USE_S3 = False


# Apache Combined Log Format 정규식 패턴
APACHE_LOG_PATTERN = re.compile(
    r'(?P<client_ip>\S+) '  # IP
    r'\S+ '  # ident (무시)
    r'\S+ '  # user (무시)
    r'\[(?P<timestamp>[^\]]+)\] '  # [10/Oct/2000:13:55:36 -0700]
    r'"(?P<http_method>\S+) '  # GET
    r'(?P<path>\S+) '  # /index.html
    r'(?P<http_version>[^"]+)" '  # HTTP/1.0
    r'(?P<status_code>\d+) '  # 200
    r'(?P<bytes_sent>\S+)'  # 2326 or -
    r'(?: "(?P<referrer>[^"]*)")?'  # "http://..." (optional)
    r'(?: "(?P<user_agent>[^"]*)")?'  # "Mozilla/..." (optional)
)


def normalize_s3_prefix(path: str) -> str:
    """
    S3 prefix를 정규화하고 검증한다.

    - 공백 제거
    - 앞/뒤 슬래시 제거
    - 디렉터리 prefix로 끝나도록 trailing slash 추가
    """
    cleaned = (path or "").strip()
    cleaned = cleaned.strip("/")
    if not cleaned:
        raise Exception("path is required. Please specify which path to read from.")
    return f"{cleaned}/"


def parse_apache_log_line(line: str) -> Dict[str, Any]:
    """
    Apache Combined Log Format 한 줄을 파싱

    Args:
        line: 로그 한 줄

    Returns:
        파싱된 dict 또는 None (파싱 실패 시)

    Example:
        >>> line = '127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /index.html HTTP/1.0" 200 2326 "http://example.com" "Mozilla/4.08"'
        >>> parse_apache_log_line(line)
        {
            'client_ip': '127.0.0.1',
            'timestamp': '10/Oct/2000:13:55:36 -0700',
            'http_method': 'GET',
            'path': '/index.html',
            'http_version': 'HTTP/1.0',
            'status_code': 200,
            'bytes_sent': 2326,
            'referrer': 'http://example.com',
            'user_agent': 'Mozilla/4.08'
        }
    """
    match = APACHE_LOG_PATTERN.match(line.strip())
    if not match:
        return None

    data = match.groupdict()

    # 데이터 타입 변환
    try:
        data['status_code'] = int(data['status_code'])
    except (ValueError, TypeError):
        data['status_code'] = 0

    # bytes_sent 처리 (- 는 0으로)
    try:
        bytes_sent = data.get('bytes_sent', '-')
        data['bytes_sent'] = 0 if bytes_sent == '-' else int(bytes_sent)
    except (ValueError, TypeError):
        data['bytes_sent'] = 0

    # None 값 처리
    data['referrer'] = data.get('referrer') or '-'
    data['user_agent'] = data.get('user_agent') or '-'

    return data


def extract_schema_from_apache_log() -> List[Dict[str, str]]:
    """
    Apache 로그의 스키마 반환

    Returns:
        스키마 리스트
    """
    return [
        {"key": "client_ip", "type": "string"},
        {"key": "timestamp", "type": "string"},
        {"key": "http_method", "type": "string"},
        {"key": "path", "type": "string"},
        {"key": "http_version", "type": "string"},
        {"key": "status_code", "type": "integer"},
        {"key": "bytes_sent", "type": "integer"},
        {"key": "referrer", "type": "string"},
        {"key": "user_agent", "type": "string"}
    ]


def read_apache_logs_from_s3(bucket: str, path: str, limit: int = None) -> Tuple[List[dict], int]:
    """
    S3에서 Apache 로그 읽기

    Args:
        bucket: S3 버킷명
        path: S3 경로
        limit: 읽을 로그 개수 (None이면 전체)

    Returns:
        (logs_data, total_files)
    """
    if not s3_client:
        raise Exception("S3 client not available")

    logs_data = []

    prefix = normalize_s3_prefix(path)

    # S3 파일 리스트
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix,
        MaxKeys=1000
    )

    if 'Contents' not in response:
        raise Exception(f"No files found in s3://{bucket}/{prefix}")

    files = response['Contents']
    total_files = len(files)

    for file_obj in files:
        key = file_obj['Key']

        # 로그 파일만 처리 (.log, .txt 등)
        if not (key.endswith('.log') or key.endswith('.txt')):
            continue

        # 파일 읽기
        file_response = s3_client.get_object(Bucket=bucket, Key=key)
        content = file_response['Body'].read().decode('utf-8')

        # 각 줄 파싱
        for line in content.strip().split('\n'):
            if not line.strip():
                continue

            parsed = parse_apache_log_line(line)
            if parsed:
                logs_data.append(parsed)

                # limit 체크
                if limit and len(logs_data) >= limit:
                    return logs_data, total_files

    return logs_data, total_files


def read_apache_logs_from_local(path: str, limit: int = None) -> Tuple[List[dict], int]:
    """
    로컬에서 Apache 로그 읽기

    Args:
        path: 로컬 경로
        limit: 읽을 로그 개수 (None이면 전체)

    Returns:
        (logs_data, total_files)
    """
    logs_data = []

    log_dir = Path(LOCAL_LOG_DIR) / path

    if not log_dir.exists():
        raise Exception(f"Path not found: {log_dir}")

    # 로그 파일 찾기
    log_files = list(log_dir.rglob("*.log")) + list(log_dir.rglob("*.txt"))

    total_files = len(log_files)

    if total_files == 0:
        raise Exception(f"No log files found in {log_dir}")

    for file_path in log_files:
        with open(file_path, 'r') as f:
            for line in f:
                if not line.strip():
                    continue

                parsed = parse_apache_log_line(line)
                if parsed:
                    logs_data.append(parsed)

                    # limit 체크
                    if limit and len(logs_data) >= limit:
                        return logs_data, total_files

    return logs_data, total_files


def read_apache_logs(bucket: str = None, path: str = None, limit: int = None) -> Dict[str, Any]:
    """
    S3 또는 로컬에서 Apache 로그 읽기 (통합 함수)

    Args:
        bucket: S3 버킷명 (필수!)
        path: 경로 (필수!)
        limit: 읽을 로그 개수 (옵션)

    Returns:
        {
            "logs_data": list,
            "total_files": int,
            "storage_type": str
        }
    """
    # bucket, path 필수 체크
    if not bucket:
        raise Exception("bucket is required. Please specify which bucket to read from.")

    if isinstance(path, str):
        path = path.strip()

    if not path:
        raise Exception("path is required. Please specify which path to read from.")

    try:
        if USE_S3 and s3_client:
            logs_data, total_files = read_apache_logs_from_s3(bucket, path, limit)
            storage_type = "s3"
        else:
            logs_data, total_files = read_apache_logs_from_local(path, limit)
            storage_type = "local"

        return {
            "logs_data": logs_data,
            "total_files": total_files,
            "storage_type": storage_type
        }
    except Exception as e:
        raise Exception(f"Failed to read Apache logs: {str(e)}")


def check_connection(bucket: str = None, path: str = None) -> Dict[str, Any]:
    """
    S3/로컬 연결 테스트

    Args:
        bucket: S3 버킷명 (필수!)
        path: 경로 (필수!)

    Returns:
        {
            "connection_valid": bool,
            "message": str,
            "storage_type": str
        }
    """
    # bucket, path 필수 체크
    if not bucket:
        raise Exception("bucket is required. Please specify which bucket to test.")

    if isinstance(path, str):
        path = path.strip()

    if not path:
        raise Exception("path is required. Please specify which path to test.")

    try:
        if USE_S3 and s3_client:
            # S3 연결 테스트
            prefix = normalize_s3_prefix(path)
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix,
                MaxKeys=1
            )

            if 'Contents' not in response or len(response['Contents']) == 0:
                return {
                    "connection_valid": False,
                    "message": f"No files found in s3://{bucket}/{prefix}",
                    "storage_type": "s3"
                }

            return {
                "connection_valid": True,
                "message": f"Successfully connected to s3://{bucket}/{prefix}",
                "storage_type": "s3"
            }
        else:
            # 로컬 파일 시스템 테스트
            log_dir = Path(LOCAL_LOG_DIR) / path

            if not log_dir.exists():
                return {
                    "connection_valid": False,
                    "message": f"Path not found: {log_dir}",
                    "storage_type": "local"
                }

            # 로그 파일 존재 여부
            log_files = list(log_dir.rglob("*.log")) + list(log_dir.rglob("*.txt"))

            if len(log_files) == 0:
                return {
                    "connection_valid": False,
                    "message": f"No log files found in {log_dir}",
                    "storage_type": "local"
                }

            return {
                "connection_valid": True,
                "message": f"Successfully connected to {log_dir}",
                "storage_type": "local"
            }

    except Exception as e:
        return {
            "connection_valid": False,
            "message": f"Connection error: {str(e)}",
            "storage_type": "s3" if USE_S3 else "local"
        }


def transform_apache_logs(
    source_bucket: str = None,
    source_path: str = None,
    target_bucket: str = None,
    target_path: str = None,
    selected_fields: List[str] = None,
    filters: Dict[str, Any] = None
) -> Dict[str, Any]:
    """
    Apache 로그 Transform 실행

    유저 S3 Apache 로그 → Parquet 변환 → 타겟 S3 저장

    Args:
        source_bucket: 소스 S3 버킷 (로그 원본, 필수!)
        source_path: 소스 S3 경로 (필수!)
        target_bucket: 타겟 S3 버킷 (저장 위치, 필수!)
        target_path: 타겟 S3 경로 (필수!)
        selected_fields: 선택할 필드 리스트 (선택, 기본값: 전체)
        filters: 필터 조건 dict (선택)
            - status_code_min: int (최소 status code)
            - status_code_max: int (최대 status code)
            - ip_patterns: List[str] (IP 패턴 리스트)
            - path_pattern: str (path regex 패턴)

    Returns:
        {
            "status": "success" | "failed",
            "records_processed": int,
            "output_path": str
        }
    """
    try:
        # 모든 파라미터 필수 체크
        if not source_bucket:
            raise Exception("source_bucket is required. Please specify where to read the logs from.")

        if not source_path:
            raise Exception("source_path is required. Please specify which path to read from.")

        if not target_bucket:
            raise Exception("target_bucket is required. Please specify where to save the transformed data.")

        if not target_path:
            raise Exception("target_path is required. Please specify which path to save to.")

        # 1. Apache 로그 읽기 (모든 로그)
        result = read_apache_logs(source_bucket, source_path, limit=None)
        logs_data = result["logs_data"]
        storage_type = result["storage_type"]

        if not logs_data:
            raise Exception("No log data found to transform")

        # 2. DataFrame 변환
        df = pd.DataFrame(logs_data)

        # 3. Transform: Select Fields
        if selected_fields and len(selected_fields) > 0:
            # Validate fields exist
            available_fields = df.columns.tolist()
            invalid_fields = [f for f in selected_fields if f not in available_fields]
            if invalid_fields:
                raise Exception(f"Invalid fields: {invalid_fields}. Available fields: {available_fields}")

            df = df[selected_fields]

        # 4. Transform: Apply Filters
        if filters:
            # Filter by status_code range
            if 'status_code_min' in filters and filters['status_code_min'] is not None:
                df = df[df['status_code'] >= filters['status_code_min']]

            if 'status_code_max' in filters and filters['status_code_max'] is not None:
                df = df[df['status_code'] <= filters['status_code_max']]

            # Filter by IP patterns
            if 'ip_patterns' in filters and filters['ip_patterns']:
                ip_mask = df['client_ip'].str.contains('|'.join(filters['ip_patterns']), regex=True, na=False)
                df = df[ip_mask]

            # Filter by path pattern
            if 'path_pattern' in filters and filters['path_pattern']:
                path_mask = df['path'].str.contains(filters['path_pattern'], regex=True, na=False)
                df = df[path_mask]

        # 5. Parquet 저장 (모든 값이 필수이므로 기본값 없음)

        if USE_S3 and s3_client:
            # 타겟 버킷 존재 여부 확인 (없으면 에러)
            try:
                s3_client.head_bucket(Bucket=target_bucket)
            except:
                raise Exception(
                    f"Target bucket '{target_bucket}' does not exist. "
                    f"Please create the bucket before running the transform."
                )

            # S3에 저장 (pandas + s3fs 사용)
            # Remove trailing slash from target_path to avoid double slashes
            clean_target_path = target_path.rstrip('/')
            output_path = f"s3://{target_bucket}/{clean_target_path}/data.parquet"

            # s3fs용 storage_options 설정 (LocalStack 지원)
            storage_options = {
                'key': AWS_ACCESS_KEY_ID,
                'secret': AWS_SECRET_ACCESS_KEY,
            }

            # LocalStack 사용 시 endpoint_url 추가
            if AWS_ENDPOINT:
                storage_options['client_kwargs'] = {
                    'endpoint_url': AWS_ENDPOINT,
                    'region_name': AWS_REGION
                }

            df.to_parquet(
                output_path,
                compression='snappy',
                index=False,
                storage_options=storage_options
            )
        else:
            # 로컬에 저장
            local_output = Path(LOCAL_LOG_DIR) / target_path / "data.parquet"
            local_output.parent.mkdir(parents=True, exist_ok=True)
            df.to_parquet(str(local_output), compression='snappy', index=False)
            output_path = str(local_output)

        return {
            "status": "success",
            "records_processed": len(df),
            "output_path": output_path,
            "source_info": {
                "bucket": source_bucket,
                "path": source_path,
                "storage_type": storage_type
            },
            "transform_info": {
                "selected_fields": selected_fields or "all",
                "filters": filters or {}
            },
            "target_info": {
                "bucket": target_bucket if USE_S3 else None,
                "path": target_path,
                "format": "parquet",
                "compression": "snappy"
            }
        }

    except Exception as e:
        return {
            "status": "failed",
            "error": str(e),
            "records_processed": 0
        }
