import os
import duckdb

# S3 설정 (환경변수에서 가져오기)
S3_ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localstack-main:4566").replace("http://", "")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
S3_REGION = os.getenv("AWS_REGION", "ap-northeast-2")


def get_duckdb_connection():
    """DuckDB 연결 생성 및 S3 설정"""
    conn = duckdb.connect()

    # httpfs 확장 설치 및 로드
    conn.execute("INSTALL httpfs; LOAD httpfs;")

    # S3 (LocalStack) 설정
    conn.execute(f"SET s3_endpoint='{S3_ENDPOINT}';")
    conn.execute(f"SET s3_access_key_id='{S3_ACCESS_KEY}';")
    conn.execute(f"SET s3_secret_access_key='{S3_SECRET_KEY}';")
    conn.execute(f"SET s3_region='{S3_REGION}';")
    conn.execute("SET s3_use_ssl=false;")
    conn.execute("SET s3_url_style='path';")

    return conn


def execute_query(sql: str) -> list[dict]:
    """SQL 쿼리 실행 후 결과 반환"""
    conn = get_duckdb_connection()
    result = conn.execute(sql).fetchdf()
    return result.to_dict(orient="records")


def get_schema(s3_path: str) -> list[dict]:
    """S3 경로의 Parquet 파일 스키마 조회"""
    conn = get_duckdb_connection()
    result = conn.execute(f"DESCRIBE SELECT * FROM '{s3_path}'").fetchdf()
    return result.to_dict(orient="records")


def preview_data(s3_path: str, limit: int = 100) -> list[dict]:
    """S3 경로의 데이터 미리보기"""
    conn = get_duckdb_connection()
    result = conn.execute(f"SELECT * FROM '{s3_path}' LIMIT {limit}").fetchdf()
    return result.to_dict(orient="records")
