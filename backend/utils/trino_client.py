import os
from trino.dbapi import connect
from trino.auth import BasicAuthentication

# Trino 설정
TRINO_HOST = os.getenv("TRINO_HOST", "trino-cluster-trino.default.svc")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")


def get_trino_connection(catalog: str = None, schema: str = None):
    """Trino 연결 생성"""
    conn = connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=catalog or TRINO_CATALOG,
        schema=schema or TRINO_SCHEMA,
    )
    return conn


def execute_query(sql: str, catalog: str = None, schema: str = None) -> list[dict]:
    """SQL 쿼리 실행 후 결과 반환"""
    conn = get_trino_connection(catalog, schema)
    cursor = conn.cursor()
    cursor.execute(sql)

    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    # Convert rows to list of dicts
    data = []
    for row in rows:
        row_dict = {}
        for col, val in zip(columns, row):
            row_dict[col] = val
        data.append(row_dict)

    cursor.close()
    conn.close()
    return data


def get_catalogs() -> list[str]:
    """사용 가능한 카탈로그 목록 조회"""
    conn = get_trino_connection()
    cursor = conn.cursor()
    cursor.execute("SHOW CATALOGS")
    catalogs = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return catalogs


def get_schemas(catalog: str) -> list[str]:
    """카탈로그의 스키마 목록 조회"""
    conn = get_trino_connection(catalog=catalog)
    cursor = conn.cursor()
    cursor.execute(f"SHOW SCHEMAS IN {catalog}")
    schemas = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return schemas


def get_tables(catalog: str, schema: str) -> list[str]:
    """스키마의 테이블 목록 조회"""
    conn = get_trino_connection(catalog=catalog, schema=schema)
    cursor = conn.cursor()
    cursor.execute(f"SHOW TABLES IN {catalog}.{schema}")
    tables = [row[0] for row in cursor.fetchall()]
    cursor.close()
    conn.close()
    return tables


def get_table_schema(catalog: str, schema: str, table: str) -> list[dict]:
    """테이블 스키마 조회"""
    conn = get_trino_connection(catalog=catalog, schema=schema)
    cursor = conn.cursor()
    cursor.execute(f"DESCRIBE {catalog}.{schema}.{table}")
    columns = []
    for row in cursor.fetchall():
        columns.append({
            "column_name": row[0],
            "data_type": row[1],
            "extra": row[2] if len(row) > 2 else None,
            "comment": row[3] if len(row) > 3 else None,
        })
    cursor.close()
    conn.close()
    return columns


def preview_table(catalog: str, schema: str, table: str, limit: int = 100) -> list[dict]:
    """테이블 데이터 미리보기"""
    sql = f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}"
    return execute_query(sql, catalog=catalog, schema=schema)
