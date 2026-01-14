import os
import trino

TRINO_HOST = os.getenv("TRINO_HOST", "trino-cluster-trino.default.svc.cluster.local")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "lakehouse")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "default")


def get_trino_connection():
    """Trino 연결"""
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def execute_trino_query(sql: str):
    """
    Trino 쿼리 실행

    Args:
        sql: SQL 쿼리

    Returns:
        SELECT 쿼리: List[dict] 결과
        DDL/DML: None
    """
    conn = get_trino_connection()
    cursor = conn.cursor()

    try:
        cursor.execute(sql)

        # SELECT 쿼리인 경우 결과 반환
        if sql.strip().upper().startswith("SELECT"):
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(columns, row)) for row in rows]

        return None

    finally:
        cursor.close()
        conn.close()
