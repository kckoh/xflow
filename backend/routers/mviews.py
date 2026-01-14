"""
Materialized Views - Trino Native

이 라우터는 Trino의 네이티브 Materialized View 기능을 활용합니다.
사용자가 SQL로 직접 CREATE/REFRESH/DROP을 실행하고,
XFlow는 목록 조회와 정보 제공만 담당합니다.
"""

from fastapi import APIRouter, HTTPException

from services.trino_client import execute_trino_query

router = APIRouter()


@router.get("")
async def list_materialized_views():
    """
    Trino에서 Materialized View 목록 조회

    사용자가 SQL로 직접 생성한 MView들을 보여줍니다.

    Example SQL (사용자가 QueryEditor에서 실행):
        CREATE MATERIALIZED VIEW lakehouse.default.daily_sales AS
        SELECT date, SUM(amount) as total FROM orders GROUP BY date;
    """
    try:
        # Trino system catalog에서 MView 목록 조회
        result = execute_trino_query("""
            SELECT
                table_name as name,
                storage_catalog as catalog,
                storage_schema as schema,
                freshness,
                comment
            FROM system.metadata.materialized_views
            WHERE storage_catalog = 'lakehouse'
            ORDER BY table_name
        """)

        return result if result else []

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to list materialized views: {str(e)}")


@router.get("/{mview_name}")
async def get_materialized_view_info(mview_name: str):
    """
    특정 Materialized View의 상세 정보 조회

    Args:
        mview_name: MView 이름
    """
    try:
        # MView 정보 조회
        info = execute_trino_query(f"""
            SELECT
                table_name as name,
                storage_catalog as catalog,
                storage_schema as schema,
                freshness,
                comment
            FROM system.metadata.materialized_views
            WHERE storage_catalog = 'lakehouse'
              AND table_name = '{mview_name}'
        """)

        if not info:
            raise HTTPException(status_code=404, detail="Materialized view not found")

        # 행 수 조회
        count = execute_trino_query(f"""
            SELECT COUNT(*) as row_count
            FROM lakehouse.default.{mview_name}
        """)

        result = info[0]
        if count:
            result['row_count'] = count[0]['row_count']

        return result

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/examples/sql")
async def get_mview_examples():
    """
    Materialized View 사용 예제 반환

    사용자가 QueryEditor에서 참고할 수 있는 SQL 예제들
    """
    return {
        "examples": [
            {
                "title": "MView 생성",
                "description": "집계 쿼리 결과를 Materialized View로 저장하여 성능 향상",
                "sql": """CREATE MATERIALIZED VIEW lakehouse.default.daily_sales_summary AS
SELECT
    date_trunc('day', order_date) as day,
    COUNT(*) as order_count,
    SUM(amount) as total_amount
FROM lakehouse.default.orders
GROUP BY 1;"""
            },
            {
                "title": "MView 조회",
                "description": "일반 테이블처럼 빠르게 조회",
                "sql": """SELECT * FROM lakehouse.default.daily_sales_summary
ORDER BY day DESC
LIMIT 10;"""
            },
            {
                "title": "MView Refresh",
                "description": "소스 데이터 변경 시 수동으로 갱신",
                "sql": """REFRESH MATERIALIZED VIEW lakehouse.default.daily_sales_summary;"""
            },
            {
                "title": "MView 삭제",
                "description": "더 이상 필요없으면 삭제",
                "sql": """DROP MATERIALIZED VIEW IF EXISTS lakehouse.default.daily_sales_summary;"""
            },
            {
                "title": "모든 MView 목록 확인",
                "description": "현재 존재하는 MView 확인",
                "sql": """SELECT table_name, freshness, comment
FROM system.metadata.materialized_views
WHERE storage_catalog = 'lakehouse';"""
            }
        ]
    }
