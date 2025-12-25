"""
Trino/Hive에 테이블 스키마 등록 스크립트

실행 방법:
    python create_hive_tables.py
"""

from trino.dbapi import connect
from trino.exceptions import TrinoUserError


def get_trino_connection():
    """Trino 연결"""
    return connect(
        host="localhost",
        port=8085,
        user="trino",
        catalog="hive",
        schema="default"
    )


def create_tables(cursor):
    """Hive 외부 테이블 생성"""

    tables = {
        'user_events': """
            CREATE TABLE IF NOT EXISTS user_events (
                user_id BIGINT,
                event_type VARCHAR,
                timestamp TIMESTAMP(3),
                amount DOUBLE,
                product_id VARCHAR,
                event_date DATE
            )
            WITH (
                external_location = 's3a://jungle-xflow/user_events/',
                format = 'PARQUET',
                partitioned_by = ARRAY['event_date']
            )
        """,
        'products': """
            CREATE TABLE IF NOT EXISTS products (
                product_id VARCHAR,
                product_name VARCHAR,
                category VARCHAR,
                price BIGINT,
                stock BIGINT
            )
            WITH (
                external_location = 's3a://jungle-xflow/products/',
                format = 'PARQUET'
            )
        """,
        'transactions': """
            CREATE TABLE IF NOT EXISTS transactions (
                transaction_id BIGINT,
                user_id BIGINT,
                product_id VARCHAR,
                quantity BIGINT,
                total_amount BIGINT,
                transaction_date TIMESTAMP(3)
            )
            WITH (
                external_location = 's3a://jungle-xflow/transactions/',
                format = 'PARQUET',
                partitioned_by = ARRAY['transaction_date']
            )
        """
    }

    for table_name, create_sql in tables.items():
        try:
            print(f"테이블 '{table_name}' 생성 중...")
            cursor.execute(create_sql)
            print(f"생성 완료")

            # 파티션 발견 (파티셔닝된 테이블만)
            if table_name in ['user_events', 'transactions']:
                print(f"파티션 발견 중...")
                cursor.execute(f"CALL system.sync_partition_metadata('default', '{table_name}', 'FULL')")
                print(f"파티션 동기화 완료")

            # 스키마 확인
            cursor.execute(f"DESCRIBE {table_name}")
            schema = cursor.fetchall()
            print(f"스키마:")
            for col in schema:
                print(f"      - {col[0]}: {col[1]}")

        except TrinoUserError as e:
            print(f"오류: {e}")


def verify_data(cursor):
    """데이터 확인"""
    tables = ['user_events', 'products', 'transactions']
    
    for table_name in tables:
        try:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"✅ {table_name}: {count}행")

            if count > 0:
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                rows = cursor.fetchall()
                print(f"샘플 데이터 (첫 3행):")
                for row in rows:
                    print(f"   {row}")

        except Exception as e:
            print(f"{table_name}: 오류 - {e}")


def main():
    try:
        conn = get_trino_connection()
        cursor = conn.cursor()

        # 테이블 생성
        create_tables(cursor)

        # 데이터 확인
        verify_data(cursor)

    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    main()
