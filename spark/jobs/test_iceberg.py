"""
Iceberg Catalog 테스트 스크립트
- Iceberg catalog 연결 확인
- CDC 네임스페이스 생성
- 테스트 테이블 생성 및 데이터 삽입
"""

from pyspark.sql import SparkSession

def test_iceberg_setup():
    """Iceberg 설정 테스트"""

    print("🚀 Starting Iceberg test...")

    # SparkSession 생성 (설정은 spark-defaults.conf에서 자동 로드)
    spark = SparkSession.builder \
        .appName("Iceberg Setup Test") \
        .getOrCreate()

    print("\n✅ SparkSession created successfully")
    print(f"   Spark Version: {spark.version}")

    # 1. Iceberg catalog 확인
    print("\n📋 Step 1: Checking Iceberg catalog...")
    try:
        catalogs = spark.sql("SHOW CATALOGS").collect()
        print("   Available catalogs:")
        for catalog in catalogs:
            print(f"     - {catalog[0]}")

        if any('iceberg' in str(c[0]) for c in catalogs):
            print("   ✅ Iceberg catalog found!")
        else:
            print("   ⚠️  Iceberg catalog not found, but continuing...")
    except Exception as e:
        print(f"   ⚠️  Could not list catalogs: {e}")

    # 2. CDC 네임스페이스 생성
    print("\n📋 Step 2: Creating CDC namespace...")
    try:
        # Use 'USE' to switch to iceberg catalog first
        spark.sql("USE iceberg")
        spark.sql("CREATE DATABASE IF NOT EXISTS cdc")
        print("   ✅ Database 'cdc' created/verified in iceberg catalog")

        # 네임스페이스 확인
        databases = spark.sql("SHOW DATABASES").collect()
        print("   Available databases in iceberg catalog:")
        for db in databases:
            print(f"     - {db[0]}")
    except Exception as e:
        print(f"   ❌ Failed to create namespace: {e}")
        return False

    # 3. 테스트 테이블 생성
    print("\n📋 Step 3: Creating test table...")
    try:
        spark.sql("USE iceberg.cdc")
        spark.sql("""
            CREATE TABLE IF NOT EXISTS test_users (
                id BIGINT,
                name STRING,
                email STRING,
                created_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (days(created_at))
            TBLPROPERTIES (
                'write.format.default'='parquet',
                'write.parquet.compression-codec'='snappy'
            )
        """)
        print("   ✅ Table 'test_users' created successfully in iceberg.cdc")
    except Exception as e:
        print(f"   ❌ Failed to create table: {e}")
        return False

    # 4. 테스트 데이터 삽입
    print("\n📋 Step 4: Inserting test data...")
    try:
        spark.sql("""
            INSERT INTO test_users VALUES
            (1, 'Alice', 'alice@example.com', TIMESTAMP'2026-01-04 10:00:00'),
            (2, 'Bob', 'bob@example.com', TIMESTAMP'2026-01-04 11:00:00'),
            (3, 'Charlie', 'charlie@example.com', TIMESTAMP'2026-01-04 12:00:00')
        """)
        print("   ✅ Test data inserted")
    except Exception as e:
        print(f"   ❌ Failed to insert data: {e}")
        return False

    # 5. 데이터 조회
    print("\n📋 Step 5: Querying test data...")
    try:
        result = spark.sql("SELECT * FROM test_users ORDER BY id")
        print("   Query result:")
        result.show()

        count = result.count()
        print(f"   ✅ Found {count} records")
    except Exception as e:
        print(f"   ❌ Failed to query data: {e}")
        return False

    # 6. 테이블 메타데이터 확인
    print("\n📋 Step 6: Checking table metadata...")
    try:
        spark.sql("DESCRIBE EXTENDED test_users").show(truncate=False)
    except Exception as e:
        print(f"   ⚠️  Could not show metadata: {e}")

    print("\n" + "="*60)
    print("✅ Iceberg setup test completed successfully!")
    print("="*60)

    spark.stop()
    return True

if __name__ == "__main__":
    success = test_iceberg_setup()
    exit(0 if success else 1)
