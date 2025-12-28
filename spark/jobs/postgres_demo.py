from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("PostgreSQL Demo") \
    .getOrCreate()

# PostgreSQL 테이블 목록 조회
tables = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/mydb") \
    .option("dbtable", "(SELECT tablename FROM pg_tables WHERE schemaname='public') t") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load()

print("\n=== Tables ===")
tables.show()

spark.stop()
