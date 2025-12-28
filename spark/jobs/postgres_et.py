from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("PostgreSQL to S3")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack-main:4566")
    .config("spark.hadoop.fs.s3a.access.key", "test")
    .config("spark.hadoop.fs.s3a.secret.key", "test")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

# PostgreSQL에서 데이터 읽기
df = (
    spark.read.format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/mydb")
    .option("dbtable", "products")
    .option("user", "postgres")
    .option("password", "postgres")
    .option("driver", "org.postgresql.Driver")
    .load()
)

print("\n=== Original ===")
df.show(5)

# Transform: drop category column
transformed = df.drop("category")

print("\n=== Transformed ===")
transformed.show(5)

# Save to S3 as Parquet with Snappy compression
output_path = "s3a://xflow-processed-data/products"
transformed.write.mode("overwrite").option("compression", "snappy").parquet(output_path)

print(f"\n=== Saved to {output_path} ===")
spark.stop()
