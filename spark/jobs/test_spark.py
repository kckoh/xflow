from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = SparkSession.builder \
    .appName("TestSparkJob") \
    .getOrCreate()

print("=" * 50)
print("✅ Spark Session Created Successfully!")
print(f"Spark Version: {spark.version}")
print(f"Application ID: {spark.sparkContext.applicationId}")
print("=" * 50)

# Create a simple DataFrame
data = [
    ("Alice", 25, "Engineering"),
    ("Bob", 30, "Sales"),
    ("Charlie", 35, "Marketing"),
    ("David", 28, "Engineering"),
    ("Eve", 32, "Sales")
]

df = spark.createDataFrame(data, ["name", "age", "department"])

print("\n📊 Sample Data:")
df.show()

# Perform simple aggregation
print("\n📈 Average Age by Department:")
df.groupBy("department").avg("age").show()

# Filter example
print("\n🔍 Engineers only:")
df.filter(col("department") == "Engineering").show()

print("\n✅ Spark job completed successfully!")

spark.stop()
