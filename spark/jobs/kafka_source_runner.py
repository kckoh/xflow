import os
import sys
import json
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def run_pipeline(config: dict):
    topic = config.get("topic")
    # For local docker environment, the service name is 'kafka'
    KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    
    logger.info(f"=== Kafka Min-Check: {topic} | Servers: {KAFKA_BOOTSTRAP_SERVERS} ===")
    
    spark = SparkSession.builder \
        .appName(f"Kafka-Min-{topic}") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")

    try:
        # Read Stream
        raw_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load()

        # Simple cast value to string
        processed_stream = raw_stream.select(col("value").cast("string"))

        # Write to Console
        query = processed_stream.writeStream \
            .format("console") \
            .option("truncate", "false") \
            .start()
            
        logger.info("Min-Check Pipeline Started.")
        query.awaitTermination()
        
    except Exception as e:
        logger.error(f"Min-Check Failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    config_b64 = os.getenv("JOB_CONFIG")
    if config_b64:
        import base64
        config = json.loads(base64.b64decode(config_b64).decode("utf-8"))
        run_pipeline(config)
    else:
        logger.error("JOB_CONFIG not found")
        sys.exit(1)
