"""
SCD2 query helpers for Delta Lake tables.

Note: ETL write/merge logic lives in etl_runner.py.
These helpers are optional and can be used in ad-hoc Spark sessions.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def query_scd2_current(spark: SparkSession, path: str) -> DataFrame:
    """
    Query only current (active) records from SCD Type 2 table
    """
    return spark.read.format("delta").load(path).filter(F.col("is_current") == True)


def query_scd2_as_of(spark: SparkSession, path: str, as_of_date: str) -> DataFrame:
    """
    Query SCD Type 2 table as of a specific date (time travel)
    """
    df = spark.read.format("delta").load(path)
    return df.filter(
        (F.col("valid_from") <= as_of_date) &
        ((F.col("valid_to") > as_of_date) | F.col("valid_to").isNull())
    )


def query_scd2_history(spark: SparkSession, path: str, primary_key_value: any, primary_key_col: str = "id") -> DataFrame:
    """
    Query full history for a specific record
    """
    df = spark.read.format("delta").load(path)
    return df.filter(F.col(primary_key_col) == primary_key_value).orderBy("valid_from")
