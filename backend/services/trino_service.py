from trino.dbapi import connect
from trino.auth import BasicAuthentication
from trino.exceptions import TrinoQueryError
from typing import List, Dict, Optional, Any
import logging
import time

from config import settings

logger = logging.getLogger(__name__)


class TrinoService:
    """Service for interacting with Trino/Hive"""

    def __init__(self):
        """Initialize Trino connection"""
        self.host = settings.trino_host
        self.port = settings.trino_port
        self.user = settings.trino_user
        self.catalog = settings.trino_catalog
        self.schema = settings.trino_schema
        self.max_retries = 5
        self.retry_delay = 2  # seconds

    def _is_retryable_error(self, error: Exception) -> bool:
        """
        Check if an error is retryable (e.g., Trino still starting up)

        Args:
            error: Exception to check

        Returns:
            True if error is retryable
        """
        error_str = str(error)

        if isinstance(error, TrinoQueryError):
            # Retry if Trino is still starting up
            if "SERVER_STARTING_UP" in error_str:
                return True
            # Retry on connection errors
            if "INTERNAL_ERROR" in error_str:
                return True

        # Retry on Hive Metastore connection errors (during startup)
        if "HIVE_METASTORE_ERROR" in error_str and "Failed connecting to Hive metastore" in error_str:
            return True

        return False

    def _get_connection(self):
        """Create Trino connection"""
        return connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=self.catalog,
            schema=self.schema,
        )

    def execute_query(self, query: str) -> List[tuple]:
        """
        Execute a query and return results with retry logic

        Args:
            query: SQL query to execute

        Returns:
            List of result tuples
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute(query)
                results = cursor.fetchall()
                cursor.close()
                conn.close()
                logger.info(f"Executed query: {query[:100]}...")
                return results
            except Exception as e:
                last_error = e

                # Check if error is retryable
                if self._is_retryable_error(e) and attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    logger.warning(
                        f"Trino not ready (attempt {attempt + 1}/{self.max_retries}). "
                        f"Retrying in {wait_time}s... Error: {e}"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    # Not retryable or max retries reached
                    logger.error(f"Error executing query: {e}\nQuery: {query}")
                    raise

        # This should not be reached, but just in case
        if last_error:
            raise last_error
        return []

    def execute_ddl(self, ddl: str) -> bool:
        """
        Execute DDL statement (CREATE TABLE, ALTER TABLE, etc.) with retry logic

        Args:
            ddl: DDL statement to execute

        Returns:
            True if successful
        """
        last_error = None

        for attempt in range(self.max_retries):
            try:
                conn = self._get_connection()
                cursor = conn.cursor()
                cursor.execute(ddl)
                cursor.close()
                conn.close()
                logger.info(f"Executed DDL: {ddl[:100]}...")
                return True
            except Exception as e:
                last_error = e

                # Check if error is retryable
                if self._is_retryable_error(e) and attempt < self.max_retries - 1:
                    wait_time = self.retry_delay * (attempt + 1)
                    logger.warning(
                        f"Trino not ready (attempt {attempt + 1}/{self.max_retries}). "
                        f"Retrying in {wait_time}s... Error: {e}"
                    )
                    time.sleep(wait_time)
                    continue
                else:
                    # Not retryable or max retries reached
                    logger.error(f"Error executing DDL: {e}\nDDL: {ddl}")
                    raise

        # This should not be reached, but just in case
        if last_error:
            raise last_error
        return False

    def table_exists(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Check if a table exists

        Args:
            table_name: Name of the table
            schema: Schema name (default: use configured schema)

        Returns:
            True if table exists, False otherwise
        """
        schema = schema or self.schema
        query = f"""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
        """
        try:
            results = self.execute_query(query)
            return len(results) > 0
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    def convert_arrow_to_hive_type(self, arrow_type: str) -> str:
        """
        Convert PyArrow data type to Hive/Trino SQL type

        Args:
            arrow_type: PyArrow type string (e.g., "int64", "string", "double")

        Returns:
            Hive/Trino SQL type (e.g., "BIGINT", "VARCHAR", "DOUBLE")
        """
        type_mapping = {
            # Integer types
            "int8": "TINYINT",
            "int16": "SMALLINT",
            "int32": "INTEGER",
            "int64": "BIGINT",
            "uint8": "SMALLINT",
            "uint16": "INTEGER",
            "uint32": "BIGINT",
            "uint64": "BIGINT",

            # Floating point
            "float": "REAL",
            "float32": "REAL",
            "float64": "DOUBLE",
            "double": "DOUBLE",

            # String types
            "string": "VARCHAR",
            "utf8": "VARCHAR",
            "large_string": "VARCHAR",
            "large_utf8": "VARCHAR",

            # Boolean
            "bool": "BOOLEAN",

            # Date/Time
            "date32": "DATE",
            "date64": "DATE",
            "timestamp[us]": "TIMESTAMP",
            "timestamp[ms]": "TIMESTAMP",
            "timestamp[ns]": "TIMESTAMP",
            "timestamp[s]": "TIMESTAMP",

            # Binary
            "binary": "VARBINARY",
            "large_binary": "VARBINARY",

            # Decimal
            "decimal128": "DECIMAL(38,10)",
            "decimal256": "DECIMAL(38,10)",
        }

        # Handle complex types
        if "timestamp" in arrow_type.lower():
            return "TIMESTAMP"
        if "decimal" in arrow_type.lower():
            return "DECIMAL(38,10)"
        if "list" in arrow_type.lower() or "array" in arrow_type.lower():
            return "ARRAY<VARCHAR>"
        if "struct" in arrow_type.lower():
            return "ROW"
        if "map" in arrow_type.lower():
            return "MAP<VARCHAR, VARCHAR>"

        return type_mapping.get(arrow_type.lower(), "VARCHAR")

    def create_external_table(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        s3_location: str,
        partitions: Optional[List[str]] = None,
        file_format: str = "PARQUET",
        schema: Optional[str] = None
    ) -> str:
        """
        Create external table DDL statement

        Args:
            table_name: Name of the table
            columns: List of column definitions [{"name": "col1", "type": "int64"}, ...]
            s3_location: S3 location (e.g., "s3a://bucket/table_name")
            partitions: List of partition column names
            file_format: File format (PARQUET, ORC, etc.)
            schema: Schema name (default: use configured schema)

        Returns:
            DDL statement string
        """
        schema = schema or self.schema
        full_table_name = f"{schema}.{table_name}"

        # Build column definitions (include ALL columns, including partition columns)
        partition_cols = set(partitions or [])
        column_defs = []

        # Add non-partition columns first
        for col in columns:
            if col["name"] not in partition_cols:
                hive_type = self.convert_arrow_to_hive_type(col["type"])
                column_defs.append(f"  {col['name']} {hive_type}")

        # Add partition columns (if not in schema, use VARCHAR)
        if partitions:
            partition_col_types = {}

            # Try to find partition column types in schema
            for col in columns:
                if col["name"] in partition_cols:
                    partition_col_types[col["name"]] = self.convert_arrow_to_hive_type(col["type"])

            # Add partition columns to column definitions
            for partition_col in partitions:
                if partition_col in partition_col_types:
                    column_defs.append(f"  {partition_col} {partition_col_types[partition_col]}")
                else:
                    # Hive-style partitioning stores as strings
                    column_defs.append(f"  {partition_col} VARCHAR")

        columns_sql = ",\n".join(column_defs)

        # Build DDL
        ddl = f"""
CREATE TABLE IF NOT EXISTS {full_table_name} (
{columns_sql}
)
"""

        # Add WITH clause
        with_clauses = [
            f"  format = '{file_format}'",
            f"  external_location = '{s3_location}'"
        ]

        # Add partitioning to WITH clause (Trino Hive connector style)
        if partitions and len(partitions) > 0:
            partitions_array = ", ".join([f"'{p}'" for p in partitions])
            with_clauses.insert(0, f"  partitioned_by = ARRAY[{partitions_array}]")

        with_sql = ",\n".join(with_clauses)
        ddl += f"""WITH (
{with_sql}
)"""

        return ddl

    def create_table(
        self,
        table_name: str,
        columns: List[Dict[str, str]],
        s3_location: str,
        partitions: Optional[List[str]] = None,
        file_format: str = "PARQUET",
        schema: Optional[str] = None
    ) -> bool:
        """
        Create external table in Trino/Hive

        Args:
            table_name: Name of the table
            columns: List of column definitions
            s3_location: S3 location
            partitions: List of partition column names
            file_format: File format
            schema: Schema name

        Returns:
            True if successful
        """
        ddl = self.create_external_table(
            table_name=table_name,
            columns=columns,
            s3_location=s3_location,
            partitions=partitions,
            file_format=file_format,
            schema=schema
        )

        logger.info(f"Creating table with DDL:\n{ddl}")
        return self.execute_ddl(ddl)

    def sync_partitions(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Sync partitions with metastore (equivalent to MSCK REPAIR TABLE in Hive)

        Args:
            table_name: Name of the table
            schema: Schema name (default: use configured schema)

        Returns:
            True if successful
        """
        schema = schema or self.schema
        full_table_name = f"{schema}.{table_name}"

        # Trino uses CALL system.sync_partition_metadata
        query = f"CALL system.sync_partition_metadata('{schema}', '{table_name}', 'FULL')"

        try:
            logger.info(f"Syncing partitions for {full_table_name}")
            self.execute_query(query)
            return True
        except Exception as e:
            logger.error(f"Error syncing partitions: {e}")
            raise

    def add_partition(
        self,
        table_name: str,
        partition_values: Dict[str, str],
        location: str,
        schema: Optional[str] = None
    ) -> bool:
        """
        Add a specific partition to the table

        Args:
            table_name: Name of the table
            partition_values: Partition key-value pairs (e.g., {"year": "2024", "month": "12"})
            location: S3 location of the partition
            schema: Schema name

        Returns:
            True if successful
        """
        schema = schema or self.schema
        full_table_name = f"{schema}.{table_name}"

        # Build partition spec
        partition_spec = ", ".join([f"{k}='{v}'" for k, v in partition_values.items()])

        ddl = f"""
ALTER TABLE {full_table_name}
ADD IF NOT EXISTS PARTITION ({partition_spec})
LOCATION '{location}'
"""

        logger.info(f"Adding partition: {partition_spec}")
        return self.execute_ddl(ddl)

    def get_table_schema(self, table_name: str, schema: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get table schema information

        Args:
            table_name: Name of the table
            schema: Schema name

        Returns:
            List of column information dictionaries
        """
        schema = schema or self.schema
        query = f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{table_name}'
            ORDER BY ordinal_position
        """

        results = self.execute_query(query)
        return [
            {
                "name": row[0],
                "type": row[1],
                "nullable": row[2] == "YES"
            }
            for row in results
        ]

    def drop_table(self, table_name: str, schema: Optional[str] = None) -> bool:
        """
        Drop a table

        Args:
            table_name: Name of the table
            schema: Schema name

        Returns:
            True if successful
        """
        schema = schema or self.schema
        full_table_name = f"{schema}.{table_name}"
        ddl = f"DROP TABLE IF EXISTS {full_table_name}"

        logger.info(f"Dropping table: {full_table_name}")
        return self.execute_ddl(ddl)


# Global instance
trino_service = TrinoService()
