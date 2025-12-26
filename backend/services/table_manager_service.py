from typing import Dict, List, Optional
import logging
from datetime import datetime

from backend.services.minio_service import minio_service
from backend.services.trino_service import trino_service
from backend.config import settings
from backend.database import get_database
from backend.models import FileProcessingHistory

logger = logging.getLogger(__name__)


class TableManagerService:
    """Service for managing automatic table creation and partition sync from Parquet files"""

    def __init__(self):
        self.minio = minio_service
        self.trino = trino_service

    def _save_processing_history(
        self,
        object_path: str,
        table_name: str,
        ddl_statement: Optional[str],
        partitions: Dict[str, str],
        num_columns: int,
        num_rows: int,
        status: str,
        error_message: Optional[str] = None
    ):
        """Save processing history to MongoDB"""
        try:
            db = get_database()
            history = FileProcessingHistory(
                object_path=object_path,
                table_name=table_name,
                ddl_statement=ddl_statement or "",
                partition_values=partitions,
                num_columns=num_columns,
                num_rows=num_rows,
                status=status,
                error_message=error_message
            )

            # Insert into MongoDB
            db.file_processing_history.insert_one(history.dict(by_alias=True, exclude_unset=True))
            logger.info(f"Saved processing history for {object_path}")
        except Exception as e:
            logger.error(f"Error saving processing history: {e}")

    def process_parquet_file(self, object_path: str) -> Dict:
        """
        Main entry point: Process a Parquet file uploaded to MinIO

        This method:
        1. Extracts table name and partition info from file path
        2. Reads metadata file if exists
        3. Detects schema from Parquet file
        4. Creates table if not exists, or syncs partitions if exists
        5. Saves processing history to database
        6. Returns processing result

        Args:
            object_path: S3 object path (e.g., "sales/year=2024/month=12/data.parquet")

        Returns:
            Dictionary with processing results
        """
        ddl_statement = None
        table_name = None
        partitions = {}
        num_columns = 0
        num_rows = 0

        try:
            logger.info(f"Processing file: {object_path}")

            # Step 1: Extract table name and partition info
            table_name = self.minio.extract_table_name(object_path)
            partitions = self.minio.parse_partition_path(object_path)

            logger.info(f"Table: {table_name}, Partitions: {partitions}")

            # Step 2: Read metadata file (contains partition column definitions)
            metadata = self.minio.get_metadata_file(table_name)
            partition_columns = []

            if metadata:
                partition_columns = metadata.get("partition_columns", [])
                logger.info(f"Metadata found. Partition columns: {partition_columns}")
            else:
                # If no metadata, infer partition columns from path
                partition_columns = list(partitions.keys())
                logger.warning(f"No metadata file. Inferred partitions: {partition_columns}")

            # Step 3: Get Parquet schema
            schema_info = self.minio.get_parquet_schema(object_path)
            columns = schema_info["columns"]
            num_columns = len(columns)
            num_rows = schema_info["num_rows"]

            logger.info(f"Schema detected: {num_columns} columns, {num_rows} rows")

            # Step 4: Check if table exists
            table_exists = self.trino.table_exists(table_name)

            if not table_exists:
                # Create new table
                result = self._create_new_table(
                    table_name=table_name,
                    columns=columns,
                    partition_columns=partition_columns,
                    object_path=object_path
                )
                action = "created"
                ddl_statement = result.get("ddl_statement")
            else:
                # Sync partitions for existing table
                result = self._sync_table_partitions(
                    table_name=table_name,
                    partitions=partitions,
                    object_path=object_path
                )
                action = "partition_synced"
                ddl_statement = None  # No DDL for partition sync

            # Save to database
            self._save_processing_history(
                object_path=object_path,
                table_name=table_name,
                ddl_statement=ddl_statement,
                partitions=partitions,
                num_columns=num_columns,
                num_rows=num_rows,
                status="success"
            )

            return {
                "success": True,
                "action": action,
                "table_name": table_name,
                "partitions": partitions,
                "num_columns": num_columns,
                "num_rows": num_rows,
                "object_path": object_path,
                "timestamp": datetime.utcnow().isoformat()
            }

        except Exception as e:
            logger.error(f"Error processing file {object_path}: {e}", exc_info=True)

            # Save error to database
            if table_name:
                self._save_processing_history(
                    object_path=object_path,
                    table_name=table_name,
                    ddl_statement=ddl_statement,
                    partitions=partitions,
                    num_columns=num_columns,
                    num_rows=num_rows,
                    status="failed",
                    error_message=str(e)
                )

            return {
                "success": False,
                "error": str(e),
                "object_path": object_path,
                "timestamp": datetime.utcnow().isoformat()
            }

    def _create_new_table(
        self,
        table_name: str,
        columns: List[Dict],
        partition_columns: List[str],
        object_path: str
    ) -> Dict:
        """
        Create a new external table in Trino/Hive

        Args:
            table_name: Name of the table to create
            columns: List of column definitions from Parquet schema
            partition_columns: List of partition column names
            object_path: S3 object path

        Returns:
            Result dictionary with DDL statement
        """
        logger.info(f"Creating new table: {table_name}")

        # Build S3 location (table root, not specific file)
        # Example: "sales/year=2024/month=12/data.parquet" -> "s3a://bucket/sales/"
        s3_location = self.minio.get_s3_uri(table_name)

        # Generate DDL
        ddl_statement = self.trino.create_external_table(
            table_name=table_name,
            columns=columns,
            s3_location=s3_location,
            partitions=partition_columns if partition_columns else None,
            file_format="PARQUET"
        )

        # Execute DDL
        self.trino.execute_ddl(ddl_statement)

        # Sync partitions (only for partitioned tables)
        if partition_columns and len(partition_columns) > 0:
            logger.info(f"Syncing partitions for {table_name}: {partition_columns}")
            self.trino.sync_partitions(table_name)
        else:
            logger.info(f"No partitions for {table_name}, skipping partition sync")

        logger.info(f"Table {table_name} created successfully")

        return {
            "table_created": True,
            "s3_location": s3_location,
            "partition_columns": partition_columns,
            "ddl_statement": ddl_statement
        }

    def _sync_table_partitions(
        self,
        table_name: str,
        partitions: Dict[str, str],
        object_path: str
    ) -> Dict:
        """
        Sync partitions for an existing table

        Args:
            table_name: Name of the table
            partitions: Partition key-value pairs
            object_path: S3 object path

        Returns:
            Result dictionary
        """
        logger.info(f"Syncing partitions for table: {table_name}")

        # Only sync partitions if the table has partitions
        if partitions and len(partitions) > 0:
            logger.info(f"Syncing partitions: {partitions}")
            self.trino.sync_partitions(table_name)
            logger.info(f"Partitions synced for {table_name}")
        else:
            logger.info(f"No partitions for {table_name}, skipping partition sync")

        return {
            "partitions_synced": bool(partitions),
            "partition_values": partitions
        }

    def process_multiple_files(self, object_paths: List[str]) -> List[Dict]:
        """
        Process multiple Parquet files in batch

        Args:
            object_paths: List of S3 object paths

        Returns:
            List of processing results
        """
        results = []

        for object_path in object_paths:
            result = self.process_parquet_file(object_path)
            results.append(result)

        return results

    def discover_and_process_tables(self, prefix: str = "") -> List[Dict]:
        """
        Discover all Parquet files in MinIO and process them

        Args:
            prefix: Prefix to filter objects (e.g., "sales/")

        Returns:
            List of processing results
        """
        logger.info(f"Discovering files with prefix: {prefix}")

        # List all objects
        objects = self.minio.list_objects(prefix=prefix)

        # Filter Parquet files only
        parquet_files = [obj for obj in objects if obj.endswith('.parquet')]

        logger.info(f"Found {len(parquet_files)} Parquet files")

        # Process each file
        return self.process_multiple_files(parquet_files)

    def rebuild_table(self, table_name: str) -> Dict:
        """
        Drop and recreate a table from existing files in MinIO

        Args:
            table_name: Name of the table to rebuild

        Returns:
            Result dictionary
        """
        logger.info(f"Rebuilding table: {table_name}")

        try:
            # Drop existing table
            if self.trino.table_exists(table_name):
                self.trino.drop_table(table_name)
                logger.info(f"Dropped existing table: {table_name}")

            # Find all files for this table
            parquet_files = [
                obj for obj in self.minio.list_objects(prefix=f"{table_name}/")
                if obj.endswith('.parquet')
            ]

            if not parquet_files:
                raise ValueError(f"No Parquet files found for table: {table_name}")

            # Process first file to create table
            result = self.process_parquet_file(parquet_files[0])

            # Sync all partitions
            if len(parquet_files) > 1:
                self.trino.sync_partitions(table_name)

            return {
                "success": True,
                "table_name": table_name,
                "files_processed": len(parquet_files),
                "result": result
            }

        except Exception as e:
            logger.error(f"Error rebuilding table {table_name}: {e}", exc_info=True)
            return {
                "success": False,
                "table_name": table_name,
                "error": str(e)
            }


# Global instance
table_manager = TableManagerService()
