from minio import Minio
from minio.error import S3Error
from io import BytesIO
import pyarrow.parquet as pq
from typing import Dict, Optional, List
import logging
import re

from config import settings

logger = logging.getLogger(__name__)


class MinIOService:
    """Service for interacting with MinIO storage"""

    def __init__(self):
        """Initialize MinIO client"""
        self.client = Minio(
            endpoint=settings.minio_endpoint,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=settings.minio_secure
        )
        self.bucket_name = settings.minio_bucket
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Create bucket if it doesn't exist"""
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Created bucket: {self.bucket_name}")
            else:
                logger.info(f"Bucket already exists: {self.bucket_name}")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

    def parse_partition_path(self, object_path: str) -> Dict[str, str]:
        """
        Parse Hive-style partition path to extract partition key-value pairs

        Args:
            object_path: S3 object path like "table_name/year=2024/month=12/data.parquet"

        Returns:
            Dictionary of partition key-value pairs
            e.g., {"year": "2024", "month": "12"}
        """
        partitions = {}

        # Match Hive-style partitions (key=value)
        pattern = r'([^/=]+)=([^/]+)'
        matches = re.findall(pattern, object_path)

        for key, value in matches:
            partitions[key] = value

        return partitions

    def extract_table_name(self, object_path: str) -> str:
        """
        Extract table name from object path

        Args:
            object_path: S3 object path like "table_name/year=2024/month=12/data.parquet"

        Returns:
            Table name (first segment of path)
        """
        return object_path.split('/')[0]

    def get_file(self, object_name: str) -> bytes:
        """
        Download file from MinIO

        Args:
            object_name: Full path of object to download

        Returns:
            File contents as bytes
        """
        try:
            response = self.client.get_object(
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            data = response.read()
            response.close()
            response.release_conn()
            return data
        except S3Error as e:
            logger.error(f"Error getting file: {e}")
            raise

    def get_parquet_schema(self, object_name: str) -> dict:
        """
        Read Parquet file schema from MinIO

        Args:
            object_name: Full path of Parquet file

        Returns:
            Dictionary containing schema information:
            {
                "columns": [{"name": "col1", "type": "int64"}, ...],
                "num_rows": 1000,
                "num_columns": 5
            }
        """
        try:
            # Download file
            file_data = self.get_file(object_name)

            # Read Parquet schema
            parquet_file = pq.ParquetFile(BytesIO(file_data))
            schema = parquet_file.schema_arrow

            # Extract column information
            columns = []
            for i in range(len(schema)):
                field = schema.field(i)
                columns.append({
                    "name": field.name,
                    "type": str(field.type),
                    "nullable": field.nullable
                })

            return {
                "columns": columns,
                "num_rows": parquet_file.metadata.num_rows,
                "num_columns": len(columns),
                "schema": schema
            }
        except Exception as e:
            logger.error(f"Error reading Parquet schema: {e}")
            raise

    def list_objects(self, prefix: str = "", recursive: bool = True) -> List[str]:
        """
        List objects in bucket with given prefix

        Args:
            prefix: Prefix to filter objects (e.g., "table_name/")
            recursive: Search recursively in subdirectories

        Returns:
            List of object names
        """
        try:
            objects = self.client.list_objects(
                bucket_name=self.bucket_name,
                prefix=prefix,
                recursive=recursive
            )
            return [obj.object_name for obj in objects]
        except S3Error as e:
            logger.error(f"Error listing objects: {e}")
            raise

    def get_metadata_file(self, table_name: str) -> Optional[dict]:
        """
        Get metadata file for a table if it exists

        Args:
            table_name: Name of the table

        Returns:
            Metadata as dictionary, or None if not found
        """
        metadata_path = f"{table_name}/{settings.metadata_file_name}"

        try:
            import json
            data = self.get_file(metadata_path)
            return json.loads(data.decode('utf-8'))
        except S3Error as e:
            logger.warning(f"Metadata file not found: {metadata_path}")
            return None
        except Exception as e:
            logger.error(f"Error reading metadata file: {e}")
            return None

    def delete_file(self, object_name: str) -> bool:
        """
        Delete file from MinIO (for rollback operations)

        Args:
            object_name: Full path of object to delete

        Returns:
            True if successful, False otherwise
        """
        try:
            self.client.remove_object(
                bucket_name=self.bucket_name,
                object_name=object_name
            )
            logger.info(f"Deleted file: s3://{self.bucket_name}/{object_name}")
            return True
        except S3Error as e:
            logger.error(f"Error deleting file: {e}")
            return False

    def get_s3_uri(self, object_name: str) -> str:
        """
        Get S3 URI for an object

        Args:
            object_name: Object path

        Returns:
            S3 URI string (s3a://bucket/path)
        """
        return f"s3a://{self.bucket_name}/{object_name}"


# Global instance
minio_service = MinIOService()
