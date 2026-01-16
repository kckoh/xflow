"""
Quality Service - Data quality checks using DuckDB on S3 Parquet files.

This service runs quality checks on data stored in S3 (LocalStack) and
stores results in MongoDB for tracking and visualization.
"""

import os
import time
import tempfile
from datetime import datetime, date
from typing import Optional, List
from urllib.parse import unquote

import boto3
import duckdb

from models import QualityResult, QualityCheck, ETLJob


# S3/LocalStack configuration (matches docker-compose.yml)
S3_ENDPOINT = os.getenv("AWS_ENDPOINT", "http://localstack-main:4566")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "test")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "test")
# Note: Production region
S3_REGION = "ap-northeast-2"


class QualityService:
    """
    Service for running data quality checks using DuckDB.
    Downloads Parquet files from S3 via boto3, then analyzes with DuckDB.
    """
    
    def __init__(self):
        env = os.getenv("ENVIRONMENT", "local")
        
        if env == "production":
            # Production: Use IRSA/IAM Role (No explicit credentials/endpoint)
            self.s3_client = boto3.client(
                's3',
                region_name=S3_REGION
            )
        else:
            # Local: Explicit endpoint (LocalStack)
            self.s3_client = boto3.client(
                's3',
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY,
                region_name=S3_REGION
            )
    
    def _parse_s3_path(self, s3_path: str) -> tuple[str, str]:
        """Parse s3://bucket/key or s3a://bucket/key format to (bucket, key)"""
        # Remove s3:// or s3a:// prefix
        path = s3_path.replace("s3a://", "").replace("s3://", "")
        parts = path.split("/", 1)
        bucket = parts[0]
        key = parts[1] if len(parts) > 1 else ""
        return bucket, key
    
    def _list_parquet_files(self, bucket: str, prefix: str) -> List[str]:
        """List all parquet files under a prefix"""
        files = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get('Contents', []):
                key = obj['Key']
                if key.endswith('.parquet'):
                    files.append(key)
        
        return files
    
    def _download_parquet_files(self, bucket: str, keys: List[str]) -> List[str]:
        """Download parquet files to temp directory, return list of local paths"""
        temp_dir = tempfile.mkdtemp()
        local_paths = []
        
        for key in keys:
            local_path = os.path.join(temp_dir, os.path.basename(key))
            self.s3_client.download_file(bucket, key, local_path)
            local_paths.append(local_path)
        
        return local_paths
    
    def _calculate_total_size(self, bucket: str, keys: List[str]) -> int:
        """Calculate total size of listed files in bytes"""
        total_bytes = 0
        try:
            for key in keys:
                response = self.s3_client.head_object(Bucket=bucket, Key=key)
                total_bytes += response['ContentLength']
        except Exception:
            pass
        return total_bytes

    async def run_quality_check(
        self,
        dataset_id: str,
        s3_path: str,
        null_threshold: float = 5.0,
        duplicate_threshold: float = 1.0,
        result_id: Optional[str] = None
    ) -> QualityResult:
        """
        Run quality check on Parquet files in S3.

        Args:
            result_id: Optional existing result ID to update (for background tasks)
        """
        start_time = time.time()

        # Get existing result or create new one
        if result_id:
            from beanie import PydanticObjectId
            result = await QualityResult.get(PydanticObjectId(result_id))
            if not result:
                raise ValueError(f"Result {result_id} not found")
        else:
            # Create new result (for backward compatibility)
            result = QualityResult(
                dataset_id=dataset_id,
                s3_path=s3_path,
                status="running",
                run_at=datetime.utcnow()
            )
            await result.insert()
        
        temp_paths = []
        
        try:
            # URL decode the path
            s3_path = unquote(s3_path)
            
            # Parse S3 path
            bucket, key = self._parse_s3_path(s3_path)
            
            # Determine if it's a file or folder
            if s3_path.endswith('.parquet'):
                # Single file
                parquet_keys = [key]
            else:
                # Folder - list all parquet files
                # Handle root bucket case (key is empty)
                prefix = key.rstrip('/') + '/' if key else ""
                parquet_keys = self._list_parquet_files(bucket, prefix)
                
                if not parquet_keys:
                    result.status = "completed"
                    result.overall_score = 0.0
                    result.error_message = "No parquet files found"
                    result.completed_at = datetime.utcnow()
                    result.duration_ms = int((time.time() - start_time) * 1000)
                    result.progress = 100.0
                    await result.save()
                    return result

            # Set total files count
            result.total_files = len(parquet_keys)
            result.progress = 0.0
            await result.save()

            print(f"[Quality Check] Processing {len(parquet_keys)} files for dataset {dataset_id}")

            # --- Strategy Selection: Sampling for large datasets ---
            total_size = self._calculate_total_size(bucket, parquet_keys)

            MB = 1024 * 1024
            is_large_data = total_size >= 100 * MB

            sample_clause = ""
            if is_large_data:
                # SAMPLING MODE: Use DuckDB TABLESAMPLE for row-level sampling
                sample_clause = " TABLESAMPLE 10%"
                print(f"[Quality Check] Large dataset detected ({total_size / MB:.1f} MB). Using 10% sampling.")

            # Process files in batches (1000 files at a time)
            # Each batch processes files from 0 to current batch end (cumulative)
            BATCH_SIZE = 1000
            num_batches = (len(parquet_keys) + BATCH_SIZE - 1) // BATCH_SIZE

            for batch_idx in range(num_batches):
                # Calculate end index for this batch (cumulative from start)
                end_idx = min((batch_idx + 1) * BATCH_SIZE, len(parquet_keys))

                print(f"[Quality Check] Batch {batch_idx + 1}/{num_batches}: Analyzing files 1-{end_idx}")

                # Create DuckDB connection for this batch
                conn = duckdb.connect(":memory:")

                # Configure S3 for DuckDB
                conn.execute("INSTALL httpfs; LOAD httpfs;")
                conn.execute("INSTALL aws; LOAD aws;")

                # Environment-based S3 configuration
                env = os.getenv("ENVIRONMENT", "local")

                if env == "production":
                    # Production (AWS): Get credentials from boto3 (supports IRSA)
                    import boto3
                    session = boto3.Session()
                    credentials = session.get_credentials()

                    # Pass credentials to DuckDB explicitly
                    conn.execute(f"""
                        SET s3_region='{S3_REGION}';
                        SET s3_endpoint='s3.{S3_REGION}.amazonaws.com';
                        SET s3_access_key_id='{credentials.access_key}';
                        SET s3_secret_access_key='{credentials.secret_key}';
                        SET s3_session_token='{credentials.token}';
                        SET s3_use_ssl=true;
                        SET s3_url_style='path';
                    """)
                else:
                    # Local (LocalStack): Explicit endpoint and credentials
                    duckdb_endpoint = S3_ENDPOINT.replace("http://", "").replace("https://", "")
                    conn.execute(f"""
                        SET s3_endpoint='{duckdb_endpoint}';
                        SET s3_use_ssl=false;
                        SET s3_url_style='path';
                        SET s3_region='{S3_REGION}';
                        SET s3_access_key_id='{S3_ACCESS_KEY}';
                        SET s3_secret_access_key='{S3_SECRET_KEY}';
                    """)
            
                # Build query for S3 paths (cumulative batch from start to end_idx)
                s3_target_paths = [f"s3://{bucket}/{k}" for k in parquet_keys[0:end_idx]]

                # Use union_by_name=True to handle files with different schemas
                if len(s3_target_paths) == 1:
                    from_clause = f"read_parquet('{s3_target_paths[0]}', union_by_name=True){sample_clause}"
                else:
                    paths_str = ", ".join([f"'{p}'" for p in s3_target_paths])
                    from_clause = f"read_parquet([{paths_str}], union_by_name=True){sample_clause}"


                checks = []
                score = 100.0

                # Add sampling info if applicable
                if is_large_data and batch_idx == 0:  # Only show once on first batch
                    checks.append(QualityCheck(
                        name="strategy_info",
                        column=None,
                        passed=True,
                        value=0.0,
                        threshold=0.0,
                        message=f"Sampling Mode: TABLESAMPLE 10% (Total size: {total_size / MB:.1f} MB)"
                    ))

                # [PERFORMANCE OPTIMIZATION] One-Pass Scan
                # Instead of running multiple queries, we build a single SQL query
                # to fetch all necessary statistics (Count, Distinct, Min, Max) at once.

                # 1. Get Schema
                # "DESCRIBE" reads metadata only (very fast)
                table_info = conn.execute(f"DESCRIBE SELECT * FROM {from_clause}").fetchall()
                column_names = [col[0] for col in table_info]
                column_types = {col[0]: col[1].upper() for col in table_info}

                result.column_count = len(column_names)

                # 2. Build Aggregation Query
                # SELECT COUNT(*) as total, COUNT(col1), MIN(col1), MAX(col1), ...
                # Note: COUNT(DISTINCT *) is not supported in DuckDB, so we use a separate query

                aggs = ["COUNT(*) as total_rows"]

                for col in column_names:
                    c = f'"{col}"'
                    aggs.append(f"COUNT({c})") # Count non-nulls
                    aggs.append(f"MIN({c})")
                    aggs.append(f"MAX({c})")

                query = f"SELECT {', '.join(aggs)} FROM {from_clause}"

                # 3. Execute Query (This is the ONLY heavy scan)
                row = conn.execute(query).fetchone()

                # 4. Get distinct count via separate query (DuckDB doesn't support COUNT(DISTINCT *))
                distinct_query = f"SELECT COUNT(*) FROM (SELECT DISTINCT * FROM {from_clause})"
                distinct_rows = conn.execute(distinct_query).fetchone()[0]

                # 5. Parse Results
                total_rows = row[0]

                result.row_count = total_rows

                # Organize column stats
                col_stats = {}
                idx = 1  # Start after total_rows (index 0)
                for col in column_names:
                    col_stats[col] = {
                        'non_null': row[idx],
                        'min': row[idx+1],
                        'max': row[idx+2]
                    }
                    idx += 3


                # Handle case with no data
                if total_rows == 0:
                    result.status = "completed"
                    result.overall_score = 0.0
                    result.error_message = "No data found"
                    result.completed_at = datetime.utcnow()
                    result.duration_ms = int((time.time() - start_time) * 1000)
                    result.progress = 100.0
                    await result.save()
                    return result

                # 5. Apply Quality Rules (In-Memory Processing)

                # A) Null Checks
                null_counts = {}
                for col in column_names:
                    non_null = col_stats[col]['non_null']
                    null_count = total_rows - non_null
                    null_counts[col] = null_count

                    null_ratio = 0.0
                    if total_rows > 0:
                        null_ratio = (null_count / total_rows) * 100

                    # Dynamic Scoring: Deduct 0.25 points for every 1% of nulls
                    # Example: 10% null -> -2.5 points. 100% null -> -25 points.
                    penalty = null_ratio * 0.25
                    passed = penalty < 5.0 # Consider failed if penalty is high (> 20% null)

                    checks.append(QualityCheck(
                        name="null_check",
                        column=col,
                        passed=passed,
                        value=round(null_ratio, 2),
                        threshold=20.0, # Visual threshold
                        message=f"{null_ratio:.2f}% nulls (-{penalty:.1f} pts)" if penalty > 0 else None
                    ))

                # Find max null penalty
                max_null_ratio = 0.0
                if null_counts:
                    max_null_ratio = max([(c / total_rows * 100) for c in null_counts.values()])

                null_penalty = min(25.0, max_null_ratio * 0.25)
                score -= null_penalty

                result.null_counts = null_counts

                # B) Duplicate Check
                duplicate_count = max(0, total_rows - distinct_rows)  # Prevent negative
                result.duplicate_count = duplicate_count

                duplicate_ratio = 0.0
                if total_rows > 0:
                    duplicate_ratio = (duplicate_count / total_rows) * 100

                # Dynamic Scoring: Deduct 0.5 points for every 1% of duplicates
                # Example: 10% dup -> -5 points. 50% dup -> -25 points.
                dup_penalty = min(25.0, duplicate_ratio * 0.5)

                passed = dup_penalty < 5.0 # Warn if > 10% dup
                checks.append(QualityCheck(
                    name="duplicate_check",
                    column=None,
                    passed=passed,
                    value=round(duplicate_ratio, 2),
                    threshold=10.0,
                    message=f"{duplicate_ratio:.2f}% duplicates (-{dup_penalty:.1f} pts)" if dup_penalty > 0 else None
                ))

                score -= dup_penalty


                # C) Freshness Check
                time_cols = [
                    col for col, dtype in column_types.items()
                    if 'TIMESTAMP' in dtype or 'DATE' in dtype
                    or any(k in col.lower() for k in ['created_at', 'updated_at', 'timestamp', 'date', 'time'])
                ]

                for col in time_cols:
                    max_val = col_stats[col]['max']
                    if max_val:
                        try:
                            now = datetime.utcnow()
                            if isinstance(max_val, datetime):
                                diff = now - max_val
                                days = diff.total_seconds() / 86400
                            elif isinstance(max_val, date):
                                dt_val = datetime.combine(max_val, datetime.min.time())
                                diff = now - dt_val
                                days = diff.total_seconds() / 86400
                            else:
                                days = 0

                            days = max(0.0, days)

                            # Calculate penalty for this column
                            # Logic: 1 point deduction per 100 days
                            # 1 year (365 days) -> -3.65 points
                            col_penalty = min(25.0, days / 100.0)
                            passed = col_penalty < 5.0 # Warn if > 500 days old

                            checks.append(QualityCheck(
                                name="freshness_check",
                                column=col,
                                passed=passed,
                                value=round(days, 1),
                                threshold=500.0, # days
                                message=f"{days:.1f} days old (-{col_penalty:.1f} pts)" if col_penalty > 0 else None
                            ))

                        except Exception:
                            pass

                # Deduct freshness penalty (find the most recent column)
                freshness_penalty = 0.0
                if time_cols:
                    min_lag = 999999.0
                    found_valid = False
                    for col in time_cols:
                        m_val = col_stats[col]['max']
                        if m_val:
                            try:
                                now = datetime.utcnow()
                                if isinstance(m_val, datetime):
                                    days = (now - m_val).total_seconds() / 86400
                                elif isinstance(m_val, date):
                                    days = (now - datetime.combine(m_val, datetime.min.time())).total_seconds() / 86400
                                else:
                                    days = 0
                                days = max(0.0, days)
                                if days < min_lag:
                                    min_lag = days
                                    found_valid = True
                            except:
                                pass

                    if found_valid:
                        freshness_penalty = min(25.0, min_lag / 100.0)

                score -= freshness_penalty

                # D) Validity Check
                target_keywords = ['price', 'amount', 'count', 'quantity', 'cost', 'age']

                validity_penalty = 0.0

                for col in column_names:
                    dtype = column_types[col]
                    # Check numeric types
                    if any(t in dtype for t in ['INT', 'DOUBLE', 'FLOAT', 'DECIMAL', 'NUMERIC', 'LONG', 'SHORT', 'TINY']):
                        lower_col = col.lower()
                        if any(kw in lower_col for kw in target_keywords):
                            min_val = col_stats[col]['min']
                            try:
                                if min_val is not None and min_val < 0:
                                    passed = False
                                    penalty = 10.0
                                    checks.append(QualityCheck(
                                        name="validity_check",
                                        column=col,
                                        passed=False,
                                        value=float(min_val),
                                        threshold=0.0,
                                        message=f"Negative value found (-10 pts)"
                                    ))
                                    validity_penalty += penalty
                            except:
                                pass

                validity_penalty = min(25.0, validity_penalty)
                score -= validity_penalty

                # 6. Calculate final score for this batch
                result.checks = checks
                result.overall_score = max(0.0, score)
                result.duration_ms = int((time.time() - start_time) * 1000)

                # Update progress
                result.processed_files = end_idx
                result.progress = (end_idx / len(parquet_keys)) * 100

                # Save partial result
                await result.save()

                print(f"[Quality Check] Batch {batch_idx + 1}/{num_batches} done. Progress: {result.progress:.1f}%, Score: {result.overall_score:.1f}")

            # All batches completed
            result.status = "completed"
            result.completed_at = datetime.utcnow()
            result.progress = 100.0
            result.duration_ms = int((time.time() - start_time) * 1000)
            await result.save()

            # Update Dataset with row_count
            try:
                from beanie import PydanticObjectId
                from models import Dataset

                dataset = await Dataset.get(PydanticObjectId(dataset_id))
                if dataset:
                    dataset.row_count = result.row_count
                    dataset.updated_at = datetime.utcnow()
                    await dataset.save()
                    print(f"Updated Dataset {dataset.name} with row count: {result.row_count}")
            except Exception as e:
                print(f"Warning: Failed to update Dataset row_count: {str(e)}")

            print(f"[Quality Check] Completed! Total rows: {result.row_count}, Score: {result.overall_score:.1f}")

            return result
            
        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            result.completed_at = datetime.utcnow()
            result.duration_ms = int((time.time() - start_time) * 1000)
            await result.save()
            raise
        
        finally:
            # Cleanup temp files
            for path in temp_paths:
                try:
                    os.unlink(path)
                except:
                    pass
    
    async def get_latest_result(self, dataset_id: str) -> Optional[QualityResult]:
        """Get the most recent quality result for a Dataset."""
        return await QualityResult.find_one(
            QualityResult.dataset_id == dataset_id,
            sort=[("run_at", -1)]
        )

    async def get_result_history(
        self,
        dataset_id: str,
        limit: int = 10
    ) -> list[QualityResult]:
        """Get quality result history for a Dataset."""
        return await QualityResult.find(
            QualityResult.dataset_id == dataset_id,
            sort=[("run_at", -1)],
            limit=limit
        ).to_list()

    async def get_dashboard_summary(self):
        """
        Get aggregated quality metrics for the dashboard.
        Returns latest result for every Dataset.
        """
        # MongoDB Aggregation to get the latest result for each Dataset
        pipeline = [
            {"$sort": {"run_at": -1}},
            {"$group": {
                "_id": "$dataset_id",
                "latest": {"$first": "$$ROOT"}
            }},
            {"$replaceRoot": {"newRoot": "$latest"}}
        ]

        latest_results = await QualityResult.aggregate(pipeline).to_list()

        # Calculate statistics
        total_datasets = len(latest_results)
        if total_datasets == 0:
            return {
                "summary": {
                    "total_count": 0,
                    "avg_score": 0,
                    "healthy_count": 0,
                    "warning_count": 0,
                    "critical_count": 0
                },
                "results": []
            }

        total_score = sum(r["overall_score"] for r in latest_results)
        avg_score = total_score / total_datasets

        # Categorize
        # Healthy: 90-100, Warning: 70-89, Critical: < 70
        healthy_count = sum(1 for r in latest_results if r["overall_score"] >= 90)
        warning_count = sum(1 for r in latest_results if 70 <= r["overall_score"] < 90)
        critical_count = sum(1 for r in latest_results if r["overall_score"] < 70)

        # Sort results by score check (critical on top)
        latest_results.sort(key=lambda x: x["overall_score"])

        # Convert ObjectId to string for JSON serialization
        for r in latest_results:
            if "_id" in r:
                r["_id"] = str(r["_id"])

        return {
            "summary": {
                "total_count": total_datasets,
                "avg_score": round(avg_score, 1),
                "healthy_count": healthy_count,
                "warning_count": warning_count,
                "critical_count": critical_count
            },
            "results": latest_results
        }


# Singleton instance
quality_service = QualityService()
