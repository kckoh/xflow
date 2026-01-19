"""
RDS Snapshot Export Service - RDS Snapshot to S3 Export 관리
역할: RDS Snapshot 생성 및 S3 Export를 통한 대용량 초기 로드 지원
"""
import os
import time
import logging
from datetime import datetime
from typing import Optional

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

AWS_REGION = os.getenv("AWS_REGION", "ap-northeast-2")


class RDSSnapshotExportService:
    """RDS Snapshot Export 관리 서비스"""

    def __init__(self, region: str = None):
        self.region = region or AWS_REGION
        self.rds_client = boto3.client("rds", region_name=self.region)

    def create_snapshot(
        self,
        db_instance_id: str,
        snapshot_id: str = None,
        tags: list = None,
    ) -> dict:
        """
        RDS DB 인스턴스의 스냅샷 생성

        Args:
            db_instance_id: RDS DB 인스턴스 식별자
            snapshot_id: 스냅샷 식별자 (없으면 자동 생성)
            tags: 스냅샷에 적용할 태그 리스트

        Returns:
            스냅샷 생성 응답 dict
        """
        if not snapshot_id:
            timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
            snapshot_id = f"xflow-{db_instance_id}-{timestamp}"

        logger.info(f"Creating RDS snapshot: {snapshot_id} from {db_instance_id}")

        try:
            params = {
                "DBInstanceIdentifier": db_instance_id,
                "DBSnapshotIdentifier": snapshot_id,
            }

            if tags:
                params["Tags"] = tags

            response = self.rds_client.create_db_snapshot(**params)

            logger.info(f"Snapshot creation initiated: {snapshot_id}")
            return {
                "snapshot_id": snapshot_id,
                "status": response["DBSnapshot"]["Status"],
                "arn": response["DBSnapshot"]["DBSnapshotArn"],
            }

        except ClientError as e:
            logger.error(f"Failed to create snapshot: {e}")
            raise

    def wait_for_snapshot_available(
        self,
        snapshot_id: str,
        timeout_minutes: int = 60,
        poll_interval_seconds: int = 30,
    ) -> bool:
        """
        스냅샷이 available 상태가 될 때까지 대기

        Args:
            snapshot_id: 스냅샷 식별자
            timeout_minutes: 타임아웃 (분)
            poll_interval_seconds: 폴링 간격 (초)

        Returns:
            성공 여부
        """
        logger.info(f"Waiting for snapshot {snapshot_id} to become available...")
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60

        while time.time() - start_time < timeout_seconds:
            try:
                response = self.rds_client.describe_db_snapshots(
                    DBSnapshotIdentifier=snapshot_id
                )
                status = response["DBSnapshots"][0]["Status"]

                logger.info(f"Snapshot {snapshot_id} status: {status}")

                if status == "available":
                    logger.info(f"Snapshot {snapshot_id} is now available")
                    return True
                elif status in ["failed", "deleted"]:
                    logger.error(f"Snapshot {snapshot_id} failed with status: {status}")
                    return False

                time.sleep(poll_interval_seconds)

            except ClientError as e:
                logger.error(f"Error checking snapshot status: {e}")
                time.sleep(poll_interval_seconds)

        logger.error(f"Timeout waiting for snapshot {snapshot_id}")
        return False

    def start_export_task(
        self,
        snapshot_id: str,
        s3_bucket: str,
        s3_prefix: str,
        iam_role_arn: str,
        kms_key_id: str = None,
        export_only: list = None,
    ) -> dict:
        """
        RDS 스냅샷을 S3로 Export

        Args:
            snapshot_id: 스냅샷 식별자
            s3_bucket: 대상 S3 버킷
            s3_prefix: S3 경로 prefix
            iam_role_arn: Export용 IAM Role ARN
            kms_key_id: KMS 키 ID (암호화된 스냅샷인 경우 필수)
            export_only: Export할 테이블 목록 (전체 Export시 None)

        Returns:
            Export task 정보 dict
        """
        # Get snapshot ARN
        response = self.rds_client.describe_db_snapshots(
            DBSnapshotIdentifier=snapshot_id
        )
        snapshot_arn = response["DBSnapshots"][0]["DBSnapshotArn"]

        # Generate unique export task ID
        timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        export_task_id = f"export-{snapshot_id}-{timestamp}"

        logger.info(f"Starting export task: {export_task_id}")
        logger.info(f"  Snapshot: {snapshot_arn}")
        logger.info(f"  Destination: s3://{s3_bucket}/{s3_prefix}")

        try:
            params = {
                "ExportTaskIdentifier": export_task_id,
                "SourceArn": snapshot_arn,
                "S3BucketName": s3_bucket,
                "S3Prefix": s3_prefix,
                "IamRoleArn": iam_role_arn,
            }

            if kms_key_id:
                params["KmsKeyId"] = kms_key_id

            if export_only:
                # Format: database.schema.table or database.table
                params["ExportOnly"] = export_only

            response = self.rds_client.start_export_task(**params)

            return {
                "export_task_id": export_task_id,
                "status": response["Status"],
                "s3_bucket": s3_bucket,
                "s3_prefix": f"{s3_prefix}/{export_task_id}",
                "snapshot_arn": snapshot_arn,
            }

        except ClientError as e:
            logger.error(f"Failed to start export task: {e}")
            raise

    def get_export_task_status(self, export_task_id: str) -> dict:
        """
        Export task 상태 조회

        Args:
            export_task_id: Export task 식별자

        Returns:
            Export task 상태 정보 dict
        """
        try:
            response = self.rds_client.describe_export_tasks(
                ExportTaskIdentifier=export_task_id
            )

            if not response["ExportTasks"]:
                return {"status": "NOT_FOUND", "export_task_id": export_task_id}

            task = response["ExportTasks"][0]
            return {
                "export_task_id": export_task_id,
                "status": task["Status"],
                "percent_progress": task.get("PercentProgress", 0),
                "s3_bucket": task.get("S3Bucket"),
                "s3_prefix": task.get("S3Prefix"),
                "failure_cause": task.get("FailureCause"),
                "total_extracted_data_in_gb": task.get("TotalExtractedDataInGB", 0),
            }

        except ClientError as e:
            logger.error(f"Error getting export task status: {e}")
            raise

    def wait_for_export_complete(
        self,
        export_task_id: str,
        timeout_minutes: int = 180,
        poll_interval_seconds: int = 60,
    ) -> dict:
        """
        Export task 완료 대기

        Args:
            export_task_id: Export task 식별자
            timeout_minutes: 타임아웃 (분) - 기본 3시간
            poll_interval_seconds: 폴링 간격 (초)

        Returns:
            최종 Export task 상태 dict
        """
        logger.info(f"Waiting for export task {export_task_id} to complete...")
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60

        while time.time() - start_time < timeout_seconds:
            status_info = self.get_export_task_status(export_task_id)
            status = status_info.get("status")
            progress = status_info.get("percent_progress", 0)

            logger.info(f"Export task {export_task_id}: {status} ({progress}%)")

            if status == "COMPLETE":
                logger.info(f"Export task {export_task_id} completed successfully")
                return status_info

            elif status in ["FAILED", "CANCELED"]:
                failure_cause = status_info.get("failure_cause", "Unknown")
                logger.error(f"Export task failed: {failure_cause}")
                raise RuntimeError(f"Export task failed: {failure_cause}")

            time.sleep(poll_interval_seconds)

        raise TimeoutError(f"Export task {export_task_id} timed out after {timeout_minutes} minutes")

    def cleanup_snapshot(self, snapshot_id: str) -> bool:
        """
        스냅샷 삭제 (Export 완료 후 정리)

        Args:
            snapshot_id: 스냅샷 식별자

        Returns:
            성공 여부
        """
        logger.info(f"Deleting snapshot: {snapshot_id}")

        try:
            self.rds_client.delete_db_snapshot(DBSnapshotIdentifier=snapshot_id)
            logger.info(f"Snapshot {snapshot_id} deleted successfully")
            return True

        except ClientError as e:
            if e.response["Error"]["Code"] == "DBSnapshotNotFound":
                logger.warning(f"Snapshot {snapshot_id} not found (already deleted)")
                return True
            logger.error(f"Failed to delete snapshot: {e}")
            return False

    def build_snapshot_export_path(
        self,
        export_info: dict,
        database: str,
        schema: str,
        table: str,
    ) -> str:
        """
        Export된 Parquet 파일의 S3 경로 생성

        Args:
            export_info: Export task 정보
            database: 데이터베이스 이름
            schema: 스키마 이름 (public 등)
            table: 테이블 이름

        Returns:
            S3A 형식의 전체 경로 (s3a://bucket/prefix/db.schema.table/)
        """
        s3_bucket = export_info.get("s3_bucket")
        s3_prefix = export_info.get("s3_prefix")

        # RDS Export format: {prefix}/{export_task_id}/{database}.{schema}.{table}/
        # Parquet files are stored in this directory
        table_path = f"{database}.{schema}.{table}"

        return f"s3a://{s3_bucket}/{s3_prefix}/{table_path}/"

    def execute_full_export_workflow(
        self,
        db_instance_id: str,
        s3_bucket: str,
        s3_prefix: str,
        iam_role_arn: str,
        export_only: list = None,
        kms_key_id: str = None,
        cleanup_after: bool = True,
        snapshot_timeout_minutes: int = 60,
        export_timeout_minutes: int = 180,
    ) -> dict:
        """
        전체 Export 워크플로우 실행 (스냅샷 생성 → 대기 → Export → 완료 대기 → 정리)

        Args:
            db_instance_id: RDS DB 인스턴스 식별자
            s3_bucket: 대상 S3 버킷
            s3_prefix: S3 경로 prefix
            iam_role_arn: Export용 IAM Role ARN
            export_only: Export할 테이블 목록
            kms_key_id: KMS 키 ID
            cleanup_after: 완료 후 스냅샷 삭제 여부
            snapshot_timeout_minutes: 스냅샷 대기 타임아웃
            export_timeout_minutes: Export 대기 타임아웃

        Returns:
            워크플로우 결과 dict
        """
        logger.info(f"Starting full export workflow for {db_instance_id}")
        result = {
            "db_instance_id": db_instance_id,
            "started_at": datetime.utcnow().isoformat(),
        }

        try:
            # Step 1: Create snapshot
            snapshot_result = self.create_snapshot(db_instance_id)
            snapshot_id = snapshot_result["snapshot_id"]
            result["snapshot_id"] = snapshot_id

            # Step 2: Wait for snapshot to be available
            if not self.wait_for_snapshot_available(
                snapshot_id, timeout_minutes=snapshot_timeout_minutes
            ):
                raise RuntimeError(f"Snapshot {snapshot_id} failed to become available")

            # Step 3: Start export task
            export_result = self.start_export_task(
                snapshot_id=snapshot_id,
                s3_bucket=s3_bucket,
                s3_prefix=s3_prefix,
                iam_role_arn=iam_role_arn,
                kms_key_id=kms_key_id,
                export_only=export_only,
            )
            export_task_id = export_result["export_task_id"]
            result["export_task_id"] = export_task_id
            result["s3_bucket"] = export_result["s3_bucket"]
            result["s3_prefix"] = export_result["s3_prefix"]

            # Step 4: Wait for export to complete
            export_status = self.wait_for_export_complete(
                export_task_id, timeout_minutes=export_timeout_minutes
            )
            result["status"] = "COMPLETE"
            result["total_extracted_data_in_gb"] = export_status.get("total_extracted_data_in_gb", 0)

            # Step 5: Cleanup snapshot if requested
            if cleanup_after:
                self.cleanup_snapshot(snapshot_id)
                result["snapshot_cleaned"] = True

            result["completed_at"] = datetime.utcnow().isoformat()
            logger.info(f"Export workflow completed successfully: {result}")
            return result

        except Exception as e:
            result["status"] = "FAILED"
            result["error"] = str(e)
            result["completed_at"] = datetime.utcnow().isoformat()
            logger.error(f"Export workflow failed: {e}")
            raise
