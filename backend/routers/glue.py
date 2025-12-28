"""
AWS Glue Catalog API
S3 데이터 레이크의 메타데이터 조회 (데이터베이스, 테이블, 스키마)
"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException
from botocore.exceptions import ClientError
from utils.aws_client import get_aws_client
from schemas.glue import (
    DatabaseSchema,
    DatabaseListResponse,
    TableColumn,
    PartitionKey,
    TableListItem,
    TableListResponse,
    TableSchema,
    SyncS3Response
)

router = APIRouter()


@router.get("/databases", response_model=DatabaseListResponse)
async def list_databases():
    """
    Glue Catalog 데이터베이스 목록 조회
    사이드바 데이터 소스 목록에 사용
    """
    try:
        glue = get_aws_client('glue')
        response = glue.get_databases()

        databases = [
            DatabaseSchema(
                name=db["Name"],
                description=db.get("Description", ""),
                location_uri=db.get("LocationUri", ""),
                create_time=db.get("CreateTime").isoformat() if db.get("CreateTime") else None
            )
            for db in response["DatabaseList"]
        ]

        return DatabaseListResponse(databases=databases)
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Glue API Error: {str(e)}")


@router.get("/databases/{database_name}/tables", response_model=TableListResponse)
async def list_tables(database_name: str):
    """
    특정 데이터베이스의 테이블 목록 조회
    데이터베이스 클릭 시 테이블 목록 표시에 사용
    """
    try:
        glue = get_aws_client('glue')
        response = glue.get_tables(DatabaseName=database_name)

        tables = [
            TableListItem(
                name=table["Name"],
                database_name=table["DatabaseName"],
                create_time=table.get("CreateTime").isoformat() if table.get("CreateTime") else None,
                update_time=table.get("UpdateTime").isoformat() if table.get("UpdateTime") else None,
                table_type=table.get("TableType", ""),
                storage_location=table.get("StorageDescriptor", {}).get("Location", ""),
                column_count=len(table.get("StorageDescriptor", {}).get("Columns", []))
            )
            for table in response["TableList"]
        ]

        return TableListResponse(tables=tables)
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Glue API Error: {str(e)}")


@router.get("/tables/{database_name}/{table_name}", response_model=TableSchema)
async def get_table_schema(database_name: str, table_name: str):
    """
    테이블 스키마 상세 정보 조회
    테이블 클릭 시 컬럼 정보 표시에 사용
    """
    try:
        glue = get_aws_client('glue')
        response = glue.get_table(DatabaseName=database_name, Name=table_name)
        table = response["Table"]

        storage_desc = table.get("StorageDescriptor", {})

        columns = [
            TableColumn(
                name=col["Name"],
                type=col["Type"],
                comment=col.get("Comment", "")
            )
            for col in storage_desc.get("Columns", [])
        ]

        # TODO 나중에 조정
        partition_keys = [
            PartitionKey(
                name=pk["Name"],
                type=pk["Type"],
                comment=pk.get("Comment", "")
            )
            for pk in table.get("PartitionKeys", [])
        ]

        return TableSchema(
            name=table["Name"],
            database_name=table["DatabaseName"],
            table_type=table.get("TableType", ""),
            create_time=table.get("CreateTime").isoformat() if table.get("CreateTime") else None,
            update_time=table.get("UpdateTime").isoformat() if table.get("UpdateTime") else None,
            storage_location=storage_desc.get("Location", ""),
            input_format=storage_desc.get("InputFormat", ""),
            output_format=storage_desc.get("OutputFormat", ""),
            columns=columns,
            partition_keys=partition_keys
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Glue API Error: {str(e)}")


@router.post("/sync-s3", response_model=SyncS3Response)
async def sync_s3_data():
    """
    S3 데이터 동기화
    개별 테이블 Glue Crawler를 실행하여 S3의 새로운 데이터를 Glue Catalog에 반영
    (버킷 루트를 크롤하는 크롤러는 제외하고, 개별 테이블/폴더를 크롤하는 크롤러만 실행)
    """
    try:
        glue = get_aws_client('glue')

        # 모든 크롤러 조회
        response = glue.list_crawlers()
        all_crawler_names = response.get('CrawlerNames', [])

        if not all_crawler_names:
            return SyncS3Response(
                message="No crawlers found in the system",
                crawlers_started=[],
                total_crawlers=0
            )

        crawlers_started = []

        # 각 크롤러 검사 및 실행
        for crawler_name in all_crawler_names:
            try:
                # 크롤러 정보 조회
                crawler_info = glue.get_crawler(Name=crawler_name)
                crawler_state = crawler_info["Crawler"]["State"]

                # S3 타겟 경로 확인
                s3_targets = crawler_info["Crawler"].get("Targets", {}).get("S3Targets", [])

                if not s3_targets:
                    continue

                # 개별 테이블 크롤러인지 확인 (버킷 루트가 아닌 경우만)
                is_table_crawler = False
                for target in s3_targets:
                    path = target.get("Path", "").rstrip('/')

                    # S3 경로 파싱: s3://bucket/table/ 형태인지 확인
                    # 예: s3://xflow-raw-data/products/ → ['s3:', '', 'xflow-raw-data', 'products']
                    parts = [p for p in path.split('/') if p]

                    # parts 길이가 3 이상이면 개별 테이블 크롤러
                    # parts 길이가 2면 버킷 루트 크롤러 (s3://bucket/)
                    if len(parts) >= 3:
                        is_table_crawler = True
                        break

                # 버킷 루트 크롤러는 건너뛰기
                if not is_table_crawler:
                    continue

                # READY 상태일 때만 시작
                if crawler_state == "READY":
                    glue.start_crawler(Name=crawler_name)
                    crawlers_started.append(crawler_name)
                elif crawler_state == "RUNNING":
                    # 이미 실행 중인 경우
                    crawlers_started.append(f"{crawler_name} (already running)")
            except ClientError as e:
                # 크롤러 접근 오류는 무시하고 계속 진행
                error_code = e.response.get('Error', {}).get('Code', '')
                if error_code != 'EntityNotFoundException':
                    print(f"Warning: Error checking crawler {crawler_name}: {str(e)}")

        if not crawlers_started:
            return SyncS3Response(
                message="No crawlers were started. All crawlers may be currently running.",
                crawlers_started=[],
                total_crawlers=0
            )

        return SyncS3Response(
            message=f"Successfully started {len(crawlers_started)} crawler(s). New tables will appear shortly.",
            crawlers_started=crawlers_started,
            total_crawlers=len(crawlers_started)
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Glue API Error: {str(e)}")
