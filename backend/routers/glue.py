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
    TableSchema
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
