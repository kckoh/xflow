"""
Data Lake API - Glue Catalog & Athena Query
S3 데이터 레이크의 메타데이터 조회 및 쿼리 실행
"""
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query
from botocore.exceptions import ClientError
from utils.aws_client import get_aws_client

router = APIRouter()


# ==================== Glue Catalog APIs ====================

@router.get("/databases")
async def list_databases():
    """
    Glue Catalog 데이터베이스 목록 조회
    사이드바 데이터 소스 목록에 사용
    """
    try:
        glue = get_aws_client('glue')
        response = glue.get_databases()

        return {
            "databases": [
                {
                    "name": db["Name"],
                    "description": db.get("Description", ""),
                    "location_uri": db.get("LocationUri", ""),
                    "create_time": db.get("CreateTime").isoformat() if db.get("CreateTime") else None
                }
                for db in response["DatabaseList"]
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Glue API Error: {str(e)}")


@router.get("/databases/{database_name}/tables")
async def list_tables(database_name: str):
    """
    특정 데이터베이스의 테이블 목록 조회
    데이터베이스 클릭 시 테이블 목록 표시에 사용
    """
    try:
        glue = get_aws_client('glue')
        response = glue.get_tables(DatabaseName=database_name)

        return {
            "tables": [
                {
                    "name": table["Name"],
                    "database_name": table["DatabaseName"],
                    "create_time": table.get("CreateTime").isoformat() if table.get("CreateTime") else None,
                    "update_time": table.get("UpdateTime").isoformat() if table.get("UpdateTime") else None,
                    "table_type": table.get("TableType", ""),
                    "storage_location": table.get("StorageDescriptor", {}).get("Location", ""),
                    "column_count": len(table.get("StorageDescriptor", {}).get("Columns", []))
                }
                for table in response["TableList"]
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Glue API Error: {str(e)}")


@router.get("/tables/{database_name}/{table_name}")
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

        return {
            "name": table["Name"],
            "database_name": table["DatabaseName"],
            "table_type": table.get("TableType", ""),
            "create_time": table.get("CreateTime").isoformat() if table.get("CreateTime") else None,
            "update_time": table.get("UpdateTime").isoformat() if table.get("UpdateTime") else None,
            "storage_location": storage_desc.get("Location", ""),
            "input_format": storage_desc.get("InputFormat", ""),
            "output_format": storage_desc.get("OutputFormat", ""),
            "columns": [
                {
                    "name": col["Name"],
                    "type": col["Type"],
                    "comment": col.get("Comment", "")
                }
                for col in storage_desc.get("Columns", [])
            ],
            "partition_keys": [
                {
                    "name": pk["Name"],
                    "type": pk["Type"],
                    "comment": pk.get("Comment", "")
                }
                for pk in table.get("PartitionKeys", [])
            ]
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Glue API Error: {str(e)}")


# ==================== Athena Query APIs ====================

@router.post("/query/execute")
async def execute_query(query: str = Query(..., description="SQL query to execute")):
    """
    Athena 쿼리 실행
    쿼리 에디터에서 SQL 실행 시 사용
    """
    try:
        athena = get_aws_client('athena')

        response = athena.start_query_execution(
            QueryString=query,
            WorkGroup='xflow-workgroup',
            QueryExecutionContext={'Database': 'xflow_db'},
            ResultConfiguration={'OutputLocation': 's3://xflow-athena-results/'}
        )

        return {
            "query_execution_id": response["QueryExecutionId"],
            "query": query
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Athena API Error: {str(e)}")


@router.get("/query/{query_execution_id}/status")
async def get_query_status(query_execution_id: str):
    """
    쿼리 실행 상태 확인
    쿼리 실행 중 상태 폴링에 사용
    """
    try:
        athena = get_aws_client('athena')
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)

        execution = response["QueryExecution"]
        status = execution["Status"]

        return {
            "query_execution_id": query_execution_id,
            "state": status["State"],
            "state_change_reason": status.get("StateChangeReason", ""),
            "submission_date_time": status.get("SubmissionDateTime").isoformat() if status.get("SubmissionDateTime") else None,
            "completion_date_time": status.get("CompletionDateTime").isoformat() if status.get("CompletionDateTime") else None
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Athena API Error: {str(e)}")


@router.get("/query/{query_execution_id}/results")
async def get_query_results(query_execution_id: str):
    """
    쿼리 결과 조회
    쿼리 완료 후 결과 데이터 가져오기
    """
    try:
        athena = get_aws_client('athena')

        # 먼저 쿼리 상태 확인
        execution_response = athena.get_query_execution(QueryExecutionId=query_execution_id)
        state = execution_response["QueryExecution"]["Status"]["State"]

        if state != "SUCCEEDED":
            raise HTTPException(
                status_code=400,
                detail=f"Query is not completed. Current state: {state}"
            )

        # 결과 조회
        response = athena.get_query_results(QueryExecutionId=query_execution_id)

        # 컬럼 정보 추출
        column_info = response["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
        columns = [col["Name"] for col in column_info]

        # 데이터 행 추출 (첫 번째 행은 헤더이므로 제외)
        rows = response["ResultSet"]["Rows"][1:]  # Skip header row

        data = []
        for row in rows:
            row_data = {}
            for i, col_name in enumerate(columns):
                row_data[col_name] = row["Data"][i].get("VarCharValue", "")
            data.append(row_data)

        return {
            "columns": columns,
            "data": data,
            "row_count": len(data)
        }
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Athena API Error: {str(e)}")
