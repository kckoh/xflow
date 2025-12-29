"""
AWS Athena Query API
S3 데이터 레이크에 대한 SQL 쿼리 실행 및 결과 조회
"""

from botocore.exceptions import ClientError
from fastapi import APIRouter, HTTPException
from schemas.athena import (
    QueryExecutionResponse,
    QueryRequest,
    QueryResultsResponse,
    QueryStatusResponse,
)
from utils.aws_client import get_aws_client

router = APIRouter()


@router.post("/query", response_model=QueryExecutionResponse)
async def execute_query(request: QueryRequest):
    """
    Athena 쿼리 실행
    쿼리 에디터에서 SQL 실행 시 사용
    """
    try:
        athena = get_aws_client("athena")

        response = athena.start_query_execution(
            QueryString=request.query,
            WorkGroup="primary",
            QueryExecutionContext={"Database": "xflow_db"},
            ResultConfiguration={"OutputLocation": "s3://xflow-athena-results/"},
        )

        return QueryExecutionResponse(
            query_execution_id=response["QueryExecutionId"], query=request.query
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Athena API Error: {str(e)}")


@router.get("/{query_execution_id}/status", response_model=QueryStatusResponse)
async def get_query_status(query_execution_id: str):
    """
    쿼리 실행 상태 확인
    쿼리 실행 중 상태 폴링에 사용
    """
    try:
        athena = get_aws_client("athena")
        response = athena.get_query_execution(QueryExecutionId=query_execution_id)

        execution = response["QueryExecution"]
        status = execution["Status"]

        return QueryStatusResponse(
            query_execution_id=query_execution_id,
            state=status["State"],
            state_change_reason=status.get("StateChangeReason", ""),
            submission_date_time=status.get("SubmissionDateTime").isoformat()
            if status.get("SubmissionDateTime")
            else None,
            completion_date_time=status.get("CompletionDateTime").isoformat()
            if status.get("CompletionDateTime")
            else None,
        )
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Athena API Error: {str(e)}")


@router.get("/{query_execution_id}/results", response_model=QueryResultsResponse)
async def get_query_results(query_execution_id: str):
    """
    쿼리 결과 조회
    쿼리 완료 후 결과 데이터 가져오기
    """
    try:
        athena = get_aws_client("athena")

        # 먼저 쿼리 상태 확인
        execution_response = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )
        state = execution_response["QueryExecution"]["Status"]["State"]

        if state != "SUCCEEDED":
            raise HTTPException(
                status_code=400,
                detail=f"Query is not completed. Current state: {state}",
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

        return QueryResultsResponse(columns=columns, data=data, row_count=len(data))
    except ClientError as e:
        raise HTTPException(status_code=500, detail=f"Athena API Error: {str(e)}")
