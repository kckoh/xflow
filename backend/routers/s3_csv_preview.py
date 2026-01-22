"""
S3 CSV Preview API
Preview and extract schema from S3 CSV files
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from bson import ObjectId
import io

import database

router = APIRouter()


class S3CSVPreviewRequest(BaseModel):
    connection_id: str
    bucket: str
    path: str
    limit: Optional[int] = 10


class S3CSVPreviewResponse(BaseModel):
    valid: bool
    columns: Optional[List[Dict[str, str]]] = None
    preview_data: Optional[List[Dict[str, Any]]] = None
    total_rows: Optional[int] = None
    error: Optional[str] = None


@router.post("/preview-csv", response_model=S3CSVPreviewResponse)
async def preview_s3_csv(request: S3CSVPreviewRequest):
    """
    Preview S3 CSV file and extract schema

    Steps:
    1. Get connection details
    2. Read CSV file from S3
    3. Infer schema from CSV headers and data types
    4. Return preview data and schema
    """
    import boto3
    import os
    import pandas as pd

    try:
        # Get MongoDB database
        db = database.mongodb_client[database.DATABASE_NAME]

        # 1. Get connection details
        try:
            connection = await db.connections.find_one({"_id": ObjectId(request.connection_id)})
        except:
            raise HTTPException(status_code=404, detail=f"Connection not found: {request.connection_id}")

        if not connection:
            raise HTTPException(status_code=404, detail=f"Connection not found: {request.connection_id}")

        config = connection.get("config", {})

        # 2. Use provided bucket and path
        bucket = request.bucket
        path = request.path

        if not bucket or not path:
            raise HTTPException(status_code=400, detail="S3 bucket and path are required")

        # 3. Configure boto3 S3 client
        access_key = config.get("access_key") or config.get("access_key_id") or config.get("aws_access_key_id")
        secret_key = config.get("secret_key") or config.get("secret_access_key") or config.get("aws_secret_access_key")
        endpoint = (
            config.get("endpoint")
            or config.get("endpoint_url")
            or os.getenv("AWS_ENDPOINT")
            or os.getenv("S3_ENDPOINT_URL")
        )
        region = config.get("region") or config.get("region_name") or os.getenv("AWS_DEFAULT_REGION") or "us-east-1"

        boto3_kwargs = {
            'region_name': region
        }

        # Only add credentials if they exist in config, otherwise boto3 will use env vars or IAM role
        if access_key and secret_key:
            boto3_kwargs['aws_access_key_id'] = access_key
            boto3_kwargs['aws_secret_access_key'] = secret_key
        elif endpoint:
            # LocalStack requires dummy credentials
            boto3_kwargs['aws_access_key_id'] = "test"
            boto3_kwargs['aws_secret_access_key'] = "test"

        if endpoint:
            boto3_kwargs['endpoint_url'] = endpoint

        s3_client = boto3.client('s3', **boto3_kwargs)

        # 4. List and read CSV files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=10)

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise HTTPException(status_code=404, detail=f"No CSV files found in s3://{bucket}/{path}")

        # Find first CSV file
        csv_key = None
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip directories
                continue
            if key.lower().endswith('.csv'):
                csv_key = key
                break

        if not csv_key:
            raise HTTPException(status_code=404, detail=f"No CSV files found in s3://{bucket}/{path}")

        # 5. Read CSV file with pandas
        file_obj = s3_client.get_object(Bucket=bucket, Key=csv_key)
        csv_content = file_obj['Body'].read()

        # Use pandas to read CSV
        df = pd.read_csv(io.BytesIO(csv_content))

        total_rows = len(df)

        # 6. Infer schema from dataframe
        columns = []
        for col_name in df.columns:
            dtype = df[col_name].dtype

            # Map pandas dtype to our type system
            if dtype == 'int64':
                col_type = 'integer'
            elif dtype == 'float64':
                col_type = 'float'
            elif dtype == 'bool':
                col_type = 'boolean'
            elif dtype == 'datetime64[ns]':
                col_type = 'timestamp'
            else:
                col_type = 'string'

            columns.append({
                'name': col_name,
                'type': col_type
            })

        # 7. Get preview data (first N rows)
        preview_df = df.head(request.limit)
        preview_data = preview_df.to_dict('records')

        # 8. Return results
        return S3CSVPreviewResponse(
            valid=True,
            columns=columns,
            preview_data=preview_data,
            total_rows=total_rows
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return S3CSVPreviewResponse(
            valid=False,
            error=f"Failed to preview CSV: {str(e)}"
        )
