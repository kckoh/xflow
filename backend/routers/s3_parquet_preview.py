"""
S3 Parquet Preview API
Preview and extract schema from S3 Parquet files
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from bson import ObjectId
import io

import database

router = APIRouter()


class S3ParquetPreviewRequest(BaseModel):
    connection_id: str
    bucket: str
    path: str
    limit: Optional[int] = 10


class S3ParquetPreviewResponse(BaseModel):
    valid: bool
    columns: Optional[List[Dict[str, str]]] = None
    preview_data: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None


@router.post("/preview-parquet", response_model=S3ParquetPreviewResponse)
async def preview_s3_parquet(request: S3ParquetPreviewRequest):
    """
    Preview S3 Parquet file and extract schema
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

        boto3_kwargs = {'region_name': region}

        if access_key and secret_key:
            boto3_kwargs['aws_access_key_id'] = access_key
            boto3_kwargs['aws_secret_access_key'] = secret_key
        elif endpoint:
            boto3_kwargs['aws_access_key_id'] = "test"
            boto3_kwargs['aws_secret_access_key'] = "test"

        if endpoint:
            boto3_kwargs['endpoint_url'] = endpoint

        s3_client = boto3.client('s3', **boto3_kwargs)

        # 4. List and read Parquet files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=10)

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise HTTPException(status_code=404, detail=f"No files found in s3://{bucket}/{path}")

        # Find first Parquet file
        parquet_key = None
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):
                continue
            if key.lower().endswith('.parquet'):
                parquet_key = key
                break

        if not parquet_key:
            raise HTTPException(status_code=404, detail=f"No Parquet files found in s3://{bucket}/{path}")

        # 5. Read Parquet file
        file_obj = s3_client.get_object(Bucket=bucket, Key=parquet_key)
        parquet_content = file_obj['Body'].read()
        df = pd.read_parquet(io.BytesIO(parquet_content))

        # 6. Infer schema
        columns = []
        for col_name in df.columns:
            dtype = str(df[col_name].dtype)

            if 'int' in dtype:
                col_type = 'integer'
            elif 'float' in dtype:
                col_type = 'float'
            elif dtype == 'bool':
                col_type = 'boolean'
            elif 'datetime' in dtype:
                col_type = 'timestamp'
            else:
                col_type = 'string'

            columns.append({'name': col_name, 'type': col_type})

        # 7. Get preview data
        preview_df = df.head(request.limit)
        preview_data = []
        for _, row in preview_df.iterrows():
            row_dict = {}
            for col in df.columns:
                val = row[col]
                if pd.isna(val):
                    row_dict[col] = None
                elif isinstance(val, pd.Timestamp):
                    row_dict[col] = val.isoformat()
                else:
                    row_dict[col] = val
            preview_data.append(row_dict)

        return S3ParquetPreviewResponse(
            valid=True,
            columns=columns,
            preview_data=preview_data
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return S3ParquetPreviewResponse(
            valid=False,
            error=f"Failed to preview Parquet: {str(e)}"
        )
