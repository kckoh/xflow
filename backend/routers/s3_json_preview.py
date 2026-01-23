"""
S3 JSON Preview API
Preview and extract schema from S3 JSON files
"""
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import List, Dict, Any, Optional
from bson import ObjectId
import io

import database

router = APIRouter()


class S3JSONPreviewRequest(BaseModel):
    connection_id: str
    bucket: str
    path: str
    limit: Optional[int] = 10


class S3JSONPreviewResponse(BaseModel):
    valid: bool
    columns: Optional[List[Dict[str, str]]] = None
    preview_data: Optional[List[Dict[str, Any]]] = None
    total_rows: Optional[int] = None
    error: Optional[str] = None


@router.post("/preview-json", response_model=S3JSONPreviewResponse)
async def preview_s3_json(request: S3JSONPreviewRequest):
    """
    Preview S3 JSON file and extract schema

    Steps:
    1. Get connection details
    2. Read JSON file from S3
    3. Infer schema from JSON data
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

        # 4. List and read JSON files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=10)

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise HTTPException(status_code=404, detail=f"No JSON files found in s3://{bucket}/{path}")

        # Find first JSON file
        json_key = None
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip directories
                continue
            if key.lower().endswith('.json'):
                json_key = key
                break

        if not json_key:
            raise HTTPException(status_code=404, detail=f"No JSON files found in s3://{bucket}/{path}")

        # 5. Read JSON file with pandas (only first N rows to avoid OOM)
        file_obj = s3_client.get_object(Bucket=bucket, Key=json_key)
        json_content = file_obj['Body'].read()

        # Try to read as JSON lines (newline-delimited JSON) or array of objects
        try:
            # First try newline-delimited JSON (more common for big data)
            # Parse dates automatically for timestamp column detection
            df = pd.read_json(io.BytesIO(json_content), lines=True, nrows=min(request.limit + 20, 100), convert_dates=True)
        except:
            # Fallback to regular JSON array
            df = pd.read_json(io.BytesIO(json_content), convert_dates=True)
            # Limit rows after reading
            df = df.head(min(request.limit + 20, 100))

        # 6. Infer schema from dataframe
        columns = []
        for col_name in df.columns:
            dtype = str(df[col_name].dtype)

            # Map pandas dtype to our type system
            if 'int' in dtype:
                col_type = 'integer'
            elif 'float' in dtype:
                col_type = 'float'
            elif dtype == 'bool':
                col_type = 'boolean'
            elif 'datetime' in dtype:
                col_type = 'timestamp'
            elif dtype == 'object':
                # Check if it's nested JSON (dict/list)
                sample = df[col_name].dropna().iloc[0] if len(df[col_name].dropna()) > 0 else None
                if isinstance(sample, (dict, list)):
                    col_type = 'json'
                else:
                    col_type = 'string'
            else:
                col_type = 'string'

            columns.append({
                'name': col_name,
                'type': col_type
            })

        # 7. Get preview data (first N rows)
        preview_df = df.head(request.limit)

        # Convert to dict, handling nested JSON objects
        preview_data = []
        for _, row in preview_df.iterrows():
            row_dict = {}
            for col in df.columns:
                val = row[col]
                # Convert nested objects to strings for JSON serialization
                if isinstance(val, (dict, list)):
                    import json
                    row_dict[col] = json.dumps(val)
                elif pd.isna(val):
                    row_dict[col] = None
                else:
                    row_dict[col] = val
            preview_data.append(row_dict)

        # 8. Return results (no total_rows since we only read a sample)
        return S3JSONPreviewResponse(
            valid=True,
            columns=columns,
            preview_data=preview_data,
            total_rows=None  # Don't count total rows to avoid loading entire file
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return S3JSONPreviewResponse(
            valid=False,
            error=f"Failed to preview JSON: {str(e)}"
        )
