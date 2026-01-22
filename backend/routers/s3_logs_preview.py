"""
S3 Logs Preview API
Test S3 log parsing with regex, field selection, and filtering
"""
from fastapi import APIRouter, HTTPException
from typing import List, Dict, Any, Optional
import re
from bson import ObjectId

import database
from schemas.s3_logs import (
    S3LogPreviewRequest,
    S3LogPreviewResponse,
    RegexTestRequest,
    S3LogTestRequest,
    RegexTestResponse,
    GenerateRegexRequest,
    GenerateRegexResponse,
)

router = APIRouter()


def create_s3_client_from_config(config: dict):
    """
    Create boto3 S3 client from connection config.
    Supports both user credentials and LocalStack.
    
    Args:
        config: Connection config dict from MongoDB
        
    Returns:
        Configured boto3 S3 client
    """
    import boto3
    import os
    
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

    return boto3.client('s3', **boto3_kwargs)


@router.post("/test-regex", response_model=RegexTestResponse)
async def test_regex_pattern(request: RegexTestRequest):
    """
    Test regex pattern with actual S3 log files

    Steps:
    1. Get source dataset (S3 bucket/path)
    2. Read sample log files from S3 (50 lines)
    3. Test regex parsing
    4. Return raw logs vs parsed results
    """
    import boto3
    import os

    try:
        # Get MongoDB database
        db = database.mongodb_client[database.DATABASE_NAME]

        # 1. Get source dataset
        try:
            source_dataset = await db.source_datasets.find_one({"_id": ObjectId(request.source_dataset_id)})
        except:
            raise HTTPException(status_code=404, detail=f"Source dataset not found: {request.source_dataset_id}")

        if not source_dataset:
            raise HTTPException(status_code=404, detail=f"Source dataset not found: {request.source_dataset_id}")

        # 2. Get connection details
        connection_id = source_dataset.get("connection_id")
        if not connection_id:
            raise HTTPException(status_code=400, detail="Connection ID not found in source dataset")

        connection = await db.connections.find_one({"_id": ObjectId(connection_id)})
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        config = connection.get("config", {})

        # 3. Get S3 configuration
        bucket = source_dataset.get("bucket") or config.get("bucket")
        path = source_dataset.get("path") or config.get("path", "")

        if not bucket or not path:
            raise HTTPException(status_code=400, detail="S3 bucket and path are required")

        # 4. Configure boto3 S3 client
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

        # 5. List and read log files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=10)

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise HTTPException(status_code=404, detail=f"No log files found in s3://{bucket}/{path}")

        # Read first log file (up to 50 lines for testing)
        log_lines = []
        last_error = None
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip directories
                continue

            try:
                file_obj = s3_client.get_object(Bucket=bucket, Key=key)
                # Stream file and read only first 50 lines to avoid OOM with large files
                body = file_obj['Body']
                lines = []
                for _ in range(50):
                    line = body.readline()
                    if not line:  # EOF
                        break
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        lines.append(decoded)

                if lines:
                    log_lines.extend(lines)
                    break
            except Exception as e:
                last_error = e
                continue

        if not log_lines:
            if last_error:
                print(f"Failed to read S3 log content from s3://{bucket}/{path}: {last_error}")
            raise HTTPException(status_code=400, detail="Could not read log file content")

        total_lines = len(log_lines)

        # 6. Test regex pattern
        try:
            compiled_pattern = re.compile(request.custom_regex)
            named_groups = list(compiled_pattern.groupindex.keys())
            if not named_groups:
                raise ValueError("Regex pattern must contain at least one named group (?P<field_name>pattern)")
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"Invalid regex pattern: {str(e)}")

        # Parse each line
        parsed_rows = []
        for line in log_lines:
            match = compiled_pattern.match(line)
            if match:
                row = match.groupdict()
                parsed_rows.append(row)

        parsed_lines = len(parsed_rows)

        if parsed_lines == 0:
            return RegexTestResponse(
                valid=False,
                sample_logs=log_lines[:request.limit],
                parsed_rows=[],
                fields_extracted=named_groups,
                total_lines=total_lines,
                parsed_lines=0,
                error="No log lines matched the regex pattern. Please check your regex."
            )

        # 7. Return results
        return RegexTestResponse(
            valid=True,
            sample_logs=log_lines[:request.limit],
            parsed_rows=parsed_rows[:request.limit],
            fields_extracted=named_groups,
            total_lines=total_lines,
            parsed_lines=parsed_lines
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return RegexTestResponse(
            valid=False,
            error=f"Error: {str(e)}"
        )


@router.post("/test", response_model=RegexTestResponse)
async def test_s3_log_parsing_direct(request: S3LogTestRequest):
    """
    Test S3 log parsing without saving source dataset
    Only requires connection_id + bucket + path + regex

    This is used in SourceWizard where dataset hasn't been saved yet.
    """
    import boto3
    import os

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

        # 4. List and read log files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=10)

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise HTTPException(status_code=404, detail=f"No log files found in s3://{bucket}/{path}")

        # Read first log file (up to 50 lines for testing)
        log_lines = []
        last_error = None
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip directories
                continue

            try:
                file_obj = s3_client.get_object(Bucket=bucket, Key=key)
                # Stream file and read only first 50 lines to avoid OOM with large files
                body = file_obj['Body']
                lines = []
                for _ in range(50):
                    line = body.readline()
                    if not line:  # EOF
                        break
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        lines.append(decoded)

                if lines:
                    log_lines.extend(lines)
                    break
            except Exception as e:
                last_error = e
                continue

        if not log_lines:
            if last_error:
                print(f"Failed to read S3 log content from s3://{bucket}/{path}: {last_error}")
            raise HTTPException(status_code=400, detail="Could not read log file content")

        total_lines = len(log_lines)

        # 5. Test regex pattern
        try:
            compiled_pattern = re.compile(request.custom_regex)
            named_groups = list(compiled_pattern.groupindex.keys())
            if not named_groups:
                raise ValueError("Regex pattern must contain at least one named group (?P<field_name>pattern)")
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"Invalid regex pattern: {str(e)}")

        # Parse each line
        parsed_rows = []
        for line in log_lines:
            match = compiled_pattern.match(line)
            if match:
                row = match.groupdict()
                parsed_rows.append(row)

        parsed_lines = len(parsed_rows)

        if parsed_lines == 0:
            return RegexTestResponse(
                valid=False,
                sample_logs=log_lines[:request.limit],
                parsed_rows=[],
                fields_extracted=named_groups,
                total_lines=total_lines,
                parsed_lines=0,
                error="No log lines matched the regex pattern. Please check your regex."
            )

        # 6. Return results
        return RegexTestResponse(
            valid=True,
            sample_logs=log_lines[:request.limit],
            parsed_rows=parsed_rows[:request.limit],
            fields_extracted=named_groups,
            total_lines=total_lines,
            parsed_lines=parsed_lines
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return RegexTestResponse(
            valid=False,
            error=f"Test failed: {str(e)}"
        )


@router.post("/preview", response_model=S3LogPreviewResponse)
async def preview_s3_logs(request: S3LogPreviewRequest):
    """
    Preview S3 log parsing with field selection and filtering

    Steps:
    1. Get source dataset with customRegex
    2. Read sample log files from S3
    3. Parse logs with regex
    4. Apply field selection
    5. Apply filters
    6. Return before/after samples
    """
    import boto3
    import os
    from datetime import datetime

    try:
        # Get MongoDB database
        db = database.mongodb_client[database.DATABASE_NAME]

        # 1. Get source dataset
        try:
            source_dataset = await db.source_datasets.find_one({"_id": ObjectId(request.source_dataset_id)})
        except:
            raise HTTPException(status_code=404, detail=f"Source dataset not found: {request.source_dataset_id}")

        if not source_dataset:
            raise HTTPException(status_code=404, detail=f"Source dataset not found: {request.source_dataset_id}")

        # 2. Get connection details
        connection_id = source_dataset.get("connection_id")
        if not connection_id:
            raise HTTPException(status_code=400, detail="Connection ID not found in source dataset")

        connection = await db.connections.find_one({"_id": ObjectId(connection_id)})
        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        config = connection.get("config", {})

        # 3. Get S3 configuration
        bucket = source_dataset.get("bucket") or config.get("bucket")
        path = source_dataset.get("path") or config.get("path", "")
        custom_regex = request.custom_regex

        if not bucket or not path:
            raise HTTPException(status_code=400, detail="S3 bucket and path are required")

        if not custom_regex:
            raise HTTPException(status_code=400, detail="Custom regex pattern is required for log parsing")

        # 4. Configure boto3 S3 client
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

        # 5. List and read log files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=10)

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise HTTPException(status_code=404, detail=f"No log files found in s3://{bucket}/{path}")

        # Read first log file (up to 100 lines for preview)
        log_lines = []
        last_error = None
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip directories
                continue

            try:
                file_obj = s3_client.get_object(Bucket=bucket, Key=key)
                # Stream file and read only first 100 lines to avoid OOM with large files
                body = file_obj['Body']
                lines = []
                for _ in range(50):
                    line = body.readline()
                    if not line:  # EOF
                        break
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    lines.append(decoded)  # Include empty lines for preview

                if lines:
                    log_lines.extend(lines)
                    break
            except Exception as e:
                last_error = e
                continue

        if not log_lines:
            if last_error:
                print(f"Failed to read S3 log content from s3://{bucket}/{path}: {last_error}")
            raise HTTPException(status_code=400, detail="Could not read log file content")

        # 6. Parse logs with regex
        try:
            compiled_pattern = re.compile(custom_regex)
            named_groups = list(compiled_pattern.groupindex.keys())
            if not named_groups:
                raise ValueError("Regex pattern must contain at least one named group (?P<field_name>pattern)")
        except re.error as e:
            raise HTTPException(status_code=400, detail=f"Invalid regex pattern: {str(e)}")

        # Parse each line
        parsed_rows = []
        for line in log_lines:
            if not line.strip():
                continue

            match = compiled_pattern.match(line)
            if match:
                row = match.groupdict()
                parsed_rows.append(row)

        if not parsed_rows:
            raise HTTPException(status_code=400, detail="No log lines matched the regex pattern. Please check your regex.")

        total_records = len(parsed_rows)

        # 7. Apply field selection (before rows = all fields)
        before_rows = parsed_rows[:request.limit]

        # Apply field selection
        selected_rows = []
        for row in parsed_rows:
            selected_row = {field: row.get(field, "") for field in request.selected_fields}
            selected_rows.append(selected_row)

        # 8. Apply filters
        filtered_rows = selected_rows.copy()
        filters = request.filters

        # Status code filter
        if filters.get("statusCodeField") and (filters.get("statusCodeMin") or filters.get("statusCodeMax")):
            status_field = filters["statusCodeField"]
            min_code = int(filters["statusCodeMin"]) if filters.get("statusCodeMin") else None
            max_code = int(filters["statusCodeMax"]) if filters.get("statusCodeMax") else None

            def status_filter(row):
                try:
                    status = int(row.get(status_field, 0))
                    if min_code and status < min_code:
                        return False
                    if max_code and status > max_code:
                        return False
                    return True
                except:
                    return False

            filtered_rows = [row for row in filtered_rows if status_filter(row)]

        # Timestamp filter
        if filters.get("timestampField") and (filters.get("timestampFrom") or filters.get("timestampTo")):
            from datetime import datetime, timezone
            timestamp_field = filters["timestampField"]
            timestamp_from = filters.get("timestampFrom")
            timestamp_to = filters.get("timestampTo")

            def parse_ts(value):
                if not value:
                    return None
                try:
                    return datetime.fromisoformat(value)
                except ValueError:
                    try:
                        return datetime.strptime(value, "%d/%b/%Y:%H:%M:%S %z")
                    except ValueError:
                        return None

            def to_utc(dt):
                if dt is None:
                    return None
                if dt.tzinfo is None:
                    return dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)

            dt_from = to_utc(parse_ts(timestamp_from))
            dt_to = to_utc(parse_ts(timestamp_to))

            def timestamp_filter(row):
                try:
                    ts = row.get(timestamp_field, "")
                    dt_value = to_utc(parse_ts(ts))
                    if not dt_value:
                        return False
                    if dt_from and dt_value < dt_from:
                        return False
                    if dt_to and dt_value > dt_to:
                        return False
                    return True
                except:
                    return False

            filtered_rows = [row for row in filtered_rows if timestamp_filter(row)]

        filtered_records = len(filtered_rows)

        # 9. Return results
        return S3LogPreviewResponse(
            valid=True,
            before_rows=before_rows,
            after_rows=filtered_rows[:request.limit],
            total_records=total_records,
            filtered_records=filtered_records
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return S3LogPreviewResponse(
            valid=False,
            error=f"Error: {str(e)}"
        )


@router.post("/generate-regex", response_model=GenerateRegexResponse)
async def generate_regex_from_s3_logs(request: GenerateRegexRequest):
    """
    Generate regex pattern using AI from S3 log samples
    
    Steps:
    1. Get connection details
    2. Read sample log files from S3 (5-10 lines)
    3. Call Bedrock AI to generate regex pattern
    4. Return generated pattern + sample logs
    """
    from services.bedrock_service import get_bedrock_service
    
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

        # 3. Create S3 client from config
        s3_client = create_s3_client_from_config(config)

        # 4. List and read log files
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=path, MaxKeys=10)

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise HTTPException(status_code=404, detail=f"No log files found in s3://{bucket}/{path}")

        # Read first log file (up to 10 lines for AI analysis)
        log_lines = []
        last_error = None
        for obj in response['Contents']:
            key = obj['Key']
            if key.endswith('/'):  # Skip directories
                continue

            try:
                file_obj = s3_client.get_object(Bucket=bucket, Key=key)
                # Stream file and read only first 10 lines
                body = file_obj['Body']
                lines = []
                for _ in range(10):
                    line = body.readline()
                    if not line:  # EOF
                        break
                    decoded = line.decode('utf-8', errors='ignore').strip()
                    if decoded:
                        lines.append(decoded)

                if lines:
                    log_lines.extend(lines)
                    break
            except Exception as e:
                last_error = e
                continue

        if not log_lines:
            if last_error:
                print(f"Failed to read S3 log content from s3://{bucket}/{path}: {last_error}")
            raise HTTPException(status_code=400, detail="Could not read log file content")

        # 5. Call AI to generate regex
        bedrock_service = get_bedrock_service()
        regex_pattern = bedrock_service.generate_sql(
            question="Generate a Python regex pattern with named groups to parse these log lines",
            prompt_type="regex_pattern_log",
            metadata={"sample_logs": log_lines[:request.limit]}
        )

        return GenerateRegexResponse(
            success=True,
            regex_pattern=regex_pattern,
            sample_logs=log_lines[:request.limit]
        )

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return GenerateRegexResponse(
            success=False,
            error=f"Failed to generate regex: {str(e)}"
        )
