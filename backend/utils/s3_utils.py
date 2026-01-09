"""
S3 utility functions for calculating file sizes and managing S3 operations.
"""
import os
import re
from typing import Optional

import boto3
from botocore.exceptions import ClientError


def get_s3_client():
    """
    Create and return a configured S3 client.
    Supports both LocalStack and AWS environments.
    """
    # Check if using LocalStack (development)
    aws_endpoint = os.getenv("AWS_ENDPOINT_URL")
    
    if aws_endpoint:
        # LocalStack configuration
        s3_client = boto3.client(
            's3',
            endpoint_url=aws_endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
    else:
        # Real AWS configuration (IRSA or credentials)
        s3_client = boto3.client(
            's3',
            region_name=os.getenv("AWS_REGION", "us-east-1")
        )
    
    return s3_client


def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Parse S3 path to extract bucket and prefix.
    
    Args:
        s3_path: S3 path in format s3://bucket/path or s3a://bucket/path
        
    Returns:
        Tuple of (bucket_name, prefix)
        
    Raises:
        ValueError: If path format is invalid
    """
    # Remove s3:// or s3a:// prefix
    path = re.sub(r'^s3a?://', '', s3_path)
    
    # Split into bucket and prefix
    parts = path.split('/', 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ''
    
    # Remove trailing slash from prefix
    prefix = prefix.rstrip('/')
    
    if not bucket:
        raise ValueError(f"Invalid S3 path: {s3_path}")
    
    return bucket, prefix


async def calculate_s3_size_bytes(s3_path: str) -> int:
    """
    Calculate the total size of all files in an S3 path.
    
    Args:
        s3_path: S3 path (e.g., s3://bucket/path/ or s3a://bucket/path/)
        
    Returns:
        Total file size in bytes
        
    Raises:
        ValueError: If path format is invalid
        ClientError: If S3 access fails
    """
    try:
        # Parse S3 path
        bucket, prefix = parse_s3_path(s3_path)
        
        # Get S3 client
        s3_client = get_s3_client()
        
        # Calculate total size
        total_size = 0
        continuation_token = None
        
        while True:
            # List objects with pagination
            list_params = {
                'Bucket': bucket,
                'Prefix': prefix
            }
            
            if continuation_token:
                list_params['ContinuationToken'] = continuation_token
            
            response = s3_client.list_objects_v2(**list_params)
            
            # Sum up file sizes
            if 'Contents' in response:
                for obj in response['Contents']:
                    total_size += obj['Size']
            
            # Check if there are more results
            if response.get('IsTruncated'):
                continuation_token = response.get('NextContinuationToken')
            else:
                break
        
        print(f"Calculated S3 size for {s3_path}: {total_size} bytes ({total_size / (1024**3):.2f} GB)")
        return total_size
        
    except ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"S3 access error for {s3_path}: {error_code} - {str(e)}")
        raise
    except Exception as e:
        print(f"Error calculating S3 size for {s3_path}: {str(e)}")
        raise


async def check_s3_path_exists(s3_path: str) -> bool:
    """
    Check if an S3 path exists and contains at least one object.
    
    Args:
        s3_path: S3 path to check
        
    Returns:
        True if path exists and has objects, False otherwise
    """
    try:
        bucket, prefix = parse_s3_path(s3_path)
        s3_client = get_s3_client()
        
        response = s3_client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            MaxKeys=1
        )
        
        return response.get('KeyCount', 0) > 0
        
    except Exception as e:
        print(f"Error checking S3 path {s3_path}: {str(e)}")
        return False
