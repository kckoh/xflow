"""
AWS 클라이언트 공통 유틸리티
LocalStack과 실제 AWS 환경 자동 전환
"""
import os
import boto3


# 환경 설정
AWS_ENDPOINT = os.getenv('AWS_ENDPOINT', 'http://localhost:4566')  # LocalStack
AWS_REGION = os.getenv('AWS_REGION', 'ap-northeast-2')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID', 'test')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')


def get_aws_client(service: str):
    """
    AWS 클라이언트 생성 (LocalStack/AWS 자동 전환)
    Args:
        service: AWS 서비스 이름 (예: 'glue', 'athena', 's3', 'ec2')
    Returns:
        boto3 client
    """
    kwargs = {
        'region_name': AWS_REGION,
        'aws_access_key_id': AWS_ACCESS_KEY_ID,
        'aws_secret_access_key': AWS_SECRET_ACCESS_KEY
    }

    # LocalStack 사용 시에만 endpoint_url 설정
    if AWS_ENDPOINT and AWS_ENDPOINT != 'None':
        kwargs['endpoint_url'] = AWS_ENDPOINT

    return boto3.client(service, **kwargs)