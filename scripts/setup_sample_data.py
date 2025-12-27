import pandas as pd
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta

def create_s3_client():
    """LocalStack S3 클라이언트 생성"""
    return boto3.client(
        's3',
        endpoint_url='http://localhost:4566',  # LocalStack endpoint
        aws_access_key_id='test',              # Terraform variables.tf의 기본값
        aws_secret_access_key='test',          # Terraform variables.tf의 기본값
        region_name='ap-northeast-2'
    )

def create_sample_datasets():
    # 1. 사용자 활동 로그
    user_events = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
        'event_type': ['login', 'purchase', 'view', 'logout', 'purchase',
                       'login', 'view', 'purchase', 'logout', 'login'],
        'timestamp': pd.date_range('2025-01-01', periods=10, freq='h'),
        'amount': [None, 100.5, None, None, 250.0,
                   None, None, 150.0, None, None],
        'product_id': [None, 'P001', 'P002', None, 'P003',
                       None, 'P001', 'P002', None, None]
    })

    # 2. 제품 정보
    products = pd.DataFrame({
        'product_id': ['P001', 'P002', 'P003', 'P004', 'P005'],
        'product_name': ['노트북', '마우스', '키보드', '모니터', '헤드셋'],
        'category': ['전자기기', '액세서리', '액세서리', '전자기기', '액세서리'],
        'price': [1500000, 30000, 80000, 300000, 120000],
        'stock': [50, 200, 150, 30, 80]
    })

    # 3. 판매 트랜잭션
    base_date = datetime(2025, 1, 1)
    transactions = pd.DataFrame({
        'transaction_id': range(1, 21),
        'user_id': [1, 2, 3, 4, 5, 1, 2, 3, 4, 5,
                    1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
        'product_id': ['P001', 'P002', 'P003', 'P001', 'P002',
                       'P004', 'P005', 'P001', 'P003', 'P002',
                       'P002', 'P003', 'P004', 'P005', 'P001',
                       'P003', 'P002', 'P004', 'P001', 'P005'],
        'quantity': [1, 2, 1, 1, 3, 1, 1, 2, 1, 1,
                     2, 1, 1, 1, 1, 1, 2, 1, 1, 1],
        'total_amount': [1500000, 60000, 80000, 1500000, 90000,
                         300000, 120000, 3000000, 80000, 30000,
                         60000, 80000, 300000, 120000, 1500000,
                         80000, 60000, 300000, 1500000, 120000],
        'transaction_date': [base_date + timedelta(days=i) for i in range(20)]
    })

    return {
        'user_events': user_events,
        'products': products,
        'transactions': transactions
    }

def upload_to_s3(s3_client, bucket_name, datasets):
    """LocalStack S3에 Parquet 파일 업로드 (파티셔닝 적용)"""

    # 버킷 존재 확인 및 생성
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"버킷 '{bucket_name}' 이미 존재")
    except:
        try:
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-2'}
            )
            print(f"버킷 '{bucket_name}' 생성 완료")
        except Exception as e:
            print(f"버킷 생성 중 오류: {e}")

    # user_events - event_date로 파티셔닝
    print("user_events 업로드 중 (파티셔닝: event_date)")
    df = datasets['user_events']
    df['event_date'] = df['timestamp'].dt.date

    for date, group in df.groupby('event_date'):
        partition_df = group.drop('event_date', axis=1)

        table = pa.Table.from_pandas(partition_df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        object_key = f"user_events/event_date={date}/data.parquet"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=buf.getvalue(),
            ContentType='application/octet-stream'
        )
        print(f"  ✓ {object_key} ({len(partition_df)}행)")

    # transactions - transaction_date로 파티셔닝
    print("transactions 업로드 중 (파티셔닝: transaction_date)")
    df = datasets['transactions']
    df['trans_date'] = df['transaction_date'].dt.date

    for date, group in df.groupby('trans_date'):
        partition_df = group.drop('trans_date', axis=1)

        table = pa.Table.from_pandas(partition_df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        object_key = f"transactions/transaction_date={date}/data.parquet"
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=buf.getvalue(),
            ContentType='application/octet-stream'
        )
        print(f"{object_key} ({len(partition_df)}행)")

    # products - 파티셔닝 없음 (작은 마스터 테이블)
    print("products 업로드 중 (파티셔닝 없음)")
    df = datasets['products']
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    object_key = "products/data.parquet"
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=buf.getvalue(),
        ContentType='application/octet-stream'
    )
    print(f"  ✓ {object_key} ({len(df)}행)")

def main():
    s3_client = create_s3_client()
    datasets = create_sample_datasets()

    # Terraform에서 생성한 버킷 이름 사용
    bucket_name = "xflow-raw-data"  # terraform/localstack/s3.tf 참조

    upload_to_s3(s3_client, bucket_name, datasets)

    print("업로드 완료!")
    print(f"버킷: {bucket_name}")
    print("LocalStack 엔드포인트: http://localhost:4566")

if __name__ == "__main__":
    main()
