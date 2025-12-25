import pandas as pd
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime, timedelta

def create_minio_client():
    return Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
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

def upload_to_minio(client, bucket_name, datasets):
    """MinIO에 Parquet 파일 업로드 (파티셔닝 적용)"""
    try:
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"버킷 '{bucket_name}' 생성 완료")
        else:
            print(f"버킷 '{bucket_name}' 이미 존재")
    except Exception as e:
        print(f"버킷 확인 중 오류: {e}")

    # user_events - event_date로 파티셔닝
    df = datasets['user_events']
    df['event_date'] = df['timestamp'].dt.date

    for date, group in df.groupby('event_date'):
        partition_df = group.drop('event_date', axis=1)

        table = pa.Table.from_pandas(partition_df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        object_name = f"user_events/event_date={date}/data.parquet"
        client.put_object(bucket_name, object_name, buf,
                         length=buf.getbuffer().nbytes,
                         content_type="application/octet-stream")
        print(f"{object_name} ({len(partition_df)}행)")

    # transactions - transaction_date로 파티셔닝
    print(f"transactions 업로드 중 (파티셔닝: transaction_date)...")
    df = datasets['transactions']
    df['trans_date'] = df['transaction_date'].dt.date

    for date, group in df.groupby('trans_date'):
        partition_df = group.drop('trans_date', axis=1)

        table = pa.Table.from_pandas(partition_df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        object_name = f"transactions/transaction_date={date}/data.parquet"
        client.put_object(bucket_name, object_name, buf,
                         length=buf.getbuffer().nbytes,
                         content_type="application/octet-stream")
        print(f"{object_name} ({len(partition_df)}행)")

    # products - 파티셔닝 없음 (작은 마스터 테이블)
    print(f"products 업로드 중 (파티셔닝 없음)...")
    df = datasets['products']
    table = pa.Table.from_pandas(df)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    object_name = f"products/data.parquet"
    client.put_object(bucket_name, object_name, buf,
                     length=buf.getbuffer().nbytes,
                     content_type="application/octet-stream")
    print(f"{object_name} ({len(df)}행)")

def main():
    client = create_minio_client()

    datasets = create_sample_datasets()

    bucket_name = "jungle-xflow"
    upload_to_minio(client, bucket_name, datasets)

    print("http://localhost:9001")

if __name__ == "__main__":
    main()
