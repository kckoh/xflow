"""
샘플 데이터 생성 스크립트
S3 + Glue Catalog, MongoDB에 테스트 데이터 삽입
"""
import asyncio
import os
from datetime import datetime
import pandas as pd
import boto3
from motor.motor_asyncio import AsyncIOMotorClient


# ==================== 1. S3 + Glue Catalog 샘플 데이터 ====================

def create_s3_sample_data():
    """
    S3에 샘플 Parquet 파일 업로드 + Glue Catalog에 테이블 등록
    """
    print("📦 Creating S3 + Glue sample data...")

    # AWS 클라이언트 (LocalStack)
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('AWS_ENDPOINT', 'http://localhost:4566'),
        region_name=os.getenv('AWS_REGION', 'ap-northeast-2'),
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

    glue = boto3.client(
        'glue',
        endpoint_url=os.getenv('AWS_ENDPOINT', 'http://localhost:4566'),
        region_name=os.getenv('AWS_REGION', 'ap-northeast-2'),
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )

    # 샘플 데이터프레임 생성
    users_df = pd.DataFrame({
        'user_id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com', 'david@example.com', 'eve@example.com'],
        'age': [25, 30, 35, 28, 32],
        'signup_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    })

    products_df = pd.DataFrame({
        'product_id': [101, 102, 103, 104, 105],
        'product_name': ['Laptop', 'Mouse', 'Keyboard', 'Monitor', 'Headphones'],
        'category': ['Electronics', 'Accessories', 'Accessories', 'Electronics', 'Audio'],
        'price': [1200.00, 25.99, 79.99, 350.00, 199.99],
        'stock': [50, 200, 150, 30, 80]
    })

    # Parquet 파일로 저장
    users_df.to_parquet('/tmp/users.parquet', index=False)
    products_df.to_parquet('/tmp/products.parquet', index=False)

    # S3 버킷 확인 및 생성
    bucket_name = 'xflow-raw-data'
    try:
        s3.head_bucket(Bucket=bucket_name)
    except:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={'LocationConstraint': os.getenv('AWS_REGION', 'ap-northeast-2')}
        )

    # S3 업로드
    s3.upload_file('/tmp/users.parquet', bucket_name, 'users/users.parquet')
    s3.upload_file('/tmp/products.parquet', bucket_name, 'products/products.parquet')
    print(f"✅ Uploaded sample files to s3://{bucket_name}/")

    # Glue Database 생성 (이미 있으면 스킵)
    try:
        glue.create_database(
            DatabaseInput={
                'Name': 'sample_db',
                'Description': 'Sample database for testing'
            }
        )
        print("✅ Created Glue database: sample_db")
    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Glue database already exists: sample_db")

    # Glue Table 생성 - Users
    try:
        glue.create_table(
            DatabaseName='sample_db',
            TableInput={
                'Name': 'users',
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'user_id', 'Type': 'bigint', 'Comment': 'User ID'},
                        {'Name': 'name', 'Type': 'string', 'Comment': 'User name'},
                        {'Name': 'email', 'Type': 'string', 'Comment': 'Email address'},
                        {'Name': 'age', 'Type': 'int', 'Comment': 'User age'},
                        {'Name': 'signup_date', 'Type': 'string', 'Comment': 'Signup date'}
                    ],
                    'Location': f's3://{bucket_name}/users/',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        print("✅ Created Glue table: sample_db.users")
    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Glue table already exists: sample_db.users")

    # Glue Table 생성 - Products
    try:
        glue.create_table(
            DatabaseName='sample_db',
            TableInput={
                'Name': 'products',
                'StorageDescriptor': {
                    'Columns': [
                        {'Name': 'product_id', 'Type': 'bigint', 'Comment': 'Product ID'},
                        {'Name': 'product_name', 'Type': 'string', 'Comment': 'Product name'},
                        {'Name': 'category', 'Type': 'string', 'Comment': 'Product category'},
                        {'Name': 'price', 'Type': 'double', 'Comment': 'Product price'},
                        {'Name': 'stock', 'Type': 'int', 'Comment': 'Stock quantity'}
                    ],
                    'Location': f's3://{bucket_name}/products/',
                    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
                    'SerdeInfo': {
                        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
                    }
                },
                'TableType': 'EXTERNAL_TABLE'
            }
        )
        print("✅ Created Glue table: sample_db.products")
    except glue.exceptions.AlreadyExistsException:
        print("ℹ️  Glue table already exists: sample_db.products")


# ==================== 2. MongoDB 샘플 데이터 ====================

async def create_mongodb_sample_data():
    """
    MongoDB에 샘플 컬렉션 및 문서 삽입
    """
    print("\n🍃 Creating MongoDB sample data...")

    mongo_url = os.getenv('MONGODB_URL', 'mongodb://mongo:mongo@localhost:27017')
    client = AsyncIOMotorClient(mongo_url)

    # 샘플 데이터베이스 생성
    db = client['ecommerce']

    # Orders 컬렉션
    orders = db['orders']
    await orders.delete_many({})  # 기존 데이터 삭제
    await orders.insert_many([
        {
            'order_id': 'ORD-001',
            'user_id': 1,
            'product_ids': [101, 102],
            'total_amount': 1225.99,
            'status': 'completed',
            'order_date': datetime(2024, 1, 15)
        },
        {
            'order_id': 'ORD-002',
            'user_id': 2,
            'product_ids': [103, 104],
            'total_amount': 429.99,
            'status': 'pending',
            'order_date': datetime(2024, 1, 16)
        },
        {
            'order_id': 'ORD-003',
            'user_id': 3,
            'product_ids': [105],
            'total_amount': 199.99,
            'status': 'shipped',
            'order_date': datetime(2024, 1, 17)
        }
    ])
    print(f"✅ Inserted {await orders.count_documents({})} documents into ecommerce.orders")

    # Reviews 컬렉션
    reviews = db['reviews']
    await reviews.delete_many({})
    await reviews.insert_many([
        {
            'review_id': 'REV-001',
            'product_id': 101,
            'user_id': 1,
            'rating': 5,
            'comment': 'Excellent laptop!',
            'review_date': datetime(2024, 1, 20)
        },
        {
            'review_id': 'REV-002',
            'product_id': 102,
            'user_id': 2,
            'rating': 4,
            'comment': 'Good mouse, but a bit small',
            'review_date': datetime(2024, 1, 21)
        },
        {
            'review_id': 'REV-003',
            'product_id': 105,
            'user_id': 3,
            'rating': 5,
            'comment': 'Amazing sound quality!',
            'review_date': datetime(2024, 1, 22)
        }
    ])
    print(f"✅ Inserted {await reviews.count_documents({})} documents into ecommerce.reviews")

    client.close()


# ==================== 메인 실행 ====================

async def main():
    """
    샘플 데이터 생성 메인 함수
    """
    print("🚀 Starting sample data creation...\n")

    # 1. S3 + Glue
    create_s3_sample_data()

    # 2. MongoDB
    await create_mongodb_sample_data()

    print("\n✨ All sample data created successfully!")
    print("\n📝 Next steps:")
    print("1. Run indexing: POST http://localhost:8000/api/opensearch/index")
    print("2. Search data: GET http://localhost:8000/api/opensearch/search?q=user")


if __name__ == "__main__":
    asyncio.run(main())
