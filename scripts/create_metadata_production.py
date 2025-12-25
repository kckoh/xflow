"""
이 파일은 ETL 팀이 메타데이터를 작성할 때 참고할 표준 스키마입니다.
"""

import pandas as pd
from minio import Minio
import pyarrow as pa
import pyarrow.parquet as pq
import io
from datetime import datetime
from typing import Dict, List


def create_minio_client():
    return Minio(
        "localhost:9000",
        access_key="minio",
        secret_key="minio123",
        secure=False
    )


def create_tables_metadata_production():
    """
    테이블 메타데이터 
    """
    return pd.DataFrame({
        # ========== 기본 정보 ==========
        'catalog_name': ['hive', 'hive', 'hive'],
        'database_name': ['default', 'default', 'default'],
        'table_name': ['user_events', 'products', 'transactions'],
        'table_type': ['EXTERNAL_TABLE', 'EXTERNAL_TABLE', 'EXTERNAL_TABLE'],

        # ========== 비즈니스 메타데이터 ==========
        'display_name': ['사용자 이벤트', '제품 마스터', '주문 트랜잭션'],
        'description': [
            '사용자의 모든 활동 이벤트를 추적하는 로그 테이블. 로그인, 구매, 조회 등의 행동 데이터 포함.',
            '판매 가능한 모든 제품의 마스터 데이터. 가격, 재고, 카테고리 정보 포함.',
            '실제 발생한 모든 구매 트랜잭션. 매출 분석 및 정산의 기준이 되는 핵심 테이블.'
        ],
        'business_domain': ['customer_behavior', 'product_catalog', 'sales'],
        'business_owner': ['marketing-team', 'product-team', 'sales-team'],
        'technical_owner': ['data-eng-team', 'data-eng-team', 'data-eng-team'],
        'data_steward': ['john.doe@company.com', 'jane.smith@company.com', 'bob.wilson@company.com'],

        # ========== 데이터 계보 (Lineage) ==========
        'source_system': ['web-application', 'erp-system', 'order-system'],
        'source_database': ['app_db', 'product_db', 'order_db'],
        'source_table': ['event_logs', 'products', 'transactions'],
        'upstream_tables': [
            '',  # source 테이블이라 upstream 없음
            '',
            'user_events,products'  # transactions는 user_events와 products에 의존
        ],
        'downstream_tables': [
            'transactions,user_analysis',
            'transactions,product_analysis',
            'revenue_report,sales_kpi'
        ],

        # ========== ETL 정보 ==========
        'etl_job_name': ['ingest_user_events', 'sync_products', 'load_transactions'],
        'etl_type': ['streaming', 'batch', 'batch'],
        'load_frequency': ['real-time', 'daily', 'hourly'],
        'load_schedule': ['continuous', '0 2 * * *', '0 * * * *'],  # cron format
        'transformation_logic': [
            'Raw logs → Parsed JSON → Partitioned by event_date',
            'Full sync from ERP every night',
            'Incremental load with CDC (Change Data Capture)'
        ],
        'last_loaded_at': [datetime(2025, 1, 25, 15, 30), datetime(2025, 1, 25, 2, 0), datetime(2025, 1, 25, 16, 0)],
        'next_load_at': [None, datetime(2025, 1, 26, 2, 0), datetime(2025, 1, 25, 17, 0)],
        'sla_hours': [1, 24, 2],  # SLA: 데이터가 얼마나 최신이어야 하는지

        # ========== 스토리지 정보 ==========
        'storage_location': [
            's3a://jungle-xflow/user_events/',
            's3a://jungle-xflow/products/',
            's3a://jungle-xflow/transactions/'
        ],
        'storage_format': ['PARQUET', 'PARQUET', 'PARQUET'],
        'compression_type': ['SNAPPY', 'SNAPPY', 'SNAPPY'],
        'partition_keys': ['event_date', '', 'transaction_date'],
        'partition_count': [1, 0, 20],
        'file_count': [1, 1, 20],

        # ========== 데이터 통계 ==========
        'row_count': [10, 5, 20],
        'size_bytes': [2048, 1024, 4096],
        'size_mb': [0.002, 0.001, 0.004],
        'avg_row_size_bytes': [204, 204, 204],

        # ========== 데이터 품질 ==========
        'data_quality_score': [95.5, 100.0, 98.2],  # 0-100
        'completeness_pct': [98.0, 100.0, 100.0],  # null 비율
        'uniqueness_pct': [100.0, 100.0, 100.0],  # primary key uniqueness
        'validity_rules': [
            'user_id > 0, event_type IN (login,purchase,view,logout)',
            'price > 0, stock >= 0',
            'total_amount > 0, quantity > 0'
        ],
        'last_quality_check': [datetime(2025, 1, 25, 12, 0), datetime(2025, 1, 25, 12, 0), datetime(2025, 1, 25, 12, 0)],

        # ========== 거버넌스 및 컴플라이언스 ==========
        'classification': ['SENSITIVE', 'PUBLIC', 'CONFIDENTIAL'],
        'contains_pii': [True, False, True],
        'pii_columns': ['user_id', '', 'user_id'],
        'gdpr_applicable': [True, False, True],
        'retention_days': [730, 3650, 2555],  # 2년, 10년, 7년
        'data_masking_required': [True, False, True],
        'encryption_at_rest': [True, False, True],
        'access_level': ['restricted', 'public', 'confidential'],

        # ========== 사용 및 인기도 ==========
        'popularity_score': [85, 60, 95],  # 0-100, 사용 빈도
        'query_count_last_30d': [1250, 320, 2100],
        'user_count_last_30d': [45, 12, 78],
        'top_users': [
            'analyst-team,marketing-team,data-science-team',
            'product-team,pricing-team',
            'finance-team,sales-team,executive-team'
        ],

        # ========== 버전 및 변경 이력 ==========
        'schema_version': ['v2.1', 'v1.0', 'v3.0'],
        'created_at': [datetime(2024, 6, 1), datetime(2024, 1, 1), datetime(2024, 3, 15)],
        'created_by': ['data-eng@company.com', 'product@company.com', 'data-eng@company.com'],
        'updated_at': [datetime(2025, 1, 25), datetime(2025, 1, 20), datetime(2025, 1, 25)],
        'updated_by': ['data-eng@company.com', 'product@company.com', 'data-eng@company.com'],
        'change_log': [
            '2025-01-25: Added product_id column',
            '2025-01-20: Price update for new products',
            '2025-01-25: Schema upgrade to v3.0'
        ],

        # ========== 태그 및 카테고리 ==========
        'tags': [
            'event,user,behavior,analytics',
            'master,product,inventory',
            'transaction,sales,revenue,kpi'
        ],
        'data_tier': ['hot', 'warm', 'hot'],  # hot/warm/cold (access frequency)
        'criticality': ['high', 'medium', 'critical'],

        # ========== 문서 및 리소스 ==========
        'documentation_url': [
            'https://wiki.company.com/data/user_events',
            'https://wiki.company.com/data/products',
            'https://wiki.company.com/data/transactions'
        ],
        'dashboard_url': [
            'https://dashboard.company.com/user-behavior',
            'https://dashboard.company.com/products',
            'https://dashboard.company.com/sales'
        ],
        'example_queries_url': [
            'https://wiki.company.com/queries/user_events',
            'https://wiki.company.com/queries/products',
            'https://wiki.company.com/queries/transactions'
        ],

        # ========== 메타데이터 자체 정보 ==========
        'metadata_version': ['1.0.0', '1.0.0', '1.0.0'],
        'metadata_updated_at': [datetime.now(), datetime.now(), datetime.now()],
        'metadata_source': ['automated', 'automated', 'automated'],
        'is_active': [True, True, True],
    })


def create_columns_metadata_production():
    """
    칼럼 메타데이터 (AWS Glue Column 수준)
    """

    # 여기서는 user_events.user_id 칼럼만 상세 예시로 작성
    # 실제로는 모든 칼럼에 대해 작성해야 함
    columns_data = [
        {
            # ========== 기본 정보 ==========
            'table_name': 'user_events',
            'column_name': 'user_id',
            'ordinal_position': 1,
            'data_type': 'bigint',
            'type_precision': 19,
            'type_scale': 0,
            'type_length': None,

            # ========== 제약 조건 ==========
            'nullable': False,
            'default_value': None,
            'is_primary_key': False,
            'is_foreign_key': False,
            'is_partition_key': False,
            'is_sort_key': False,
            'is_unique': False,
            'is_indexed': True,

            # ========== 비즈니스 메타데이터 ==========
            'display_name': '사용자ID',
            'description': '시스템 내에서 사용자를 고유하게 식별하는 ID. 회원가입 시 자동 생성되며 변경 불가.',
            'business_definition': '고객 마스터 테이블의 customer_id와 동일한 값. 비회원은 음수값 사용.',
            'business_rules': 'user_id > 0 for registered users, user_id < 0 for guest users',
            'calculation_logic': 'Auto-increment from user registration system',

            # ========== 데이터 품질 통계 ==========
            'distinct_count': 5,
            'null_count': 0,
            'null_percentage': 0.0,
            'min_value': '1',
            'max_value': '5',
            'avg_value': '3.0',
            'median_value': '3.0',
            'std_dev': 1.41,

            # ========== 샘플 데이터 ==========
            'example_values': '1, 2, 3, 4, 5',
            'example_description': '실제 사용자 ID는 1부터 시작하는 연속된 정수',
            'sample_data': '[1, 2, 3, 4, 5]',

            # ========== 거버넌스 ==========
            'classification': 'PII',
            'is_pii': True,
            'is_sensitive': True,
            'pii_type': 'identifier',
            'masking_rule': 'hash_with_salt',
            'encryption_required': True,
            'gdpr_category': 'personal_identifier',

            # ========== 계보 (Lineage) ==========
            'source_column': 'app_db.users.id',
            'source_transformation': 'Direct mapping without transformation',
            'derived_from': '',
            'used_in_columns': 'transactions.user_id,user_profile.user_id',

            # ========== 태그 및 카테고리 ==========
            'tags': 'identifier,user,pii,key',
            'domain_tags': 'customer_domain',
            'technical_tags': 'indexed,non_null',

            # ========== 사용 정보 ==========
            'usage_frequency': 'very_high',
            'commonly_filtered': True,
            'commonly_joined': True,
            'commonly_grouped': False,
            'query_performance_impact': 'low',  # indexed column

            # ========== 데이터 타입 세부사항 ==========
            'physical_type': 'INT64',
            'logical_type': 'identifier',
            'encoding': 'PLAIN',
            'compression': 'SNAPPY',

            # ========== 변경 이력 ==========
            'created_at': datetime(2024, 6, 1),
            'created_by': 'data-eng@company.com',
            'updated_at': datetime(2025, 1, 20),
            'updated_by': 'data-eng@company.com',
            'change_log': '2025-01-20: Added index for performance',
            'schema_version': 'v2.1',

            # ========== 문서 ==========
            'documentation': 'See https://wiki.company.com/data/columns/user_id',
            'related_terms': 'customer_id,member_id,account_id',

            # ========== 메타데이터 ==========
            'metadata_version': '1.0.0',
            'metadata_updated_at': datetime.now(),
            'is_active': True,
        },
        # ... 다른 칼럼들도 동일한 형식으로 작성
        # (간결성을 위해 생략, 실제로는 모든 16개 칼럼에 대해 작성)
    ]

    return pd.DataFrame(columns_data)


def create_relationships_metadata_production():
    """
    관계 메타데이터 (Foreign Key, Join Relationships).
    """
    return pd.DataFrame({
        # ========== 기본 정보 ==========
        'relationship_id': ['rel_001', 'rel_002'],
        'relationship_name': ['fk_transactions_user', 'fk_transactions_product'],
        'relationship_type': ['foreign_key', 'foreign_key'],

        # ========== 관계 정의 ==========
        'from_table': ['transactions', 'transactions'],
        'from_column': ['user_id', 'product_id'],
        'to_table': ['user_events', 'products'],
        'to_column': ['user_id', 'product_id'],
        'cardinality': ['many_to_one', 'many_to_one'],

        # ========== 제약 조건 ==========
        'constraint_name': ['fk_trans_user', 'fk_trans_product'],
        'on_delete': ['CASCADE', 'RESTRICT'],
        'on_update': ['CASCADE', 'CASCADE'],
        'is_enforced': [False, False],  # Hive doesn't enforce FK

        # ========== 비즈니스 의미 ==========
        'description': [
            '각 트랜잭션은 반드시 한 명의 사용자에 의해 발생. 사용자 삭제 시 트랜잭션도 함께 삭제.',
            '각 트랜잭션은 반드시 하나의 제품에 대한 것. 제품 삭제 시 트랜잭션은 유지 (historical data).'
        ],
        'business_rule': [
            'Every transaction must have a valid user',
            'Every transaction must reference an existing product'
        ],

        # ========== JOIN 성능 정보 ==========
        'join_frequency': ['very_high', 'high'],
        'join_selectivity': [0.8, 0.6],  # 0-1, higher = more selective
        'avg_join_time_ms': [45, 32],
        'recommended_join_type': ['INNER JOIN', 'LEFT JOIN'],

        # ========== 데이터 품질 ==========
        'referential_integrity_pct': [100.0, 99.5],  # % of valid references
        'orphaned_records_count': [0, 1],
        'last_integrity_check': [datetime(2025, 1, 25), datetime(2025, 1, 25)],

        # ========== 메타데이터 ==========
        'created_at': [datetime(2024, 3, 15), datetime(2024, 3, 15)],
        'created_by': ['data-eng@company.com', 'data-eng@company.com'],
        'updated_at': [datetime(2025, 1, 20), datetime(2025, 1, 20)],
        'is_active': [True, True],
        'metadata_version': ['1.0.0', '1.0.0'],
    })

def upload_metadata_to_s3(client, bucket_name):
    """메타데이터를 S3에 업로드"""
    # 메타데이터 생성
    tables_df = create_tables_metadata_production()
    columns_df = create_columns_metadata_production()
    relationships_df = create_relationships_metadata_production()

    metadata = {
        'tables': tables_df,
        'columns': columns_df,
        'relationships': relationships_df
    }

    # Parquet 업로드
    for name, df in metadata.items():
        print(f"   - 행 개수: {len(df)}")
        print(f"   - 칼럼 개수: {len(df.columns)}")

        table = pa.Table.from_pandas(df)
        buf = io.BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        object_name = f"metadata/{name}.parquet"
        client.put_object(bucket_name, object_name, buf,
                         length=buf.getbuffer().nbytes,
                         content_type="application/octet-stream")
        print(f"s3a://{bucket_name}/{object_name} 업로드 완료")

def main():
    client = create_minio_client()
    bucket_name = "jungle-xflow"
    upload_metadata_to_s3(client, bucket_name)

if __name__ == "__main__":
    main()
