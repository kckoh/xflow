### download_jars.sh 사용법

- 실행: `./scripts/download_jars.sh`
- 이 스크립트는 "다운로드만" 합니다 (컨테이너에 자동 설치되지 않음)
- 다운로드 파일: hadoop-aws, aws-java-sdk-bundle JAR
- 저장 위치: `trino/jars/`, `hive/lib/`
- 컨테이너 반영:
  - 1. 볼륨 마운트가 연결돼 있으면 재시작
  - 2. 또는 `docker cp`로 직접 복사 (스크립트 출력의 예시 명령 참고)

---

### scripts 사용법

`setup_sample_data.py` 실행 -> `create_hive_tables.py` 실행 -> `create_metadata_production.py` 실행

### 1. tables.parquet (테이블 메타데이터)

**필수 필드**:

- `table_name`: 테이블 이름
- `description`: 테이블 설명
- `business_owner`: 비즈니스 담당자
- `source_system`: 원천 시스템
- `etl_job_name`: ETL 작업 이름
- `load_frequency`: 적재 주기
- `classification`: 데이터 분류 (PUBLIC/SENSITIVE/CONFIDENTIAL)
- `contains_pii`: 개인정보 포함 여부

**주요 카테고리** (총 66개 필드):

#### 기본 정보

- catalog_name, database_name, table_name, table_type

#### 비즈니스 메타데이터

- display_name, description, business_domain, business_owner, technical_owner, data_steward

#### 데이터 계보 (Lineage)

- source_system, source_database, source_table
- upstream_tables, downstream_tables

#### ETL 정보

- etl_job_name, etl_type, load_frequency, load_schedule
- transformation_logic, last_loaded_at, next_load_at, sla_hours

#### 스토리지 정보

- storage_location, storage_format, compression_type
- partition_keys, partition_count, file_count

#### 데이터 통계

- row_count, size_bytes, size_mb, avg_row_size_bytes

#### 데이터 품질

- data_quality_score, completeness_pct, uniqueness_pct
- validity_rules, last_quality_check

#### 거버넌스 및 컴플라이언스

- classification, contains_pii, pii_columns
- gdpr_applicable, retention_days, data_masking_required
- encryption_at_rest, access_level

#### 사용 및 인기도

- popularity_score, query_count_last_30d, user_count_last_30d, top_users

#### 버전 및 변경 이력

- schema_version, created_at, created_by, updated_at, updated_by, change_log

#### 태그 및 카테고리

- tags, data_tier (hot/warm/cold), criticality

#### 문서 및 리소스

- documentation_url, dashboard_url, example_queries_url

---

### 2. columns.parquet (칼럼 메타데이터)

**필수 필드**:

- `table_name`: 테이블 이름
- `column_name`: 칼럼 이름
- `data_type`: 데이터 타입
- `description`: 칼럼 설명
- `is_pii`: 개인정보 여부
- `classification`: 데이터 분류

**주요 카테고리** (총 65개 필드):

#### 기본 정보

- table_name, column_name, ordinal_position
- data_type, type_precision, type_scale, type_length

#### 제약 조건

- nullable, default_value
- is_primary_key, is_foreign_key, is_partition_key, is_sort_key
- is_unique, is_indexed

#### 비즈니스 메타데이터

- display_name, description, business_definition
- business_rules, calculation_logic

#### 데이터 품질 통계

- distinct_count, null_count, null_percentage
- min_value, max_value, avg_value, median_value, std_dev

#### 샘플 데이터

- example_values, example_description, sample_data

#### 거버넌스

- classification, is_pii, is_sensitive, pii_type
- masking_rule, encryption_required, gdpr_category

#### 계보 (Lineage)

- source_column, source_transformation
- derived_from, used_in_columns

#### 태그 및 카테고리

- tags, domain_tags, technical_tags

#### 사용 정보

- usage_frequency, commonly_filtered, commonly_joined
- commonly_grouped, query_performance_impact

#### 데이터 타입 세부사항

- physical_type, logical_type, encoding, compression

#### 변경 이력

- created_at, created_by, updated_at, updated_by
- change_log, schema_version

---

### 3. relationships.parquet (관계 메타데이터)

**필수 필드**:

- `from_table`, `from_column`: 시작 테이블/칼럼
- `to_table`, `to_column`: 대상 테이블/칼럼
- `relationship_type`: 관계 타입 (foreign_key, one_to_many, etc.)
- `cardinality`: 카디널리티

**주요 카테고리** (총 26개 필드):

#### 기본 정보

- relationship_id, relationship_name, relationship_type

#### 관계 정의

- from_table, from_column, to_table, to_column, cardinality

#### 제약 조건

- constraint_name, on_delete, on_update, is_enforced

#### 비즈니스 의미

- description, business_rule

#### JOIN 성능 정보

- join_frequency, join_selectivity, avg_join_time_ms, recommended_join_type

#### 데이터 품질

- referential_integrity_pct, orphaned_records_count, last_integrity_check

---

## 📁 파일 설명

### create_metadata_production.py

**AWS Glue 기반 프로덕션급 메타데이터 스키마 생성**

- 목적: ETL 팀이 참고할 표준 메타데이터 형식 정의
- 출력: tables.parquet, columns.parquet, relationships.parquet
- 위치: s3://jungle-xflow/metadata/

### setup_sample_data.py

**샘플 데이터 생성 및 MinIO 업로드 (파티셔닝 적용)**

- 테이블: user_events, products, transactions
- 파티셔닝: user_events (event_date), transactions (transaction_date)
- 위치: s3://jungle-xflow/{table_name}/

### create_hive_tables.py

**Trino/Hive 테이블 생성 및 파티션 동기화**

- MinIO의 Parquet 파일을 Trino에서 쿼리 가능하도록 테이블 등록
- 파티션 자동 발견

---

1. **메타데이터 작성**

```python
# tables.parquet에 추가할 데이터
new_table_metadata = {
    'table_name': 'your_table_name',
    'description': '상세한 테이블 설명',
    'business_owner': 'your-team',
    'technical_owner': 'data-eng-team',
    'source_system': 'source_database',
    'etl_job_name': 'your_etl_job',
    'load_frequency': 'daily',  # or hourly, real-time
    'classification': 'CONFIDENTIAL',  # or PUBLIC, SENSITIVE
    'contains_pii': True,  # or False
    # ... 모든 필수 필드 작성
}
```

2. **칼럼 메타데이터 작성**

모든 칼럼에 대해 상세한 메타데이터 작성:

```python
new_column_metadata = {
    'table_name': 'your_table_name',
    'column_name': 'your_column',
    'data_type': 'varchar',
    'description': '칼럼 설명',
    'is_pii': False,
    'classification': 'PUBLIC',
    # ... 모든 필수 필드 작성
}
```

3. **관계 정의**

Foreign Key나 JOIN 관계가 있으면:

```python
new_relationship = {
    'from_table': 'your_table',
    'from_column': 'foreign_key_col',
    'to_table': 'referenced_table',
    'to_column': 'primary_key_col',
    'relationship_type': 'foreign_key',
    'cardinality': 'many_to_one'
}
```

### 필수 규칙

#### 반드시 지켜야 할 사항

1. **PII 데이터 표시**

   - `is_pii = True` 설정
   - `pii_type` 명시 (identifier, name, email, etc.)
   - `masking_rule` 정의

2. **데이터 분류**

   - `classification`: PUBLIC / SENSITIVE / CONFIDENTIAL
   - 민감도에 맞는 `access_level` 설정

3. **데이터 계보**

   - `source_system`, `source_table` 명시
   - `upstream_tables`, `downstream_tables` 작성

4. **데이터 품질**

   - `validity_rules` 정의
   - `data_quality_score` 주기적 업데이트

5. **문서화**
   - `description`: 명확하고 상세한 설명
   - `business_definition`: 비즈니스 관점 정의
   - `documentation_url`: Wiki 링크

---
