# Scripts - Automated Data Lake

자동화된 데이터 레이크 시스템을 위한 스크립트 모음입니다.

## 시스템 개요

Parquet 파일이 MinIO에 업로드되면 자동으로:

1. MinIO가 FastAPI 웹훅으로 이벤트 전송
2. 파일 스키마 자동 감지
3. Trino/Hive 외부 테이블 자동 생성
4. 파티션 자동 동기화
5. 처리 이력 PostgreSQL에 저장

## 주요 스크립트

### test-data-lake.sh

전체 데이터 레이크 시스템을 테스트하는 **올인원 스크립트**

**자동 실행 내용:**

1. Docker 서비스 확인
2. 테스트 데이터 생성 및 MinIO 업로드
   - `sales`: 파티션 (year/month/day)
   - `users_data`: 파티션 (signup_year/signup_month)
   - `products`: 파티션 없음
3. 웹훅을 통한 자동 테이블 생성 확인
4. 검증 방법 안내

**사용법:**

```bash
./scripts/test-data-lake.sh
```

### download_jars.sh

Trino/Hive S3 연동에 필요한 JAR 파일 다운로드

**다운로드 파일:**

- hadoop-aws JAR
- aws-java-sdk-bundle JAR

**저장 위치:**

- `trino/jars/`
- `hive/lib/`

**사용법:**

```bash
./scripts/download_jars.sh
```

## MinIO Event 설정

MinIO 이벤트 알림은 **docker-compose.yml에 통합**되어 자동으로 설정됩니다.
