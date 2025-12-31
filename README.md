# XFlow - Data Pipeline Platform

ETL 파이프라인을 설정 기반으로 실행할 수 있는 데이터 플랫폼

## Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│  Frontend       │────▶│  Backend API    │────▶│  MongoDB        │
│  (React)        │     │  (FastAPI)      │     │  (ETL Jobs)     │
└─────────────────┘     └────────┬────────┘     └─────────────────┘
                                 │
                                 ▼ Trigger
                        ┌─────────────────┐
                        │  Airflow DAG    │
                        │  (Orchestrator) │
                        └────────┬────────┘
                                 │
                                 ▼ spark-submit
                        ┌─────────────────┐
                        │  Spark ETL      │
                        │  Runner         │
                        └────────┬────────┘
                                 │
                    ┌────────────┴────────────┐
                    ▼                         ▼
            ┌───────────┐             ┌───────────┐
            │ PostgreSQL│             │ S3/MinIO  │
            │ (Source)  │             │ (Dest)    │
            └───────────┘             └───────────┘
```

## Quick Start

```bash
# 1. Start all services
docker compose up -d

# 2. Download Spark JAR dependencies
mkdir -p spark/jars
curl -fsSLO --output-dir spark/jars https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
curl -fsSLO --output-dir spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -fsSLO --output-dir spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 3. Load sample data
docker compose exec -T postgres psql -U postgres -d mydb < init-fake-data.sql
```

## Services

| Service | URL | Credentials |
|---------|-----|-------------|
| Frontend | http://localhost:5173 | - |
| Backend API | http://localhost:8000 | - |
| Airflow | http://localhost:8080 | admin/admin |
| Spark UI | http://localhost:8081 | - |
| MinIO Console | http://localhost:9001 | minio/minio123 |
| PostgreSQL | localhost:5433 | postgres/postgres |
| Neo4j | http://localhost:7474 | neo4j/password |
| Kafka UI | http://localhost:8084 | - |

## ETL Pipeline

### 1. Create RDB Source (데이터 소스 등록)

```bash
curl -X POST http://localhost:8000/api/rdb-sources/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-main",
    "description": "Main PostgreSQL database",
    "type": "postgres",
    "host": "postgres",
    "port": 5432,
    "database_name": "mydb",
    "user_name": "postgres",
    "password": "postgres"
  }'
```

### 2. Create ETL Job (ETL 작업 정의)

```bash
curl -X POST http://localhost:8000/api/etl-jobs/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products_etl",
    "description": "Extract products, transform, save to S3",
    "source": {
      "type": "rdb",
      "connection_id": "<rdb_source_id>",
      "table": "products"
    },
    "transforms": [
      {"type": "drop-columns", "config": {"columns": ["category"]}},
      {"type": "filter", "config": {"expression": "price > 100"}}
    ],
    "destination": {
      "type": "s3",
      "path": "s3a://xflow-processed-data/products",
      "format": "parquet",
      "options": {"compression": "snappy"}
    }
  }'
```

### 3. Run ETL Job (ETL 실행)

```bash
curl -X POST http://localhost:8000/api/etl-jobs/<job_id>/run
```

### 4. Check Job Status (실행 상태 확인)

```bash
# List all job runs
curl http://localhost:8000/api/job-runs/

# Get specific run
curl http://localhost:8000/api/job-runs/<run_id>
```

## Supported Transforms

| Type | Config | Description |
|------|--------|-------------|
| `select-fields` | `{"columns": ["col1", "col2"]}` | 특정 컬럼만 선택 |
| `drop-columns` | `{"columns": ["col1", "col2"]}` | 컬럼 삭제 |
| `filter` | `{"expression": "price > 100"}` | SQL 표현식으로 필터링 |

## API Endpoints

### ETL Jobs
- `POST /api/etl-jobs/` - Create ETL job
- `GET /api/etl-jobs/` - List all jobs
- `GET /api/etl-jobs/{id}` - Get job detail
- `PUT /api/etl-jobs/{id}` - Update job
- `DELETE /api/etl-jobs/{id}` - Delete job
- `POST /api/etl-jobs/{id}/run` - Trigger job execution

### Job Runs
- `GET /api/job-runs/` - List all runs
- `GET /api/job-runs/{id}` - Get run detail

### RDB Sources
- `POST /api/rdb-sources/` - Create source
- `GET /api/rdb-sources/` - List sources
- `DELETE /api/rdb-sources/{id}` - Delete source

## Manual Spark Job

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --driver-memory 2g \
  --jars /opt/spark/jars/extra/postgresql-42.7.4.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar \
  /opt/spark/jobs/postgres_etl.py
```

## Database Migrations (Alembic)

```bash
# Create migration
alembic revision --autogenerate -m "description"

# Apply migration
alembic upgrade head

# Verify
docker compose exec postgres psql -U postgres -d mydb -c "\d users"
```

## Development

### Backend
```bash
cd backend
uvicorn main:app --reload
```

### Frontend
```bash
cd frontend
npm install
npm run dev
```

## Troubleshooting

### Spark OOM (Exit code 137)
- Use `--master 'local[*]'` instead of cluster mode
- Increase `--driver-memory 2g`

### Airflow DAG not showing
```bash
docker compose restart airflow-scheduler
```

### LocalStack unhealthy
```bash
docker compose restart localstack
```

### frontend 배포
npm run build
vite build

### S3 업로드
aws s3 sync dist/ s3://xflows --delete

### CloudFront 캐시 무효화
aws cloudfront create-invalidation --distribution-id E3J1KK599NCLXA --paths "/*"
