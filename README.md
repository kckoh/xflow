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

## EKS 배포

### ECR 로그인 (공통)
```bash
aws ecr get-login-password --region ap-northeast-2 > /tmp/ecr_password.txt
docker login --username AWS --password-stdin 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com < /tmp/ecr_password.txt
rm /tmp/ecr_password.txt
```

---

### Backend 배포

```bash
# 1. 이미지 빌드 & 푸시
cd backend
docker build --platform linux/amd64 -t 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest .
docker push 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest

# 2. ConfigMap 업데이트 (환경변수 변경 시)
kubectl apply -f k8s/backend/configmap.yaml

# 3. 재배포
kubectl rollout restart deployment backend -n default

# 4. 상태 확인
kubectl rollout status deployment backend -n default
kubectl get pods -n default -l app=backend

# 5. 로그 확인
kubectl logs -n default -l app=backend --tail=100 -f
```

**처음 배포 시 (전체 리소스 생성):**
```bash
kubectl apply -f k8s/backend/
```

**설정 파일:**
- `k8s/backend/deployment.yaml` - Deployment
- `k8s/backend/configmap.yaml` - 환경변수
- `k8s/backend/serviceaccount.yaml` - IRSA (S3 접근용)

---

### Airflow 배포

```bash
# 1. 이미지 빌드 & 푸시 (DAG 포함)
cd airflow
docker build --platform linux/amd64 -t 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-airflow:latest .
docker push 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-airflow:latest

# 2. 재배포
kubectl rollout restart deployment -n airflow airflow-scheduler airflow-webserver airflow-triggerer airflow-dag-processor

# 3. 상태 확인
kubectl get pods -n airflow

# 4. 로그 확인
kubectl logs -n airflow -l component=scheduler --tail=100 -f
```

**처음 배포 시 (Helm):**
```bash
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
    --namespace airflow \
    --create-namespace \
    --version 1.15.0 \
    -f k8s/airflow/values.yaml \
    --wait
```

**Spark RBAC (Airflow가 SparkApplication 관리용):**
```bash
kubectl apply -f k8s/airflow/spark-rbac.yaml
```

**설정 파일:**
- `k8s/airflow/values.yaml` - Helm values
- `k8s/airflow/spark-rbac.yaml` - Spark 권한
- `k8s/airflow/ingress.yaml` - Ingress 설정

---

### Spark 배포

```bash
# 1. 이미지 빌드 & 푸시
cd spark
docker build --platform linux/amd64 -f Dockerfile.k8s -t 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest .
docker push 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest

# Spark는 job 실행 시 자동으로 새 이미지 pull (imagePullPolicy: Always)
```

**Spark Operator 설치 (처음만):**
```bash
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set webhook.enable=true
```

---

### 배포 상태 확인
```bash
kubectl get pods -n default          # Backend, MongoDB
kubectl get pods -n airflow          # Airflow
kubectl get pods -n spark-jobs       # Spark Jobs
kubectl get sparkapplication -n spark-jobs
```

---

### Spark Job 관리
```bash
# 실행 중인 job 확인
kubectl get sparkapplication -n spark-jobs

# job 로그 확인 (실시간)
kubectl logs -f <driver-pod-name> -n spark-jobs

# job 삭제
kubectl delete sparkapplication <name> -n spark-jobs

# 모든 job 삭제
kubectl delete sparkapplication --all -n spark-jobs
```
