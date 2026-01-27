# XFlow - Modern Data Platform

**설정 기반의 시각적 데이터 파이프라인 플랫폼**

코드 없이 웹 UI에서 ETL/ELT 파이프라인을 구성하고 실행할 수 있는 데이터 플랫폼입니다.

<div align="center">

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-green.svg)
![React](https://img.shields.io/badge/React-18.3-blue.svg)
![Spark](https://img.shields.io/badge/Spark-3.5-orange.svg)

</div>

---

## 주요 기능

- **비주얼 ETL 에디터**: 드래그 앤 드롭으로 데이터 파이프라인 구성
- **다중 소스 지원**: PostgreSQL, MySQL, MongoDB, S3, REST API
- **실시간 & 배치**: CDC(Change Data Capture) + 배치 ETL
- **데이터 카탈로그**: 자동화된 메타데이터 관리 및 검색
- **데이터 품질**: 자동 품질 체크 및 모니터링
- **AI 쿼리 어시스턴트**: 자연어 → SQL 변환 (AWS Bedrock)
- **리니지 추적**: 소스부터 타겟까지 데이터 흐름 시각화
- **SQL Lab**: DuckDB/Trino 기반 인터랙티브 쿼리 실행
- **Kubernetes 지원**: Production-ready K8s 배포 설정

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                     FRONTEND (React + Vite)                          │
│   Landing │ Dataset │ Catalog │ Query │ Quality │ Admin     │
└─────────────────────────┬────────────────────────────────────────────┘
                          │ REST API
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│              BACKEND API (FastAPI)                                   │
│   27 Routers │ 17 Services │ MongoDB ODM │ PostgreSQL ORM            │
└────┬──────┬──────────┬──────────┬──────────-────────────────────────--
     │      │          │          │          
     ▼      ▼          ▼          ▼          
┌────────┐ │    ┌──────────┐ ┌──────────┐ 
│Airflow │ │    │OpenSearch│ │  Trino   │ 
│  DAG   │ │    │(Search)  │ │ (Query)  │
└────┬───┘ │    └──────────┘ └──────────┘ 
     │     │
     ▼     ▼
┌─────────────┐
│ Spark       │
│ ETL Runner  │
└──────┬──────┘
       │
   ┌───┴────────────────┬──────────────┐
   ▼                    ▼              ▼
┌──────────┐      ┌──────────┐   ┌──────────┐
│PostgreSQL│      │  S3      │   │ MongoDB  │
│(Source/  │      │ (Data    │   │(Metadata)│
│ Dest)    │      │  Lake)   │   │          │
└──────────┘      └──────────┘   └──────────┘
```

## Quick Start

### Local Development (Docker Compose)

```bash
# 1. Clone repository
git clone https://github.com/yourusername/xflow.git
cd xflow

# 2. Start all services (15+ containers)
docker compose up -d

# 3. Download Spark JAR dependencies
mkdir -p spark/jars
curl -fsSLO --output-dir spark/jars https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
curl -fsSLO --output-dir spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -fsSLO --output-dir spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

# 4. Load sample data
docker compose exec -T postgres psql -U postgres -d mydb < init-fake-data.sql

# 5. Access the platform
open http://localhost:5173  # Frontend
```

## Services

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Frontend** | http://localhost:5173 | - | React 웹 UI |
| **Backend API** | http://localhost:8000 | - | FastAPI 서버 |
| **API Docs** | http://localhost:8000/docs | - | Swagger UI |
| **Airflow** | http://localhost:8080 | admin/admin | DAG 오케스트레이션 |
| **Spark Master UI** | http://localhost:8081 | - | Spark 클러스터 모니터링 |
| **OpenSearch** | http://localhost:9200 | - | 전문 검색 엔진 |
| **OpenSearch Dashboards** | http://localhost:5601 | - | 데이터 시각화 |
| **Trino** | http://localhost:8082 | - | 분산 SQL 쿼리 |
| **Kafka UI** | http://localhost:8084 | - | Kafka 토픽 관리 |
| **PostgreSQL** | localhost:5433 | postgres/postgres | 관계형 DB |
| **MongoDB** | localhost:27017 | - | 문서형 DB (메타데이터) |



## Core Features

### 1. Visual ETL Pipeline Builder

웹 UI에서 드래그 앤 드롭으로 데이터 파이프라인을 구성하세요.

```
[Source Node] → [Transform Node] → [Target Node]
     ↓                 ↓                  ↓
  PostgreSQL      Filter/Select        S3 Parquet
  MongoDB         SQL Transform        Delta Lake
  S3 Files        Union/Join           PostgreSQL
  REST API        Custom Logic         MongoDB
```

**지원하는 변환 타입 (10+):**
- `select-fields`: 특정 컬럼만 선택
- `drop-columns`: 컬럼 제거
- `filter`: SQL 표현식 필터링
- `union`: 다중 입력 병합
- `sql`: 임의의 Spark SQL 실행
- `s3-select-fields`: S3 로그 필드 추출
- `s3-filter`: S3 로그 필터링

### 2. Data Catalog & Discovery

- **자동 메타데이터 수집**: 스키마, 통계, 샘플 데이터
- **전문 검색**: OpenSearch 기반 (한국어 Nori 분석기)
- **도메인 조직화**: 비즈니스 도메인별 데이터 자산 그룹화
- **리니지 시각화**: 소스 → 변환 → 타겟 데이터 흐름

### 3. Data Quality

DuckDB 기반 Parquet 파일 품질 체크:
- Row count validation
- Null value detection
- Duplicate check
- Data quality scoring

### 4. AI-Powered Query Assistant

AWS Bedrock 통합:
```
자연어: "지난달 매출이 가장 높은 상품 10개를 보여줘"
   ↓
SQL: SELECT product_name, SUM(sales) as total_sales
     FROM products WHERE date >= '2026-01-01'
     GROUP BY product_name ORDER BY total_sales DESC LIMIT 10
```

### 5. SQL Lab (Interactive Query)

- **DuckDB**: S3 Parquet 파일 직접 쿼리 (서버리스)
- **Trino**: 분산 SQL 엔진 (대용량 데이터)
- **Query History**: 실행 이력 저장 및 재사용

### 6. Change Data Capture (CDC)

Debezium + Kafka를 통한 실시간 데이터 동기화:
```
PostgreSQL (logical replication)
    ↓
Kafka Topics (per table)
    ↓
S3 Data Lake / Delta Lake
```

---

## API Usage Examples

### 1. Create Connection (데이터 소스 등록)

```bash
curl -X POST http://localhost:8000/api/connections/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "postgres-main",
    "description": "Main PostgreSQL database",
    "type": "postgres",
    "host": "postgres",
    "port": 5432,
    "database": "mydb",
    "username": "postgres",
    "password": "postgres"
  }'
```

### 2. Create Dataset (ETL 파이프라인)

```bash
curl -X POST http://localhost:8000/api/datasets/ \
  -H "Content-Type: application/json" \
  -d '{
    "name": "products_to_s3",
    "description": "Extract products, transform, save to S3",
    "nodes": [
      {
        "id": "source1",
        "type": "rdb-source",
        "config": {
          "connection_id": "<connection_id>",
          "table": "products"
        }
      },
      {
        "id": "transform1",
        "type": "filter",
        "config": {
          "expression": "price > 100"
        }
      },
      {
        "id": "target1",
        "type": "s3-target",
        "config": {
          "path": "s3a://xflow-data/products",
          "format": "parquet"
        }
      }
    ],
    "edges": [
      {"from": "source1", "to": "transform1"},
      {"from": "transform1", "to": "target1"}
    ]
  }'
```

### 3. Run Dataset (파이프라인 실행)

```bash
curl -X POST http://localhost:8000/api/datasets/<dataset_id>/run
```

### 4. Check Status (실행 상태 확인)

```bash
# List all runs
curl http://localhost:8000/api/job-runs/

# Get specific run with logs
curl http://localhost:8000/api/job-runs/<run_id>
```

## Technology Stack

### Frontend
- **React 18.3** + Vite 6.0
- **Tailwind CSS 4.1**
- **React Router 7.10**
- **XYFlow** - DAG 시각화
- **Recharts** - 차트/그래프
- 110+ custom components

### Backend
- **FastAPI 0.115** - REST API 프레임워크
- **Beanie 1.26** - MongoDB ODM
- **SQLAlchemy 2.0** - PostgreSQL ORM
- **SlowAPI** - Rate limiting
- **boto3** - AWS S3/IAM 연동

### Data Processing
- **Apache Spark 3.5** - 분산 ETL 실행
- **Apache Airflow 2.x** - DAG 오케스트레이션
- **DuckDB 1.1** - 서버리스 SQL 쿼리
- **Trino 435** - 분산 SQL 엔진

### Storage & Search
- **PostgreSQL** - 관계형 데이터
- **MongoDB** - 파이프라인 메타데이터
- **OpenSearch 2.11** - 전문 검색 
- **MinIO / S3** - 객체 저장소 (데이터 레이크)

### Infrastructure
- **Docker** + **Docker Compose** - 로컬 개발
- **Kubernetes** - 프로덕션 배포
- **Terraform** - Infrastructure as Code
- **Prometheus** + **Grafana** - 모니터링

---

## API Endpoints (27 Routers)

### Core Pipeline
- `POST /api/datasets/` - ETL 파이프라인 생성
- `GET /api/datasets/` - 파이프라인 목록
- `GET /api/datasets/{id}` - 파이프라인 상세
- `PUT /api/datasets/{id}` - 파이프라인 수정
- `DELETE /api/datasets/{id}` - 파이프라인 삭제
- `POST /api/datasets/{id}/run` - 파이프라인 실행

### Connections
- `POST /api/connections/` - 데이터 소스 연결
- `GET /api/connections/` - 연결 목록
- `POST /api/connections/{id}/test` - 연결 테스트
- `DELETE /api/connections/{id}` - 연결 삭제

### Job Runs
- `GET /api/job-runs/` - 실행 이력
- `GET /api/job-runs/{id}` - 실행 상세 (로그 포함)
- `POST /api/job-runs/{id}/cancel` - 실행 취소

### Data Catalog
- `GET /api/catalog/` - 데이터 카탈로그 탐색
- `GET /api/catalog/{id}` - 데이터셋 메타데이터
- `GET /api/catalog/{id}/lineage` - 데이터 리니지
- `POST /api/catalog/search` - 전문 검색

### Domains
- `POST /api/domains/` - 도메인 생성
- `GET /api/domains/` - 도메인 목록
- `PUT /api/domains/{id}` - 도메인 수정

### Data Quality
- `GET /api/quality/` - 품질 체크 결과
- `POST /api/quality/{dataset_id}/check` - 품질 체크 실행
- `GET /api/quality/{dataset_id}/score` - 품질 점수

### Query Execution
- `POST /api/duckdb/query` - DuckDB 쿼리 실행
- `POST /api/trino/query` - Trino 쿼리 실행
- `GET /api/query-history/` - 쿼리 이력

### AI Assistant
- `POST /api/ai/text-to-sql` - 자연어 → SQL 변환
- `POST /api/ai/query-explain` - SQL 쿼리 설명

### CDC & Streaming
- `POST /api/cdc/connectors/` - CDC 커넥터 생성
- `GET /api/cdc/connectors/` - 커넥터 목록
- `POST /api/kafka-streaming/topics` - Kafka 토픽 관리

### Schema Detection
- `POST /api/s3-csv/schema` - S3 CSV 스키마 추출
- `POST /api/s3-parquet/schema` - S3 Parquet 스키마 추출
- `POST /api/s3-json/schema` - S3 JSON 스키마 추출

### Admin
- `POST /api/admin/users/` - 사용자 생성
- `GET /api/admin/users/` - 사용자 목록
- `PUT /api/admin/roles/{id}` - 권한 관리

---

## Development

### Backend Development

```bash
cd backend

# Install dependencies
pip install -r requirements.txt

# Run with hot reload
uvicorn main:app --reload --host 0.0.0.0 --port 8000

# API docs available at:
# - http://localhost:8000/docs (Swagger UI)
# - http://localhost:8000/redoc (ReDoc)
```

### Frontend Development

```bash
cd frontend

# Install dependencies
npm install

# Run dev server (Vite)
npm run dev  # Runs on http://localhost:5173

# Build for production
npm run build

# Preview production build
npm run preview
```

### Database Migrations (Alembic)

```bash
cd backend

# Create new migration
alembic revision --autogenerate -m "add new table"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1

# Verify
docker compose exec postgres psql -U postgres -d mydb -c "\dt"
```

### Manual Spark Job Submission

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --driver-memory 2g \
  --jars /opt/spark/jars/extra/postgresql-42.7.4.jar,\
/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,\
/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar \
  /opt/spark/jobs/etl_runner.py \
  --config '{"source": {"type": "rdb", ...}}'
```

---

## Project Structure

```
xflow/
├── backend/                   # FastAPI application
│   ├── routers/              # API route handlers (27 files)
│   ├── services/             # Business logic (17 files)
│   ├── models.py             # MongoDB models (Beanie)
│   ├── database.py           # PostgreSQL config
│   ├── main.py               # FastAPI app
│   └── requirements.txt      # Python dependencies
├── frontend/                  # React application
│   ├── src/
│   │   ├── pages/           # Page components (14)
│   │   ├── components/      # Reusable components (110+)
│   │   ├── services/        # API client
│   │   ├── context/         # React context
│   │   └── hooks/           # Custom hooks
│   ├── package.json
│   └── vite.config.js
├── airflow/                   # Orchestration
│   ├── dags/                 # DAG definitions
│   │   ├── dataset_dag.py
│   │   └── etl_common.py
│   └── Dockerfile
├── spark/                     # Data processing
│   ├── jobs/                 # ETL scripts (14)
│   │   └── etl_runner.py    # Main ETL engine (2,000+ lines)
│   ├── jars/                 # JDBC/Hadoop JARs
│   └── Dockerfile.k8s
├── k8s/                       # Kubernetes manifests
│   ├── backend/              # Backend deployment
│   ├── airflow/              # Airflow Helm values
│   ├── spark/                # Spark Operator CRDs
│   ├── opensearch/           # OpenSearch cluster
│   ├── kafka/                # Kafka cluster
│   ├── mongodb/              # MongoDB StatefulSet
│   ├── redis/                # Redis cache
│   └── trino/                # Trino coordinator
├── terraform/                 # Infrastructure as Code
│   └── localstack/           # LocalStack config (S3, IAM)
├── docker-compose.yml         # Local development (15+ services)
├── init-fake-data.sql         # Sample data
└── README.md
```

---

## Troubleshooting

### Spark Job Failed (Exit Code 137 - OOM)

```bash
# Use local mode instead of cluster
--master 'local[*]'

# Increase driver memory
--driver-memory 2g

# Increase executor memory
--executor-memory 2g
```

### Airflow DAG Not Appearing

```bash
# Restart scheduler
docker compose restart airflow-scheduler

# Check DAG errors
docker compose logs airflow-scheduler

# Verify DAG file syntax
docker compose exec airflow-scheduler python -c "import sys; sys.path.append('/opt/airflow/dags'); import dataset_dag"
```

### MongoDB Connection Refused

```bash
# Check MongoDB status
docker compose ps mongodb

# Restart MongoDB
docker compose restart mongodb

# Check logs
docker compose logs mongodb
```

### LocalStack S3 Not Working

```bash
# Restart LocalStack
docker compose restart localstack

# Wait for healthy status
docker compose ps localstack

# Recreate buckets
aws --endpoint-url=http://localhost:4566 s3 mb s3://xflow-data
```

### OpenSearch Cluster Yellow/Red

```bash
# Check cluster health
curl http://localhost:9200/_cluster/health?pretty

# Restart OpenSearch
docker compose restart opensearch

# Check disk space
docker system df
```

### Kafka Broker Not Ready

```bash
# Restart Kafka cluster
docker compose restart kafka zookeeper

# Check broker logs
docker compose logs kafka --tail=100

# List topics
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

---

## Deployment

### Frontend Deployment (S3 + CloudFront)

```bash
cd frontend

# Build production bundle
npm run build

# Upload to S3
aws s3 sync dist/ s3://xflows --delete

# Invalidate CloudFront cache
aws cloudfront create-invalidation \
  --distribution-id E3J1KK599NCLXA \
  --paths "/*"
```

### Kubernetes (AWS EKS) Deployment

#### Prerequisites

1. **EKS Cluster Setup**
```bash
# Install eksctl
brew install eksctl

# Create EKS cluster
eksctl create cluster \
  --name xflow-cluster \
  --region ap-northeast-2 \
  --nodegroup-name standard-workers \
  --node-type t3.xlarge \
  --nodes 3 \
  --nodes-min 1 \
  --nodes-max 5 \
  --managed
```

2. **Install Required Tools**
```bash
# Helm
brew install helm

# kubectl
brew install kubectl

# AWS CLI
brew install awscli
```

#### ECR Login (Common)

```bash
aws ecr get-login-password --region ap-northeast-2 > /tmp/ecr_password.txt
docker login --username AWS \
  --password-stdin 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com < /tmp/ecr_password.txt
rm /tmp/ecr_password.txt
```

---

#### 1. Backend Deployment

```bash
cd backend

# Build & push image
docker build --platform linux/amd64 \
  -t 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest .
docker push 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest

# Deploy (first time)
kubectl apply -f k8s/backend/

# Update deployment
kubectl rollout restart deployment backend -n default

# Check status
kubectl rollout status deployment backend -n default
kubectl get pods -n default -l app=backend

# View logs
kubectl logs -n default -l app=backend --tail=100 -f
```

**Configuration Files:**
- `k8s/backend/deployment.yaml` - Deployment spec
- `k8s/backend/configmap.yaml` - Environment variables
- `k8s/backend/serviceaccount.yaml` - IRSA (IAM role for S3)
- `k8s/backend/service.yaml` - ClusterIP service

---

#### 2. Airflow Deployment

```bash
cd airflow

# Build & push image (includes DAGs)
docker build --platform linux/amd64 \
  -t 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-airflow:latest .
docker push 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-airflow:latest

# Install Airflow (first time)
helm repo add apache-airflow https://airflow.apache.org
helm upgrade --install airflow apache-airflow/airflow \
    --namespace airflow \
    --create-namespace \
    --version 1.15.0 \
    -f k8s/airflow/values.yaml \
    --wait

# Deploy Spark RBAC
kubectl apply -f k8s/airflow/spark-rbac.yaml

# Update DAGs (after changes)
kubectl rollout restart deployment -n airflow \
  airflow-scheduler \
  airflow-webserver \
  airflow-triggerer \
  airflow-dag-processor

# Check status
kubectl get pods -n airflow

# View logs
kubectl logs -n airflow -l component=scheduler --tail=100 -f
```

**Configuration Files:**
- `k8s/airflow/values.yaml` - Helm chart values
- `k8s/airflow/spark-rbac.yaml` - Spark Operator permissions
- `k8s/airflow/ingress.yaml` - ALB Ingress config

---

#### 3. Spark Deployment

```bash
cd spark

# Build & push image
docker build --platform linux/amd64 -f Dockerfile.k8s \
  -t 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest .
docker push 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest

# Install Spark Operator (first time)
helm repo add spark-operator https://kubeflow.github.io/spark-operator
helm install spark-operator spark-operator/spark-operator \
    --namespace spark-operator \
    --create-namespace \
    --set webhook.enable=true

# Jobs are automatically created by Airflow DAGs
# Monitor jobs:
kubectl get sparkapplication -n spark-jobs
```

**Spark Job Management:**
```bash
# List running jobs
kubectl get sparkapplication -n spark-jobs

# View job details
kubectl describe sparkapplication <name> -n spark-jobs

# View driver logs
kubectl logs -f <driver-pod-name> -n spark-jobs

# Delete job
kubectl delete sparkapplication <name> -n spark-jobs

# Delete all completed jobs
kubectl delete sparkapplication --all -n spark-jobs --field-selector status.applicationState.state=COMPLETED
```

---

#### 4. OpenSearch Deployment

```bash
# Deploy OpenSearch cluster
kubectl apply -f k8s/opensearch/

# Check cluster health
kubectl get pods -n opensearch

# View logs
kubectl logs -n opensearch -l app=opensearch
```

---

#### 5. Monitoring & Logging

```bash
# Install Prometheus + Grafana
helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm install prometheus prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --create-namespace

# Access Grafana
kubectl port-forward -n monitoring svc/prometheus-grafana 3000:80
# Open http://localhost:3000 (admin/prom-operator)
```

---

#### Infrastructure Summary

**Deployed Services:**
```bash
kubectl get pods -n default          # Backend, MongoDB
kubectl get pods -n airflow          # Airflow (scheduler, webserver, workers)
kubectl get pods -n spark-jobs       # Spark applications
kubectl get pods -n opensearch       # OpenSearch cluster
kubectl get pods -n monitoring       # Prometheus, Grafana
```

---

## Use Cases

1. **E-Commerce Analytics**: Extract orders from PostgreSQL → Transform → Store in S3 Data Lake → Query with Trino
2. **Data Lake Ingestion**: Schedule daily S3 log parsing → Parquet conversion → Catalog registration
3. **Cross-Database Joins**: Union data from multiple sources (PostgreSQL + MongoDB + S3) → Single view
4. **Data Quality Monitoring**: Automated quality checks on every pipeline run → Alerts on failures

---

## License

This project is licensed under the MIT License. See `LICENSE` file for details.

---

## Acknowledgments

- **Apache Airflow** - Workflow orchestration
- **Apache Spark** - Distributed data processing
- **FastAPI** - Modern Python web framework
- **React** + **XYFlow** - Interactive UI components
- **OpenSearch** - Full-text search engine
- **Trino** - Distributed SQL query engine

---

## Support

- **Documentation**: [GitHub Wiki](https://github.com/yourusername/xflow/wiki)
- **Issues**: [GitHub Issues](https://github.com/yourusername/xflow/issues)
- **Discussions**: [GitHub Discussions](https://github.com/yourusername/xflow/discussions)

---

## Roadmap

- [ ] Real-time streaming transformations (Flink integration)
- [ ] ML model deployment pipeline
- [ ] Data version control (lakeFS integration)
- [ ] Advanced lineage visualization (Neo4j integration)
- [ ] Multi-tenancy support
- [ ] SaaS deployment option

---

<div align="center">

**Built with ❤️ for the Data Engineering Community**

[⭐ Star this repo](https://github.com/yourusername/xflow) | [🐛 Report Bug](https://github.com/yourusername/xflow/issues) | [💡 Request Feature](https://github.com/yourusername/xflow/issues)

</div>
