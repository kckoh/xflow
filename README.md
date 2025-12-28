# react-fastapi
# fastapi orm migration
alembic revision --autogenerate -m ""

3. Apply migration
alembic upgrade head

4. Verify
docker compose exec postgres psql -U postgres -d mydb -c "\d users"
psql -U postgres -d mydb

# Start fastapi backend
docker compose up backend

background로 돌리기
docker compose up -d backend

# Start postgres first, then airflow
docker compose up -d postgres
docker compose up airflow-init
docker compose up -d airflow-webserver airflow-scheduler

Access: http://localhost:8080


# Start MinIO
docker compose up -d minio && docker compose up minio-init

# Start Spark
docker compose up -d spark-master spark-worker
Access:
- MinIO Console: http://localhost:9001
minio/minio123
- Spark UI: http://localhost:8081

## Spark JAR Dependencies

Spark ETL jobs require the following JAR files. Download them before running Spark jobs:

```bash
# Create jars directory
mkdir -p spark/jars

# Download PostgreSQL JDBC driver
curl -fsSLO --output-dir spark/jars https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Download AWS SDK for S3 (LocalStack)
curl -fsSLO --output-dir spark/jars https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -fsSLO --output-dir spark/jars https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

### Run Spark ETL Job

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master 'local[*]' \
  --jars /opt/spark/jars/extra/postgresql-42.7.4.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar,/opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar \
  /opt/spark/jobs/postgres_et.py
```


hive 설정

hive 폴더 안에 lib 폴더 생성 후 postgresql-42.6.0.jar  만들기


# fake data 추가

docker compose exec -T postgres psql -U postgres -d mydb < init-fake-data.sql

Row 수 확인
docker compose exec -T postgres psql -U postgres -d mydb -c "SELECT n_live_tup as row_count FROM pg_stat_user_tables WHERE schemaname = 'public' ORDER BY n_live_tup DESC;"

# users table issue
drop the current user table and use alembic tool to migrate
apply init fake data
