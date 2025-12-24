# react-fastapi
alembic revision --autogenerate -m ""

# 3. Apply migration
alembic upgrade head

# 4. Verify
docker compose exec postgres psql -U postgres -d mydb -c "\d users"
psql -U postgres -d mydb



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


hive 설정

hive 폴더 안에 lib 폴더 생성 후 postgresql-42.6.0.jar  만들기
