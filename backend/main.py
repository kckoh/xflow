import logging
import os

from dotenv import load_dotenv

# ⚠️ 중요: 다른 import보다 먼저 .env 파일 로드해야 함!
# routers.logs → utils.log_utils → os.getenv() 순서로 실행되기 때문
load_dotenv()

# Logging 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    force=True,  # 기존 설정 덮어쓰기
)
# 모든 logger가 출력되도록 root logger 레벨 설정
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("services").setLevel(logging.INFO)

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import (
    admin,
    ai,
    auth,
    catalog,
    connections,
    domains,
    duckdb,
    trino,
    datasets,
    job_runs,
    logs,
    metadata,
    opensearch,
    s3_logs_preview,
    s3_csv_preview,
    s3_json_preview,
    source_datasets,
    sql_test,
    users,
)
from routers.transforms import select_fields  # 추후 type 추가 예정 (예: join ...)

from database import close_db, init_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup: Initialize MongoDB connection
    await init_db()

    # Startup: Initialize OpenSearch connection
    from utils.opensearch_client import initialize_opensearch
    initialize_opensearch()

    # Startup: Initialize Redis connection
    from utils.redis_client import redis_client
    try:
        await redis_client.connect()
        logging.info("Redis connected successfully")
    except Exception as e:
        logging.warning(f"Redis connection failed (using in-memory fallback): {e}")

    yield

    # Shutdown: Close Redis connection
    try:
        await redis_client.disconnect()
    except Exception:
        pass

    # Shutdown: Close MongoDB connection
    await close_db()


from slowapi import _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.middleware import SlowAPIMiddleware
from utils.limiter import limiter

# Create FastAPI app with lifespan
app = FastAPI(
    title="Jungle Data Structures API",
    version="1.0.0",
    lifespan=lifespan,
    redirect_slashes=False,
)

app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.add_middleware(SlowAPIMiddleware)

# Get CORS origins from environment
CORS_ORIGINS = os.getenv("CORS_ORIGINS", "http://localhost:5173").split(",")

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Include routers
app.include_router(auth.router, prefix="/api", tags=["auth"])
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(connections.router, prefix="/api/connections", tags=["connections"])
app.include_router(catalog.router, prefix="/api/catalog", tags=["catalog"])
app.include_router(metadata.router, prefix="/api/metadata", tags=["metadata"])
app.include_router(opensearch.router, prefix="/api/opensearch", tags=["opensearch"])

app.include_router(
    select_fields.router,
    prefix="/api/rdb-transform/select-fields",
    tags=["select-fields"],
)

# Datasets and Job Runs
app.include_router(datasets.router, prefix="/api/datasets", tags=["datasets"])
app.include_router(job_runs.router, prefix="/api/job-runs", tags=["job-runs"])

# Source Datasets
app.include_router(source_datasets.router, tags=["source-datasets"])

# S3 Logs Preview (S3 log parsing test)
app.include_router(s3_logs_preview.router, prefix="/api/s3-logs", tags=["s3-logs"])

# S3 CSV Preview (S3 CSV schema extraction)
app.include_router(s3_csv_preview.router, prefix="/api/s3-csv", tags=["s3-csv"])

# S3 JSON Preview (S3 JSON schema extraction)
app.include_router(s3_json_preview.router, prefix="/api/s3-json", tags=["s3-json"])

# DuckDB Query Engine
app.include_router(duckdb.router, prefix="/api/duckdb", tags=["duckdb"])

# Trino Query Engine
app.include_router(trino.router, prefix="/api/trino", tags=["trino"])

# SQL Test (DuckDB-based query testing)
app.include_router(sql_test.router, prefix="/api/sql", tags=["sql-test"])

# Kafka Streaming
from routers import kafka_streaming

app.include_router(kafka_streaming.router)

# Domains (CRUD + ETL Job Import)
app.include_router(domains.router, prefix="/api/domains", tags=["domains"])

# CDC (Change Data Capture)
from routers import cdc

app.include_router(cdc.router)

# Logs (Event logging to S3/Local)
app.include_router(logs.router)

# Admin (User management - admin only)
app.include_router(admin.router, prefix="/api/admin", tags=["admin"])

# AI Query Assistant (Text-to-SQL)
app.include_router(ai.router, prefix="/api/ai", tags=["ai"])

# Quality (Data quality checks)
from routers import quality

app.include_router(quality.router, prefix="/api/quality", tags=["quality"])


# Root route - Test database connection
@app.get("/")
def read_root():
    return {"message": "Connected to FastAPI + MongoDB"}


# Health check for Kubernetes
@app.get("/health")
def health_check():
    return {"status": "healthy"}


# add a test api
@app.get("/test")
def test_check():
    return {"status": "test"}
