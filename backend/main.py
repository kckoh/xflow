import os
import logging
from dotenv import load_dotenv

# ⚠️ 중요: 다른 import보다 먼저 .env 파일 로드해야 함!
# routers.logs → utils.log_utils → os.getenv() 순서로 실행되기 때문
load_dotenv()

# Logging 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    force=True  # 기존 설정 덮어쓰기
)
# 모든 logger가 출력되도록 root logger 레벨 설정
logging.getLogger().setLevel(logging.INFO)
logging.getLogger("services").setLevel(logging.INFO)

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth, users, connections, catalog, etl_jobs, job_runs, opensearch, duckdb, metadata, domains, logs, admin, ai
from routers.transforms import select_fields # 추후 type 추가 예정 (예: join ...)
from database import init_db, close_db

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

    yield

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
    redirect_slashes=False
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

app.include_router(select_fields.router, prefix="/api/rdb-transform/select-fields", tags=["select-fields"])

# ETL Jobs and Job Runs
app.include_router(etl_jobs.router, prefix="/api/etl-jobs", tags=["etl-jobs"])
app.include_router(job_runs.router, prefix="/api/job-runs", tags=["job-runs"])

# DuckDB Query Engine
app.include_router(duckdb.router, prefix="/api/duckdb", tags=["duckdb"])

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
