import os
from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import auth, users, rdb_sources, aws, catalog, rdb_tables, glue, athena
from database import init_db, close_db


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for startup and shutdown events.
    """
    # Startup: Initialize MongoDB connection
    await init_db()
    yield
    # Shutdown: Close MongoDB connection
    await close_db()


# Create FastAPI app with lifespan
app = FastAPI(
    title="Jungle Data Structures API",
    version="1.0.0",
    lifespan=lifespan
)

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
app.include_router(rdb_sources.router, prefix="/api/rdb-sources", tags=["rdb-sources"])
app.include_router(aws.router, prefix="/api/aws", tags=["aws"])
app.include_router(catalog.router, prefix="/api/catalog", tags=["catalog"])
app.include_router(rdb_tables.router, prefix="/api/rdb-tables", tags=["rdb-tables"])
app.include_router(glue.router, prefix="/api/glue", tags=["glue"])
app.include_router(athena.router, prefix="/api/athena", tags=["athena"])



# Root route - Test database connection
@app.get("/")
def read_root():
    return {"message": "Connected to FastAPI + MongoDB"}
