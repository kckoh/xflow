from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from backend.routers import auth, users, data_lake
from backend.database import connect_to_mongodb, close_mongodb_connection


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan events for MongoDB connection"""
    # Startup
    await connect_to_mongodb()
    yield
    # Shutdown
    await close_mongodb_connection()


# Create FastAPI app
app = FastAPI(title="Jungle Data Structures API", version="1.0.0", lifespan=lifespan)

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite default + React default
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods (GET, POST, PUT, DELETE, etc.)
    allow_headers=["*"],  # Allows all headers
)

# Include routers
app.include_router(auth.router, prefix="/api", tags=["auth"])
app.include_router(users.router, prefix="/users", tags=["users"])
app.include_router(data_lake.router, tags=["data-lake"])


# Root route - Test database connection
@app.get("/")
def read_root():
    return {"message": "Connected to FastAPI + MongoDB"}
