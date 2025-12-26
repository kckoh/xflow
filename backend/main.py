from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from backend.routers import auth, users, data_lake

# Create FastAPI app
app = FastAPI(title="Jungle Data Structures API", version="1.0.0")

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
    return {"message": "Connected to FastAPI + PostgreSQL"}
