# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

XFlow is a full-stack web application with a React frontend and FastAPI backend, using PostgreSQL and Neo4j databases.

## Commands

### Backend (FastAPI)
```bash
# Run from /backend directory
cd backend

# Start the backend server
uvicorn main:app --reload

# Database migrations (Alembic)
alembic revision --autogenerate -m "migration message"
alembic upgrade head
```

### Frontend (React + Vite)
```bash
# Run from /frontend directory
cd frontend

# Install dependencies
npm install

# Start development server (runs on port 5173)
npm run dev

# Build for production
npm run build
```

### Docker Services
```bash
# Start PostgreSQL and Neo4j databases
docker compose up -d

# PostgreSQL: localhost:5433 (user: postgres, password: postgres, db: mydb)
# Neo4j: localhost:7474 (browser), localhost:7687 (bolt) (user: neo4j, password: password)
```

## Architecture

### Backend Structure (`/backend`)
- **main.py**: FastAPI app entry point with CORS configuration
- **database.py**: SQLAlchemy engine setup and session management (connects to PostgreSQL on port 5433)
- **models.py**: SQLAlchemy ORM models (User model)
- **dependencies.py**: Shared dependencies including in-memory session store and auth helpers
- **routers/**: API route handlers (auth.py for signup/login/logout, users.py for user operations)
- **schemas/**: Pydantic models for request/response validation
- **alembic/**: Database migration configuration

### Frontend Structure (`/frontend`)
- Built with Vite + React 18 + Tailwind CSS v4
- **src/App.jsx**: Main app component with routing
- **src/pages/**: Page components (home.jsx, signup.jsx)
- **src/components/**: Reusable components (ProtectedRoute.jsx)
- **src/context/**: React context providers (AuthContext.jsx)

### API Endpoints
- Backend serves API at `http://localhost:8000`
- Frontend runs at `http://localhost:5173`
- Auth endpoints use `/api` prefix (e.g., `/api/signup`, `/api/login`)
- Session-based auth using `X-Session-ID` header

### Database
- PostgreSQL stores relational data (users table)
- Neo4j available for graph data (not yet integrated in code)
