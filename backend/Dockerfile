# syntax=docker/dockerfile:1
FROM python:3.10-slim

WORKDIR /app

# Install uv (Rust 기반, pip보다 10-100배 빠름)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Install dependencies with uv (병렬 설치)
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/uv \
    uv pip install --system -r requirements.txt

# Copy application code
COPY . .

EXPOSE 8000

# Run with hot-reload for development
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
