# syntax=docker/dockerfile:1
FROM python:3.10-slim

WORKDIR /app

# Install dependencies with BuildKit cache (much faster rebuilds)
COPY requirements.txt .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt

# Copy application code
COPY . .

EXPOSE 8000

# Run with hot-reload for development
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]
