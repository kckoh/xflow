#!/bin/bash

# Data Lake End-to-End Test Script
# This script runs the complete test flow:
# 1. Setup MinIO events
# 2. Generate and upload test data
# 3. Verify tables were created

set -e

echo "Data Lake End-to-End Test"
echo "=" | tr -d '\n'; printf '%.0s=' {1..50}; echo
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "Docker is not running. Please start Docker first."
    exit 1
fi

# Check if MinIO container is running
if ! docker ps | grep -q minio; then
    echo "MinIO container is not running."
    echo "Starting services with docker-compose..."
    docker-compose up -d minio postgres neo4j
    echo "Waiting for services to be ready..."
    sleep 10
fi

echo "Docker services are running"
echo ""

# Step 1: Setup MinIO Event Notifications
echo "Step 1: Setting up MinIO Event Notifications"
echo "-" | tr -d '\n'; printf '%.0s-' {1..50}; echo
./scripts/setup-minio-events.sh
echo ""

# Step 2: Wait a bit for MinIO to be fully ready
echo "Waiting for MinIO to be fully ready..."
sleep 5
echo ""

# Step 3: Generate and upload test data
echo "Step 2: Generating and uploading test data"
echo "-" | tr -d '\n'; printf '%.0s-' {1..50}; echo
python3 scripts/create-test-data.py
echo ""

# Step 4: Instructions for verification
echo "Step 3: Verification"
echo "-" | tr -d '\n'; printf '%.0s-' {1..50}; echo
echo ""
echo "Test data has been uploaded to MinIO!"
echo ""
echo "Next steps to verify:"
echo ""
echo "Start FastAPI backend:"
echo "   cd backend"
echo "   uvicorn main:app --reload"
echo ""
echo "Check if webhooks were triggered:"
echo "   - Look for webhook calls in FastAPI logs"
echo "   - Tables should be created automatically"
echo ""
echo "View processing history:"
echo "   curl http://localhost:8000/api/data-lake/history | jq"
echo ""
echo "View created tables:"
echo "   curl http://localhost:8000/api/data-lake/tables | jq"
echo ""
echo "Check MinIO console:"
echo "   http://localhost:9001"
echo "   (Credentials in docker-compose.yml)"
echo ""
echo "Query tables in Trino:"
echo "   docker exec -it trino trino"
echo "   SHOW TABLES FROM hive.default;"
echo "   SELECT * FROM hive.default.sales LIMIT 10;"
echo ""
echo "=" | tr -d '\n'; printf '%.0s=' {1..50}; echo
echo "Test setup complete!"
