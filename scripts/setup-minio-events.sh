#!/bin/bash

# MinIO Event Notification Setup Script
# This script configures MinIO to send webhook notifications when Parquet files are uploaded

set -e

echo "🔧 Setting up MinIO Event Notifications..."

# Configuration
MINIO_ALIAS="myminio"
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minio"
MINIO_SECRET_KEY="minio123"
BUCKET_NAME="datalake"
WEBHOOK_ENDPOINT="http://host.docker.internal:8000/api/data-lake/webhook/minio"

echo "📋 Configuration:"
echo "  - MinIO Endpoint: $MINIO_ENDPOINT"
echo "  - Bucket: $BUCKET_NAME"
echo "  - Webhook: $WEBHOOK_ENDPOINT"
echo ""

# Wait for MinIO to be ready
echo "⏳ Waiting for MinIO to be ready..."
until curl -sf $MINIO_ENDPOINT/minio/health/live > /dev/null 2>&1; do
    echo "   Waiting for MinIO..."
    sleep 2
done
echo "✅ MinIO is ready!"
echo ""

# Configure MinIO alias
echo "🔑 Configuring MinIO client..."
mc alias set $MINIO_ALIAS $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY
echo ""

# Enable webhook notification in MinIO
echo "🪝 Configuring webhook notification..."
mc admin config set $MINIO_ALIAS notify_webhook:1 \
    endpoint="$WEBHOOK_ENDPOINT" \
    queue_limit="10" \
    queue_dir="" \
    comment="FastAPI Data Lake Webhook"

echo "✅ Webhook configured!"
echo ""

# Restart MinIO to apply changes
echo "🔄 Restarting MinIO service..."
docker restart minio
echo "   Waiting for MinIO to restart..."
sleep 5

# Wait for MinIO to be ready again
until curl -sf $MINIO_ENDPOINT/minio/health/live > /dev/null 2>&1; do
    echo "   Waiting for MinIO..."
    sleep 2
done
echo "✅ MinIO restarted!"
echo ""

# Set up event notification for the bucket
echo "📢 Setting up bucket event notifications..."
mc event add $MINIO_ALIAS/$BUCKET_NAME \
    arn:minio:sqs::1:webhook \
    --event put \
    --suffix .parquet

echo "✅ Event notification configured for bucket: $BUCKET_NAME"
echo ""

# List current event notifications
echo "📋 Current event notifications:"
mc event list $MINIO_ALIAS/$BUCKET_NAME
echo ""

echo "🎉 MinIO Event Notification setup complete!"
echo ""
echo "📝 Next steps:"
echo "  1. Start your FastAPI server:"
echo "     uvicorn main:app --reload"
echo ""
echo "  2. Upload a Parquet file to test:"
echo "     mc cp test.parquet $MINIO_ALIAS/$BUCKET_NAME/sales/year=2024/month=12/test.parquet"
echo ""
echo "  3. Check the FastAPI logs to see the webhook trigger!"
