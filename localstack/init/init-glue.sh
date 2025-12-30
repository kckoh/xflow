#!/bin/bash
# LocalStack Init Script - Creates Glue resources on startup
# This runs automatically when LocalStack is ready

echo "=== Initializing Glue Resources ==="

# Create Glue Database
awslocal glue create-database \
  --database-input '{"Name": "xflow_db", "Description": "XFlow main Glue catalog database"}' \
  2>/dev/null && echo "Created database: xflow_db" || echo "Database xflow_db already exists"

# Create S3 buckets if not exist
for bucket in xflow-raw-data xflow-processed-data xflow-data-lake xflow-athena-results; do
  awslocal s3 mb s3://$bucket 2>/dev/null && echo "Created bucket: $bucket" || echo "Bucket $bucket already exists"
done

echo "=== Glue Resources Initialized ==="
