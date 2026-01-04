#!/bin/bash
set -e

# Configuration
S3_BUCKET="s3://xflows"
CLOUDFRONT_DISTRIBUTION_ID="E3J1KK599NCLXA"

echo "📦 Installing dependencies..."
npm install

echo "🔨 Building frontend..."
npm run build

echo "📤 Uploading to S3..."
aws s3 sync dist/ ${S3_BUCKET} --delete

echo "🔄 Invalidating CloudFront cache..."
aws cloudfront create-invalidation --distribution-id ${CLOUDFRONT_DISTRIBUTION_ID} --paths "/*"

echo "✅ Frontend deploy complete!"
