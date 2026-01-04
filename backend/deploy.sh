#!/bin/bash
set -e

# Configuration
ECR_REPO="134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend"
NAMESPACE="default"
DEPLOYMENT="backend"

echo "🔨 Building Docker image..."
docker build --platform linux/amd64 -t ${ECR_REPO}:latest .

echo "📤 Pushing to ECR..."
docker push ${ECR_REPO}:latest

echo "🔄 Restarting deployment..."
kubectl rollout restart deployment ${DEPLOYMENT} -n ${NAMESPACE}

echo "⏳ Waiting for rollout..."
kubectl rollout status deployment ${DEPLOYMENT} -n ${NAMESPACE} --timeout=120s

echo "✅ Deploy complete!"
