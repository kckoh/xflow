#!/bin/bash
set -e

# Configuration
AWS_REGION="ap-northeast-2"
AWS_ACCOUNT_ID="134059028370"
ECR_REPO="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/xflow-airflow"
NAMESPACE="airflow"

echo "=== Airflow EKS Deployment ==="

# Step 1: Create ECR repository (if not exists)
echo "[1/5] Creating ECR repository..."
aws ecr describe-repositories --repository-names xflow-airflow --region ${AWS_REGION} 2>/dev/null || \
    aws ecr create-repository --repository-name xflow-airflow --region ${AWS_REGION}

# Step 2: Login to ECR
echo "[2/5] Logging into ECR..."
aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com

# Step 3: Build and push Docker image
echo "[3/5] Building and pushing Docker image..."
cd "$(dirname "$0")/../../airflow"
docker build --platform linux/amd64 -t ${ECR_REPO}:latest .
docker push ${ECR_REPO}:latest

# Step 4: Add Helm repo
echo "[4/5] Setting up Helm..."
helm repo add apache-airflow https://airflow.apache.org 2>/dev/null || true
helm repo update

# Step 5: Deploy with Helm
echo "[5/5] Deploying Airflow..."
cd "$(dirname "$0")"
helm upgrade --install airflow apache-airflow/airflow \
    --namespace ${NAMESPACE} \
    --create-namespace \
    -f values.yaml \
    --wait

echo ""
echo "=== Deployment Complete ==="
echo ""
echo "To access Airflow UI:"
echo "  kubectl port-forward svc/airflow-webserver 8080:8080 -n ${NAMESPACE}"
echo "  Open http://localhost:8080"
echo ""
echo "Default credentials: admin / admin"
echo ""
