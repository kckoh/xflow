#!/bin/bash
# ===========================================
# XFlow EKS Test Environment Commands
# Cluster: xflow-test2 (ap-northeast-2)
# ===========================================

# Configuration
AWS_REGION="ap-northeast-2"
CLUSTER_NAME="xflow-test2"
NODEGROUP_NAME="ng-spot"

# Get AWS Account ID dynamically
AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ECR_REGISTRY="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"

echo "AWS Account ID: ${AWS_ACCOUNT_ID}"
echo "ECR Registry: ${ECR_REGISTRY}"

# ----- START ALL -----

# 1. Scale up Spot nodes (~2 min)
eksctl scale nodegroup --cluster ${CLUSTER_NAME} --name ${NODEGROUP_NAME} --nodes 2 --region ${AWS_REGION}

# 2. Wait for nodes to be Ready
kubectl get nodes -w

# 3. Install Helm chart (run from charts/xflow directory)
# cd /path/to/xflow/charts/xflow
helm upgrade --install xflow . -f values-eks-test.yaml -n xflow \
  --set trino.ingress.enabled=false \
  --set global.imageRegistry="" \
  --set backend.image.repository=${ECR_REGISTRY}/xflow-backend-test \
  --set airflow.images.airflow.repository=${ECR_REGISTRY}/xflow-airflow-test

# 4. Watch pod status
kubectl get pods -n xflow -w

# ----- PORT FORWARD -----

# Backend API: http://localhost:8000
kubectl port-forward svc/backend 8000:80 -n xflow &

# Airflow: http://localhost:8080
kubectl port-forward svc/xflow-webserver 8080:8080 -n xflow &

# Grafana: http://localhost:3000 (admin / xflow-grafana-test)
kubectl port-forward svc/xflow-grafana 3000:80 -n xflow &

# OpenSearch Dashboards: http://localhost:5601
kubectl port-forward svc/opensearch-dashboards 5601:5601 -n xflow &

# Trino: http://localhost:8081
kubectl port-forward svc/xflow-trino 8081:8080 -n xflow &

# ----- STOP ALL -----

# Uninstall Helm release
helm uninstall xflow -n xflow

# Scale down Spot nodes (saves cost)
eksctl scale nodegroup --cluster ${CLUSTER_NAME} --name ${NODEGROUP_NAME} --nodes 0 --region ${AWS_REGION}

# ----- USEFUL COMMANDS -----

# Check node status
kubectl get nodes

# Check pod status
kubectl get pods -n xflow

# Check nodegroup status
eksctl get nodegroup --cluster ${CLUSTER_NAME} --region ${AWS_REGION}
