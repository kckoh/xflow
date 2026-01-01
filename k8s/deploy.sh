#!/bin/bash

set -e

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "🚀 XFlow Kubernetes Deployment Script"
echo "======================================"

# Check if minikube is running
if ! minikube status > /dev/null 2>&1; then
    echo "⚠️  Minikube is not running. Starting minikube..."
    minikube start --memory=4096 --cpus=2
fi

# Use minikube's docker daemon
echo "📦 Configuring Docker to use Minikube..."
eval $(minikube docker-env)

# Build backend image
echo "🔨 Building backend Docker image..."
docker build -t xflow-backend:latest "$PROJECT_DIR/backend"

# Apply Kubernetes manifests
echo "☸️  Applying Kubernetes manifests..."
kubectl apply -k "$SCRIPT_DIR"

# Wait for deployments
echo "⏳ Waiting for MongoDB to be ready..."
kubectl wait --for=condition=ready pod -l app=mongodb -n xflow --timeout=120s

echo "⏳ Waiting for Backend to be ready..."
kubectl wait --for=condition=ready pod -l app=backend -n xflow --timeout=120s

# Get service URL
echo ""
echo "✅ Deployment Complete!"
echo "======================================"
echo ""
echo "📊 Pod Status:"
kubectl get pods -n xflow
echo ""
echo "🌐 Services:"
kubectl get svc -n xflow
echo ""
echo "🔗 Access Backend API:"
echo "   minikube service backend -n xflow --url"
echo ""
echo "   Or use port-forward:"
echo "   kubectl port-forward svc/backend 8000:8000 -n xflow"
