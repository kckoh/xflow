#!/bin/bash
# ===========================================
# XFlow EKS Test Environment Commands
# Cluster: xflow-test2 (ap-northeast-2)
# ===========================================

# ----- START ALL -----

# 1. Scale up Spot nodes (~2 min)
eksctl scale nodegroup --cluster xflow-test2 --name ng-spot --nodes 2 --region ap-northeast-2

# 2. Wait for nodes to be Ready
kubectl get nodes -w

# 3. Install Helm chart
cd /Users/kckoh/Desktop/xflow/charts/xflow
helm upgrade --install xflow . -f values-eks-test.yaml -n xflow \
  --set trino.ingress.enabled=false \
  --set global.imageRegistry="" \
  --set backend.image.repository=134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend-test \
  --set airflow.images.airflow.repository=134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-airflow-test

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
eksctl scale nodegroup --cluster xflow-test2 --name ng-spot --nodes 0 --region ap-northeast-2

# ----- USEFUL COMMANDS -----

# Check node status
kubectl get nodes

# Check pod status
kubectl get pods -n xflow

# Check nodegroup status
eksctl get nodegroup --cluster xflow-test2 --region ap-northeast-2
