# Backend Kubernetes Deployment

## Files

- `deployment.yaml` - Backend Deployment 설정
- `configmap.yaml` - 환경 변수 설정
- `serviceaccount.yaml` - IRSA ServiceAccount (S3 접근용)

## 배포 명령어

```bash
# 1. Docker 이미지 빌드
cd /path/to/xflow/backend
docker build -t 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest .

# 2. ECR 푸시
docker push 134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest

# 3. ConfigMap 적용 (설정 변경 시)
kubectl apply -f k8s/backend/configmap.yaml

# 4. 배포 재시작
kubectl rollout restart deployment backend -n default

# 5. 상태 확인
kubectl rollout status deployment backend -n default
kubectl get pods -n default -l app=backend
```

## 전체 재배포 (처음 또는 전체 업데이트 시)

```bash
kubectl apply -f k8s/backend/
kubectl rollout restart deployment backend -n default
```

## 환경 변수 (ConfigMap)

| 변수 | 설명 | 예시 |
|------|------|------|
| MONGODB_DATABASE | MongoDB 데이터베이스 이름 | xflow |
| CORS_ORIGINS | 허용된 CORS origins | https://xflows.net |
| AIRFLOW_DAG_ID | Airflow DAG ID | etl_job_dag_k8s |
| AIRFLOW_BASE_URL | Airflow API URL | http://airflow-webserver.airflow:8080/api/v1 |
| ENVIRONMENT | 환경 (local/production) | production |
| AWS_REGION | AWS 리전 | ap-northeast-2 |

## IRSA (IAM Roles for Service Accounts)

백엔드는 `backend-sa` ServiceAccount를 사용하여 S3에 접근합니다.

**생성 명령어:**
```bash
eksctl create iamserviceaccount \
  --cluster=xflow-cluster \
  --namespace=default \
  --name=backend-sa \
  --attach-policy-arn=arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess \
  --approve
```

**IAM Role ARN:** `arn:aws:iam::134059028370:role/eksctl-xflow-cluster-addon-iamserviceaccount--Role1-VcpbdbiD8GJi`

## 로그 확인

```bash
kubectl logs -n default -l app=backend --tail=100 -f
```
