# XFlow Infrastructure Setup

이 디렉토리에는 XFlow를 AWS EKS에 배포하기 위한 인프라 설정 파일들이 있습니다.

## 파일 구조

```
infra/
├── cluster.yaml   # eksctl 클러스터 설정
├── setup.sh       # 자동 설정 스크립트
└── README.md      # 이 문서
```

## 사전 요구사항

```bash
# AWS CLI
aws --version

# eksctl
eksctl version

# kubectl
kubectl version --client

# helm
helm version

# docker
docker --version
```

## 빠른 시작

### 1. 자동 설정 (권장)

```bash
cd infra
chmod +x setup.sh
./setup.sh
```

이 스크립트가 자동으로 생성하는 것들:
- ECR 레포지토리 3개
- S3 버킷 3개
- EKS 클러스터 + 노드 그룹
- IRSA (ServiceAccount + IAM Role 연결)
- AWS Load Balancer Controller

### 2. 수동 설정

#### Step 1: ECR 레포지토리 생성
```bash
aws ecr create-repository --repository-name xflow-backend
aws ecr create-repository --repository-name xflow-airflow
aws ecr create-repository --repository-name xflow-spark
```

#### Step 2: S3 버킷 생성
```bash
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)

aws s3api create-bucket \
  --bucket xflow-delta-lake-${ACCOUNT_ID} \
  --region ap-northeast-2 \
  --create-bucket-configuration LocationConstraint=ap-northeast-2

aws s3api create-bucket \
  --bucket xflow-airflow-logs-${ACCOUNT_ID} \
  --region ap-northeast-2 \
  --create-bucket-configuration LocationConstraint=ap-northeast-2

aws s3api create-bucket \
  --bucket xflow-loki-logs-${ACCOUNT_ID} \
  --region ap-northeast-2 \
  --create-bucket-configuration LocationConstraint=ap-northeast-2
```

#### Step 3: EKS 클러스터 생성
```bash
eksctl create cluster -f cluster.yaml
```

#### Step 4: AWS Load Balancer Controller 설치
```bash
helm repo add eks https://aws.github.io/eks-charts
helm repo update

helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  -n kube-system \
  --set clusterName=xflow-cluster \
  --set serviceAccount.create=false \
  --set serviceAccount.name=aws-load-balancer-controller
```

---

## 도메인 및 ACM 인증서 설정

### Option A: Route53에서 도메인 구매

1. AWS Console → Route53 → Registered domains
2. "Register domain" 클릭
3. 원하는 도메인 검색 및 구매 (예: xflows.net)

### Option B: 외부 도메인 사용

1. 외부에서 도메인 구매 (Namecheap, GoDaddy 등)
2. Route53에서 Hosted Zone 생성
3. 외부 도메인의 NS 레코드를 Route53 NS로 변경

### ACM 인증서 발급

```bash
# 와일드카드 인증서 요청
aws acm request-certificate \
  --domain-name "*.xflows.net" \
  --validation-method DNS \
  --region ap-northeast-2

# 출력된 Certificate ARN 저장
# 예: arn:aws:acm:ap-northeast-2:123456789012:certificate/xxx-xxx-xxx
```

**DNS 검증:**
1. AWS Console → ACM → 인증서 선택
2. "Create records in Route53" 클릭 (자동 DNS 검증)
3. 상태가 "Issued"로 변경될 때까지 대기 (5-30분)

---

## XFlow 배포

### 1. values-eks.yaml 수정

```bash
cd ../charts/xflow

# 필수 값들 수정
vim values-eks.yaml
```

수정할 값들:
```yaml
global:
  imageRegistry: "YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com"
  aws:
    accountId: "YOUR_ACCOUNT_ID"
  domain: "your-domain.com"
  tls:
    acm:
      certificateArn: "arn:aws:acm:ap-northeast-2:YOUR_ACCOUNT_ID:certificate/xxx"

mongodb:
  auth:
    rootPassword: "YOUR_SECURE_PASSWORD"
```

### 2. 이미지 빌드 및 푸시

```bash
# ECR 로그인
aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com

# Backend
docker build -t YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest ../backend
docker push YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-backend:latest

# Airflow
docker build -t YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-airflow:latest ../airflow
docker push YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-airflow:latest

# Spark
docker build -t YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest -f ../spark/Dockerfile.k8s ../spark
docker push YOUR_ACCOUNT_ID.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest
```

### 3. Helm 배포

```bash
cd ../charts/xflow

# 의존성 업데이트
helm dependency update

# 배포
helm install xflow . -f values-eks.yaml -n xflow --create-namespace

# 또는 업그레이드
helm upgrade --install xflow . -f values-eks.yaml -n xflow
```

### 4. 상태 확인

```bash
# Pod 상태
kubectl get pods -n xflow
kubectl get pods -n monitoring
kubectl get pods -n spark-jobs

# Ingress 상태 (ALB 주소 확인)
kubectl get ingress -n xflow
kubectl get ingress -n monitoring

# 로그 확인
kubectl logs -f deployment/backend -n xflow
```

---

## DNS 설정

ALB가 생성되면 DNS 레코드를 추가해야 합니다.

```bash
# ALB 주소 확인
kubectl get ingress -n xflow -o jsonpath='{.items[0].status.loadBalancer.ingress[0].hostname}'
```

Route53에서 CNAME 또는 Alias 레코드 생성:
- `api.your-domain.com` → ALB 주소
- `grafana.your-domain.com` → ALB 주소
- `opensearch.your-domain.com` → ALB 주소

---

## 삭제

```bash
# Helm 릴리스 삭제
helm uninstall xflow -n xflow

# EKS 클러스터 삭제
eksctl delete cluster --name xflow-cluster --region ap-northeast-2

# S3 버킷 삭제 (데이터가 있으면 먼저 비워야 함)
aws s3 rb s3://xflow-delta-lake-${ACCOUNT_ID} --force
aws s3 rb s3://xflow-airflow-logs-${ACCOUNT_ID} --force
aws s3 rb s3://xflow-loki-logs-${ACCOUNT_ID} --force

# ECR 레포지토리 삭제
aws ecr delete-repository --repository-name xflow-backend --force
aws ecr delete-repository --repository-name xflow-airflow --force
aws ecr delete-repository --repository-name xflow-spark --force
```

---

## 비용 예상

| 리소스 | 예상 비용 (월) |
|--------|---------------|
| EKS Control Plane | $72 |
| EC2 (t3.large x2) | ~$120 |
| ALB | ~$20 |
| S3 | 사용량에 따라 |
| Route53 Hosted Zone | $0.50 |
| **합계** | **~$210+** |

> Spot 인스턴스 사용시 EC2 비용 70% 절감 가능
