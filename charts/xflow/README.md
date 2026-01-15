# XFlow Helm Chart

XFlow는 데이터 파이프라인 플랫폼으로, EKS, GKE, AKS 등 다양한 Kubernetes 환경에서 배포할 수 있습니다.

## 컴포넌트

| 컴포넌트 | 설명 | 기본 활성화 |
|---------|------|------------|
| Backend | FastAPI 기반 API 서버 | Yes |
| MongoDB | NoSQL 데이터베이스 | Yes |
| OpenSearch | 검색 엔진 + Dashboards | Yes |
| Spark | ETL 작업용 (Spark on K8s) | Yes |
| Airflow | 워크플로우 오케스트레이션 | Yes |
| Trino | 분산 SQL 쿼리 엔진 | Yes |

## 사전 요구사항

- Kubernetes 1.25+
- Helm 3.x
- kubectl

### 클라우드별 추가 요구사항

**AWS EKS:**
- AWS Load Balancer Controller
- IAM roles for IRSA (S3 접근용)
- ACM 인증서 (HTTPS용)

**GCP GKE:**
- GCE Ingress Controller
- Workload Identity 설정
- Managed Certificate 또는 cert-manager

**Azure AKS:**
- Application Gateway Ingress Controller
- Workload Identity 설정

## 빠른 시작

### 1. 차트 의존성 빌드

```bash
cd charts/xflow
helm dependency build
```

### 2. 환경별 설치

#### AWS EKS

```bash
helm install xflow ./charts/xflow \
  -f ./charts/xflow/values-eks.yaml \
  --set global.imageRegistry=123456789012.dkr.ecr.ap-northeast-2.amazonaws.com \
  --set global.aws.accountId=123456789012 \
  --set global.domain=xflows.net \
  --set global.tls.acm.certificateArn=arn:aws:acm:ap-northeast-2:123456789012:certificate/xxx \
  --set mongodb.auth.rootPassword=YOUR_SECURE_PASSWORD \
  -n xflow --create-namespace
```

#### GCP GKE

```bash
helm install xflow ./charts/xflow \
  -f ./charts/xflow/values-gke.yaml \
  --set global.imageRegistry=gcr.io/my-project \
  --set global.gcp.projectId=my-project \
  --set global.domain=xflows.net \
  --set mongodb.auth.rootPassword=YOUR_SECURE_PASSWORD \
  -n xflow --create-namespace
```

#### Azure AKS

```bash
helm install xflow ./charts/xflow \
  -f ./charts/xflow/values-aks.yaml \
  --set global.imageRegistry=myacr.azurecr.io \
  --set global.azure.subscriptionId=xxx-xxx-xxx \
  --set global.domain=xflows.net \
  --set mongodb.auth.rootPassword=YOUR_SECURE_PASSWORD \
  -n xflow --create-namespace
```

#### 로컬 (Minikube)

```bash
# 로컬 이미지 빌드
docker build -t xflow-backend:local ./backend

# 설치
helm install xflow ./charts/xflow \
  -f ./charts/xflow/values-minikube.yaml \
  --set mongodb.auth.rootPassword=localdev \
  -n xflow --create-namespace

# 서비스 접근 (port-forward)
kubectl port-forward svc/backend 8000:80 -n xflow
```

## 설정

### 주요 설정 값

| 파라미터 | 설명 | 기본값 |
|---------|------|--------|
| `global.imageRegistry` | 컨테이너 이미지 레지스트리 | `""` |
| `global.cloudProvider` | 클라우드 프로바이더 (aws/gcp/azure/local) | `aws` |
| `global.domain` | 기본 도메인 | `""` |
| `global.storageClass` | 스토리지 클래스 | 클라우드별 기본값 |
| `global.tls.enabled` | TLS 활성화 | `false` |

### 컴포넌트 활성화/비활성화

```yaml
backend:
  enabled: true

mongodb:
  enabled: true

opensearch:
  enabled: true

spark:
  enabled: true

airflow:
  enabled: true

trino:
  enabled: true
```

### IRSA/Workload Identity 설정

#### AWS EKS (IRSA)

```yaml
backend:
  serviceAccount:
    annotations:
      eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT:role/ROLE_NAME
```

#### GCP GKE (Workload Identity)

```yaml
backend:
  serviceAccount:
    annotations:
      iam.gke.io/gcp-service-account: SA_NAME@PROJECT.iam.gserviceaccount.com
```

#### Azure AKS (Workload Identity)

```yaml
backend:
  serviceAccount:
    annotations:
      azure.workload.identity/client-id: CLIENT_ID
```

## 업그레이드

```bash
helm upgrade xflow ./charts/xflow \
  -f ./charts/xflow/values-eks.yaml \
  -n xflow
```

## 삭제

```bash
helm uninstall xflow -n xflow
```

## 디버깅

### 템플릿 렌더링 확인

```bash
helm template xflow ./charts/xflow \
  -f ./charts/xflow/values-eks.yaml \
  --debug
```

### Dry-run 테스트

```bash
helm install xflow ./charts/xflow \
  -f ./charts/xflow/values-eks.yaml \
  --dry-run
```

### 차트 검증

```bash
helm lint ./charts/xflow
```

## 파일 구조

```
charts/xflow/
├── Chart.yaml              # 차트 메타데이터
├── values.yaml             # 기본 values
├── values-eks.yaml         # AWS EKS용
├── values-gke.yaml         # GCP GKE용
├── values-aks.yaml         # Azure AKS용
├── values-minikube.yaml    # 로컬용
├── README.md
└── templates/
    ├── _helpers.tpl        # 헬퍼 함수
    ├── namespace.yaml
    ├── backend/
    │   ├── deployment.yaml
    │   ├── service.yaml
    │   ├── configmap.yaml
    │   ├── secret.yaml
    │   ├── serviceaccount.yaml
    │   ├── rbac.yaml
    │   └── ingress.yaml
    ├── mongodb/
    │   ├── statefulset.yaml
    │   ├── service.yaml
    │   ├── secret.yaml
    │   └── pvc.yaml
    ├── opensearch/
    │   ├── statefulset.yaml
    │   ├── service.yaml
    │   ├── pvc.yaml
    │   ├── dashboards-deployment.yaml
    │   ├── dashboards-service.yaml
    │   └── ingress.yaml
    └── spark/
        ├── namespace.yaml
        ├── serviceaccount.yaml
        └── rbac.yaml
```

## 트러블슈팅

### Pod가 시작되지 않는 경우

```bash
kubectl describe pod <pod-name> -n xflow
kubectl logs <pod-name> -n xflow
```

### Ingress가 작동하지 않는 경우

1. Ingress Controller가 설치되어 있는지 확인
2. 인증서 ARN이 올바른지 확인 (AWS)
3. 도메인 DNS 설정 확인

### MongoDB 연결 실패

1. MongoDB Pod 상태 확인: `kubectl get pods -n xflow -l app=mongodb`
2. Secret이 올바르게 생성되었는지 확인: `kubectl get secret mongodb-secret -n xflow`

## 라이선스

MIT License
