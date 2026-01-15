#!/bin/bash
# ===========================================
# XFlow Infrastructure Setup Script
# ===========================================
# 이 스크립트는 XFlow를 배포하기 위한 모든 AWS 인프라를 생성합니다.
#
# 사용법:
#   chmod +x setup.sh
#   ./setup.sh
#
# 사전 요구사항:
#   - AWS CLI 설치 및 설정 (aws configure)
#   - eksctl 설치
#   - kubectl 설치
#   - helm 설치

set -e

# ===========================================
# 설정 변수 (여기만 수정하세요!)
# ===========================================
CLUSTER_NAME="xflow-test2"
REGION="ap-northeast-2"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
DOMAIN=""  # 도메인이 있으면 입력 (예: xflows.net)

# S3 버킷 이름
BUCKET_DATALAKE="xflow-delta-lake-${ACCOUNT_ID}"
BUCKET_AIRFLOW_LOGS="xflow-airflow-logs-${ACCOUNT_ID}"
BUCKET_LOKI_LOGS="xflow-loki-logs-${ACCOUNT_ID}"

# ECR 레포지토리 이름
ECR_BACKEND="xflow-backend-test"
ECR_AIRFLOW="xflow-airflow-test"
ECR_SPARK="xflow-spark-test"

# ===========================================
# 색상 출력
# ===========================================
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# ===========================================
# 1. ECR 레포지토리 생성
# ===========================================
create_ecr_repos() {
    log_info "Creating ECR repositories..."

    for repo in $ECR_BACKEND $ECR_AIRFLOW $ECR_SPARK; do
        if aws ecr describe-repositories --repository-names $repo --region $REGION 2>/dev/null; then
            log_warn "ECR repository '$repo' already exists, skipping..."
        else
            aws ecr create-repository \
                --repository-name $repo \
                --region $REGION \
                --image-scanning-configuration scanOnPush=true
            log_info "Created ECR repository: $repo"
        fi
    done

    echo ""
    log_info "ECR Registry: ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
}

# ===========================================
# 2. S3 버킷 생성
# ===========================================
create_s3_buckets() {
    log_info "Creating S3 buckets..."

    for bucket in $BUCKET_DATALAKE $BUCKET_AIRFLOW_LOGS $BUCKET_LOKI_LOGS; do
        if aws s3api head-bucket --bucket $bucket 2>/dev/null; then
            log_warn "S3 bucket '$bucket' already exists, skipping..."
        else
            aws s3api create-bucket \
                --bucket $bucket \
                --region $REGION \
                --create-bucket-configuration LocationConstraint=$REGION
            log_info "Created S3 bucket: $bucket"
        fi
    done
}

# ===========================================
# 3. EKS 클러스터 생성
# ===========================================
create_eks_cluster() {
    log_info "Creating EKS cluster (this may take 15-20 minutes)..."

    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    CLUSTER_YAML="${SCRIPT_DIR}/cluster.yaml"

    # cluster.yaml의 변수 치환 (임시 파일 사용)
    TEMP_CLUSTER_YAML="${SCRIPT_DIR}/cluster-temp.yaml"
    sed "s/xflow-cluster/${CLUSTER_NAME}/g" "${CLUSTER_YAML}" | sed "s/ap-northeast-2/${REGION}/g" > "${TEMP_CLUSTER_YAML}"

    if eksctl get cluster --name $CLUSTER_NAME --region $REGION 2>/dev/null; then
        log_warn "EKS cluster '$CLUSTER_NAME' already exists, skipping..."
    else
        eksctl create cluster -f "${TEMP_CLUSTER_YAML}"
        log_info "EKS cluster created successfully!"
    fi

    # 임시 파일 삭제
    rm -f "${TEMP_CLUSTER_YAML}"

    # kubeconfig 업데이트
    aws eks update-kubeconfig --name $CLUSTER_NAME --region $REGION
}

# ===========================================
# 4. AWS Load Balancer Controller 설치
# ===========================================
install_lb_controller() {
    log_info "Installing AWS Load Balancer Controller..."

    # Helm repo 추가
    helm repo add eks https://aws.github.io/eks-charts
    helm repo update

    # 이미 설치되어 있는지 확인
    if helm list -n kube-system | grep -q aws-load-balancer-controller; then
        log_warn "AWS Load Balancer Controller already installed, skipping..."
    else
        helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
            -n kube-system \
            --set clusterName=$CLUSTER_NAME \
            --set serviceAccount.create=false \
            --set serviceAccount.name=aws-load-balancer-controller
        log_info "AWS Load Balancer Controller installed!"
    fi

    # 설치 확인
    kubectl wait --for=condition=available --timeout=120s deployment/aws-load-balancer-controller -n kube-system
}

# ===========================================
# 5. Namespace 생성
# ===========================================
create_namespaces() {
    log_info "Creating namespaces..."

    kubectl create namespace xflow --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace spark-jobs --dry-run=client -o yaml | kubectl apply -f -
    kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

    log_info "Namespaces created!"
}

# ===========================================
# 6. Docker 이미지 빌드 및 푸시
# ===========================================
build_and_push_images() {
    log_info "Building and pushing Docker images..."

    # ECR 로그인
    aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com

    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

    # Backend
    log_info "Building backend image..."
    docker build -t ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_BACKEND}:latest ${PROJECT_ROOT}/backend
    docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_BACKEND}:latest

    # Airflow
    log_info "Building airflow image..."
    docker build -t ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_AIRFLOW}:latest ${PROJECT_ROOT}/airflow
    docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_AIRFLOW}:latest

    # Spark
    log_info "Building spark image..."
    docker build -t ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_SPARK}:latest -f ${PROJECT_ROOT}/spark/Dockerfile.k8s ${PROJECT_ROOT}/spark
    docker push ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com/${ECR_SPARK}:latest

    log_info "All images pushed to ECR!"
}

# ===========================================
# 7. Helm Chart 배포
# ===========================================
deploy_xflow() {
    log_info "Deploying XFlow via Helm..."

    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

    cd ${PROJECT_ROOT}/charts/xflow
    helm dependency update

    # values-eks.yaml 사용하여 배포
    helm upgrade --install xflow . \
        -f values-eks.yaml \
        --set global.imageRegistry=${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com \
        --set global.aws.accountId=${ACCOUNT_ID} \
        --set global.aws.region=${REGION} \
        --set global.domain=${DOMAIN} \
        --set mongodb.auth.rootPassword=xflow-mongodb-pass \
        --set monitoring.loki.loki.storage.bucketNames.chunks=${BUCKET_LOKI_LOGS} \
        --set monitoring.loki.loki.storage.bucketNames.ruler=${BUCKET_LOKI_LOGS} \
        --set monitoring.loki.loki.storage.bucketNames.admin=${BUCKET_LOKI_LOGS} \
        --set storage.s3.dataLakeBucket=${BUCKET_DATALAKE} \
        --set storage.s3.airflowLogsBucket=${BUCKET_AIRFLOW_LOGS} \
        -n xflow

    log_info "XFlow deployed!"
}

# ===========================================
# 출력 정보
# ===========================================
print_summary() {
    echo ""
    echo "==========================================="
    echo -e "${GREEN}XFlow Infrastructure Setup Complete!${NC}"
    echo "==========================================="
    echo ""
    echo "AWS Account ID: ${ACCOUNT_ID}"
    echo "Region: ${REGION}"
    echo "EKS Cluster: ${CLUSTER_NAME}"
    echo ""
    echo "ECR Registry: ${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
    echo "  - ${ECR_BACKEND}"
    echo "  - ${ECR_AIRFLOW}"
    echo "  - ${ECR_SPARK}"
    echo ""
    echo "S3 Buckets:"
    echo "  - ${BUCKET_DATALAKE}"
    echo "  - ${BUCKET_AIRFLOW_LOGS}"
    echo "  - ${BUCKET_LOKI_LOGS}"
    echo ""
    echo "==========================================="
    echo -e "${YELLOW}다음 단계:${NC}"
    echo "==========================================="
    echo ""
    echo "1. 도메인 설정:"
    echo "   - Route53에서 도메인 구매 또는 연결"
    echo "   - ACM에서 인증서 발급 (*.your-domain.com)"
    echo ""
    echo "2. values-eks.yaml 업데이트:"
    echo "   - global.domain: your-domain.com"
    echo "   - global.tls.acm.certificateArn: arn:aws:acm:..."
    echo ""
    echo "3. XFlow 배포:"
    echo "   helm upgrade --install xflow ./charts/xflow -f values-eks.yaml -n xflow"
    echo ""
    echo "4. 상태 확인:"
    echo "   kubectl get pods -n xflow"
    echo "   kubectl get ingress -n xflow"
    echo ""
}

# ===========================================
# 메인 실행
# ===========================================
main() {
    echo "==========================================="
    echo "XFlow Infrastructure Setup"
    echo "==========================================="
    echo ""
    echo "AWS Account: ${ACCOUNT_ID}"
    echo "Region: ${REGION}"
    echo "Cluster: ${CLUSTER_NAME}"
    echo ""

    read -p "계속하시겠습니까? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        log_error "Aborted."
        exit 1
    fi

    create_ecr_repos
    create_s3_buckets
    create_eks_cluster
    install_lb_controller
    create_namespaces

    echo ""
    read -p "Docker 이미지를 빌드하고 푸시하시겠습니까? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        build_and_push_images
    fi

    print_summary
}

# 스크립트 실행
main "$@"
