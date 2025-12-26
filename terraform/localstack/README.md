# LocalStack Terraform Configuration

이 디렉토리는 LocalStack에서 AWS 리소스를 관리하기 위한 Terraform 설정을 포함합니다.

## 사전 요구사항

1. Terraform 설치
```bash
brew install terraform
```

2. LocalStack 실행 중
```bash
docker compose up localstack -d
```

## 사용 방법

### 1. 환경 변수 설정

**중요**: 처음 사용하는 경우 terraform.tfvars 파일을 생성해야 합니다.

```bash
cd terraform/localstack

# 예제 파일을 복사하여 실제 설정 파일 생성
cp terraform.tfvars.example terraform.tfvars
```

`terraform.tfvars` 파일을 열어서 실제 비밀번호를 입력하세요:

```hcl
# Database Credentials
rds_password        = "your_secure_rds_password"
documentdb_password = "your_secure_documentdb_password"
```

**주의**: `terraform.tfvars` 파일은 .gitignore에 포함되어 있으므로 Git에 커밋되지 않습니다. 비밀번호를 안전하게 관리하세요.

### 2. 초기화

```bash
terraform init
```

### 3. 계획 확인

```bash
terraform plan
```

### 4. 리소스 생성

```bash
terraform apply
```

### 5. 리소스 확인

```bash
# VPC 확인
aws ec2 describe-vpcs --endpoint-url=http://localhost:4566

# Subnet 확인
aws ec2 describe-subnets --endpoint-url=http://localhost:4566

# RDS 확인
aws rds describe-db-instances --endpoint-url=http://localhost:4566

# S3 버킷 확인
aws s3 ls --endpoint-url=http://localhost:4566

# DocumentDB 확인 (LocalStack Pro만 지원)
aws docdb describe-db-clusters --endpoint-url=http://localhost:4566
```

### 6. 리소스 삭제

```bash
terraform destroy
```

## 구조

```
terraform/localstack/
├── main.tf          # Provider 설정
├── variables.tf     # 변수 정의
├── vpc.tf           # VPC, Subnet, IGW, Security Group
├── rds.tf           # RDS PostgreSQL
├── documentdb.tf    # DocumentDB Cluster
├── s3.tf            # S3 Buckets
├── outputs.tf       # Output 값
├── terraform.tfvars.example  # 환경 변수 예제 (Git 커밋 가능)
└── README.md        # 이 파일

# Git에서 제외되는 파일
terraform.tfvars     # 실제 비밀번호 (절대 커밋 금지)
```

## 생성되는 리소스

### 네트워크
- VPC (10.0.0.0/16)
- Public Subnet (10.0.1.0/24) - ap-northeast-2a
- Private Subnet 1 (10.0.2.0/24) - ap-northeast-2a
- Private Subnet 2 (10.0.3.0/24) - ap-northeast-2b
- Internet Gateway
- Route Table (public)
- Security Group (모든 트래픽 허용 - 개발 환경)

### 데이터베이스
- **RDS PostgreSQL** (db.t3.micro)
  - Engine: PostgreSQL 14.7
  - Private Subnet에 배치
  - 다중 AZ 서브넷 그룹

- **DocumentDB Cluster** (db.t3.medium)
  - MongoDB 호환 문서 데이터베이스
  - Private Subnet에 배치
  - **참고**: LocalStack에서는 DocumentDB가 완전히 지원되지 않을 수 있음

### 스토리지
- **S3 Buckets**
  - xflow-data-lake: 데이터 레이크 메인 버킷 (버저닝 활성화)
  - xflow-raw-data: 원본 데이터 저장
  - xflow-processed-data: 처리된 데이터 저장

## 커스터마이징

`variables.tf` 파일을 수정하거나 `terraform.tfvars` 파일을 생성하여 값을 변경할 수 있습니다:

```hcl
# terraform.tfvars
vpc_cidr           = "10.1.0.0/16"
public_subnet_cidr = "10.1.1.0/24"
private_subnet_cidr = "10.1.2.0/24"
project_name       = "my-project"
```

## Outputs 확인

```bash
terraform output
```

## 팁

- LocalStack은 무료 버전에서 일부 기능만 지원합니다
- Pro 버전을 사용 중이라면 더 많은 AWS 서비스를 사용할 수 있습니다
- 실제 AWS와 동일한 Terraform 코드를 사용할 수 있습니다 (provider endpoint만 변경)
