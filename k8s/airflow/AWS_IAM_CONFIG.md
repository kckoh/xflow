# Airflow AWS IAM Configuration

이 문서는 Airflow가 S3 로그에 접근하기 위해 필요한 AWS IAM 설정을 기록합니다.
현재 Terraform/IaC로 관리되지 않으므로, 클러스터 재생성 시 수동 설정이 필요합니다.

## 1. IAM Role

**Role Name:** `eksctl-xflow-cluster-addon-iamserviceaccount--Role1-bLIJeAhC8jWI`

**ARN:** `arn:aws:iam::134059028370:role/eksctl-xflow-cluster-addon-iamserviceaccount--Role1-bLIJeAhC8jWI`

### Trust Policy

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::134059028370:oidc-provider/oidc.eks.ap-northeast-2.amazonaws.com/id/340282C2D080A38247627BE7052E1BE5"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringLike": {
          "oidc.eks.ap-northeast-2.amazonaws.com/id/340282C2D080A38247627BE7052E1BE5:sub": "system:serviceaccount:airflow:airflow-*",
          "oidc.eks.ap-northeast-2.amazonaws.com/id/340282C2D080A38247627BE7052E1BE5:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
```

**주의:** `airflow-*` 패턴으로 airflow namespace의 모든 airflow-로 시작하는 ServiceAccount 허용

## 2. IAM Policy

**Policy Name:** `AirflowS3LogsPolicy`

**Policy ARN:** `arn:aws:iam::134059028370:policy/AirflowS3LogsPolicy`

### Policy Document

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:HeadObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::xflow-airflow-logs",
        "arn:aws:s3:::xflow-airflow-logs/*"
      ]
    }
  ]
}
```

## 3. S3 Bucket

**Bucket Name:** `xflow-airflow-logs`

**Region:** `ap-northeast-2`

**용도:** Airflow remote logging

## 4. Airflow Variables (Airflow DB에 저장됨)

Helm upgrade 후 다시 설정 필요:

```bash
kubectl exec -n airflow deploy/airflow-scheduler -- airflow variables set MONGODB_URL "mongodb://root:example@mongodb.default:27017"
kubectl exec -n airflow deploy/airflow-scheduler -- airflow variables set MONGODB_DATABASE "xflow"
kubectl exec -n airflow deploy/airflow-scheduler -- airflow variables set ENVIRONMENT "production"
```

## 5. Airflow Connections (Airflow DB에 저장됨)

```bash
kubectl exec -n airflow deploy/airflow-scheduler -- airflow connections add aws_default \
  --conn-type aws \
  --conn-extra '{"region_name": "ap-northeast-2"}'
```

## CLI Commands for Recreation

### Trust Policy 업데이트
```bash
cat > /tmp/trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Federated": "arn:aws:iam::134059028370:oidc-provider/oidc.eks.ap-northeast-2.amazonaws.com/id/340282C2D080A38247627BE7052E1BE5"
      },
      "Action": "sts:AssumeRoleWithWebIdentity",
      "Condition": {
        "StringLike": {
          "oidc.eks.ap-northeast-2.amazonaws.com/id/340282C2D080A38247627BE7052E1BE5:sub": "system:serviceaccount:airflow:airflow-*",
          "oidc.eks.ap-northeast-2.amazonaws.com/id/340282C2D080A38247627BE7052E1BE5:aud": "sts.amazonaws.com"
        }
      }
    }
  ]
}
EOF

aws iam update-assume-role-policy \
  --role-name eksctl-xflow-cluster-addon-iamserviceaccount--Role1-bLIJeAhC8jWI \
  --policy-document file:///tmp/trust-policy.json
```

### IAM Policy 업데이트
```bash
cat > /tmp/s3-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:HeadObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::xflow-airflow-logs",
        "arn:aws:s3:::xflow-airflow-logs/*"
      ]
    }
  ]
}
EOF

aws iam create-policy-version \
  --policy-arn arn:aws:iam::134059028370:policy/AirflowS3LogsPolicy \
  --policy-document file:///tmp/s3-policy.json \
  --set-as-default
```
