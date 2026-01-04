# Monitoring Stack

Grafana + Loki + Promtail 기반 로그 모니터링 시스템

## 구성요소

| 컴포넌트 | 역할 |
|----------|------|
| **Loki** | 로그 저장 (S3 백엔드) |
| **Promtail** | Pod 로그 수집 |
| **Grafana** | 로그 조회 UI |

## 설치

```bash
cd k8s/monitoring
chmod +x install.sh
./install.sh
```

## 접속 정보

- **URL**: https://grafana.xflows.net
- **ID**: admin
- **PW**: xflow2024 (변경 필요)

## 로그 조회 방법

1. Grafana 접속
2. 좌측 메뉴 → **Explore**
3. 상단에서 **Loki** 선택
4. 쿼리 입력 후 **Run query**

### 쿼리 예시

```logql
# backend 로그
{namespace="xflow", pod=~"backend.*"}

# airflow 로그
{namespace="airflow"}

# 에러만
{namespace="xflow"} |= "ERROR"

# 특정 키워드 검색
{namespace="xflow"} |~ "exception|error|fail"
```

## AWS 리소스

- **S3 버킷**: xflow-loki-logs
- **IAM Role (Loki)**: xflow-loki-s3
- **IAM Role (Grafana)**: xflow-grafana-cloudwatch

## 삭제

```bash
helm uninstall grafana -n monitoring
helm uninstall promtail -n monitoring
helm uninstall loki -n monitoring
kubectl delete namespace monitoring
```
