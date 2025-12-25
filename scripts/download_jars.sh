#!/bin/bash
# Trino/Hive용 JAR 파일 다운로드 스크립트
#
# 사용법: ./scripts/download_jars.sh

set -e  # 오류 발생 시 중단

echo "Trino/Hive JAR 파일 다운로드"

# 프로젝트 루트로 이동
cd "$(dirname "$0")/.."

# 디렉토리 생성
echo "📁 디렉토리 생성 중..."
mkdir -p trino/jars
mkdir -p hive/lib

echo ""
echo "JAR 파일 다운로드 중..."

# Hadoop AWS (940KB)
echo "  - hadoop-aws-3.3.4.jar 다운로드 중..."
curl -L -o trino/jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# AWS SDK Bundle (268MB - 시간이 걸릴 수 있음)
echo "  - aws-java-sdk-bundle-1.12.262.jar 다운로드 중 (268MB, 시간이 걸릴 수 있습니다)..."
curl -L -o trino/jars/aws-java-sdk-bundle-1.12.262.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar

echo ""
echo "📋 Hive lib에 복사 중..."
cp trino/jars/hadoop-aws-3.3.4.jar hive/lib/
cp trino/jars/aws-java-sdk-bundle-1.12.262.jar hive/lib/

echo ""
echo "JAR 파일 다운로드 완료!"
echo ""
echo "다운로드된 파일:"
ls -lh trino/jars/
echo ""
echo "다음 단계:"
echo "  1. Docker 컨테이너가 실행 중이면 재시작하세요"
echo "     docker compose restart trino hive-metastore"
echo "  2. 또는 컨테이너에 JAR 복사:"
echo "     docker cp trino/jars/hadoop-aws-3.3.4.jar trino:/usr/lib/trino/plugin/hive/"
echo "     docker cp trino/jars/aws-java-sdk-bundle-1.12.262.jar trino:/usr/lib/trino/plugin/hive/"
echo "     docker cp hive/lib/hadoop-aws-3.3.4.jar hive-metastore:/opt/hive/lib/"
echo "     docker cp hive/lib/aws-java-sdk-bundle-1.12.262.jar hive-metastore:/opt/hive/lib/"
