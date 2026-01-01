#!/bin/bash

# Spark JAR Download Script
# Downloads necessary JAR files for MongoDB and AWS connectivity

set -e

JAR_DIR="$(dirname "$0")/../spark/jars"
mkdir -p "$JAR_DIR"
cd "$JAR_DIR"

echo "📦 Downloading Spark JARs..."

# MongoDB Spark Connector
if [ ! -f "mongo-spark-connector_2.12-10.3.0.jar" ]; then
    echo "  → mongo-spark-connector..."
    curl -sLO https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar
fi

# MongoDB BSON
if [ ! -f "bson-4.11.1.jar" ]; then
    echo "  → bson..."
    curl -sLO https://repo1.maven.org/maven2/org/mongodb/bson/4.11.1/bson-4.11.1.jar
fi

# MongoDB Driver Core
if [ ! -f "mongodb-driver-core-4.11.1.jar" ]; then
    echo "  → mongodb-driver-core..."
    curl -sLO https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/4.11.1/mongodb-driver-core-4.11.1.jar
fi

# MongoDB Driver Sync
if [ ! -f "mongodb-driver-sync-4.11.1.jar" ]; then
    echo "  → mongodb-driver-sync..."
    curl -sLO https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/4.11.1/mongodb-driver-sync-4.11.1.jar
fi

# PostgreSQL JDBC (if not exists)
if [ ! -f "postgresql-42.7.4.jar" ]; then
    echo "  → postgresql..."
    curl -sLO https://jdbc.postgresql.org/download/postgresql-42.7.4.jar
fi

# AWS SDK Bundle (if not exists)
if [ ! -f "aws-java-sdk-bundle-1.12.262.jar" ]; then
    echo "  → aws-java-sdk-bundle..."
    curl -sLO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
fi

# Hadoop AWS (if not exists)
if [ ! -f "hadoop-aws-3.3.4.jar" ]; then
    echo "  → hadoop-aws..."
    curl -sLO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
fi

echo ""
echo "✅ All JARs downloaded!"
ls -la "$JAR_DIR"
