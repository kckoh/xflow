"""
Spark Service - Spark Job 실행 관리
역할: Spark Operator를 통해 K8s에서 CDC Streaming Job 실행/중지
"""
import os
import json
import logging

logger = logging.getLogger(__name__)

ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
SPARK_IMAGE = os.getenv("SPARK_IMAGE", "134059028370.dkr.ecr.ap-northeast-2.amazonaws.com/xflow-spark:latest")
SPARK_NAMESPACE = os.getenv("SPARK_NAMESPACE", "spark-jobs")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "xflow-kafka-kafka-bootstrap.kafka:9092")


class SparkService:
    """Spark Job 관리 서비스 (K8s Spark Operator)"""

    K8S_CLIENT = None

    @classmethod
    def _get_k8s_client(cls):
        """Kubernetes 클라이언트 (lazy init)"""
        if cls.K8S_CLIENT is None:
            try:
                from kubernetes import client, config
                # K8s 환경에서는 in-cluster config
                if ENVIRONMENT == "production":
                    config.load_incluster_config()
                else:
                    config.load_kube_config()
                cls.K8S_CLIENT = client.CustomObjectsApi()
            except Exception as e:
                logger.error(f"K8s 클라이언트 초기화 실패: {e}")
                raise
        return cls.K8S_CLIENT

    @classmethod
    def _build_spark_application(
        cls,
        config: dict,
        app_name: str,
        script_path: str,
        node_selector: dict | None = None,
        tolerations: list | None = None,
    ) -> dict:
        """SparkApplication CRD 생성"""
        config_json = json.dumps(config)

        spark_app = {
            "apiVersion": "sparkoperator.k8s.io/v1beta2",
            "kind": "SparkApplication",
            "metadata": {
                "name": app_name.lower().replace("_", "-")[:63],  # K8s name 제약
                "namespace": SPARK_NAMESPACE,
            },
            "spec": {
                "type": "Python",
                "pythonVersion": "3",
                "mode": "cluster",
                "image": SPARK_IMAGE,
                "imagePullPolicy": "Always",
                "mainApplicationFile": f"local://{script_path}",
                "arguments": [config_json],
                "sparkVersion": "3.5.0",
                "sparkConf": {
                    # Kafka 설정
                    "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                    # Ivy 캐시 경로 (컨테이너 내 쓰기 가능한 경로 사용)
                    "spark.jars.ivy": "/tmp/.ivy2",
                    "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2",
                    "spark.executor.extraJavaOptions": "-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2",
                    # S3 설정
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
                    "spark.sql.shuffle.partitions": "24",
                },
                "driver": {
                    "cores": 1,
                    "memory": "2g",
                    "serviceAccount": "spark-sa",
                    "env": [
                        {"name": "AWS_REGION", "value": "ap-northeast-2"},
                        {"name": "KAFKA_BOOTSTRAP_SERVERS", "value": KAFKA_BOOTSTRAP_SERVERS},
                        {"name": "ENVIRONMENT", "value": "production"},
                    ],
                },
                "executor": {
                    "cores": 2,
                    "instances": 1,
                    "memory": "4g",
                    "env": [
                        {"name": "AWS_REGION", "value": "ap-northeast-2"},
                        {"name": "KAFKA_BOOTSTRAP_SERVERS", "value": KAFKA_BOOTSTRAP_SERVERS},
                        {"name": "ENVIRONMENT", "value": "production"},
                    ],
                },
                "restartPolicy": {
                    "type": "Always",  # Streaming은 항상 재시작
                },
            },
        }

        if node_selector:
            spark_app["spec"]["driver"]["nodeSelector"] = node_selector
            spark_app["spec"]["executor"]["nodeSelector"] = node_selector

        if tolerations:
            spark_app["spec"]["driver"]["tolerations"] = tolerations
            spark_app["spec"]["executor"]["tolerations"] = tolerations

        return spark_app

    @classmethod
    def submit_job(
        cls,
        config: dict,
        app_name: str,
        script_path: str = "/opt/spark/jobs/kafka_streaming_runner.py",
        node_selector: dict | None = None,
        tolerations: list | None = None,
    ) -> str:
        """
        Spark Streaming Job 실행 (K8s SparkApplication 생성)

        Args:
            config: Dataset 설정 (sources, transforms, targets)
            app_name: Spark 애플리케이션 이름
            script_path: 실행할 파이썬 스크립트 경로
        """
        try:
            k8s_client = cls._get_k8s_client()

            # SparkApplication 생성
            spark_app = cls._build_spark_application(
                config,
                app_name,
                script_path,
                node_selector=node_selector,
                tolerations=tolerations,
            )
            app_name_normalized = spark_app["metadata"]["name"]

            # 기존 앱 삭제 (있으면)
            try:
                k8s_client.delete_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=SPARK_NAMESPACE,
                    plural="sparkapplications",
                    name=app_name_normalized,
                )
                logger.info(f"기존 SparkApplication 삭제: {app_name_normalized}")
                import time
                time.sleep(2)  # 삭제 대기
            except Exception:
                pass  # 없으면 무시

            # 새 앱 생성
            k8s_client.create_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=SPARK_NAMESPACE,
                plural="sparkapplications",
                body=spark_app,
            )

            logger.info(f"Spark Job 시작: {app_name_normalized}")
            return "started"

        except Exception as e:
            logger.error(f"Job 시작 실패: {e}")
            raise

    @classmethod
    def stop_job(cls, app_name: str) -> bool:
        """Spark Job 중지 (SparkApplication 삭제)"""
        try:
            k8s_client = cls._get_k8s_client()
            app_name_normalized = app_name.lower().replace("_", "-")[:63]

            k8s_client.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=SPARK_NAMESPACE,
                plural="sparkapplications",
                name=app_name_normalized,
            )

            logger.info(f"Spark Job 중지: {app_name_normalized}")
            return True

        except Exception as e:
            logger.warning(f"Job 중지 실패 (이미 없을 수 있음): {e}")
            return False

    @classmethod
    def get_job_status(cls, app_name: str) -> dict:
        """Spark Job 상태 조회"""
        try:
            k8s_client = cls._get_k8s_client()
            app_name_normalized = app_name.lower().replace("_", "-")[:63]

            result = k8s_client.get_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=SPARK_NAMESPACE,
                plural="sparkapplications",
                name=app_name_normalized,
            )

            status = result.get("status", {})
            return {
                "name": app_name_normalized,
                "state": status.get("applicationState", {}).get("state", "UNKNOWN"),
                "driver_info": status.get("driverInfo", {}),
            }

        except Exception as e:
            logger.warning(f"Job 상태 조회 실패: {e}")
            return {"name": app_name, "state": "NOT_FOUND", "error": str(e)}
