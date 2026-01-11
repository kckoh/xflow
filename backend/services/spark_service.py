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
    def _build_spark_application(cls, config: dict, app_name: str, script_path: str) -> dict:
        """SparkApplication CRD 생성"""
        config_json = json.dumps(config)

        return {
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
                    # Kafka/AWS JARs are now pre-installed in /opt/spark/jars/
                    # S3 설정
                    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                    "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.WebIdentityTokenCredentialsProvider",
                    "spark.sql.shuffle.partitions": "24",
                    # Spark 3.5.0 V2 Engine stability fix
                    "spark.sql.streaming.v2.enabled": "false",
                    "spark.sql.sources.useV1SourceList": "*",
                    "spark.sql.sources.write.v2.enabled": "false",
                    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                    "spark.kryo.registrationRequired": "false",
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

    @classmethod
    def submit_job(cls, config: dict, app_name: str, runner_type: str = "cdc") -> str:
        """
        Spark Streaming Job 실행
        - Prod: K8s SparkOperator
        - Dev: Spark Standalone REST API
        """
        # Runner Mapping
        # Note: Local path in spark-master container
        runner_map = {
            "cdc": "/opt/spark/jobs/unified_cdc_runner.py",
            "kafka": "/opt/spark/jobs/kafka_source_runner.py",
            "batch": "/opt/spark/jobs/etl_runner.py"
        }
        script_path = runner_map.get(runner_type, runner_map["cdc"])

        if ENVIRONMENT in ["development", "local"]:
            return cls._submit_job_local(config, app_name, script_path)
        else:
            # Production (K8s) existing logic
            try:
                k8s_client = cls._get_k8s_client()

                # SparkApplication 생성
                spark_app = cls._build_spark_application(config, app_name, script_path)
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
    def _submit_job_local(cls, config: dict, app_name: str, script_path: str) -> str:
        """Local Spark Master REST API Submission"""
        import httpx
        import json
        
        url = "http://spark-master:6066/v1/submissions/create"
        
        # Determine script resource path (in spark-master container)
        # Using file:// scheme
        app_resource = f"file://{script_path}"
        
        import base64
        config_str = json.dumps(config)
        config_b64 = base64.b64encode(config_str.encode('utf-8')).decode('utf-8')
        
        payload = {
            "action": "CreateSubmissionRequest",
            "appArgs": [script_path, "dummy_arg_for_spark"],
            "appResource": app_resource,
            "clientSparkVersion": "3.5.0",
            "mainClass": "org.apache.spark.deploy.PythonRunner", # Python requires this main class via REST
            "environmentVariables": {
                "SPARK_ENV_LOADED": "1",
                "JOB_CONFIG": config_b64
            },
            "sparkProperties": {
                "spark.master": "spark://spark-master:7077",
                "spark.app.name": app_name,
                "spark.submit.deployMode": "cluster",
                # Kafka/AWS JARs pre-installed in Docker image
                # S3 Config for LocalStack
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
                "spark.hadoop.fs.s3a.endpoint": "http://localstack-main:4566",
                "spark.hadoop.fs.s3a.access.key": "test",
                "spark.hadoop.fs.s3a.secret.key": "test",
                "spark.hadoop.fs.s3a.path.style.access": "true",
                "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
                # Spark 3.5.0 V2 Engine stability fix
                "spark.sql.streaming.v2.enabled": "false",
                "spark.sql.sources.useV1SourceList": "*",
                "spark.sql.sources.write.v2.enabled": "false",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.kryo.registrationRequired": "false"
            }
        }
        
        try:
            logger.info(f"Submitting to Spark Master: {url}")
            response = httpx.post(url, json=payload, timeout=10.0)
            data = response.json()
            
            if data.get("success"):
                submission_id = data.get("submissionId")
                logger.info(f"Spark Job Submitted Local: ID={submission_id}")
                return submission_id
            else:
                msg = data.get("message", "Unknown error")
                logger.error(f"Spark Submission Failed: {msg}")
                raise Exception(f"Spark Master rejected submission: {msg}")
                
        except Exception as e:
            logger.error(f"Failed to submit local job: {e}")
            raise

    @classmethod
    def stop_job(cls, app_name: str, submission_id: str = None) -> bool:
        """Spark Job 중지"""
        if ENVIRONMENT in ["development", "local"]:
            if not submission_id:
                logger.warning("Local mode requires submission_id to stop job")
                return False
            return cls._stop_job_local(submission_id)
        else:
            # Production (K8s) existing logic
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
    def _stop_job_local(cls, submission_id: str) -> bool:
        """Local Spark Master REST API Kill"""
        import httpx
        url = f"http://spark-master:6066/v1/submissions/kill/{submission_id}"
        
        try:
            logger.info(f"Killing Spark Job: {url}")
            response = httpx.post(url, timeout=10.0)
            data = response.json()
            
            if data.get("success"):
                 logger.info(f"Spark Job Killed Local: ID={submission_id}")
                 return True
            else:
                 logger.error(f"Spark Kill Failed: {data.get('message')}")
                 return False
        except Exception as e:
            logger.error(f"Failed to kill local job: {e}")
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
