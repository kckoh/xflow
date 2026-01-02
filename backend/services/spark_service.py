"""
Spark Service - Spark Job 실행 관리
역할: spark-master 컨테이너에 명령을 보내서 Job 실행/중지
"""
import docker
import json
import logging

logger = logging.getLogger(__name__)


class SparkService:
    """Spark Job 관리 서비스"""
    
    CLIENT = None
    CONTAINER_NAME = "spark-master"
    
    @classmethod
    def _get_client(cls):
        """Docker 클라이언트 (lazy init)"""
        if cls.CLIENT is None:
            cls.CLIENT = docker.from_env()
        return cls.CLIENT
    
    @classmethod
    def submit_job(cls, config: dict, app_name: str) -> str:
        """
        Spark Streaming Job 실행
        
        Args:
            config: Dataset 설정 (sources, transforms, targets)
            app_name: Spark 애플리케이션 이름
        """
        config_json = json.dumps(config).replace('"', '\\"')
        
        cmd = f'''
            /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --name "{app_name}" \
            --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
            --conf spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2 \
            --jars /opt/spark/jars/extra/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/extra/hadoop-aws-3.3.4.jar \
            /opt/spark/jobs/dataset_cdc_runner.py \
            "{config_json}"
        '''
        
        try:
            client = cls._get_client()
            container = client.containers.get(cls.CONTAINER_NAME)
            
            # detach=True: 백그라운드 실행 (Streaming은 무한 루프이므로)
            # 로그 디버깅을 위해 파일로 리다이렉트
            cmd_with_log = f"/bin/bash -c '{cmd} > /opt/spark/work/spark_job.log 2>&1'"
            container.exec_run(cmd_with_log, detach=True)
            
            logger.info(f"Spark Job 시작: {app_name}")
            return "started"
            
        except docker.errors.NotFound:
            logger.error(f"컨테이너 없음: {cls.CONTAINER_NAME}")
            raise RuntimeError(f"Container {cls.CONTAINER_NAME} not found")
        except Exception as e:
            logger.error(f"Job 시작 실패: {e}")
            raise
    
    @classmethod
    def stop_job(cls, app_name: str) -> bool:
        """Spark Job 중지"""
        try:
            client = cls._get_client()
            container = client.containers.get(cls.CONTAINER_NAME)
            
            # 프로세스 이름으로 kill
            container.exec_run(f"pkill -f '{app_name}'")
            
            logger.info(f"Spark Job 중지 요청: {app_name}")
            return True
            
        except Exception as e:
            logger.warning(f"Job 중지 실패: {e}")
            return False
