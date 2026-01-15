import json
import logging
import os
import shlex

import docker

from services.spark_service import SparkService

logger = logging.getLogger(__name__)

ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
SPARK_MASTER_CONTAINER = os.getenv("SPARK_MASTER_CONTAINER", "spark-master")
SPARK_MASTER_URL = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
SPARK_SUBMIT_PACKAGES = os.getenv(
    "SPARK_SUBMIT_PACKAGES",
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.2.0",
)


class KafkaStreamingService:
    @classmethod
    def submit_job(cls, config: dict, app_name: str) -> str:
        if ENVIRONMENT == "local":
            return cls._submit_local(config, app_name)

        # Default to Spark Operator path in production
        return SparkService.submit_job(
            config,
            app_name,
            script_path="/opt/spark/jobs/kafka_streaming_runner.py",
        )

    @classmethod
    def stop_job(cls, app_name: str) -> bool:
        if ENVIRONMENT == "local":
            return cls._stop_local(app_name)

        return SparkService.stop_job(app_name)

    @classmethod
    def get_job_status(cls, app_name: str) -> dict:
        if ENVIRONMENT == "local":
            return cls._get_status_local(app_name)

        return SparkService.get_job_status(app_name)

    @classmethod
    def _submit_local(cls, config: dict, app_name: str) -> str:
        client = docker.from_env()
        container = client.containers.get(SPARK_MASTER_CONTAINER)

        config_json = json.dumps(config)
        config_arg = shlex.quote(config_json)
        pid_file = f"/tmp/{app_name}.pid"
        log_file = f"/tmp/{app_name}.log"

        cmd = (
            "bash -lc "
            + shlex.quote(
                " ".join(
                    [
                        "mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars "
                        "/tmp/.ivy2/cache/org.apache.kafka "
                        "/tmp/.ivy2/cache/io.delta "
                        "/tmp/.ivy2/cache/org.apache.hadoop "
                        "/tmp/.ivy2/cache/org.apache.spark &&",
                        "SPARK_SUBMIT_OPTS='-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2'",
                        "nohup",
                        "/opt/spark/bin/spark-submit",
                        "--master",
                        SPARK_MASTER_URL,
                        "--name",
                        app_name,
                        "--packages",
                        SPARK_SUBMIT_PACKAGES,
                        "--conf",
                        "spark.jars.ivy=/tmp/.ivy2",
                        "--conf",
                        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                        "--conf",
                        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                        "/opt/spark/jobs/kafka_streaming_runner.py",
                        config_arg,
                        f"> {log_file} 2>&1 & echo $! > {pid_file}",
                    ]
                )
            )
        )

        result = container.exec_run(cmd, tty=False, stdout=True, stderr=True)
        if result.exit_code != 0:
            raise RuntimeError(result.output.decode("utf-8", errors="ignore"))

        logger.info("Kafka streaming job started: %s", app_name)
        return "started"

    @classmethod
    def _stop_local(cls, app_name: str) -> bool:
        client = docker.from_env()
        container = client.containers.get(SPARK_MASTER_CONTAINER)
        pid_file = f"/tmp/{app_name}.pid"

        cmd = (
            "bash -lc "
            + shlex.quote(
                "set +e; "
                f"pids=\"\"; "
                f"if [ -f {pid_file} ]; then "
                f"  pid=$(cat {pid_file} || true); "
                f"  if [ -n \"$pid\" ]; then pids=\"$pids $pid\"; fi; "
                f"fi; "
                f"pids=\"$pids $(ps -ef | grep -E 'kafka_streaming_runner.py.*{app_name}' | grep -v grep | awk '{{print $2}}')\"; "
                f"pids=\"$pids $(ps -ef | grep -E 'SparkSubmit.*--name {app_name}' | grep -v grep | awk '{{print $2}}')\"; "
                f"for p in $pids; do "
                f"  if [ -n \"$p\" ]; then "
                f"    kill -TERM $p 2>/dev/null || true; "
                f"    pkill -TERM -P $p 2>/dev/null || true; "
                f"  fi; "
                f"done; "
                f"sleep 2; "
                f"for p in $pids; do "
                f"  if [ -n \"$p\" ]; then "
                f"    kill -KILL $p 2>/dev/null || true; "
                f"    pkill -KILL -P $p 2>/dev/null || true; "
                f"  fi; "
                f"done; "
                f"rm -f {pid_file}; "
                f"exit 0"
            )
        )

        result = container.exec_run(cmd, tty=False, stdout=True, stderr=True)
        if result.exit_code != 0:
            logger.warning("Kafka streaming stop returned non-zero exit: %s", result.output)
            return False

        logger.info("Kafka streaming job stopped: %s", app_name)
        return True

    @classmethod
    def _get_status_local(cls, app_name: str) -> dict:
        client = docker.from_env()
        container = client.containers.get(SPARK_MASTER_CONTAINER)
        pid_file = f"/tmp/{app_name}.pid"

        cmd = (
            "bash -lc "
            + shlex.quote(
                " ".join(
                    [
                        f"if [ -f {pid_file} ]; then",
                        "pid=$(cat",
                        pid_file,
                        ");",
                        "if ps -p $pid > /dev/null 2>&1; then",
                        "echo RUNNING;",
                        "else echo STOPPED; fi;",
                        "else",
                        f"if ps -ef | grep -E '[k]afka_streaming_runner.py.*{app_name}' > /dev/null; then",
                        "echo RUNNING;",
                        "else echo STOPPED; fi;",
                        "fi",
                    ]
                )
            )
        )

        result = container.exec_run(cmd, tty=False, stdout=True, stderr=True)
        if result.exit_code != 0:
            logger.warning("Kafka streaming status check failed: %s", result.output)
            return {"name": app_name, "state": "UNKNOWN"}

        state = result.output.decode("utf-8", errors="ignore").strip() or "UNKNOWN"
        return {"name": app_name, "state": state}
