import hashlib
import logging
import os
import re
from datetime import datetime, timedelta

import dateutil.parser
import requests
from airflow.operators.python import PythonOperator

from airflow import DAG

# Setup Logging
logger = logging.getLogger(__name__)

# Config
# Auto-detect environment: Kubernetes or Docker Compose
IS_K8S = os.getenv("KUBERNETES_SERVICE_HOST") is not None

if IS_K8S:
    # Kubernetes environment - use full service DNS with xflow namespace (port 80)
    BACKEND_URL = os.getenv("BACKEND_URL", "http://backend.xflow.svc.cluster.local/api/datasets")
    BACKEND_API_BASE_URL = os.getenv("BACKEND_API_BASE_URL", "http://backend.xflow.svc.cluster.local/api")
else:
    # Docker Compose environment - use simple service name
    BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000/api/datasets")
    BACKEND_API_BASE_URL = os.getenv("BACKEND_API_BASE_URL", "http://backend:8000/api")

GENERIC_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "dataset_dag_k8s")


def get_schedule_hash(schedule_str):
    """Get short hash of schedule string for versioning DAG IDs"""
    if not schedule_str:
        return "none"
    return hashlib.md5(schedule_str.encode()).hexdigest()[:8]


def parse_schedule(schedule_str):
    """Parse custom schedule string to Airflow schedule_interval"""
    if not schedule_str:
        return None

    # Handle Interval Format: @interval:P<days>DT<hours>H<minutes>M
    if schedule_str.startswith("@interval:"):
        # Try full format with minutes
        match = re.search(r"P(\d+)DT(\d+)H(\d+)M", schedule_str)
        if match:
            d, h, m = map(int, match.groups())
            if d == 0 and h == 0 and m == 0:
                pass
            else:
                return timedelta(days=d, hours=h, minutes=m)

        # Fallback to old format (just in case)
        match_old = re.search(r"P(\d+)DT(\d+)H", schedule_str)
        if match_old:
            d, h = map(int, match_old.groups())
            return timedelta(days=d, hours=h)

    return schedule_str


def create_scheduler_dag(job_id, job_name, schedule_str, schedule_interval, start_date):
    """Create a lightweight DAG that triggers the main ETL DAG"""
    # Include schedule hash in DAG ID so changing schedule creates a new DAG
    schedule_hash = get_schedule_hash(schedule_str)
    dag_id = f"scheduler_{job_id}_{schedule_hash}"

    default_args = {
        "owner": "airflow",
        "depends_on_past": False,
        "start_date": start_date,
        "retries": 0,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False,
        is_paused_upon_creation=False,  # Start unpaused!
        tags=["etl_scheduler", "dynamic"],
        description=f"Scheduler for ETL Job: {job_name}",
    )

    with dag:
        def trigger_via_backend(job_id: str):
            url = f"{BACKEND_API_BASE_URL}/datasets/{job_id}/run"
            response = requests.post(url, timeout=10)
            if response.status_code not in (200, 201):
                raise RuntimeError(
                    f"Failed to trigger job via backend: {response.status_code} {response.text}"
                )

        trigger = PythonOperator(
            task_id="trigger_etl_job",
            python_callable=trigger_via_backend,
            op_kwargs={"job_id": job_id},
        )

    return dag


# Main Logic to Load DAGs
try:
    # Set a short timeout to prevent blocking the scheduler for too long
    response = requests.get(BACKEND_URL, timeout=5)

    if response.status_code == 200:
        jobs = response.json()

        # Track which DAGs should be active
        active_dag_ids = set()

        for job in jobs:
            try:
                job_id = job.get("id")

                # Filter: active or scheduled status AND schedule exists
                if job.get("status") in ["active", "scheduled"] and job.get("schedule"):
                    job_name = job.get("name", f"job_{job_id}")
                    schedule_str = job.get("schedule")
                    ui_params = job.get("ui_params", {})

                    # Generate DAG ID with schedule hash
                    schedule_hash = get_schedule_hash(schedule_str)
                    dag_id = f"scheduler_{job_id}_{schedule_hash}"
                    active_dag_ids.add(dag_id)

                    # Determine start_date
                    start_date = datetime(2024, 1, 1)  # Default
                    if ui_params and ui_params.get("startDate"):
                        try:
                            # Parse ISO Format
                            start_date = dateutil.parser.parse(ui_params["startDate"])
                        except:
                            pass

                    schedule_interval = parse_schedule(schedule_str)

                    # Adjust start_date to make first run at the specified time (not after interval)
                    # Airflow executes after the interval, so we subtract it
                    if schedule_interval and isinstance(schedule_interval, timedelta):
                        # Interval schedules (custom time)
                        start_date = start_date - schedule_interval

                    if schedule_interval:
                        dag = create_scheduler_dag(
                            job_id,
                            job_name,
                            schedule_str,
                            schedule_interval,
                            start_date,
                        )

                        # Register the DAG to globals so Airflow finds it
                        globals()[dag.dag_id] = dag

                        logger.info(
                            f"Created dynamic DAG: {dag.dag_id} with schedule {schedule_interval}"
                        )

            except Exception as e:
                logger.error(f"Failed to create DAG for job {job.get('id')}: {e}")

        # Note: Pausing inactive DAGs is disabled to prevent worker timeout during DAG parsing
        # DAGs can be manually paused/unpaused in the Airflow UI if needed

except Exception as e:
    logger.error(f"Failed to fetch jobs from backend: {e}")
