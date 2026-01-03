import os
import re
import logging
import hashlib
from datetime import datetime, timedelta
import dateutil.parser
import requests
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Setup Logging
logger = logging.getLogger(__name__)

# Config
# Use the backend service name in Docker Compose usually, or host.docker.internal
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000/api/etl-jobs")
GENERIC_DAG_ID = os.getenv("AIRFLOW_DAG_ID", "etl_job_dag")

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
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': start_date,
        'retries': 0,
    }

    dag = DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule_interval=schedule_interval,
        catchup=False,
        is_paused_upon_creation=False,  # Start unpaused!
        tags=['etl_scheduler', 'dynamic'],
        description=f"Scheduler for ETL Job: {job_name}"
    )

    with dag:
        trigger = TriggerDagRunOperator(
            task_id='trigger_etl_job',
            trigger_dag_id=GENERIC_DAG_ID,
            conf={'job_id': job_id},
            wait_for_completion=False,
            poke_interval=10
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
                job_id = job.get('id')

                # Filter: active status AND schedule exists
                if job.get('status') == 'active' and job.get('schedule'):
                    job_name = job.get("name", f"job_{job_id}")
                    schedule_str = job.get("schedule")
                    ui_params = job.get("ui_params", {})

                    # Generate DAG ID with schedule hash
                    schedule_hash = get_schedule_hash(schedule_str)
                    dag_id = f"scheduler_{job_id}_{schedule_hash}"
                    active_dag_ids.add(dag_id)

                    # Determine start_date
                    start_date = datetime(2024, 1, 1) # Default
                    if ui_params and ui_params.get("startDate"):
                         try:
                            # Parse ISO Format
                            start_date = dateutil.parser.parse(ui_params["startDate"])
                         except:
                            pass

                    schedule_interval = parse_schedule(schedule_str)

                    # Adjust start_date to make first run at the specified time (not after interval)
                    # Airflow executes after the interval, so we subtract it
                    if schedule_interval:
                        if isinstance(schedule_interval, timedelta):
                            # Interval schedules (custom time)
                            start_date = start_date - schedule_interval
                        elif schedule_frequency == 'hourly' and ui_params.get('hourInterval'):
                            # Hourly schedules
                            hours = int(ui_params['hourInterval'])
                            start_date = start_date - timedelta(hours=hours)
                        elif schedule_frequency == 'daily':
                            # Daily schedules
                            start_date = start_date - timedelta(days=1)
                        elif schedule_frequency == 'weekly':
                            # Weekly schedules
                            start_date = start_date - timedelta(weeks=1)
                        elif schedule_frequency == 'monthly':
                            # Monthly schedules (approximate)
                            start_date = start_date - timedelta(days=30)

                    if schedule_interval:
                        dag = create_scheduler_dag(job_id, job_name, schedule_str, schedule_interval, start_date)

                        # Register the DAG to globals so Airflow finds it
                        globals()[dag.dag_id] = dag

                        logger.info(f"Created dynamic DAG: {dag.dag_id} with schedule {schedule_interval}")

            except Exception as e:
                logger.error(f"Failed to create DAG for job {job.get('id')}: {e}")

        # Note: Pausing inactive DAGs is disabled to prevent worker timeout during DAG parsing
        # DAGs can be manually paused/unpaused in the Airflow UI if needed

except Exception as e:
    logger.error(f"Failed to fetch jobs from backend: {e}")
