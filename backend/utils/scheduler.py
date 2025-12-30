"""
ETL Job Scheduler Service
Manages scheduled execution of ETL jobs using APScheduler and Airflow API
"""
import os
import logging
from datetime import datetime
from typing import Optional

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz

from models import ETLJob

logger = logging.getLogger(__name__)

# Global scheduler instance
scheduler: Optional[AsyncIOScheduler] = None

# Airflow API configuration
AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://airflow-webserver:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")


async def trigger_airflow_dag(job_id: str, job_name: str):
    """
    Trigger Airflow DAG for an ETL job via REST API

    Args:
        job_id: MongoDB ETL job ID
        job_name: ETL job name (for logging)
    """
    dag_id = "etl_job_dag"  # The main ETL DAG
    dag_run_id = f"scheduled_{job_id}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    url = f"{AIRFLOW_URL}/api/v1/dags/{dag_id}/dagRuns"

    payload = {
        "dag_run_id": dag_run_id,
        "conf": {
            "job_id": job_id
        }
    }

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                url,
                json=payload,
                auth=(AIRFLOW_USERNAME, AIRFLOW_PASSWORD),
                timeout=30.0
            )

            if response.status_code in [200, 201]:
                logger.info(f"✅ Triggered Airflow DAG for job '{job_name}' (ID: {job_id}), run_id: {dag_run_id}")
                return True
            else:
                logger.error(f"❌ Failed to trigger Airflow DAG for job '{job_name}': {response.status_code} - {response.text}")
                return False

    except Exception as e:
        logger.error(f"❌ Error triggering Airflow DAG for job '{job_name}': {str(e)}")
        return False


def get_scheduler() -> AsyncIOScheduler:
    """Get the global scheduler instance"""
    global scheduler
    if scheduler is None:
        raise RuntimeError("Scheduler not initialized. Call initialize_scheduler() first.")
    return scheduler


async def initialize_scheduler():
    """Initialize APScheduler and load all active schedules from MongoDB"""
    global scheduler

    if scheduler is not None:
        logger.warning("Scheduler already initialized")
        return scheduler

    # Create scheduler with AsyncIO
    scheduler = AsyncIOScheduler(timezone=pytz.UTC)
    scheduler.start()
    logger.info("🚀 APScheduler started")

    # Load all scheduled jobs from MongoDB
    await load_scheduled_jobs()

    return scheduler


async def load_scheduled_jobs():
    """Load all active schedules from MongoDB and register with APScheduler"""
    try:
        # Find all jobs with schedules enabled
        jobs = await ETLJob.find({"schedule_enabled": True}).to_list()

        logger.info(f"📋 Loading {len(jobs)} scheduled jobs...")

        for job in jobs:
            if job.schedule_cron:
                await add_job_to_scheduler(
                    job_id=str(job.id),
                    job_name=job.name,
                    cron=job.schedule_cron,
                    timezone=job.schedule_timezone
                )

        logger.info(f"✅ Loaded {len(jobs)} scheduled jobs")

    except Exception as e:
        logger.error(f"❌ Error loading scheduled jobs: {str(e)}")


async def add_job_to_scheduler(
    job_id: str,
    job_name: str,
    cron: str,
    timezone: str = "UTC"
):
    """
    Add or update a job in the scheduler

    Args:
        job_id: MongoDB ETL job ID
        job_name: ETL job name
        cron: Cron expression (e.g., "0 0 * * *")
        timezone: Timezone for schedule execution
    """
    global scheduler

    if scheduler is None:
        raise RuntimeError("Scheduler not initialized")

    # Create unique job ID for APScheduler
    apscheduler_job_id = f"etl_job_{job_id}"

    try:
        # Parse cron expression
        trigger = CronTrigger.from_crontab(cron, timezone=pytz.timezone(timezone))

        # Remove existing job if it exists
        if scheduler.get_job(apscheduler_job_id):
            scheduler.remove_job(apscheduler_job_id)
            logger.info(f"🔄 Updating schedule for job '{job_name}' (ID: {job_id})")
        else:
            logger.info(f"➕ Adding schedule for job '{job_name}' (ID: {job_id})")

        # Add job to scheduler
        scheduler.add_job(
            trigger_airflow_dag,
            trigger=trigger,
            id=apscheduler_job_id,
            name=f"ETL: {job_name}",
            args=[job_id, job_name],
            replace_existing=True
        )

        # Get next run time
        job_obj = scheduler.get_job(apscheduler_job_id)
        next_run = job_obj.next_run_time if job_obj else None

        logger.info(f"✅ Scheduled '{job_name}' with cron '{cron}' (next run: {next_run})")
        return True

    except Exception as e:
        logger.error(f"❌ Error adding job to scheduler: {str(e)}")
        return False


async def remove_job_from_scheduler(job_id: str, job_name: str):
    """
    Remove a job from the scheduler

    Args:
        job_id: MongoDB ETL job ID
        job_name: ETL job name (for logging)
    """
    global scheduler

    if scheduler is None:
        raise RuntimeError("Scheduler not initialized")

    apscheduler_job_id = f"etl_job_{job_id}"

    try:
        if scheduler.get_job(apscheduler_job_id):
            scheduler.remove_job(apscheduler_job_id)
            logger.info(f"🗑️ Removed schedule for job '{job_name}' (ID: {job_id})")
            return True
        else:
            logger.warning(f"⚠️ Job '{job_name}' not found in scheduler")
            return False

    except Exception as e:
        logger.error(f"❌ Error removing job from scheduler: {str(e)}")
        return False


async def shutdown_scheduler():
    """Shutdown the scheduler gracefully"""
    global scheduler

    if scheduler is not None:
        scheduler.shutdown()
        logger.info("🛑 APScheduler shutdown")
        scheduler = None
