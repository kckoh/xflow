from datetime import datetime
from typing import List

from beanie import PydanticObjectId
from fastapi import APIRouter, HTTPException, status
from croniter import croniter
import pytz

from models import ETLJob
from schemas.etl_schedule import (
    ScheduleConfig,
    SchedulePreset,
    ScheduleUpdateRequest,
    ScheduleResponse,
    SCHEDULE_PRESETS,
)
from utils.scheduler import add_job_to_scheduler, remove_job_from_scheduler

router = APIRouter()


@router.get("/presets", response_model=List[SchedulePreset])
async def get_schedule_presets():
    """Get predefined schedule presets (hourly, daily, weekly, monthly)"""
    return SCHEDULE_PRESETS


@router.get("/{job_id}", response_model=ScheduleResponse)
async def get_job_schedule(job_id: str):
    """Get schedule configuration for an ETL job"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="ETL job not found")

    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")

    # Calculate next run time if schedule is enabled
    next_run = None
    if job.schedule_enabled and job.schedule_cron:
        try:
            tz = pytz.timezone(job.schedule_timezone)
            now = datetime.now(tz)
            cron = croniter(job.schedule_cron, now)
            next_run = cron.get_next(datetime).isoformat()
        except Exception as e:
            print(f"Error calculating next run: {e}")

    return ScheduleResponse(
        enabled=job.schedule_enabled,
        cron=job.schedule_cron,
        timezone=job.schedule_timezone,
        next_run=next_run,
    )


@router.put("/{job_id}", response_model=ScheduleResponse)
async def update_job_schedule(job_id: str, schedule: ScheduleUpdateRequest):
    """Update schedule configuration for an ETL job"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="ETL job not found")

    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")

    # Validate cron expression if provided
    if schedule.enabled and not schedule.cron:
        raise HTTPException(
            status_code=400,
            detail="Cron expression is required when schedule is enabled"
        )

    if schedule.cron:
        try:
            # Validate cron expression
            croniter(schedule.cron)
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid cron expression: {str(e)}"
            )

    # Update job schedule
    job.schedule_enabled = schedule.enabled
    job.schedule_cron = schedule.cron
    job.schedule_timezone = schedule.timezone
    job.updated_at = datetime.utcnow()

    await job.save()

    # Sync with APScheduler
    if schedule.enabled and schedule.cron:
        # Add/update job in scheduler
        await add_job_to_scheduler(
            job_id=job_id,
            job_name=job.name,
            cron=schedule.cron,
            timezone=schedule.timezone
        )
    else:
        # Remove job from scheduler if disabled
        await remove_job_from_scheduler(job_id=job_id, job_name=job.name)

    # Calculate next run time
    next_run = None
    if job.schedule_enabled and job.schedule_cron:
        try:
            tz = pytz.timezone(job.schedule_timezone)
            now = datetime.now(tz)
            cron = croniter(job.schedule_cron, now)
            next_run = cron.get_next(datetime).isoformat()
        except Exception as e:
            print(f"Error calculating next run: {e}")

    return ScheduleResponse(
        enabled=job.schedule_enabled,
        cron=job.schedule_cron,
        timezone=job.schedule_timezone,
        next_run=next_run,
    )


@router.delete("/{job_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_job_schedule(job_id: str):
    """Disable schedule for an ETL job"""
    try:
        job = await ETLJob.get(PydanticObjectId(job_id))
    except Exception:
        raise HTTPException(status_code=404, detail="ETL job not found")

    if not job:
        raise HTTPException(status_code=404, detail="ETL job not found")

    # Disable schedule
    job.schedule_enabled = False
    job.updated_at = datetime.utcnow()

    await job.save()

    # Remove from APScheduler
    await remove_job_from_scheduler(job_id=job_id, job_name=job.name)

    return None
