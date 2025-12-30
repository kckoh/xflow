from typing import Optional
from pydantic import BaseModel, Field, field_validator


class ScheduleConfig(BaseModel):
    """Schedule configuration for ETL jobs"""
    enabled: bool = False
    cron: Optional[str] = Field(None, description="Cron expression (e.g., '0 0 * * *' for daily at midnight)")
    timezone: str = Field("UTC", description="Timezone for schedule execution")

    @field_validator('cron')
    @classmethod
    def validate_cron(cls, v):
        """Validate cron expression format"""
        if v is None:
            return v

        # Basic cron validation (5 fields)
        parts = v.strip().split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 fields (minute hour day month weekday)")

        return v


class SchedulePreset(BaseModel):
    """Predefined schedule presets for common intervals"""
    name: str  # "hourly", "daily", "weekly", "monthly"
    cron: str  # Cron expression
    description: str  # Human-readable description


# Predefined schedule presets
SCHEDULE_PRESETS = [
    SchedulePreset(
        name="hourly",
        cron="0 * * * *",
        description="Every hour at minute 0"
    ),
    SchedulePreset(
        name="daily",
        cron="0 0 * * *",
        description="Every day at midnight (00:00)"
    ),
    SchedulePreset(
        name="weekly",
        cron="0 0 * * 0",
        description="Every Sunday at midnight (00:00)"
    ),
    SchedulePreset(
        name="monthly",
        cron="0 0 1 * *",
        description="First day of every month at midnight (00:00)"
    ),
]


class ScheduleUpdateRequest(BaseModel):
    """Request body for updating ETL job schedule"""
    enabled: bool
    cron: Optional[str] = None
    timezone: str = "UTC"


class ScheduleResponse(BaseModel):
    """Response for schedule configuration"""
    enabled: bool
    cron: Optional[str]
    timezone: str
    next_run: Optional[str] = None  # Next scheduled run time (ISO format)

    class Config:
        from_attributes = True
