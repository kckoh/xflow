from typing import Optional, Dict
from datetime import datetime

def generate_schedule(frequency: str, ui_params: Optional[Dict] = None) -> Optional[str]:
    """
    Generates an Airflow-compatible schedule string from UI parameters.
    
    Args:
        frequency: 'hourly', 'daily', 'weekly', 'monthly', 'interval'
        ui_params: Dictionary containing 'startDate', 'intervalDays', 'intervalHours', 'intervalMinutes'
        
    Returns:
        - CRON expression (e.g., "0 14 * * *")
        - Interval string (e.g., "@interval:P1DT5H30M")
        - None if invalid
    """
    if not frequency:
        return None
        
    # Handle Custom Interval
    if frequency == 'interval':
        if not ui_params:
            return None
        days = int(ui_params.get('intervalDays', 0))
        hours = int(ui_params.get('intervalHours', 0))
        minutes = int(ui_params.get('intervalMinutes', 0))
        
        # Format for our custom DAG parser: @interval:P<days>DT<hours>H<minutes>M
        return f"@interval:P{days}DT{hours}H{minutes}M"

    # Handle Standard Frequencies based on Start Date
    if ui_params and ui_params.get('startDate'):
        try:
            # Handle ISO string (e.g. "2023-01-01T14:30")
            dt = datetime.fromisoformat(ui_params['startDate'].replace('Z', '+00:00'))
            minute = dt.minute
            hour = dt.hour
            day_of_month = dt.day
            day_of_week = dt.strftime('%w') # 0 = Sunday
            
            if frequency == 'hourly':
                # For hourly, we use the interval from uiParams if available, or default to 1
                interval = int(ui_params.get('hourInterval', 1))
                return f"{minute} */{interval} * * *"
                
            if frequency == 'daily':
                return f"{minute} {hour} * * *"
                
            if frequency == 'weekly':
                return f"{minute} {hour} * * {day_of_week}"
                
            if frequency == 'monthly':
                return f"{minute} {hour} {day_of_month} * *"
                
        except ValueError:
            pass # Invalid date format fallback
            
    # Fallback for when no startDate is provided (should limit this in UI)
    defaults = {
        "hourly": "0 * * * *",
        "daily": "0 0 * * *",
        "weekly": "0 0 * * 0",
        "monthly": "0 0 1 * *",
    }
    
    return defaults.get(frequency)
