import datetime
from zoneinfo import ZoneInfo
from typing import Optional

def get_timezone(config_manager) -> ZoneInfo:
    """Gets the configured timezone as a ZoneInfo object. Defaults to UTC."""
    tz_string = config_manager.settings.get("timezone", "UTC")
    try:
        return ZoneInfo(tz_string)
    except Exception:
        return ZoneInfo("UTC")

def get_local_now(config_manager=None) -> datetime.datetime:
    """Returns the current time in the configured timezone, as a naive datetime."""
    tz = get_timezone(config_manager) if config_manager else ZoneInfo("UTC")
    now_utc = datetime.datetime.now(datetime.timezone.utc)
    return now_utc.astimezone(tz).replace(tzinfo=None)

def convert_to_local_naive(dt: datetime.datetime, config_manager=None) -> datetime.datetime:
    """Converts an aware datetime to a naive datetime in the configured timezone."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # If it's already naive, assume it's UTC and add tzinfo before converting
        dt = dt.replace(tzinfo=datetime.timezone.utc)
    tz = get_timezone(config_manager) if config_manager else ZoneInfo("UTC")
    return dt.astimezone(tz).replace(tzinfo=None)

def convert_from_local_naive(dt: datetime.datetime, config_manager=None) -> datetime.datetime:
    """Converts a naive datetime (assumed to be in local timezone) to an aware UTC datetime."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(datetime.timezone.utc)
    
    tz = get_timezone(config_manager) if config_manager else ZoneInfo("UTC")
    dt_aware = dt.replace(tzinfo=tz)
    return dt_aware.astimezone(datetime.timezone.utc)
