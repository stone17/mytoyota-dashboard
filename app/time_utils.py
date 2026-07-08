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
    """Returns the current UTC time as a naive datetime (for database storage)."""
    return datetime.datetime.utcnow()

def convert_to_local_naive(dt: datetime.datetime, config_manager=None) -> datetime.datetime:
    """Converts an aware datetime to a naive UTC datetime (for database storage)."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=None)

def convert_utc_to_local_naive(dt: datetime.datetime, config_manager=None) -> datetime.datetime:
    """Converts a naive UTC datetime to a naive datetime in the configured local timezone."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt_utc_aware = dt.replace(tzinfo=datetime.timezone.utc)
    else:
        dt_utc_aware = dt
    tz = get_timezone(config_manager) if config_manager else ZoneInfo("UTC")
    dt_local_aware = dt_utc_aware.astimezone(tz)
    return dt_local_aware.replace(tzinfo=None)

def get_sqlite_offset_string(config_manager) -> str:
    """Returns the SQLite modifier string for the current timezone offset in minutes (e.g., '+120 minutes')."""
    tz = get_timezone(config_manager)
    now = datetime.datetime.now(tz)
    offset_seconds = now.utcoffset().total_seconds()
    offset_minutes = int(offset_seconds // 60)
    sign = '+' if offset_minutes >= 0 else ''
    return f"{sign}{offset_minutes} minutes"

def convert_from_local_naive(dt: datetime.datetime, config_manager=None) -> datetime.datetime:
    """Converts a naive datetime (assumed to be in local timezone) to an aware UTC datetime."""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(datetime.timezone.utc)
    
    tz = get_timezone(config_manager) if config_manager else ZoneInfo("UTC")
    dt_aware = dt.replace(tzinfo=tz)
    return dt_aware.astimezone(datetime.timezone.utc)
