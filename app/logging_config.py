# app/logging_config.py
import logging
import sys
import datetime
from .config import config_manager
from . import time_utils

class TimezoneFormatter(logging.Formatter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cached_tz = None
        self._cached_tz_obj = None

    def formatTime(self, record, datefmt=None):
        dt = datetime.datetime.fromtimestamp(record.created, tz=datetime.timezone.utc)
        tz_str = config_manager.settings.get("timezone", "UTC") if config_manager.settings else "UTC"
        if tz_str != self._cached_tz:
            self._cached_tz = tz_str
            self._cached_tz_obj = time_utils.get_timezone(config_manager)
        
        local_dt = dt.astimezone(self._cached_tz_obj)
        if datefmt:
            return local_dt.strftime(datefmt)
        else:
            # Default ISO8601-like format matching logging's default behavior
            return local_dt.strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]

def setup_logging():
    """
    Configures logging for the application based on settings from the ConfigManager.
    This function can be called multiple times to dynamically update log levels.
    """
    logging_settings = config_manager.settings.get("logging", {})
    log_levels = logging_settings.get("levels", {})

    app_log_level = log_levels.get("app", "INFO").upper()

    root_logger = logging.getLogger()
    formatter = TimezoneFormatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    
    # Set up basic config only if no handlers exist, to avoid adding duplicate handlers on reload.
    if not root_logger.hasHandlers():
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        root_logger.setLevel(app_log_level)
    else:
        # If handlers exist, just update the level of the root logger and formatters.
        root_logger.setLevel(app_log_level)
        for handler in root_logger.handlers:
            handler.setFormatter(formatter)

    logging.info(f"Logging configured. Application level: {app_log_level}")
