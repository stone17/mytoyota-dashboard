# app/logging_config.py
import logging
import sys
import datetime
from .config import config_manager
from . import time_utils

class TimezoneFormatter(logging.Formatter):
    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
        tz = time_utils.get_timezone(config_manager)
        return dt.astimezone(tz).timetuple()

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
