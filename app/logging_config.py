# app/logging_config.py
import logging
import sys
from .config import config_manager

def setup_logging():
    """
    Configures logging for the application based on settings from the ConfigManager.
    This function can be called multiple times to dynamically update log levels.
    """
    logging_settings = config_manager.settings.get("logging", {})
    log_levels = logging_settings.get("levels", {})

    app_log_level = log_levels.get("app", "INFO").upper()

    root_logger = logging.getLogger()
    
    # Set up basic config only if no handlers exist, to avoid adding duplicate handlers on reload.
    if not root_logger.hasHandlers():
        logging.basicConfig(
            level=app_log_level,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            stream=sys.stdout,
        )
    else:
        # If handlers exist, just update the level of the root logger.
        root_logger.setLevel(app_log_level)

    logging.info(f"Logging configured. Application level: {app_log_level}")
