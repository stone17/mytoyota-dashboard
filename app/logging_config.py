# app/logging_config.py
import logging
import sys
from .config import settings

def setup_logging():
    log_level = settings.get("logging_level", "INFO").upper()
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
    )
    logging.info(f"Logging configured with level: {log_level}")