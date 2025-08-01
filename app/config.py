# app/config.py
import yaml
import logging
from pathlib import Path

_LOGGER = logging.getLogger(__name__)

# Centralize the data directory definition and ensure it exists.
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

CONFIG_PATH = DATA_DIR / "mytoyota_config.yaml"
settings = {}

def load_config():
    """Loads or reloads the configuration from the YAML file into the global settings dict."""
    global settings
    try:
        with open(CONFIG_PATH, 'r') as f:
            settings.clear()
            settings.update(yaml.safe_load(f) or {})
        _LOGGER.info("Configuration loaded/reloaded successfully.")
    except Exception as e:
        _LOGGER.error(f"Error loading configuration from {CONFIG_PATH}: {e}")
        settings.clear()

# Initial load on application startup
load_config()
