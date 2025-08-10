# app/config.py
import yaml
import logging
from pathlib import Path

_LOGGER = logging.getLogger(__name__)

# Centralize the data directory definition and ensure it exists.
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DEFAULT_CONFIG_PATH = DATA_DIR / "mytoyota_config.yaml"
USER_CONFIG_PATH = DATA_DIR / "user_config.yaml"

settings = {}

def deep_merge(source, destination):
    """Recursively merge dictionaries. User settings (source) overwrite defaults (destination)."""
    for key, value in source.items():
        if isinstance(value, dict):
            node = destination.setdefault(key, {})
            deep_merge(value, node)
        else:
            destination[key] = value
    return destination

def load_config():
    """
    Loads configuration by reading the default config file first,
    then overriding it with any settings from the user config file.
    """
    global settings
    
    # 1. Start with the default settings
    try:
        with open(DEFAULT_CONFIG_PATH, 'r') as f:
            default_settings = yaml.safe_load(f) or {}
    except FileNotFoundError:
        _LOGGER.warning(f"Default config file not found at {DEFAULT_CONFIG_PATH}. Using empty defaults.")
        default_settings = {}
    
    # 2. Load user-specific settings
    user_settings = {}
    try:
        with open(USER_CONFIG_PATH, 'r') as f:
            user_settings = yaml.safe_load(f) or {}
    except FileNotFoundError:
        # This is normal if the user hasn't saved any settings yet.
        pass

    # 3. Merge user settings over the defaults
    settings = deep_merge(source=user_settings, destination=default_settings)
    _LOGGER.info("Configuration loaded successfully (defaults merged with user settings).")


# Initial load on application startup
load_config()