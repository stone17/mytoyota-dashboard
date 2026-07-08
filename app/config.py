# app/config.py
import yaml
import logging
import os
import time
from pathlib import Path

_LOGGER = logging.getLogger(__name__)

# Centralize the data directory definition and ensure it exists.
DATA_DIR = Path(__file__).parent.parent / "data"
DATA_DIR.mkdir(exist_ok=True)

DEFAULT_CONFIG_PATH = DATA_DIR / "mytoyota_config.yaml"

class ConfigManager:
    """A class to manage loading, merging, and updating application configuration."""

    def __init__(self):
        """Initializes the ConfigManager and performs the initial configuration load."""
        self.settings = {}
        self.load()

    @property
    def user_config_path(self) -> Path:
        """Returns the dynamically calculated path to the user config file."""
        return DATA_DIR / "user_config.yaml"

    def _deep_merge(self, source: dict, destination: dict) -> dict:
        """
        Richer, more robust recursive merge of dictionaries.
        The 'source' dictionary's values overwrite the 'destination' dictionary's values.
        """
        for key, value in source.items():
            if isinstance(value, dict) and key in destination and isinstance(destination[key], dict):
                self._deep_merge(value, destination[key])
            else:
                destination[key] = value
        return destination

    def load(self):
        """
        Loads configuration by reading the default config file first,
        then overriding it with any settings from the user config file.
        """
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
            with open(self.user_config_path, 'r') as f:
                user_settings = yaml.safe_load(f) or {}
        except FileNotFoundError:
            pass

        # 3. Merge user settings over the defaults and store in the instance
        self.settings = self._deep_merge(source=user_settings, destination=default_settings)

        # 4. Set Timezone
        timezone = self.settings.get("timezone")
        if timezone:
            os.environ['TZ'] = timezone
            if hasattr(time, 'tzset'):
                time.tzset()

        _LOGGER.info("Configuration loaded successfully.")

    def update_and_reload(self, path_keys: list, new_value: dict) -> bool:
        """
        Updates a setting in user_config.yaml and reloads the entire configuration.
        """
        try:
            # 1. Read existing user config
            try:
                with open(self.user_config_path, 'r') as f:
                    current_user_config = yaml.safe_load(f) or {}
            except FileNotFoundError:
                current_user_config = {}

            # 2. Construct the full path for deep_merge
            new_settings_full_path = {}
            temp = new_settings_full_path
            for key in path_keys[:-1]:
                temp = temp.setdefault(key, {})
            temp[path_keys[-1]] = new_value
            
            updated_user_config = self._deep_merge(new_settings_full_path, current_user_config)

            # 3. Write back to user_config.yaml
            with open(self.user_config_path, 'w') as f:
                yaml.dump(updated_user_config, f, default_flow_style=False, sort_keys=False)

            # 4. Reload config into memory by calling self.load()
            self.load()
            _LOGGER.info(f"Settings updated and reloaded: {'.'.join(path_keys)} set to {new_value}")
            return True
        except Exception as e:
            _LOGGER.error(f"Error updating settings for {'.'.join(path_keys)}: {e}", exc_info=True)
            return False


# Create a single, shared instance for the entire application to use.
# Other modules should import this instance.
config_manager = ConfigManager()