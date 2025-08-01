# app/credentials_manager.py
import json
import os
import logging
from pathlib import Path

from . import security
from .config import settings, DATA_DIR

_LOGGER = logging.getLogger(__name__)

CREDENTIALS_FILE = DATA_DIR / "credentials.json"

def save_credentials(username: str, password: str):
    """Encrypts and saves credentials to the credentials file."""
    encrypted_password = security.encrypt_password(password)
    if not encrypted_password:
        raise ValueError("Failed to encrypt password.")
    
    data = {"username": username, "password": encrypted_password}
    with open(CREDENTIALS_FILE, 'w') as f:
        json.dump(data, f, indent=2)
    _LOGGER.info(f"Credentials saved for user: {username}")

def load_credentials() -> tuple[str | None, str | None]:
    """
    Loads credentials securely.
    Priority: Environment Vars > credentials.json > mytoyota_config.yaml.
    Returns a tuple of (username, password).
    """
    _LOGGER.debug("Attempting to load credentials...")
    username_env = os.environ.get("MYT_USERNAME")
    password_env = os.environ.get("MYT_PASSWORD")
    if username_env and password_env:
        _LOGGER.info("Loading credentials from environment variables.")
        return username_env, password_env

    _LOGGER.debug(f"Checking for credentials file at: {CREDENTIALS_FILE}")
    if CREDENTIALS_FILE.exists():
        _LOGGER.debug("Credentials file found.")
        try:
            with open(CREDENTIALS_FILE, 'r') as f:
                data = json.load(f)
            username = data.get("username")
            encrypted_pass = data.get("password")
            _LOGGER.debug(f"Read username '{username}' from file.")
            if not encrypted_pass:
                _LOGGER.warning("Password field is missing from credentials file.")

            password = security.decrypt_password(encrypted_pass)
            if not password:
                _LOGGER.error("Password decryption failed.")

            if username and password:
                _LOGGER.info("Successfully loaded credentials from secure file.")
                return username, password
            else:
                _LOGGER.error("Failed to get username/password from secure file after processing.")
        except Exception as e:
            _LOGGER.error(f"Could not load/decrypt from {CREDENTIALS_FILE}: {e}", exc_info=True)
    else:
        _LOGGER.debug("Credentials file not found.")

    _LOGGER.debug("Falling back to mytoyota_config.yaml for credentials.")
    config_creds = settings.get("credentials", {})
    if config_creds.get("username") and config_creds.get("password"):
        _LOGGER.warning("Loading credentials from mytoyota_config.yaml. Please migrate via Settings page.")
        return config_creds.get("username"), config_creds.get("password")

    _LOGGER.error("All credential sources exhausted. No credentials loaded.")
    return None, None

def get_username() -> str | None:
    """Loads just the username to display on the settings page."""
    username, _ = load_credentials()
    return username