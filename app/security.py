# app/security.py
import logging
from pathlib import Path
from cryptography.fernet import Fernet
from .config import DATA_DIR

KEY_FILE = DATA_DIR / "secrets.key"

def generate_key():
    """Generates a new Fernet key and saves it to the key file."""
    key = Fernet.generate_key()
    with open(KEY_FILE, "wb") as key_file:
        key_file.write(key)
    logging.info("New encryption key generated and saved.")
    return key

def load_key():
    """Loads the Fernet key from the key file, generating it if it doesn't exist."""
    if not KEY_FILE.exists():
        return generate_key()
    with open(KEY_FILE, "rb") as key_file:
        return key_file.read()

try:
    _key = load_key()
    _fernet = Fernet(_key)
except Exception as e:
    logging.critical(f"Could not load or generate encryption key: {e}")
    _fernet = None

def encrypt_password(password: str) -> str | None:
    """Encrypts a password string."""
    return _fernet.encrypt(password.encode()).decode() if _fernet and password else None

def decrypt_password(encrypted_password: str) -> str | None:
    """Decrypts an encrypted password string."""
    if not _fernet or not encrypted_password:
        return None
    try:
        return _fernet.decrypt(encrypted_password.encode()).decode()
    except Exception:
        logging.error("Failed to decrypt password. The key may have changed or data is corrupt.")
        return None