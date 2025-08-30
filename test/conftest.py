# tests/conftest.py
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from unittest.mock import patch
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.main import app
from app.database import Base

@pytest_asyncio.fixture(scope="function")
async def client():
    """A test client for making requests to the FastAPI application."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac

@pytest.fixture(scope="function")
def test_data_dir(tmp_path):
    """
    Creates a temporary data directory for each test function and patches the
    application's DATA_DIR constant to use it. This isolates file-based tests.
    """
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    with patch("app.config.DATA_DIR", data_dir), \
         patch("app.credentials_manager.DATA_DIR", data_dir), \
         patch("app.fetcher.DATA_DIR", data_dir), \
         patch("app.database.DATA_DIR", data_dir):
        from app.config import config_manager
        config_manager.load()  # Reload config to use the new path
        yield data_dir

@pytest.fixture(scope="function")
def test_db_session(test_data_dir):
    """
    Provides a clean, in-memory SQLite database session for each test function
    by patching the database engine used by the application.
    """
    test_engine = create_engine("sqlite:///:memory:")
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=test_engine)

    # Patch both the engine and the SessionLocal factory to ensure full isolation
    with patch("app.database.engine", test_engine), \
         patch("app.database.SessionLocal", TestSessionLocal):
        Base.metadata.create_all(bind=test_engine)
        yield
        Base.metadata.drop_all(bind=test_engine)