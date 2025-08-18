"""Test database utilities for webservice async testing."""
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from reia.config.settings import TestWebserviceSettings
from reia.webservice.database import sessionmanager
from reia.webservice.main import app


def get_test_async_engine():
    """Create test async database engine."""
    test_config = TestWebserviceSettings()
    return create_async_engine(
        test_config.db_connection_string,
        echo=False
    )


def get_test_async_session():
    """Create test async database session."""
    engine = get_test_async_engine()
    return async_sessionmaker(
        engine,
        expire_on_commit=False
    )


async def get_test_client():
    """Create test client for FastAPI webservice."""
    test_config = TestWebserviceSettings()

    # Configure sessionmanager with test database
    sessionmanager._engine = create_async_engine(
        test_config.db_connection_string,
        echo=False
    )
    sessionmanager._sessionmaker = async_sessionmaker(
        autoflush=False,
        bind=sessionmanager._engine
    )

    transport = ASGITransport(app=app)
    client = AsyncClient(transport=transport, base_url="http://test")

    return client, sessionmanager


async def cleanup_test_client(sessionmanager):
    """Clean up test client resources."""
    if sessionmanager._engine:
        await sessionmanager._engine.dispose()
        sessionmanager._engine = None
        sessionmanager._sessionmaker = None
