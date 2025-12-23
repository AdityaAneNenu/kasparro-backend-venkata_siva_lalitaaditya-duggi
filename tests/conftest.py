"""Pytest configuration and fixtures."""

import os
import tempfile
import uuid
from datetime import datetime
from typing import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import StaticPool, create_engine
from sqlalchemy.orm import Session, sessionmaker

from core.config import Settings
from core.models import Base, RunStatus, SourceType


@pytest.fixture(scope="session")
def test_settings() -> Settings:
    """Override settings for testing."""
    return Settings(
        DATABASE_URL="sqlite:///:memory:",
        API_KEY="test-api-key",
        LOG_LEVEL="DEBUG",
        LOG_FORMAT="text",
    )


@pytest.fixture(scope="function")
def db_engine():
    """Create test database engine with shared in-memory database.

    Using StaticPool ensures all connections share the same in-memory database.
    """
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,  # This ensures all connections use the same memory DB
    )
    Base.metadata.create_all(bind=engine)
    yield engine
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture(scope="function")
def db_session(db_engine) -> Generator[Session, None, None]:
    """Create test database session."""
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


@pytest.fixture(scope="function")
def test_client(db_engine, db_session, monkeypatch) -> Generator[TestClient, None, None]:
    """Create test FastAPI client with properly isolated database."""
    import core.database as db_module

    # Create a session factory bound to the test engine
    TestSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)

    # Patch the module-level globals FIRST
    monkeypatch.setattr(db_module, "_engine", db_engine)
    monkeypatch.setattr(db_module, "_SessionLocal", TestSessionLocal)

    # Patch the getter functions to return our test objects
    monkeypatch.setattr(db_module, "get_engine", lambda: db_engine)
    monkeypatch.setattr(db_module, "get_session_local", lambda: TestSessionLocal)

    # Patch init_db to do nothing (we've already created tables)
    monkeypatch.setattr(db_module, "init_db", lambda: None)

    # Import app AFTER patching
    from api.main import app
    from core.database import get_db

    # Override FastAPI dependency
    def override_get_db():
        db = TestSessionLocal()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_get_db

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()


@pytest.fixture
def sample_csv_file() -> Generator[str, None, None]:
    """Create a sample CSV file for testing."""
    content = """id,title,description,category,author,date
1,First Article,This is the first article,Technology,John Doe,2024-01-15
2,Second Article,This is the second article,Science,Jane Smith,2024-01-16
3,Third Article,This is the third article,Technology,Bob Wilson,2024-01-17
4,Fourth Article,This is the fourth article,Business,Alice Brown,2024-01-18
5,Fifth Article,This is the fifth article,Science,Charlie Davis,2024-01-19
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        f.flush()
        yield f.name

    if os.path.exists(f.name):
        os.remove(f.name)


@pytest.fixture
def sample_csv_with_quirks() -> Generator[str, None, None]:
    """Create a CSV file with data quality issues."""
    content = '''id;name;desc;value;active
1;Item One;"Description with ""quotes""";100.5;true
2;Item Two;  Whitespace  ;200;yes
3;Item Three;N/A;-50.25;false
4;;Missing name;0;1
5;Item Five;Normal item;1000;0
'''
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        f.flush()
        yield f.name

    if os.path.exists(f.name):
        os.remove(f.name)


@pytest.fixture
def sample_api_response() -> list:
    """Sample API response data."""
    return [
        {
            "id": "api-001",
            "title": "API Article 1",
            "description": "Description from API",
            "content": "Full content of the article",
            "author": "API Author",
            "category": "API Category",
            "tags": ["tag1", "tag2"],
            "url": "https://example.com/article1",
            "created_at": "2024-01-15T10:30:00Z",
        },
        {
            "id": "api-002",
            "title": "API Article 2",
            "description": "Another description",
            "content": "More content",
            "author": "Another Author",
            "category": "Tech",
            "tags": "comma,separated,tags",
            "url": "https://example.com/article2",
            "created_at": "2024-01-16T14:00:00Z",
        },
    ]


@pytest.fixture
def sample_rss_feed() -> str:
    """Sample RSS feed XML."""
    return """<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
    <channel>
        <title>Test Feed</title>
        <link>https://example.com</link>
        <description>A test RSS feed</description>
        <item>
            <guid>rss-001</guid>
            <title>RSS Article 1</title>
            <description>Description from RSS feed</description>
            <link>https://example.com/rss/1</link>
            <author>RSS Author</author>
            <pubDate>Mon, 15 Jan 2024 10:00:00 +0000</pubDate>
            <category>RSS Category</category>
        </item>
        <item>
            <guid>rss-002</guid>
            <title>RSS Article 2</title>
            <description>Another RSS description</description>
            <link>https://example.com/rss/2</link>
            <pubDate>Tue, 16 Jan 2024 12:00:00 +0000</pubDate>
        </item>
    </channel>
</rss>
"""


@pytest.fixture
def sample_unified_data(db_session) -> list:
    """Create sample unified data records."""
    from core.models import UnifiedData

    records = []
    for i in range(10):
        record = UnifiedData(
            source_type=(
                SourceType.CSV if i % 3 == 0 else (SourceType.API if i % 3 == 1 else SourceType.RSS)
            ),
            source_id=f"test-{i}",
            raw_id=i + 1,
            title=f"Test Article {i}",
            description=f"Description for article {i}",
            category="Test Category" if i % 2 == 0 else "Other",
            author=f"Author {i}",
            published_at=datetime(2024, 1, i + 1) if i < 28 else None,
        )
        db_session.add(record)
        records.append(record)

    db_session.commit()
    return records


@pytest.fixture
def sample_etl_runs(db_session) -> list:
    """Create sample ETL run records."""
    import uuid

    from core.models import ETLRun

    runs = []
    for i in range(5):
        run = ETLRun(
            run_id=str(uuid.uuid4()),
            source_type=(
                SourceType.CSV if i % 3 == 0 else (SourceType.API if i % 3 == 1 else SourceType.RSS)
            ),
            status=RunStatus.SUCCESS if i % 4 != 3 else RunStatus.FAILED,
            started_at=datetime(2024, 1, i + 1, 10, 0, 0),
            completed_at=datetime(2024, 1, i + 1, 10, 5, 0) if i % 4 != 3 else None,
            duration_seconds=300.0 if i % 4 != 3 else None,
            records_extracted=100 + i * 10,
            records_loaded=95 + i * 10,
            records_failed=5 if i % 4 == 3 else 0,
        )
        db_session.add(run)
        runs.append(run)

    db_session.commit()
    return runs
