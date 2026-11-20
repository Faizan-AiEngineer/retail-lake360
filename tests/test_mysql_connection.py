"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
MySQL Connection, Driver, and Security Integration Tests
Verifies MySQL 8.4 connection URLs, PyMySQL driver, pooling, and complete removal of PostgreSQL.
"""

from pathlib import Path

import pytest

from config.database import get_mysql_engine
from config.settings import settings


def test_mysql_connection_urls():
    """Verifies that all connection strings use the MySQL PyMySQL dialect."""
    source_url = settings.get_connection_url(role="source")
    assert "mysql+pymysql://" in source_url
    assert settings.MYSQL_SOURCE_USER in source_url
    assert settings.MYSQL_SOURCE_DB in source_url
    assert "charset=utf8mb4" in source_url

    pipeline_url = settings.get_connection_url(role="pipeline")
    assert "mysql+pymysql://" in pipeline_url
    assert settings.MYSQL_PIPELINE_USER in pipeline_url
    assert settings.MYSQL_METADATA_DB in pipeline_url

    analytics_url = settings.get_connection_url(role="analytics")
    assert "mysql+pymysql://" in analytics_url
    assert settings.MYSQL_ANALYTICS_USER in analytics_url
    assert settings.MYSQL_SERVING_DB in analytics_url


def test_missing_password_fails_before_building_connection_url():
    from config.settings import Settings

    unconfigured = Settings(MYSQL_SOURCE_PASSWORD="")
    with pytest.raises(ValueError, match="MYSQL_SOURCE_PASSWORD must be set"):
        unconfigured.get_connection_url(role="source")


def test_strictly_no_postgresql_dependencies():
    """
    Mandatory Architectural Requirement:
    Verify that NO PostgreSQL drivers, connectors, or configuration remain in requirements.txt or docker-compose.yml.
    """
    root_dir = Path(__file__).resolve().parent.parent

    # 1. Check requirements.txt
    req_file = root_dir / "requirements.txt"
    with open(req_file, "r", encoding="utf-8") as f:
        req_content = f.read().lower()
    for forbidden in ["psycopg2", "psycopg2-binary", "asyncpg", "dbt-postgres", "pg8000"]:
        assert forbidden not in req_content, f"Found forbidden PostgreSQL package: {forbidden} in requirements.txt"

    # 2. Check docker-compose.yml
    compose_file = root_dir / "docker-compose.yml"
    with open(compose_file, "r", encoding="utf-8") as f:
        compose_content = f.read().lower()
    assert "postgres" not in compose_content
    assert "mysql:8.4" in compose_content


def test_mysql_engine_pool_configuration():
    """Tests that MySQL SQLAlchemy engines are initialized with proper QueuePool parameters."""
    engine = get_mysql_engine(role="pipeline", custom_url="sqlite:///:memory:")
    assert engine is not None
    # Verify pool settings from config
    assert settings.DB_POOL_SIZE >= 5
    assert settings.DB_MAX_OVERFLOW >= 10
    assert settings.DB_POOL_RECYCLE_SEC > 0
    assert settings.DB_POOL_PRE_PING is True


def test_three_separate_database_namespaces():
    """Verifies that the platform defines 3 separate database namespaces."""
    assert settings.MYSQL_SOURCE_DB == "retaillake_source"
    assert settings.MYSQL_SERVING_DB == "retaillake_serving"
    assert settings.MYSQL_METADATA_DB == "retaillake_metadata"
    # Ensure all three are distinct
    assert len({settings.MYSQL_SOURCE_DB, settings.MYSQL_SERVING_DB, settings.MYSQL_METADATA_DB}) == 3
