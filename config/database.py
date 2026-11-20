"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Database Connection Engine & Safe Connection Pooling (MySQL 8.4 LTS via PyMySQL)
Seamlessly connects to MySQL 8.4 LTS with connection pooling, and provides local persistence fallback.
"""

import os
import socket
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import Generator, Optional, Dict, Any, List
from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import QueuePool

from config.settings import settings

logger = logging.getLogger("retaillake.database")

# Engine cache to avoid duplicate connection pools
_engines: Dict[str, Engine] = {}


def _is_mysql_available(host: str, port: int, timeout: float = 1.0) -> bool:
    """Quick socket check to determine if MySQL daemon is accepting connections."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def get_mysql_engine(
    role: str = "pipeline",
    db_name: Optional[str] = None,
    custom_url: Optional[str] = None,
    echo: bool = False
) -> Engine:
    """
    Creates and caches a pooled SQLAlchemy Engine connected to MySQL 8.4 via PyMySQL.
    Configures QueuePool, pre-ping verification, and recycling to avoid stale connections.
    If MySQL server is not locally running, falls back to persistent local storage in data/.
    """
    cache_key = custom_url or f"{role}:{db_name or 'default'}"
    if cache_key in _engines:
        return _engines[cache_key]

    target_db = db_name or (
        settings.MYSQL_SOURCE_DB if role == "source" else
        settings.MYSQL_SERVING_DB if role == "analytics" else
        settings.MYSQL_METADATA_DB
    )

    if custom_url:
        url = custom_url
    else:
        # Check if MySQL daemon is accessible
        if _is_mysql_available(settings.MYSQL_HOST, settings.MYSQL_PORT):
            url = settings.get_connection_url(role=role, db_name=target_db)
        else:
            # Persistent standalone storage fallback
            data_dir = settings.BASE_DIR / "data"
            os.makedirs(data_dir, exist_ok=True)
            sqlite_path = data_dir / f"{target_db}.db"
            url = f"sqlite:///{sqlite_path.as_posix()}"
            logger.info(f"MySQL not detected at {settings.MYSQL_HOST}:{settings.MYSQL_PORT}. Using local persistent engine: {url}")

    if url.startswith("sqlite"):
        engine = create_engine(url, echo=echo)
        _engines[cache_key] = engine
        return engine

    engine = create_engine(
        url,
        poolclass=QueuePool,
        pool_size=settings.DB_POOL_SIZE,
        max_overflow=settings.DB_MAX_OVERFLOW,
        pool_recycle=settings.DB_POOL_RECYCLE_SEC,
        pool_timeout=settings.DB_POOL_TIMEOUT_SEC,
        pool_pre_ping=settings.DB_POOL_PRE_PING,
        echo=echo,
        connect_args={
            "charset": "utf8mb4",
            "connect_timeout": 10,
        }
    )
    _engines[cache_key] = engine
    return engine


@contextmanager
def get_db_session(role: str = "pipeline", db_name: Optional[str] = None) -> Generator[Session, None, None]:
    """
    Safe transaction management context manager.
    Commits on success, rolls back on exception, and ensures session closure.
    """
    engine = get_mysql_engine(role=role, db_name=db_name)
    session_factory = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    session: Session = session_factory()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Database transaction rolled back due to error: {e}")
        raise
    finally:
        session.close()


@contextmanager
def get_db_connection(role: str = "pipeline", db_name: Optional[str] = None):
    """
    Direct raw connection context manager with safe release to pool.
    """
    engine = get_mysql_engine(role=role, db_name=db_name)
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()


def execute_parameterized_query(
    query: str,
    params: Optional[Dict[str, Any]] = None,
    role: str = "pipeline",
    db_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    Executes a safe parameterized query using SQLAlchemy TextClause.
    Prevents SQL injection and returns rows as dictionaries.
    """
    with get_db_connection(role=role, db_name=db_name) as conn:
        result = conn.execute(text(query), params or {})
        if result.returns_rows:
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]
        return []


def execute_statement(
    statement: str,
    params: Optional[Dict[str, Any]] = None,
    role: str = "pipeline",
    db_name: Optional[str] = None
) -> int:
    """
    Executes an INSERT / UPDATE / DELETE statement within a transaction and returns rowcount.
    """
    engine = get_mysql_engine(role=role, db_name=db_name)
    with engine.begin() as conn:
        result = conn.execute(text(statement), params or {})
        return result.rowcount
