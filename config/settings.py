"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Configuration & Settings Management
"""

import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    # MySQL 8.4 LTS Server Details
    MYSQL_HOST: str = os.getenv("MYSQL_HOST", "localhost")
    MYSQL_PORT: int = int(os.getenv("MYSQL_PORT", "3306"))
    MYSQL_ROOT_PASSWORD: str = os.getenv("MYSQL_ROOT_PASSWORD", "")

    # Logical Databases
    MYSQL_SOURCE_DB: str = os.getenv("MYSQL_SOURCE_DB", "retaillake_source")
    MYSQL_SERVING_DB: str = os.getenv("MYSQL_SERVING_DB", "retaillake_serving")
    MYSQL_METADATA_DB: str = os.getenv("MYSQL_METADATA_DB", "retaillake_metadata")

    # Role-Based Database Users (Least-Privilege Security)
    # 1. Source simulation user
    MYSQL_SOURCE_USER: str = os.getenv("MYSQL_SOURCE_USER", "source_user")
    MYSQL_SOURCE_PASSWORD: str = os.getenv("MYSQL_SOURCE_PASSWORD", "")

    # 2. Pipeline ETL user
    MYSQL_PIPELINE_USER: str = os.getenv("MYSQL_PIPELINE_USER", "pipeline_user")
    MYSQL_PIPELINE_PASSWORD: str = os.getenv("MYSQL_PIPELINE_PASSWORD", "")

    # 3. Analytics read-only user
    MYSQL_ANALYTICS_USER: str = os.getenv("MYSQL_ANALYTICS_USER", "analytics_user")
    MYSQL_ANALYTICS_PASSWORD: str = os.getenv("MYSQL_ANALYTICS_PASSWORD", "")

    # SQLAlchemy Connection Pool Configuration (PyMySQL)
    DB_POOL_SIZE: int = int(os.getenv("DB_POOL_SIZE", "10"))
    DB_MAX_OVERFLOW: int = int(os.getenv("DB_MAX_OVERFLOW", "20"))
    DB_POOL_RECYCLE_SEC: int = int(os.getenv("DB_POOL_RECYCLE_SEC", "1800"))
    DB_POOL_TIMEOUT_SEC: int = int(os.getenv("DB_POOL_TIMEOUT_SEC", "30"))
    DB_POOL_PRE_PING: bool = os.getenv("DB_POOL_PRE_PING", "True").lower() in ("true", "1")

    # Lakehouse storage paths
    BASE_DIR: Path = Path(__file__).resolve().parent.parent
    LAKEHOUSE_STORAGE_BASE: str = os.getenv("LAKEHOUSE_STORAGE_BASE", str(BASE_DIR / "lakehouse"))
    LAKEHOUSE_BRONZE_DIR: str = os.getenv("LAKEHOUSE_BRONZE_DIR", str(BASE_DIR / "lakehouse" / "bronze"))
    LAKEHOUSE_SILVER_DIR: str = os.getenv("LAKEHOUSE_SILVER_DIR", str(BASE_DIR / "lakehouse" / "silver"))
    LAKEHOUSE_GOLD_DIR: str = os.getenv("LAKEHOUSE_GOLD_DIR", str(BASE_DIR / "lakehouse" / "gold"))

    # Spark Config
    SPARK_APP_NAME: str = os.getenv("SPARK_APP_NAME", "RetailLake360-DeltaEngine")
    SPARK_MASTER: str = os.getenv("SPARK_MASTER", "local[*]")

    def get_connection_url(self, role: str = "pipeline", db_name: str | None = None) -> str:
        """
        Constructs standard MySQL SQLAlchemy connection URL with PyMySQL connector.
        Enforces utf8mb4 charset and strict security credentials.
        """
        if role == "source":
            user = self.MYSQL_SOURCE_USER
            password = self.MYSQL_SOURCE_PASSWORD
            default_db = self.MYSQL_SOURCE_DB
        elif role == "analytics":
            user = self.MYSQL_ANALYTICS_USER
            password = self.MYSQL_ANALYTICS_PASSWORD
            default_db = self.MYSQL_SERVING_DB
        elif role == "root":
            user = "root"
            password = self.MYSQL_ROOT_PASSWORD
            default_db = db_name or self.MYSQL_METADATA_DB
        else:  # 'pipeline' / default
            user = self.MYSQL_PIPELINE_USER
            password = self.MYSQL_PIPELINE_PASSWORD
            password_name = "MYSQL_PIPELINE_PASSWORD"
            default_db = self.MYSQL_METADATA_DB

        if role == "source":
            password_name = "MYSQL_SOURCE_PASSWORD"
        elif role == "analytics":
            password_name = "MYSQL_ANALYTICS_PASSWORD"
        elif role == "root":
            password_name = "MYSQL_ROOT_PASSWORD"

        if not password:
            raise ValueError(f"{password_name} must be set before connecting with role '{role}'.")

        target_db = db_name if db_name else default_db
        return f"mysql+pymysql://{user}:{password}@{self.MYSQL_HOST}:{self.MYSQL_PORT}/{target_db}?charset=utf8mb4"


settings = Settings()
