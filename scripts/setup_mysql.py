"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
MySQL 8.4 LTS Setup & Initialization Script
Executes SQL initialization files to create databases, users, schemas, and views.
"""

import logging
import sys
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config.settings import settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("retaillake.setup_mysql")


def run_sql_file(conn, file_path: Path):
    logger.info(f"Executing: {file_path.name}")
    with open(file_path, "r", encoding="utf-8") as f:
        sql_content = f.read()

    # Split statements by semicolon while ignoring comments
    statements = []
    current_stmt = []
    for line in sql_content.splitlines():
        trimmed = line.strip()
        if not trimmed or trimmed.startswith("--"):
            continue
        current_stmt.append(line)
        if trimmed.endswith(";"):
            statements.append("\n".join(current_stmt))
            current_stmt = []

    for stmt in statements:
        if stmt.strip():
            conn.execute(text(stmt))


def quote_sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def initialize_users(conn):
    passwords = {
        "source_user": ("MYSQL_SOURCE_PASSWORD", settings.MYSQL_SOURCE_PASSWORD),
        "pipeline_user": ("MYSQL_PIPELINE_PASSWORD", settings.MYSQL_PIPELINE_PASSWORD),
        "analytics_user": ("MYSQL_ANALYTICS_PASSWORD", settings.MYSQL_ANALYTICS_PASSWORD),
    }
    for role, (variable, password) in passwords.items():
        if not password:
            raise ValueError(f"{variable} must be set before initializing MySQL.")

    conn.exec_driver_sql("SET SESSION sql_mode = CONCAT(@@sql_mode, ',NO_BACKSLASH_ESCAPES')")
    for role, (_, password) in passwords.items():
        conn.exec_driver_sql(
            f"CREATE USER IF NOT EXISTS '{role}'@'%' IDENTIFIED BY {quote_sql_string(password)}"
        )

    conn.exec_driver_sql(
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER, LOCK TABLES "
        "ON `retaillake_source`.* TO 'source_user'@'%'"
    )
    conn.exec_driver_sql(
        "GRANT SELECT ON `retaillake_source`.* TO 'pipeline_user'@'%'"
    )
    conn.exec_driver_sql(
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER, CREATE VIEW "
        "ON `retaillake_serving`.* TO 'pipeline_user'@'%'"
    )
    conn.exec_driver_sql(
        "GRANT SELECT, INSERT, UPDATE, DELETE, CREATE, DROP, INDEX, ALTER "
        "ON `retaillake_metadata`.* TO 'pipeline_user'@'%'"
    )
    conn.exec_driver_sql(
        "GRANT SELECT ON `retaillake_serving`.* TO 'analytics_user'@'%'"
    )
    conn.exec_driver_sql(
        "GRANT SELECT ON `retaillake_metadata`.`pipeline_runs` TO 'analytics_user'@'%'"
    )
    conn.exec_driver_sql(
        "GRANT SELECT ON `retaillake_metadata`.`data_quality_results` TO 'analytics_user'@'%'"
    )
    conn.exec_driver_sql("FLUSH PRIVILEGES")


def setup_mysql_environment():
    if not settings.MYSQL_ROOT_PASSWORD:
        raise ValueError("MYSQL_ROOT_PASSWORD must be set before initializing MySQL.")

    root_url = URL.create(
        "mysql+pymysql",
        username="root",
        password=settings.MYSQL_ROOT_PASSWORD,
        host=settings.MYSQL_HOST,
        port=settings.MYSQL_PORT,
        query={"charset": "utf8mb4"},
    )
    logger.info(f"Connecting to MySQL 8.4 at {settings.MYSQL_HOST}:{settings.MYSQL_PORT} as root...")

    try:
        engine = create_engine(root_url, isolation_level="AUTOCOMMIT")
        init_dir = Path(__file__).resolve().parent.parent / "docker" / "mysql" / "init"
        sql_files = sorted(init_dir.glob("*.sql"))

        with engine.connect() as conn:
            for sql_file in sql_files:
                run_sql_file(conn, sql_file)
            initialize_users(conn)

        logger.info("✅ All MySQL 8.4 LTS databases, users, schemas, and views successfully initialized!")
        return True
    except SQLAlchemyError as e:
        logger.error(f"❌ Failed to setup MySQL: {e}")
        raise


if __name__ == "__main__":
    setup_mysql_environment()
