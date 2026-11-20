"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Lakehouse Audit Logger & Metadata Engine (MySQL retaillake_metadata)
Records pipeline execution runs, data quality results, watermarks, manifests, and audit events.
"""

import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from sqlalchemy import text

from config.settings import settings
from config.database import get_mysql_engine

logger = logging.getLogger("retaillake.audit")


class LakehouseAuditLogger:
    def __init__(self, run_id: str, metadata_engine=None):
        self.run_id = run_id
        self._engine = metadata_engine

    def get_engine(self):
        if self._engine is None:
            self._engine = get_mysql_engine(role="pipeline", db_name=settings.MYSQL_METADATA_DB)
            if self._engine.dialect.name == "sqlite":
                self._init_sqlite_schema()
        return self._engine

    def _init_sqlite_schema(self):
        try:
            with self._engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS pipeline_runs (
                        run_id TEXT PRIMARY KEY, pipeline_name TEXT, trigger_type TEXT,
                        start_time TEXT, end_time TEXT, duration_seconds REAL, status TEXT,
                        records_ingested INTEGER, records_transformed INTEGER,
                        records_published INTEGER, error_message TEXT, execution_metadata TEXT
                    );
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS pipeline_checkpoints (
                        checkpoint_key TEXT PRIMARY KEY, pipeline_name TEXT, layer TEXT,
                        table_name TEXT, last_processed_timestamp TEXT, last_processed_id INTEGER,
                        records_count INTEGER, data_checksum TEXT, updated_at TEXT
                    );
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS audit_logs (
                        log_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, timestamp TEXT,
                        log_level TEXT, component TEXT, action TEXT, message TEXT, details_json TEXT
                    );
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS data_quality_results (
                        check_id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT, layer TEXT,
                        table_name TEXT, rule_name TEXT, rule_type TEXT, records_tested INTEGER,
                        records_passed INTEGER, records_failed INTEGER, status TEXT, details TEXT, checked_at TEXT
                    );
                """))
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS ingestion_manifests (
                        manifest_id TEXT PRIMARY KEY, run_id TEXT, source_table TEXT,
                        target_delta_path TEXT, row_count INTEGER, file_format TEXT,
                        file_size_bytes INTEGER, partition_info TEXT, ingested_at TEXT
                    );
                """))
        except Exception:
            pass

    def start_pipeline_run(self, pipeline_name: str, trigger_type: str = "MANUAL", metadata: Optional[Dict] = None):
        """
        Registers a new pipeline run in retaillake_metadata.pipeline_runs.
        """
        now = datetime.now()
        engine = self.get_engine()
        if engine.dialect.name == "sqlite":
            sql = text("""
                INSERT OR REPLACE INTO pipeline_runs 
                    (run_id, pipeline_name, trigger_type, start_time, status, execution_metadata)
                VALUES 
                    (:run_id, :pipeline_name, :trigger_type, :start_time, 'RUNNING', :execution_metadata);
            """)
        else:
            sql = text("""
                INSERT INTO pipeline_runs 
                    (run_id, pipeline_name, trigger_type, start_time, status, execution_metadata)
                VALUES 
                    (:run_id, :pipeline_name, :trigger_type, :start_time, 'RUNNING', :execution_metadata)
                ON DUPLICATE KEY UPDATE
                    status = 'RUNNING';
            """)
        try:
            with engine.begin() as conn:
                conn.execute(sql, {
                    "run_id": self.run_id,
                    "pipeline_name": pipeline_name,
                    "trigger_type": trigger_type,
                    "start_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "execution_metadata": json.dumps(metadata or {})
                })
        except Exception as e:
            logger.warning(f"Could not record pipeline run start to MySQL metadata: {e}")

    def finish_pipeline_run(
        self,
        status: str = "SUCCESS",
        records_ingested: int = 0,
        records_transformed: int = 0,
        records_published: int = 0,
        error_message: Optional[str] = None
    ):
        """
        Updates the pipeline run with completion status, duration, and processed metrics.
        """
        end_time = datetime.now()
        sql = text("""
            UPDATE pipeline_runs
            SET 
                end_time = :end_time,
                duration_seconds = :duration_seconds,
                status = :status,
                records_ingested = :records_ingested,
                records_transformed = :records_transformed,
                records_published = :records_published,
                error_message = :error_message
            WHERE run_id = :run_id;
        """)
        try:
            engine = self.get_engine()
            duration = 1.0
            try:
                with engine.connect() as conn:
                    st_row = conn.execute(
                        text("SELECT start_time FROM pipeline_runs WHERE run_id = :run_id"),
                        {"run_id": self.run_id}
                    ).fetchone()
                    if st_row and st_row[0]:
                        st_dt = datetime.fromisoformat(str(st_row[0])) if isinstance(st_row[0], str) else st_row[0]
                        duration = max(0.1, (end_time - st_dt).total_seconds())
            except Exception:
                duration = 1.0

            with engine.begin() as conn:
                conn.execute(sql, {
                    "run_id": self.run_id,
                    "end_time": end_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "duration_seconds": duration,
                    "status": status,
                    "records_ingested": records_ingested,
                    "records_transformed": records_transformed,
                    "records_published": records_published,
                    "error_message": error_message
                })
        except Exception as e:
            logger.warning(f"Could not finish pipeline run in MySQL metadata: {e}")

    def log(self, level: str, component: str, action: str, message: str, details: Optional[Dict] = None):
        """
        Appends an event to retaillake_metadata.audit_logs.
        """
        logger.info(f"[{level}] [{component}] {action}: {message}")
        sql = text("""
            INSERT INTO audit_logs (run_id, timestamp, log_level, component, action, message, details_json)
            VALUES (:run_id, :timestamp, :log_level, :component, :action, :message, :details_json)
        """)
        try:
            with self.get_engine().begin() as conn:
                conn.execute(sql, {
                    "run_id": self.run_id,
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "log_level": level.upper(),
                    "component": component,
                    "action": action,
                    "message": message,
                    "details_json": json.dumps(details or {})
                })
        except Exception as e:
            logger.debug(f"Audit log write bypassed: {e}")

    def record_quality_check(
        self,
        layer: str,
        table_name: str,
        rule_name: str,
        rule_type: str,
        records_tested: int,
        records_passed: int,
        records_failed: int,
        status: str,
        details: Optional[str] = None
    ):
        """
        Stores automated data quality test outcomes in retaillake_metadata.data_quality_results.
        """
        sql = text("""
            INSERT INTO data_quality_results 
                (run_id, layer, table_name, rule_name, rule_type, records_tested, records_passed, records_failed, status, details, checked_at)
            VALUES 
                (:run_id, :layer, :table_name, :rule_name, :rule_type, :records_tested, :records_passed, :records_failed, :status, :details, :checked_at)
        """)
        try:
            with self.get_engine().begin() as conn:
                conn.execute(sql, {
                    "run_id": self.run_id,
                    "layer": layer.upper(),
                    "table_name": table_name,
                    "rule_name": rule_name,
                    "rule_type": rule_type,
                    "records_tested": records_tested,
                    "records_passed": records_passed,
                    "records_failed": records_failed,
                    "status": status.upper(),
                    "details": details,
                    "checked_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                })
        except Exception as e:
            logger.debug(f"Quality check write bypassed: {e}")

    def record_manifest(self, manifest: Dict[str, Any]):
        """
        Stores Bronze/Silver ingestion file manifests.
        """
        sql = text("""
            INSERT INTO ingestion_manifests 
                (manifest_id, run_id, source_table, target_delta_path, row_count, file_format, file_size_bytes, partition_info, ingested_at)
            VALUES 
                (:manifest_id, :run_id, :source_table, :target_delta_path, :row_count, :file_format, :file_size_bytes, :partition_info, :ingested_at)
        """)
        try:
            with self.get_engine().begin() as conn:
                conn.execute(sql, manifest)
        except Exception as e:
            logger.debug(f"Manifest write bypassed: {e}")
