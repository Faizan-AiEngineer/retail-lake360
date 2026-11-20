"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Incremental Loader & Watermark Checkpointer (MySQL retaillake_metadata)
Enables safe incremental ingestion and idempotent delta loads based on timestamp watermarks.
"""

import logging
from datetime import datetime
from typing import Optional, Dict, Any
from sqlalchemy import text

from config.settings import settings
from config.database import get_mysql_engine

logger = logging.getLogger("retaillake.checkpoint")


class IncrementalCheckpointer:
    def __init__(self, metadata_engine=None):
        self._engine = metadata_engine

    def get_engine(self):
        if self._engine is None:
            self._engine = get_mysql_engine(role="pipeline", db_name=settings.MYSQL_METADATA_DB)
        return self._engine

    def get_checkpoint(self, checkpoint_key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieves the last processed watermark for a table or layer.
        """
        sql = text("""
            SELECT checkpoint_key, pipeline_name, layer, table_name, 
                   last_processed_timestamp, last_processed_id, records_count, data_checksum, updated_at
            FROM pipeline_checkpoints
            WHERE checkpoint_key = :checkpoint_key
        """)
        try:
            with self.get_engine().connect() as conn:
                row = conn.execute(sql, {"checkpoint_key": checkpoint_key}).mappings().fetchone()
                return dict(row) if row else None
        except Exception as e:
            logger.warning(f"Could not read checkpoint {checkpoint_key}: {e}")
            return None

    def update_checkpoint(
        self,
        checkpoint_key: str,
        pipeline_name: str,
        layer: str,
        table_name: str,
        last_processed_timestamp: datetime,
        last_processed_id: int = 0,
        records_count: int = 0,
        data_checksum: Optional[str] = None
    ) -> bool:
        """
        Atomically records an incremental watermark in retaillake_metadata.pipeline_checkpoints.
        """
        engine = self.get_engine()
        if engine.dialect.name == "sqlite":
            sql = text("""
                INSERT OR REPLACE INTO pipeline_checkpoints 
                    (checkpoint_key, pipeline_name, layer, table_name, last_processed_timestamp, 
                     last_processed_id, records_count, data_checksum, updated_at)
                VALUES 
                    (:checkpoint_key, :pipeline_name, :layer, :table_name, :last_processed_timestamp, 
                     :last_processed_id, :records_count, :data_checksum, CURRENT_TIMESTAMP);
            """)
        else:
            sql = text("""
                INSERT INTO pipeline_checkpoints 
                    (checkpoint_key, pipeline_name, layer, table_name, last_processed_timestamp, 
                     last_processed_id, records_count, data_checksum, updated_at)
                VALUES 
                    (:checkpoint_key, :pipeline_name, :layer, :table_name, :last_processed_timestamp, 
                     :last_processed_id, :records_count, :data_checksum, CURRENT_TIMESTAMP)
                ON DUPLICATE KEY UPDATE
                    last_processed_timestamp = VALUES(last_processed_timestamp),
                    last_processed_id = VALUES(last_processed_id),
                    records_count = VALUES(records_count),
                    data_checksum = VALUES(data_checksum),
                    updated_at = CURRENT_TIMESTAMP;
            """)
        if hasattr(last_processed_timestamp, "strftime"):
            ts_str = last_processed_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        else:
            ts_str = str(last_processed_timestamp)

        try:
            with engine.begin() as conn:
                conn.execute(sql, {
                    "checkpoint_key": checkpoint_key,
                    "pipeline_name": pipeline_name,
                    "layer": layer.upper(),
                    "table_name": table_name,
                    "last_processed_timestamp": ts_str,
                    "last_processed_id": last_processed_id,
                    "records_count": records_count,
                    "data_checksum": data_checksum
                })
            logger.info(f"Updated checkpoint '{checkpoint_key}' with timestamp {last_processed_timestamp}")
            return True
        except Exception as e:
            logger.error(f"Failed to update checkpoint {checkpoint_key}: {e}")
            return False
