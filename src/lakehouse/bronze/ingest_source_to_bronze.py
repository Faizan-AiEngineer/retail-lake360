"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Bronze Layer Ingestion Engine
Extracts raw data from MySQL retaillake_source, appends Bronze lineage metadata,
and persists raw datasets as Parquet files.
"""

import os
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import text

from config.settings import settings
from config.database import get_mysql_engine
from src.serving.audit_logger import LakehouseAuditLogger

logger = logging.getLogger("retaillake.bronze")

SOURCE_TABLES = ["customers", "stores", "products", "orders", "order_items", "inventory"]


class BronzeIngestionEngine:
    def __init__(self, run_id: str = None, source_engine=None):
        self.run_id = run_id or str(uuid.uuid4())
        self.source_engine = source_engine or get_mysql_engine(role="pipeline", db_name=settings.MYSQL_SOURCE_DB)
        self.audit_logger = LakehouseAuditLogger(run_id=self.run_id)
        os.makedirs(settings.LAKEHOUSE_BRONZE_DIR, exist_ok=True)

    def ingest_table(self, table_name: str) -> Dict[str, Any]:
        """
        Extracts table from MySQL retaillake_source, enriches with Bronze metadata,
        and saves the result as a Bronze Parquet file.
        """
        ingest_time = datetime.now()
        logger.info(f"[{self.run_id}] Ingesting source table: {table_name}")

        query = f"SELECT * FROM `{table_name}`"
        with self.source_engine.connect() as conn:
            df = pd.read_sql(text(query), conn)

        row_count = len(df)

        # Append Bronze Medallion lineage metadata
        df["_bronze_ingested_at"] = ingest_time.isoformat()
        df["_bronze_source_db"] = settings.MYSQL_SOURCE_DB
        df["_bronze_source_table"] = table_name
        df["_bronze_batch_id"] = self.run_id

        # Target path for the Bronze Parquet file
        target_path = os.path.join(settings.LAKEHOUSE_BRONZE_DIR, table_name)
        os.makedirs(target_path, exist_ok=True)
        file_path = os.path.join(target_path, f"{table_name}_part0.parquet")
        df.to_parquet(file_path, index=False, engine="pyarrow")

        file_size_bytes = os.path.getsize(file_path)

        # Record ingestion manifest in retaillake_metadata
        manifest = {
            "manifest_id": str(uuid.uuid4()),
            "run_id": self.run_id,
            "source_table": table_name,
            "target_delta_path": target_path,
            "row_count": row_count,
            "file_format": "PARQUET_DELTA",
            "file_size_bytes": file_size_bytes,
            "partition_info": None,
            "ingested_at": ingest_time.strftime("%Y-%m-%d %H:%M:%S")
        }
        self.audit_logger.record_manifest(manifest)
        self.audit_logger.log(
            level="INFO",
            component="BronzeIngest",
            action=f"ingest_{table_name}",
            message=f"Ingested {row_count} rows from {table_name} into the Bronze Parquet layer."
        )

        return {
            "table_name": table_name,
            "rows": row_count,
            "target_path": target_path,
            "size_bytes": file_size_bytes
        }

    def run_all(self) -> Dict[str, Any]:
        """
        Ingests all configured source tables into Bronze.
        """
        results = {}
        total_rows = 0
        for tbl in SOURCE_TABLES:
            res = self.ingest_table(tbl)
            results[tbl] = res
            total_rows += res["rows"]

        logger.info(f"Bronze ingestion complete: {total_rows} total rows across {len(SOURCE_TABLES)} tables.")
        return {"run_id": self.run_id, "tables": results, "total_rows": total_rows}


if __name__ == "__main__":
    engine = BronzeIngestionEngine()
    engine.run_all()
