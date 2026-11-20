"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
End-to-End Pipeline Execution CLI Runner
Data Flow:
Retail Sources -> Bronze Parquet -> Silver Parquet -> Gold Parquet -> MySQL Serving Layer -> Audit
"""

import sys
import uuid
import time
import logging
from pathlib import Path

# Add project root to path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from src.serving.audit_logger import LakehouseAuditLogger
from src.source_simulation.seed_source_db import seed_source_database
from src.lakehouse.bronze.ingest_source_to_bronze import BronzeIngestionEngine
from src.lakehouse.silver.transform_bronze_to_silver import SilverTransformationEngine
from src.lakehouse.gold.aggregate_silver_to_gold import GoldAggregationEngine
from src.serving.publisher import ServingPublisher
from src.serving.query_analyzer import MySQLQueryAnalyzer
from src.serving.incremental_loader import IncrementalCheckpointer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("retaillake.runner")


def run_full_pipeline(run_id: str = None) -> bool:
    run_id = run_id or f"run-{int(time.time())}"
    logger.info(f"================================================================")
    logger.info(f"Starting RetailLake 360 End-to-End Pipeline [Run ID: {run_id}]")
    logger.info(f"Database Engine: MySQL 8.4 LTS | Lakehouse Storage: Parquet")
    logger.info(f"================================================================")

    audit_logger = LakehouseAuditLogger(run_id=run_id)
    audit_logger.start_pipeline_run(pipeline_name="retaillake360_end_to_end", trigger_type="CLI")

    start_time = time.time()
    records_ingested = 0
    records_transformed = 0
    records_published = 0

    try:
        # Phase 1: Source Simulation & Seeding
        logger.info("\n--- Phase 1: Seeding MySQL Source Database (retaillake_source) ---")
        seed_counts = seed_source_database()
        logger.info(f"Phase 1 complete: {seed_counts}")

        # Phase 2: Bronze Ingestion
        logger.info("\n--- Phase 2: Ingesting to Bronze Parquet (/lakehouse/bronze) ---")
        bronze_engine = BronzeIngestionEngine(run_id=run_id)
        bronze_results = bronze_engine.run_all()
        records_ingested = bronze_results["total_rows"]
        logger.info(f"Phase 2 complete: {records_ingested} records ingested.")

        # Phase 3: Silver Transformation & Data Quality
        logger.info("\n--- Phase 3: Transforming to Silver Parquet (/lakehouse/silver) ---")
        silver_engine = SilverTransformationEngine(run_id=run_id)
        silver_summary = silver_engine.transform_all()
        records_transformed = sum(silver_summary.values())
        logger.info(f"Phase 3 complete: {records_transformed} records modeled into Star Schema.")

        # Phase 4: Gold Aggregations
        logger.info("\n--- Phase 4: Computing Gold Aggregated Datamarts (/lakehouse/gold) ---")
        gold_engine = GoldAggregationEngine(run_id=run_id)
        gold_models = gold_engine.compute_all_gold_models()
        logger.info(f"Phase 4 complete: {len(gold_models)} Gold datamarts generated.")

        # Phase 5: MySQL Serving Layer Publishing
        logger.info("\n--- Phase 5: Publishing Gold Datasets to MySQL Serving (retaillake_serving) ---")
        publisher = ServingPublisher(run_id=run_id)
        publish_counts = publisher.publish_all()
        records_published = sum(publish_counts.values())
        logger.info(f"Phase 5 complete: {records_published} records published via idempotent upsert.")

        # Phase 6: Query Performance Analysis (EXPLAIN ANALYZE)
        logger.info("\n--- Phase 6: Running EXPLAIN ANALYZE on MySQL Serving Queries ---")
        analyzer = MySQLQueryAnalyzer()
        benchmarks = analyzer.run_standard_benchmarks()
        for b in benchmarks:
            logger.info(f"Query Benchmark [{b.get('benchmark_label')}]: {b.get('status')}")

        # Phase 7: Watermark Checkpointing & Audit Finalization
        checkpointer = IncrementalCheckpointer()
        checkpointer.update_checkpoint(
            checkpoint_key="pipeline_master_run",
            pipeline_name="retaillake360_end_to_end",
            layer="SERVING",
            table_name="all",
            last_processed_timestamp=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time)),
            records_count=records_published
        )

        elapsed = time.time() - start_time
        audit_logger.finish_pipeline_run(
            status="SUCCESS",
            records_ingested=records_ingested,
            records_transformed=records_transformed,
            records_published=records_published
        )

        logger.info(f"================================================================")
        logger.info(f"🎉 Pipeline Run SUCCESSFUL in {elapsed:.2f} seconds!")
        logger.info(f"Ingested: {records_ingested} | Transformed: {records_transformed} | Published: {records_published}")
        logger.info(f"================================================================")
        return True

    except Exception as e:
        logger.error(f"❌ Pipeline Run FAILED: {e}", exc_info=True)
        audit_logger.finish_pipeline_run(
            status="FAILED",
            records_ingested=records_ingested,
            records_transformed=records_transformed,
            records_published=records_published,
            error_message=str(e)
        )
        return False


if __name__ == "__main__":
    run_full_pipeline()
