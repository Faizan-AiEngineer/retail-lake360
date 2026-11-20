"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Metadata, Watermark Checkpointing, and Audit Logging Integration Tests
"""

import pytest
from datetime import datetime
from sqlalchemy import text
from src.serving.audit_logger import LakehouseAuditLogger
from src.serving.incremental_loader import IncrementalCheckpointer


def test_pipeline_run_lifecycle_and_audit(sqlite_metadata_engine):
    """Tests recording pipeline start, audit logs, and completion metrics into retaillake_metadata."""
    run_id = "test-audit-run-99"
    logger = LakehouseAuditLogger(run_id=run_id, metadata_engine=sqlite_metadata_engine)

    # 1. Start run
    logger.start_pipeline_run(pipeline_name="test_pipeline", trigger_type="MANUAL")

    # 2. Append audit log
    logger.log(level="INFO", component="TestRunner", action="unit_test", message="Testing audit recording.")

    # 3. Record Data Quality check
    logger.record_quality_check(
        layer="SILVER",
        table_name="fct_orders",
        rule_name="not_null_order_id",
        rule_type="NOT_NULL",
        records_tested=100,
        records_passed=100,
        records_failed=0,
        status="PASSED"
    )

    # 4. Finish run
    logger.finish_pipeline_run(
        status="SUCCESS",
        records_ingested=100,
        records_transformed=100,
        records_published=50
    )

    # Assertions
    with sqlite_metadata_engine.connect() as conn:
        run_record = conn.execute(
            text("SELECT status, records_ingested, records_published FROM pipeline_runs WHERE run_id = :run_id"),
            {"run_id": run_id}
        ).mappings().fetchone()
        assert run_record["status"] == "SUCCESS"
        assert run_record["records_ingested"] == 100
        assert run_record["records_published"] == 50

        dq_count = conn.execute(
            text("SELECT COUNT(*) FROM data_quality_results WHERE run_id = :run_id AND status = 'PASSED'"),
            {"run_id": run_id}
        ).scalar()
        assert dq_count == 1


def test_incremental_watermark_checkpointing(sqlite_metadata_engine):
    """Tests updating and reading watermarks for incremental loading."""
    checkpointer = IncrementalCheckpointer(metadata_engine=sqlite_metadata_engine)
    now = datetime(2026, 11, 14, 11, 30, 0)

    # Update checkpoint
    updated = checkpointer.update_checkpoint(
        checkpoint_key="orders_delta_watermark",
        pipeline_name="incremental_sync",
        layer="SERVING",
        table_name="gold_daily_sales_summary",
        last_processed_timestamp=now,
        records_count=350
    )
    assert updated is True

    # Read checkpoint
    cp = checkpointer.get_checkpoint("orders_delta_watermark")
    assert cp is not None
    assert cp["checkpoint_key"] == "orders_delta_watermark"
    assert "2026-11-14 11:30:00" in cp["last_processed_timestamp"]
    assert cp["records_count"] == 350
