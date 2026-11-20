"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Serving Layer Publisher Integration Tests
Verifies idempotent loading, batch upserts, and repeatability into MySQL serving layer.
"""

import pytest
from sqlalchemy import text
from src.lakehouse.bronze.ingest_source_to_bronze import BronzeIngestionEngine
from src.lakehouse.silver.transform_bronze_to_silver import SilverTransformationEngine
from src.lakehouse.gold.aggregate_silver_to_gold import GoldAggregationEngine
from src.serving.publisher import ServingPublisher


def test_publisher_idempotency_and_loading(test_temp_lakehouse_dir, sqlite_source_engine, sqlite_serving_engine):
    """
    Verifies that publishing Gold datasets to MySQL serving is reliable, idempotent,
    and can be safely executed multiple times without duplicates or key violations.
    """
    # 1. Ingest, transform, and aggregate into test lakehouse directory
    bronze_engine = BronzeIngestionEngine(run_id="test-pub-prep", source_engine=sqlite_source_engine)
    bronze_engine.run_all()
    silver_engine = SilverTransformationEngine(run_id="test-pub-prep")
    silver_engine.transform_all()
    gold_engine = GoldAggregationEngine(run_id="test-pub-prep")
    gold_engine.compute_all_gold_models()

    # 2. Publish
    publisher = ServingPublisher(run_id="test-publisher-run", serving_engine=sqlite_serving_engine)

    # First publish run
    counts_run_1 = publisher.publish_all()
    assert counts_run_1["gold_daily_sales_summary"] > 0
    assert counts_run_1["gold_store_performance_analytics"] > 0
    assert counts_run_1["gold_customer_rfm_segments"] > 0

    with sqlite_serving_engine.connect() as conn:
        initial_stores = conn.execute(text("SELECT COUNT(*) FROM gold_store_performance_analytics")).scalar()
        assert initial_stores > 0

    # Second publish run (verifying full idempotency)
    counts_run_2 = publisher.publish_all()
    assert counts_run_1 == counts_run_2

    with sqlite_serving_engine.connect() as conn:
        final_stores = conn.execute(text("SELECT COUNT(*) FROM gold_store_performance_analytics")).scalar()
        # Row count should remain constant after second publish run (no duplicate row creation)
        assert final_stores == initial_stores
