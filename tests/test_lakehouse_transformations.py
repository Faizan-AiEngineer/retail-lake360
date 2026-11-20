"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Lakehouse Transformations (Bronze -> Silver -> Gold) Integration Tests
"""

import os
import pytest
import pandas as pd

from src.lakehouse.bronze.ingest_source_to_bronze import BronzeIngestionEngine
from src.lakehouse.silver.transform_bronze_to_silver import SilverTransformationEngine
from src.lakehouse.silver.quality_rules import DataQualityValidator
from src.lakehouse.gold.aggregate_silver_to_gold import GoldAggregationEngine


def test_bronze_ingestion_and_metadata(test_temp_lakehouse_dir, sqlite_source_engine):
    """Tests extracting raw tables from source into Bronze Parquet storage."""
    engine = BronzeIngestionEngine(run_id="test-run-bronze", source_engine=sqlite_source_engine)

    # Ingest stores
    res = engine.ingest_table("stores")
    assert res["table_name"] == "stores"
    assert res["rows"] == 10
    assert os.path.exists(res["target_path"])

    # Read back bronze parquet file
    df = pd.read_parquet(os.path.join(res["target_path"], "stores_part0.parquet"))
    assert "_bronze_ingested_at" in df.columns
    assert "_bronze_source_table" in df.columns
    assert df["_bronze_source_table"].iloc[0] == "stores"
    assert "_bronze_batch_id" in df.columns


def test_data_quality_validator():
    """Tests the automated data quality check assertions."""
    validator = DataQualityValidator(run_id="dq-test")
    test_df = pd.DataFrame({
        "id": [1, 2, 3],
        "name": ["A", "B", "C"],
        "price": [10.5, 20.0, 30.0]
    })
    # Non-null check passes
    assert validator.validate_non_null(test_df, "test_table", "id") is True
    # Range check passes
    assert validator.validate_positive_numeric(test_df, "test_table", "price") is True

    # Check with nulls
    test_df_null = pd.DataFrame({"id": [1, None, 3]})
    assert validator.validate_non_null(test_df_null, "test_table", "id") is False


def test_end_to_end_medallion_lakehouse_flow(test_temp_lakehouse_dir, sqlite_source_engine):
    """
    Tests complete Medallion Lakehouse flow:
    Source Database -> Bronze Ingest -> Silver Modeling & Quality -> Gold Business Aggregations.
    """
    # Also ingest remaining tables for full flow
    bronze_engine = BronzeIngestionEngine(run_id="test-medallion-run", source_engine=sqlite_source_engine)
    for tbl in ["products", "customers", "orders", "order_items", "inventory"]:
        bronze_engine.ingest_table(tbl)

    # Transform Bronze to Silver
    silver_engine = SilverTransformationEngine(run_id="test-medallion-run")
    silver_summary = silver_engine.transform_all()

    assert "dim_customers" in silver_summary
    assert "dim_stores" in silver_summary
    assert "dim_products" in silver_summary
    assert "fct_orders" in silver_summary
    assert "fct_order_items" in silver_summary
    assert "fct_inventory" in silver_summary

    # Verify Silver dim_products has computed margins
    prods_df = pd.read_parquet(os.path.join(test_temp_lakehouse_dir, "silver", "dim_products", "dim_products_part0.parquet"))
    assert "margin_dollars" in prods_df.columns
    assert "margin_pct" in prods_df.columns
    assert (prods_df["margin_dollars"] > 0).all()

    # Aggregate Silver to Gold
    gold_engine = GoldAggregationEngine(run_id="test-medallion-run")
    gold_models = gold_engine.compute_all_gold_models()

    assert "gold_daily_sales_summary" in gold_models
    assert "gold_monthly_sales_summary" in gold_models
    assert "gold_customer_rfm_segments" in gold_models
    assert "gold_product_category_profitability" in gold_models
    assert "gold_store_performance_analytics" in gold_models
    assert "gold_inventory_turnover_alerts" in gold_models

    # Verify Gold aggregations math
    daily_sales = gold_models["gold_daily_sales_summary"]
    assert "total_revenue" in daily_sales.columns
    assert "gross_profit" in daily_sales.columns
    assert (daily_sales["total_revenue"] >= daily_sales["gross_profit"]).all()

    rfm = gold_models["gold_customer_rfm_segments"]
    assert "rfm_segment" in rfm.columns
    valid_segments = {"Champions", "Loyal Customers", "Recent Customers", "At Risk", "Hibernating", "Promising", "Need Attention"}
    assert set(rfm["rfm_segment"].unique()).issubset(valid_segments)
