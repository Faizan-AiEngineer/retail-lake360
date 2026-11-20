"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Dashboard Queries & Read-Only Serving Layer Tests
Verifies read-only analytical queries, dashboard metrics, and EXPLAIN ANALYZE execution.
"""

import pytest
from src.dashboard.queries import DashboardQueryService
from src.serving.query_analyzer import MySQLQueryAnalyzer


def test_dashboard_query_service_read_methods(sqlite_serving_engine):
    """Verifies that all dashboard reporting methods execute cleanly against serving schema."""
    service = DashboardQueryService(engine=sqlite_serving_engine)

    kpis = service.get_executive_kpis()
    assert "total_revenue" in kpis
    assert "total_orders" in kpis
    assert "gross_margin_pct" in kpis

    sales_df = service.get_sales_trends()
    assert not sales_df.empty
    assert "revenue" in sales_df.columns

    stores_df = service.get_store_leaderboard()
    assert not stores_df.empty
    assert "store_name" in stores_df.columns

    rfm_df = service.get_rfm_segments()
    assert not rfm_df.empty
    assert "rfm_segment" in rfm_df.columns

    cat_df = service.get_category_profitability()
    assert not cat_df.empty
    assert "category" in cat_df.columns

    inv_df = service.get_inventory_alerts()
    assert not inv_df.empty
    assert "stock_status" in inv_df.columns


def test_query_analyzer_diagnostics():
    """Tests the MySQL EXPLAIN ANALYZE diagnostic wrapper."""
    analyzer = MySQLQueryAnalyzer()
    report = analyzer.analyze_query("SELECT 1 AS num")
    assert report["status"] in ("SUCCESS", "FALLBACK_MOCK")
    assert "query" in report
