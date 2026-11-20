"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
MySQL 8.4 LTS Query Performance Analyzer (EXPLAIN ANALYZE Engine)
Benchmarks, profiles, and analyzes execution plans for analytical queries on retaillake_serving.
"""

import logging
from typing import Dict, Any, List
from sqlalchemy import text

from config.settings import settings
from config.database import get_mysql_engine

logger = logging.getLogger("retaillake.analyzer")


class MySQLQueryAnalyzer:
    def __init__(self, engine=None):
        self.engine = engine or get_mysql_engine(role="analytics", db_name=settings.MYSQL_SERVING_DB)

    def analyze_query(self, query: str, params: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Runs EXPLAIN ANALYZE on a query using MySQL 8.4 syntax (or EXPLAIN QUERY PLAN on SQLite).
        Returns the execution plan and performance metrics.
        """
        is_sqlite = self.engine.dialect.name == "sqlite"
        explain_query = f"EXPLAIN QUERY PLAN {query}" if is_sqlite else f"EXPLAIN ANALYZE {query}"
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(explain_query), params or {})
                if is_sqlite:
                    plan_rows = [f"Detail: {row[-1]}" for row in result.fetchall()]
                else:
                    plan_rows = [row[0] for row in result.fetchall()]
                plan_output = "\n".join(plan_rows)

                # Diagnostic checks on plan output
                is_index_used = "Index lookup" in plan_output or "covering index" in plan_output or "Index scan" in plan_output or "USING INDEX" in plan_output
                has_full_table_scan = "Table scan" in plan_output or "SCAN" in plan_output

                return {
                    "query": query,
                    "explain_analyze_plan": plan_output,
                    "uses_index": is_index_used,
                    "full_table_scan_detected": has_full_table_scan,
                    "status": "SUCCESS"
                }
        except Exception as e:
            logger.warning(f"EXPLAIN evaluation note: {e}")
            return {
                "query": query,
                "error": str(e),
                "status": "FALLBACK_MOCK"
            }

    def run_standard_benchmarks(self) -> List[Dict[str, Any]]:
        """
        Analyzes standard queries executed by the Streamlit dashboard on retaillake_serving.
        """
        test_queries = [
            (
                "Executive KPI Aggregation",
                "SELECT * FROM vw_executive_kpis"
            ),
            (
                "Top 10 Profitable Product Categories",
                "SELECT category, sub_category, total_revenue, gross_profit, gross_margin_pct "
                "FROM gold_product_category_profitability ORDER BY gross_profit DESC LIMIT 10"
            ),
            (
                "Daily Sales Trend (Indexed by Date & Store)",
                "SELECT report_date, store_type, SUM(total_revenue) AS revenue "
                "FROM gold_daily_sales_summary "
                "WHERE report_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY) "
                "GROUP BY report_date, store_type ORDER BY report_date ASC"
            ),
            (
                "Customer Segmentation Distribution",
                "SELECT rfm_segment, COUNT(*) as cust_cnt, SUM(monetary_total) as total_spend "
                "FROM gold_customer_rfm_segments GROUP BY rfm_segment ORDER BY total_spend DESC"
            ),
            (
                "Critical Inventory Alerts",
                "SELECT store_name, product_name, stock_on_hand, reorder_threshold, stock_status "
                "FROM gold_inventory_turnover_alerts WHERE stock_status = 'CRITICAL' LIMIT 20"
            )
        ]

        reports = []
        for label, q in test_queries:
            analysis = self.analyze_query(q)
            analysis["benchmark_label"] = label
            reports.append(analysis)
        return reports


if __name__ == "__main__":
    analyzer = MySQLQueryAnalyzer()
    for report in analyzer.run_standard_benchmarks():
        print(f"\n--- {report.get('benchmark_label')} ---")
        print(report.get("explain_analyze_plan", report.get("error")))
