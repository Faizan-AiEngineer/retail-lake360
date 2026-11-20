"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Serving Layer Publisher (Gold Parquet -> MySQL retaillake_serving)
Executes idempotent, high-performance batch upserts using MySQL ON DUPLICATE KEY UPDATE.
"""

import os
import uuid
import logging
from typing import Dict, Any, List
import pandas as pd
from sqlalchemy import text

from config.settings import settings
from config.database import get_mysql_engine
from src.serving.audit_logger import LakehouseAuditLogger

logger = logging.getLogger("retaillake.publisher")


class ServingPublisher:
    def __init__(self, run_id: str = None, serving_engine=None):
        self.run_id = run_id or str(uuid.uuid4())
        self.serving_engine = serving_engine or get_mysql_engine(role="pipeline", db_name=settings.MYSQL_SERVING_DB)
        self.audit_logger = LakehouseAuditLogger(run_id=self.run_id)

    @property
    def is_sqlite(self) -> bool:
        return self.serving_engine.dialect.name == "sqlite"

    def _read_gold(self, table_name: str) -> pd.DataFrame:
        path = os.path.join(settings.LAKEHOUSE_GOLD_DIR, table_name, f"{table_name}_part0.parquet")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Gold table not found: {path}")
        df = pd.read_parquet(path)
        # Drop internal lakehouse columns before publishing to MySQL serving
        cols_to_drop = [c for c in df.columns if c.startswith("_gold_")]
        return df.drop(columns=cols_to_drop)

    def publish_daily_sales(self) -> int:
        df = self._read_gold("gold_daily_sales_summary")
        records = df.to_dict(orient="records")
        if not records:
            return 0

        if self.is_sqlite:
            with self.serving_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gold_daily_sales_summary (
                        summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        report_date TEXT, store_id INTEGER, store_name TEXT, store_type TEXT,
                        total_orders INTEGER, total_units_sold INTEGER, total_revenue REAL,
                        total_cogs REAL, gross_profit REAL, gross_margin_pct REAL,
                        avg_order_value REAL, distinct_customers INTEGER, published_at TEXT,
                        UNIQUE(report_date, store_id)
                    );
                """))
            sql = text("""
                INSERT OR REPLACE INTO gold_daily_sales_summary 
                    (report_date, store_id, store_name, store_type, total_orders, total_units_sold, 
                     total_revenue, total_cogs, gross_profit, gross_margin_pct, avg_order_value, distinct_customers)
                VALUES 
                    (:report_date, :store_id, :store_name, :store_type, :total_orders, :total_units_sold, 
                     :total_revenue, :total_cogs, :gross_profit, :gross_margin_pct, :avg_order_value, :distinct_customers);
            """)
        else:
            sql = text("""
                INSERT INTO gold_daily_sales_summary 
                    (report_date, store_id, store_name, store_type, total_orders, total_units_sold, 
                     total_revenue, total_cogs, gross_profit, gross_margin_pct, avg_order_value, distinct_customers)
                VALUES 
                    (:report_date, :store_id, :store_name, :store_type, :total_orders, :total_units_sold, 
                     :total_revenue, :total_cogs, :gross_profit, :gross_margin_pct, :avg_order_value, :distinct_customers)
                ON DUPLICATE KEY UPDATE
                    store_name = VALUES(store_name),
                    store_type = VALUES(store_type),
                    total_orders = VALUES(total_orders),
                    total_units_sold = VALUES(total_units_sold),
                    total_revenue = VALUES(total_revenue),
                    total_cogs = VALUES(total_cogs),
                    gross_profit = VALUES(gross_profit),
                    gross_margin_pct = VALUES(gross_margin_pct),
                    avg_order_value = VALUES(avg_order_value),
                    distinct_customers = VALUES(distinct_customers),
                    published_at = CURRENT_TIMESTAMP;
            """)
        with self.serving_engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def publish_monthly_sales(self) -> int:
        df = self._read_gold("gold_monthly_sales_summary")
        records = df.to_dict(orient="records")
        if not records:
            return 0

        if self.is_sqlite:
            with self.serving_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gold_monthly_sales_summary (
                        summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        year_month TEXT, store_id INTEGER, store_name TEXT, total_orders INTEGER,
                        total_revenue REAL, total_cogs REAL, gross_profit REAL, gross_margin_pct REAL,
                        avg_order_value REAL, published_at TEXT,
                        UNIQUE(year_month, store_id)
                    );
                """))
            sql = text("""
                INSERT OR REPLACE INTO gold_monthly_sales_summary 
                    (year_month, store_id, store_name, total_orders, total_revenue, total_cogs, gross_profit, gross_margin_pct, avg_order_value)
                VALUES 
                    (:year_month, :store_id, :store_name, :total_orders, :total_revenue, :total_cogs, :gross_profit, :gross_margin_pct, :avg_order_value);
            """)
        else:
            sql = text("""
                INSERT INTO gold_monthly_sales_summary 
                    (year_month, store_id, store_name, total_orders, total_revenue, total_cogs, gross_profit, gross_margin_pct, avg_order_value)
                VALUES 
                    (:year_month, :store_id, :store_name, :total_orders, :total_revenue, :total_cogs, :gross_profit, :gross_margin_pct, :avg_order_value)
                ON DUPLICATE KEY UPDATE
                    store_name = VALUES(store_name),
                    total_orders = VALUES(total_orders),
                    total_revenue = VALUES(total_revenue),
                    total_cogs = VALUES(total_cogs),
                    gross_profit = VALUES(gross_profit),
                    gross_margin_pct = VALUES(gross_margin_pct),
                    avg_order_value = VALUES(avg_order_value),
                    published_at = CURRENT_TIMESTAMP;
            """)
        with self.serving_engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def publish_customer_rfm(self) -> int:
        df = self._read_gold("gold_customer_rfm_segments")
        records = df.to_dict(orient="records")
        if not records:
            return 0

        if self.is_sqlite:
            with self.serving_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gold_customer_rfm_segments (
                        customer_id INTEGER PRIMARY KEY, customer_uid TEXT, full_name TEXT,
                        email TEXT, city TEXT, state TEXT, recency_days INTEGER,
                        frequency_orders INTEGER, monetary_total REAL, r_score INTEGER,
                        f_score INTEGER, m_score INTEGER, rfm_segment TEXT, estimated_clv REAL,
                        churn_risk TEXT, first_purchase_date TEXT, last_purchase_date TEXT, published_at TEXT
                    );
                """))
            sql = text("""
                INSERT OR REPLACE INTO gold_customer_rfm_segments
                    (customer_id, customer_uid, full_name, email, city, state, recency_days, frequency_orders, 
                     monetary_total, r_score, f_score, m_score, rfm_segment, estimated_clv, churn_risk, 
                     first_purchase_date, last_purchase_date)
                VALUES
                    (:customer_id, :customer_uid, :full_name, :email, :city, :state, :recency_days, :frequency_orders, 
                     :monetary_total, :r_score, :f_score, :m_score, :rfm_segment, :estimated_clv, :churn_risk, 
                     :first_purchase_date, :last_purchase_date);
            """)
        else:
            sql = text("""
                INSERT INTO gold_customer_rfm_segments
                    (customer_id, customer_uid, full_name, email, city, state, recency_days, frequency_orders, 
                     monetary_total, r_score, f_score, m_score, rfm_segment, estimated_clv, churn_risk, 
                     first_purchase_date, last_purchase_date)
                VALUES
                    (:customer_id, :customer_uid, :full_name, :email, :city, :state, :recency_days, :frequency_orders, 
                     :monetary_total, :r_score, :f_score, :m_score, :rfm_segment, :estimated_clv, :churn_risk, 
                     :first_purchase_date, :last_purchase_date)
                ON DUPLICATE KEY UPDATE
                    full_name = VALUES(full_name),
                    email = VALUES(email),
                    city = VALUES(city),
                    state = VALUES(state),
                    recency_days = VALUES(recency_days),
                    frequency_orders = VALUES(frequency_orders),
                    monetary_total = VALUES(monetary_total),
                    r_score = VALUES(r_score),
                    f_score = VALUES(f_score),
                    m_score = VALUES(m_score),
                    rfm_segment = VALUES(rfm_segment),
                    estimated_clv = VALUES(estimated_clv),
                    churn_risk = VALUES(churn_risk),
                    last_purchase_date = VALUES(last_purchase_date),
                    published_at = CURRENT_TIMESTAMP;
            """)
        with self.serving_engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def publish_product_category(self) -> int:
        df = self._read_gold("gold_product_category_profitability")
        records = df.to_dict(orient="records")
        if not records:
            return 0

        if self.is_sqlite:
            with self.serving_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gold_product_category_profitability (
                        profitability_id INTEGER PRIMARY KEY AUTOINCREMENT,
                        category TEXT, sub_category TEXT, total_units_sold INTEGER,
                        total_revenue REAL, total_cogs REAL, gross_profit REAL,
                        gross_margin_pct REAL, product_count INTEGER, published_at TEXT,
                        UNIQUE(category, sub_category)
                    );
                """))
            sql = text("""
                INSERT OR REPLACE INTO gold_product_category_profitability
                    (category, sub_category, total_units_sold, total_revenue, total_cogs, gross_profit, gross_margin_pct, product_count)
                VALUES
                    (:category, :sub_category, :total_units_sold, :total_revenue, :total_cogs, :gross_profit, :gross_margin_pct, :product_count);
            """)
        else:
            sql = text("""
                INSERT INTO gold_product_category_profitability
                    (category, sub_category, total_units_sold, total_revenue, total_cogs, gross_profit, gross_margin_pct, product_count)
                VALUES
                    (:category, :sub_category, :total_units_sold, :total_revenue, :total_cogs, :gross_profit, :gross_margin_pct, :product_count)
                ON DUPLICATE KEY UPDATE
                    total_units_sold = VALUES(total_units_sold),
                    total_revenue = VALUES(total_revenue),
                    total_cogs = VALUES(total_cogs),
                    gross_profit = VALUES(gross_profit),
                    gross_margin_pct = VALUES(gross_margin_pct),
                    product_count = VALUES(product_count),
                    published_at = CURRENT_TIMESTAMP;
            """)
        with self.serving_engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def publish_store_performance(self) -> int:
        df = self._read_gold("gold_store_performance_analytics")
        records = df.to_dict(orient="records")
        if not records:
            return 0

        if self.is_sqlite:
            with self.serving_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gold_store_performance_analytics (
                        store_id INTEGER PRIMARY KEY, store_code TEXT, store_name TEXT,
                        store_type TEXT, city TEXT, state TEXT, sqft_area INTEGER,
                        total_orders INTEGER, total_revenue REAL, gross_profit REAL,
                        revenue_per_sqft REAL, active_customers INTEGER, revenue_rank INTEGER, published_at TEXT
                    );
                """))
            sql = text("""
                INSERT OR REPLACE INTO gold_store_performance_analytics
                    (store_id, store_code, store_name, store_type, city, state, sqft_area, total_orders, 
                     total_revenue, gross_profit, revenue_per_sqft, active_customers, revenue_rank)
                VALUES
                    (:store_id, :store_code, :store_name, :store_type, :city, :state, :sqft_area, :total_orders, 
                     :total_revenue, :gross_profit, :revenue_per_sqft, :active_customers, :revenue_rank);
            """)
        else:
            sql = text("""
                INSERT INTO gold_store_performance_analytics
                    (store_id, store_code, store_name, store_type, city, state, sqft_area, total_orders, 
                     total_revenue, gross_profit, revenue_per_sqft, active_customers, revenue_rank)
                VALUES
                    (:store_id, :store_code, :store_name, :store_type, :city, :state, :sqft_area, :total_orders, 
                     :total_revenue, :gross_profit, :revenue_per_sqft, :active_customers, :revenue_rank)
                ON DUPLICATE KEY UPDATE
                    store_code = VALUES(store_code),
                    store_name = VALUES(store_name),
                    store_type = VALUES(store_type),
                    city = VALUES(city),
                    state = VALUES(state),
                    sqft_area = VALUES(sqft_area),
                    total_orders = VALUES(total_orders),
                    total_revenue = VALUES(total_revenue),
                    gross_profit = VALUES(gross_profit),
                    revenue_per_sqft = VALUES(revenue_per_sqft),
                    active_customers = VALUES(active_customers),
                    revenue_rank = VALUES(revenue_rank),
                    published_at = CURRENT_TIMESTAMP;
            """)
        with self.serving_engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def publish_inventory_alerts(self) -> int:
        df = self._read_gold("gold_inventory_turnover_alerts")
        records = df.to_dict(orient="records")
        if not records:
            return 0

        if self.is_sqlite:
            with self.serving_engine.begin() as conn:
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS gold_inventory_turnover_alerts (
                        alert_id INTEGER PRIMARY KEY AUTOINCREMENT, store_id INTEGER,
                        store_name TEXT, product_id INTEGER, sku TEXT, product_name TEXT,
                        category TEXT, stock_on_hand INTEGER, reorder_threshold INTEGER,
                        daily_run_rate REAL, estimated_days_of_stock REAL, stock_status TEXT,
                        reorder_urgency TEXT, published_at TEXT,
                        UNIQUE(store_id, product_id)
                    );
                """))
            sql = text("""
                INSERT OR REPLACE INTO gold_inventory_turnover_alerts
                    (store_id, store_name, product_id, sku, product_name, category, stock_on_hand, 
                     reorder_threshold, daily_run_rate, estimated_days_of_stock, stock_status, reorder_urgency)
                VALUES
                    (:store_id, :store_name, :product_id, :sku, :product_name, :category, :stock_on_hand, 
                     :reorder_threshold, :daily_run_rate, :estimated_days_of_stock, :stock_status, :reorder_urgency);
            """)
        else:
            sql = text("""
                INSERT INTO gold_inventory_turnover_alerts
                    (store_id, store_name, product_id, sku, product_name, category, stock_on_hand, 
                     reorder_threshold, daily_run_rate, estimated_days_of_stock, stock_status, reorder_urgency)
                VALUES
                    (:store_id, :store_name, :product_id, :sku, :product_name, :category, :stock_on_hand, 
                     :reorder_threshold, :daily_run_rate, :estimated_days_of_stock, :stock_status, :reorder_urgency)
                ON DUPLICATE KEY UPDATE
                    store_name = VALUES(store_name),
                    sku = VALUES(sku),
                    product_name = VALUES(product_name),
                    category = VALUES(category),
                    stock_on_hand = VALUES(stock_on_hand),
                    reorder_threshold = VALUES(reorder_threshold),
                    daily_run_rate = VALUES(daily_run_rate),
                    estimated_days_of_stock = VALUES(estimated_days_of_stock),
                    stock_status = VALUES(stock_status),
                    reorder_urgency = VALUES(reorder_urgency),
                    published_at = CURRENT_TIMESTAMP;
            """)
        with self.serving_engine.begin() as conn:
            conn.execute(sql, records)
        return len(records)

    def publish_all(self) -> Dict[str, int]:
        logger.info(f"[{self.run_id}] Publishing Gold datasets to MySQL retaillake_serving...")
        results = {
            "gold_daily_sales_summary": self.publish_daily_sales(),
            "gold_monthly_sales_summary": self.publish_monthly_sales(),
            "gold_customer_rfm_segments": self.publish_customer_rfm(),
            "gold_product_category_profitability": self.publish_product_category(),
            "gold_store_performance_analytics": self.publish_store_performance(),
            "gold_inventory_turnover_alerts": self.publish_inventory_alerts()
        }
        total_published = sum(results.values())
        self.audit_logger.log(
            level="INFO",
            component="ServingPublisher",
            action="publish_all_gold_tables",
            message=f"Successfully published {total_published} records across {len(results)} tables into MySQL serving."
        )
        return results


if __name__ == "__main__":
    ServingPublisher().publish_all()
