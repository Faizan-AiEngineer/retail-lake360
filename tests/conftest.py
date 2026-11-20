"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Pytest Fixtures & Test Database Configuration
"""

import os
import shutil
import sys
import tempfile
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

# Unit and integration tests use non-production credentials unless overridden.
for variable in (
    "MYSQL_ROOT_PASSWORD",
    "MYSQL_SOURCE_PASSWORD",
    "MYSQL_PIPELINE_PASSWORD",
    "MYSQL_ANALYTICS_PASSWORD",
):
    os.environ.setdefault(variable, "test-only-not-a-real-credential")

# Add workspace to sys.path
sys.path.append(str(Path(__file__).resolve().parent.parent))

from config.settings import settings
from src.source_simulation.data_generator import RetailDataGenerator


@pytest.fixture(scope="session")
def test_temp_lakehouse_dir():
    """Provides an isolated directory for Bronze, Silver, and Gold Parquet tests."""
    temp_dir = tempfile.mkdtemp(prefix="retaillake_test_")
    old_bronze = settings.LAKEHOUSE_BRONZE_DIR
    old_silver = settings.LAKEHOUSE_SILVER_DIR
    old_gold = settings.LAKEHOUSE_GOLD_DIR
    old_base = settings.LAKEHOUSE_STORAGE_BASE

    settings.LAKEHOUSE_STORAGE_BASE = temp_dir
    settings.LAKEHOUSE_BRONZE_DIR = os.path.join(temp_dir, "bronze")
    settings.LAKEHOUSE_SILVER_DIR = os.path.join(temp_dir, "silver")
    settings.LAKEHOUSE_GOLD_DIR = os.path.join(temp_dir, "gold")

    yield temp_dir

    # Cleanup
    settings.LAKEHOUSE_STORAGE_BASE = old_base
    settings.LAKEHOUSE_BRONZE_DIR = old_bronze
    settings.LAKEHOUSE_SILVER_DIR = old_silver
    settings.LAKEHOUSE_GOLD_DIR = old_gold
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture(scope="session")
def sqlite_source_engine():
    """In-memory SQLite engine simulating retaillake_source with seeded records."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE stores (
                store_id INTEGER PRIMARY KEY,
                store_code TEXT UNIQUE,
                store_name TEXT,
                store_type TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                sqft_area INTEGER,
                opening_date TEXT,
                is_active BOOLEAN
            );
        """))
        conn.execute(text("""
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                sku TEXT UNIQUE,
                product_name TEXT,
                category TEXT,
                sub_category TEXT,
                brand TEXT,
                unit_cost REAL,
                unit_price REAL,
                weight_kg REAL,
                is_active BOOLEAN
            );
        """))
        conn.execute(text("""
            CREATE TABLE customers (
                customer_id INTEGER PRIMARY KEY,
                customer_uid TEXT UNIQUE,
                first_name TEXT,
                last_name TEXT,
                email TEXT UNIQUE,
                phone TEXT,
                city TEXT,
                state TEXT,
                postal_code TEXT,
                country TEXT,
                registration_date TEXT,
                customer_segment TEXT,
                is_active BOOLEAN
            );
        """))
        conn.execute(text("""
            CREATE TABLE orders (
                order_id INTEGER PRIMARY KEY,
                order_number TEXT UNIQUE,
                customer_id INTEGER,
                store_id INTEGER,
                order_date TEXT,
                order_status TEXT,
                payment_method TEXT,
                subtotal_amount REAL,
                tax_amount REAL,
                shipping_fee REAL,
                discount_amount REAL,
                total_amount REAL,
                created_at TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE order_items (
                order_item_id INTEGER PRIMARY KEY,
                order_id INTEGER,
                product_id INTEGER,
                quantity INTEGER,
                unit_price REAL,
                unit_cost REAL,
                discount_amount REAL,
                total_price REAL,
                created_at TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE inventory (
                inventory_id INTEGER PRIMARY KEY,
                store_id INTEGER,
                product_id INTEGER,
                stock_on_hand INTEGER,
                reorder_threshold INTEGER,
                reorder_quantity INTEGER,
                last_restock_date TEXT
            );
        """))

    # Seed with initial records
    gen = RetailDataGenerator(seed=42)
    stores = gen.generate_stores()
    products = gen.generate_products()
    customers = gen.generate_customers(count=50)
    orders, items = gen.generate_orders_and_items(customers=customers, stores=stores, products=products, order_count=60)
    inventory = gen.generate_inventory(stores=stores, products=products)

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO stores (store_id, store_code, store_name, store_type, city, state, country, sqft_area, opening_date, is_active)
            VALUES (:store_id, :store_code, :store_name, :store_type, :city, :state, :country, :sqft_area, :opening_date, :is_active)
        """), stores)
        conn.execute(text("""
            INSERT INTO products (product_id, sku, product_name, category, sub_category, brand, unit_cost, unit_price, weight_kg, is_active)
            VALUES (:product_id, :sku, :product_name, :category, :sub_category, :brand, :unit_cost, :unit_price, :weight_kg, :is_active)
        """), products)
        conn.execute(text("""
            INSERT INTO customers (customer_id, customer_uid, first_name, last_name, email, phone, city, state, postal_code, country, registration_date, customer_segment, is_active)
            VALUES (:customer_id, :customer_uid, :first_name, :last_name, :email, :phone, :city, :state, :postal_code, :country, :registration_date, :customer_segment, :is_active)
        """), customers)
        conn.execute(text("""
            INSERT INTO orders (order_id, order_number, customer_id, store_id, order_date, order_status, payment_method, subtotal_amount, tax_amount, shipping_fee, discount_amount, total_amount, created_at)
            VALUES (:order_id, :order_number, :customer_id, :store_id, :order_date, :order_status, :payment_method, :subtotal_amount, :tax_amount, :shipping_fee, :discount_amount, :total_amount, :created_at)
        """), orders)
        conn.execute(text("""
            INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, unit_cost, discount_amount, total_price, created_at)
            VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :unit_cost, :discount_amount, :total_price, :created_at)
        """), items)
        conn.execute(text("""
            INSERT INTO inventory (inventory_id, store_id, product_id, stock_on_hand, reorder_threshold, reorder_quantity, last_restock_date)
            VALUES (:inventory_id, :store_id, :product_id, :stock_on_hand, :reorder_threshold, :reorder_quantity, :last_restock_date)
        """), inventory)

    return engine


@pytest.fixture(scope="session")
def sqlite_serving_engine():
    """In-memory SQLite engine simulating retaillake_serving."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE gold_daily_sales_summary (
                summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                report_date TEXT,
                store_id INTEGER,
                store_name TEXT,
                store_type TEXT,
                total_orders INTEGER,
                total_units_sold INTEGER,
                total_revenue REAL,
                total_cogs REAL,
                gross_profit REAL,
                gross_margin_pct REAL,
                avg_order_value REAL,
                distinct_customers INTEGER,
                published_at TEXT,
                UNIQUE(report_date, store_id)
            );
        """))
        conn.execute(text("""
            CREATE TABLE gold_monthly_sales_summary (
                summary_id INTEGER PRIMARY KEY AUTOINCREMENT,
                year_month TEXT,
                store_id INTEGER,
                store_name TEXT,
                total_orders INTEGER,
                total_revenue REAL,
                total_cogs REAL,
                gross_profit REAL,
                gross_margin_pct REAL,
                avg_order_value REAL,
                published_at TEXT,
                UNIQUE(year_month, store_id)
            );
        """))
        conn.execute(text("""
            CREATE TABLE gold_customer_rfm_segments (
                customer_id INTEGER PRIMARY KEY,
                customer_uid TEXT,
                full_name TEXT,
                email TEXT,
                city TEXT,
                state TEXT,
                recency_days INTEGER,
                frequency_orders INTEGER,
                monetary_total REAL,
                r_score INTEGER,
                f_score INTEGER,
                m_score INTEGER,
                rfm_segment TEXT,
                estimated_clv REAL,
                churn_risk TEXT,
                first_purchase_date TEXT,
                last_purchase_date TEXT,
                published_at TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE gold_product_category_profitability (
                profitability_id INTEGER PRIMARY KEY AUTOINCREMENT,
                category TEXT,
                sub_category TEXT,
                total_units_sold INTEGER,
                total_revenue REAL,
                total_cogs REAL,
                gross_profit REAL,
                gross_margin_pct REAL,
                product_count INTEGER,
                published_at TEXT,
                UNIQUE(category, sub_category)
            );
        """))
        conn.execute(text("""
            CREATE TABLE gold_store_performance_analytics (
                store_id INTEGER PRIMARY KEY,
                store_code TEXT,
                store_name TEXT,
                store_type TEXT,
                city TEXT,
                state TEXT,
                sqft_area INTEGER,
                total_orders INTEGER,
                total_revenue REAL,
                gross_profit REAL,
                revenue_per_sqft REAL,
                active_customers INTEGER,
                revenue_rank INTEGER,
                published_at TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE gold_inventory_turnover_alerts (
                alert_id INTEGER PRIMARY KEY AUTOINCREMENT,
                store_id INTEGER,
                store_name TEXT,
                product_id INTEGER,
                sku TEXT,
                product_name TEXT,
                category TEXT,
                stock_on_hand INTEGER,
                reorder_threshold INTEGER,
                daily_run_rate REAL,
                estimated_days_of_stock REAL,
                stock_status TEXT,
                reorder_urgency TEXT,
                published_at TEXT,
                UNIQUE(store_id, product_id)
            );
        """))

        # Seed sample rows for query testing
        conn.execute(text("""
            INSERT INTO gold_daily_sales_summary 
                (report_date, store_id, store_name, store_type, total_orders, total_units_sold, total_revenue, total_cogs, gross_profit, gross_margin_pct, avg_order_value, distinct_customers)
            VALUES 
                ('2026-11-01', 1, 'Manhattan Flagship Store', 'Flagship', 25, 60, 8500.0, 3800.0, 4700.0, 55.29, 340.0, 22),
                ('2026-11-02', 1, 'Manhattan Flagship Store', 'Flagship', 30, 75, 10200.0, 4500.0, 5700.0, 55.88, 340.0, 28);
        """))
        conn.execute(text("""
            INSERT INTO gold_store_performance_analytics
                (store_id, store_code, store_name, store_type, city, state, sqft_area, total_orders, total_revenue, gross_profit, revenue_per_sqft, active_customers, revenue_rank)
            VALUES
                (1, 'STR-001', 'Manhattan Flagship Store', 'Flagship', 'New York', 'NY', 12000, 55, 18700.0, 10400.0, 1.56, 50, 1);
        """))
        conn.execute(text("""
            INSERT INTO gold_customer_rfm_segments
                (customer_id, customer_uid, full_name, email, city, state, recency_days, frequency_orders, monetary_total, r_score, f_score, m_score, rfm_segment, estimated_clv, churn_risk, first_purchase_date, last_purchase_date)
            VALUES
                (1, 'uid-1', 'John Doe', 'john@example.com', 'New York', 'NY', 10, 4, 1250.0, 5, 4, 4, 'Champions', 2400.0, 'Low', '2026-01-10', '2026-11-04');
        """))
        conn.execute(text("""
            INSERT INTO gold_product_category_profitability
                (category, sub_category, total_units_sold, total_revenue, total_cogs, gross_profit, gross_margin_pct, product_count)
            VALUES
                ('Electronics', 'Audio', 135, 20250.0, 8775.0, 11475.0, 56.67, 3);
        """))
        conn.execute(text("""
            INSERT INTO gold_inventory_turnover_alerts
                (store_id, store_name, product_id, sku, product_name, category, stock_on_hand, reorder_threshold, daily_run_rate, estimated_days_of_stock, stock_status, reorder_urgency)
            VALUES
                (1, 'Manhattan Flagship Store', 1, 'ELEC-101', 'UltraHD 4K Smart Monitor 27-inch', 'Electronics', 15, 20, 1.5, 10.0, 'WARNING', 'UPCOMING');
        """))

    return engine


@pytest.fixture(scope="session")
def sqlite_metadata_engine():
    """In-memory SQLite engine simulating retaillake_metadata."""
    engine = create_engine("sqlite:///:memory:", echo=False)
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE pipeline_runs (
                run_id TEXT PRIMARY KEY,
                pipeline_name TEXT,
                trigger_type TEXT,
                start_time TEXT,
                end_time TEXT,
                duration_seconds REAL,
                status TEXT,
                records_ingested INTEGER,
                records_transformed INTEGER,
                records_published INTEGER,
                error_message TEXT,
                execution_metadata TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE pipeline_checkpoints (
                checkpoint_key TEXT PRIMARY KEY,
                pipeline_name TEXT,
                layer TEXT,
                table_name TEXT,
                last_processed_timestamp TEXT,
                last_processed_id INTEGER,
                records_count INTEGER,
                data_checksum TEXT,
                updated_at TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE audit_logs (
                log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                timestamp TEXT,
                log_level TEXT,
                component TEXT,
                action TEXT,
                message TEXT,
                details_json TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE data_quality_results (
                check_id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_id TEXT,
                layer TEXT,
                table_name TEXT,
                rule_name TEXT,
                rule_type TEXT,
                records_tested INTEGER,
                records_passed INTEGER,
                records_failed INTEGER,
                status TEXT,
                details TEXT,
                checked_at TEXT
            );
        """))
        conn.execute(text("""
            CREATE TABLE ingestion_manifests (
                manifest_id TEXT PRIMARY KEY,
                run_id TEXT,
                source_table TEXT,
                target_delta_path TEXT,
                row_count INTEGER,
                file_format TEXT,
                file_size_bytes INTEGER,
                partition_info TEXT,
                ingested_at TEXT
            );
        """))
    return engine
