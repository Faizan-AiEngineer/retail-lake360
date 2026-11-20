"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Source Database Seeder (MySQL 8.4 LTS / Standalone Engine)
Seeds retaillake_source with idempotent batch operations and transaction management.
"""

import logging
from typing import Dict, Any, List
from sqlalchemy import text
from config.database import get_mysql_engine
from src.source_simulation.data_generator import RetailDataGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("retaillake.seed_source")


def seed_source_database(
    n_customers: int = 150,
    n_orders: int = 400,
    engine=None
) -> Dict[str, int]:
    """
    Seeds retaillake_source with relational data.
    Uses parameterized SQL and MySQL ON DUPLICATE KEY UPDATE (or INSERT OR REPLACE) for full idempotency.
    """
    if engine is None:
        engine = get_mysql_engine(role="source", db_name="retaillake_source")

    is_sqlite = engine.dialect.name == "sqlite"

    generator = RetailDataGenerator()
    stores = generator.generate_stores()
    products = generator.generate_products()
    customers = generator.generate_customers(count=n_customers)
    orders, order_items = generator.generate_orders_and_items(
        customers=customers,
        stores=stores,
        products=products,
        order_count=n_orders
    )
    inventory = generator.generate_inventory(stores=stores, products=products)

    counts = {}

    with engine.begin() as conn:
        logger.info("Seeding stores...")
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS stores (
                    store_id INTEGER PRIMARY KEY, store_code TEXT UNIQUE, store_name TEXT, 
                    store_type TEXT, city TEXT, state TEXT, country TEXT, sqft_area INTEGER, 
                    opening_date TEXT, is_active BOOLEAN
                );
            """))
            store_sql = text("""
                INSERT OR REPLACE INTO stores (store_id, store_code, store_name, store_type, city, state, country, sqft_area, opening_date, is_active)
                VALUES (:store_id, :store_code, :store_name, :store_type, :city, :state, :country, :sqft_area, :opening_date, :is_active);
            """)
        else:
            store_sql = text("""
                INSERT INTO stores (store_id, store_code, store_name, store_type, city, state, country, sqft_area, opening_date, is_active)
                VALUES (:store_id, :store_code, :store_name, :store_type, :city, :state, :country, :sqft_area, :opening_date, :is_active)
                ON DUPLICATE KEY UPDATE
                    store_name = VALUES(store_name),
                    store_type = VALUES(store_type),
                    city = VALUES(city),
                    state = VALUES(state),
                    sqft_area = VALUES(sqft_area),
                    is_active = VALUES(is_active);
            """)
        conn.execute(store_sql, stores)
        counts["stores"] = len(stores)

        logger.info("Seeding products...")
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS products (
                    product_id INTEGER PRIMARY KEY, sku TEXT UNIQUE, product_name TEXT, 
                    category TEXT, sub_category TEXT, brand TEXT, unit_cost REAL, 
                    unit_price REAL, weight_kg REAL, is_active BOOLEAN
                );
            """))
            product_sql = text("""
                INSERT OR REPLACE INTO products (product_id, sku, product_name, category, sub_category, brand, unit_cost, unit_price, weight_kg, is_active)
                VALUES (:product_id, :sku, :product_name, :category, :sub_category, :brand, :unit_cost, :unit_price, :weight_kg, :is_active);
            """)
        else:
            product_sql = text("""
                INSERT INTO products (product_id, sku, product_name, category, sub_category, brand, unit_cost, unit_price, weight_kg, is_active)
                VALUES (:product_id, :sku, :product_name, :category, :sub_category, :brand, :unit_cost, :unit_price, :weight_kg, :is_active)
                ON DUPLICATE KEY UPDATE
                    product_name = VALUES(product_name),
                    unit_cost = VALUES(unit_cost),
                    unit_price = VALUES(unit_price),
                    is_active = VALUES(is_active);
            """)
        conn.execute(product_sql, products)
        counts["products"] = len(products)

        logger.info("Seeding customers...")
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id INTEGER PRIMARY KEY, customer_uid TEXT UNIQUE, first_name TEXT, 
                    last_name TEXT, email TEXT UNIQUE, phone TEXT, city TEXT, state TEXT, 
                    postal_code TEXT, country TEXT, registration_date TEXT, customer_segment TEXT, is_active BOOLEAN
                );
            """))
            customer_sql = text("""
                INSERT OR REPLACE INTO customers (customer_id, customer_uid, first_name, last_name, email, phone, city, state, postal_code, country, registration_date, customer_segment, is_active)
                VALUES (:customer_id, :customer_uid, :first_name, :last_name, :email, :phone, :city, :state, :postal_code, :country, :registration_date, :customer_segment, :is_active);
            """)
        else:
            customer_sql = text("""
                INSERT INTO customers (customer_id, customer_uid, first_name, last_name, email, phone, city, state, postal_code, country, registration_date, customer_segment, is_active)
                VALUES (:customer_id, :customer_uid, :first_name, :last_name, :email, :phone, :city, :state, :postal_code, :country, :registration_date, :customer_segment, :is_active)
                ON DUPLICATE KEY UPDATE
                    first_name = VALUES(first_name),
                    last_name = VALUES(last_name),
                    phone = VALUES(phone),
                    city = VALUES(city),
                    state = VALUES(state),
                    customer_segment = VALUES(customer_segment),
                    is_active = VALUES(is_active);
            """)
        conn.execute(customer_sql, customers)
        counts["customers"] = len(customers)

        logger.info("Seeding orders...")
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS orders (
                    order_id INTEGER PRIMARY KEY, order_number TEXT UNIQUE, customer_id INTEGER, 
                    store_id INTEGER, order_date TEXT, order_status TEXT, payment_method TEXT, 
                    subtotal_amount REAL, tax_amount REAL, shipping_fee REAL, discount_amount REAL, 
                    total_amount REAL, created_at TEXT
                );
            """))
            order_sql = text("""
                INSERT OR REPLACE INTO orders (order_id, order_number, customer_id, store_id, order_date, order_status, payment_method, subtotal_amount, tax_amount, shipping_fee, discount_amount, total_amount, created_at)
                VALUES (:order_id, :order_number, :customer_id, :store_id, :order_date, :order_status, :payment_method, :subtotal_amount, :tax_amount, :shipping_fee, :discount_amount, :total_amount, :created_at);
            """)
        else:
            order_sql = text("""
                INSERT INTO orders (order_id, order_number, customer_id, store_id, order_date, order_status, payment_method, subtotal_amount, tax_amount, shipping_fee, discount_amount, total_amount, created_at)
                VALUES (:order_id, :order_number, :customer_id, :store_id, :order_date, :order_status, :payment_method, :subtotal_amount, :tax_amount, :shipping_fee, :discount_amount, :total_amount, :created_at)
                ON DUPLICATE KEY UPDATE
                    order_status = VALUES(order_status),
                    total_amount = VALUES(total_amount);
            """)
        batch_size = 200
        for i in range(0, len(orders), batch_size):
            conn.execute(order_sql, orders[i:i + batch_size])
        counts["orders"] = len(orders)

        logger.info("Seeding order items...")
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS order_items (
                    order_item_id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, 
                    quantity INTEGER, unit_price REAL, unit_cost REAL, discount_amount REAL, 
                    total_price REAL, created_at TEXT
                );
            """))
            item_sql = text("""
                INSERT OR REPLACE INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, unit_cost, discount_amount, total_price, created_at)
                VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :unit_cost, :discount_amount, :total_price, :created_at);
            """)
        else:
            item_sql = text("""
                INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, unit_cost, discount_amount, total_price, created_at)
                VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :unit_cost, :discount_amount, :total_price, :created_at)
                ON DUPLICATE KEY UPDATE
                    quantity = VALUES(quantity),
                    total_price = VALUES(total_price);
            """)
        for i in range(0, len(order_items), batch_size):
            conn.execute(item_sql, order_items[i:i + batch_size])
        counts["order_items"] = len(order_items)

        logger.info("Seeding inventory...")
        if is_sqlite:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS inventory (
                    inventory_id INTEGER PRIMARY KEY, store_id INTEGER, product_id INTEGER, 
                    stock_on_hand INTEGER, reorder_threshold INTEGER, reorder_quantity INTEGER, 
                    last_restock_date TEXT
                );
            """))
            inventory_sql = text("""
                INSERT OR REPLACE INTO inventory (inventory_id, store_id, product_id, stock_on_hand, reorder_threshold, reorder_quantity, last_restock_date)
                VALUES (:inventory_id, :store_id, :product_id, :stock_on_hand, :reorder_threshold, :reorder_quantity, :last_restock_date);
            """)
        else:
            inventory_sql = text("""
                INSERT INTO inventory (inventory_id, store_id, product_id, stock_on_hand, reorder_threshold, reorder_quantity, last_restock_date)
                VALUES (:inventory_id, :store_id, :product_id, :stock_on_hand, :reorder_threshold, :reorder_quantity, :last_restock_date)
                ON DUPLICATE KEY UPDATE
                    stock_on_hand = VALUES(stock_on_hand),
                    reorder_threshold = VALUES(reorder_threshold),
                    last_restock_date = VALUES(last_restock_date);
            """)
        for i in range(0, len(inventory), batch_size):
            conn.execute(inventory_sql, inventory[i:i + batch_size])
        counts["inventory"] = len(inventory)

    logger.info(f"Source database seeded successfully: {counts}")
    return counts


if __name__ == "__main__":
    seed_source_database()
