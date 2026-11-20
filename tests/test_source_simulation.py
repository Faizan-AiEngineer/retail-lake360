"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Source Data Simulation & Seeding Unit/Integration Tests
"""

import pytest
from sqlalchemy import text
from src.source_simulation.data_generator import RetailDataGenerator


def test_retail_data_generator_entities():
    generator = RetailDataGenerator(seed=123)

    # 1. Stores
    stores = generator.generate_stores()
    assert len(stores) == 10
    assert any(s["store_type"] == "Flagship" for s in stores)
    assert any(s["store_type"] == "Online" for s in stores)

    # 2. Products
    products = generator.generate_products()
    assert len(products) >= 20
    for p in products:
        assert p["unit_price"] > p["unit_cost"]
        assert len(p["sku"]) > 0

    # 3. Customers
    customers = generator.generate_customers(count=50)
    assert len(customers) == 50
    emails = [c["email"] for c in customers]
    assert len(emails) == len(set(emails))  # Unique emails

    # 4. Orders and Order Items
    orders, items = generator.generate_orders_and_items(
        customers=customers,
        stores=stores,
        products=products,
        order_count=100
    )
    assert len(orders) == 100
    assert len(items) >= 100

    order_ids = {o["order_id"] for o in orders}
    for item in items:
        assert item["order_id"] in order_ids
        assert item["quantity"] >= 1
        assert item["total_price"] > 0

    # 5. Inventory
    inventory = generator.generate_inventory(stores=stores, products=products)
    assert len(inventory) == len(stores) * len(products)
    for inv in inventory:
        assert inv["stock_on_hand"] >= 0
        assert inv["reorder_threshold"] > 0


def test_source_seeding_into_database(sqlite_source_engine):
    """Tests executing batch inserts into the source database."""
    generator = RetailDataGenerator(seed=42)
    stores = generator.generate_stores()
    products = generator.generate_products()
    customers = generator.generate_customers(count=20)
    orders, items = generator.generate_orders_and_items(
        customers=customers,
        stores=stores,
        products=products,
        order_count=30
    )

    with sqlite_source_engine.begin() as conn:
        # Stores
        conn.execute(
            text("""
                INSERT OR REPLACE INTO stores (store_id, store_code, store_name, store_type, city, state, country, sqft_area, opening_date, is_active)
                VALUES (:store_id, :store_code, :store_name, :store_type, :city, :state, :country, :sqft_area, :opening_date, :is_active)
            """),
            stores
        )
        # Products
        conn.execute(
            text("""
                INSERT OR REPLACE INTO products (product_id, sku, product_name, category, sub_category, brand, unit_cost, unit_price, weight_kg, is_active)
                VALUES (:product_id, :sku, :product_name, :category, :sub_category, :brand, :unit_cost, :unit_price, :weight_kg, :is_active)
            """),
            products
        )
        # Customers
        conn.execute(
            text("""
                INSERT OR REPLACE INTO customers (customer_id, customer_uid, first_name, last_name, email, phone, city, state, postal_code, country, registration_date, customer_segment, is_active)
                VALUES (:customer_id, :customer_uid, :first_name, :last_name, :email, :phone, :city, :state, :postal_code, :country, :registration_date, :customer_segment, :is_active)
            """),
            customers
        )
        # Orders
        conn.execute(
            text("""
                INSERT OR REPLACE INTO orders (order_id, order_number, customer_id, store_id, order_date, order_status, payment_method, subtotal_amount, tax_amount, shipping_fee, discount_amount, total_amount, created_at)
                VALUES (:order_id, :order_number, :customer_id, :store_id, :order_date, :order_status, :payment_method, :subtotal_amount, :tax_amount, :shipping_fee, :discount_amount, :total_amount, :created_at)
            """),
            orders
        )
        # Items
        conn.execute(
            text("""
                INSERT OR REPLACE INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, unit_cost, discount_amount, total_price, created_at)
                VALUES (:order_item_id, :order_id, :product_id, :quantity, :unit_price, :unit_cost, :discount_amount, :total_price, :created_at)
            """),
            items
        )

    # Verification query
    with sqlite_source_engine.connect() as conn:
        store_count = conn.execute(text("SELECT COUNT(*) FROM stores")).scalar()
        order_count = conn.execute(text("SELECT COUNT(*) FROM orders")).scalar()
        assert store_count == 10
        assert order_count >= 30
