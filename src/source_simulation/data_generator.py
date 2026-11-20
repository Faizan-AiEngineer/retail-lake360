"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Synthetic Retail OLTP Data Generator
Generates realistic transactional customer, product, store, order, and inventory records.
"""

import uuid
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple


class RetailDataGenerator:
    def __init__(self, seed: int = 42):
        random.seed(seed)

    def generate_stores(self) -> List[Dict[str, Any]]:
        stores_data = [
            ("STR-001", "Manhattan Flagship Store", "Flagship", "New York", "NY", "USA", 12000, "2020-01-15"),
            ("STR-002", "Chicago Michigan Ave", "Flagship", "Chicago", "IL", "USA", 9500, "2020-06-20"),
            ("STR-003", "San Francisco Union Square", "Flagship", "San Francisco", "CA", "USA", 8200, "2021-03-10"),
            ("STR-004", "Austin Domain Outlet", "Outlet", "Austin", "TX", "USA", 6500, "2021-08-01"),
            ("STR-005", "Seattle Downtown Express", "Express", "Seattle", "WA", "USA", 3200, "2022-02-14"),
            ("STR-006", "Miami Beach Fashion Store", "Flagship", "Miami", "FL", "USA", 7800, "2022-05-30"),
            ("STR-007", "Denver Cherry Creek", "Express", "Denver", "CO", "USA", 3800, "2022-11-12"),
            ("STR-008", "Boston Newbury St", "Outlet", "Boston", "MA", "USA", 5400, "2023-04-18"),
            ("STR-009", "Atlanta Lenox Square", "Flagship", "Atlanta", "GA", "USA", 8900, "2023-09-05"),
            ("STR-010", "RetailLake Global Digital Store", "Online", "San Jose", "CA", "USA", 25000, "2019-11-01"),
        ]
        return [
            {
                "store_id": idx + 1,
                "store_code": s[0],
                "store_name": s[1],
                "store_type": s[2],
                "city": s[3],
                "state": s[4],
                "country": s[5],
                "sqft_area": s[6],
                "opening_date": s[7],
                "is_active": True,
            }
            for idx, s in enumerate(stores_data)
        ]

    def generate_products(self) -> List[Dict[str, Any]]:
        products_catalog = [
            # Electronics
            ("ELEC-101", "UltraHD 4K Smart Monitor 27-inch", "Electronics", "Monitors", "VisionTech", 180.00, 329.99, 5.2),
            ("ELEC-102", "Noise-Cancelling Wireless Headphones", "Electronics", "Audio", "SoundAura", 65.00, 149.99, 0.4),
            ("ELEC-103", "Ergonomic Mechanical Keyboard RGB", "Electronics", "Accessories", "KeyPro", 45.00, 99.99, 1.1),
            ("ELEC-104", "Precision Wireless Laser Mouse", "Electronics", "Accessories", "KeyPro", 22.00, 54.99, 0.2),
            ("ELEC-105", "Fast Charge USB-C Power Hub 100W", "Electronics", "Power", "VoltMax", 28.00, 59.99, 0.3),
            ("ELEC-106", "Smart Home Voice Assistant Speaker", "Electronics", "Smart Home", "SoundAura", 35.00, 79.99, 0.8),
            # Apparel
            ("APPR-201", "Waterproof All-Weather Trench Coat", "Apparel", "Outerwear", "UrbanNordic", 70.00, 179.99, 1.2),
            ("APPR-202", "Classic Organic Cotton Oxford Shirt", "Apparel", "Tops", "HeritageWear", 22.00, 64.99, 0.3),
            ("APPR-203", "Stretch Denim Slim-Fit Jeans", "Apparel", "Bottoms", "HeritageWear", 28.00, 79.99, 0.6),
            ("APPR-204", "Merino Wool Crewneck Sweater", "Apparel", "Knitwear", "UrbanNordic", 38.00, 98.99, 0.5),
            ("APPR-205", "Breathable Athletic Running Tee", "Apparel", "Activewear", "AeroPeak", 12.00, 34.99, 0.2),
            # Home & Kitchen
            ("HOME-301", "Barista Espresso & Cappuccino Maker", "Home & Kitchen", "Appliances", "CafféBella", 110.00, 249.99, 4.5),
            ("HOME-302", "Cold Brew Glass Carafe 1.5L", "Home & Kitchen", "Drinkware", "CafféBella", 12.00, 29.99, 0.9),
            ("HOME-303", "Cast Iron Pre-Seasoned Dutch Oven 6Qt", "Home & Kitchen", "Cookware", "IronMaster", 42.00, 89.99, 6.2),
            ("HOME-304", "Chef's Japanese Damascus Steel Knife 8in", "Home & Kitchen", "Cutlery", "IronMaster", 35.00, 84.99, 0.4),
            ("HOME-305", "Smart Air Purifier HEPA H13", "Home & Kitchen", "Appliances", "PureLiving", 75.00, 159.99, 3.8),
            # Sports & Outdoors
            ("SPRT-401", "Ultralight 2-Person Backpacking Tent", "Sports & Outdoors", "Camping", "AeroPeak", 90.00, 219.99, 2.1),
            ("SPRT-402", "Insulated Hydro Flask Bottle 32oz", "Sports & Outdoors", "Hydration", "AeroPeak", 14.00, 38.99, 0.5),
            ("SPRT-403", "High-Tension Resistance Bands Set", "Sports & Outdoors", "Fitness", "AeroPeak", 10.00, 24.99, 0.6),
            ("SPRT-404", "Adjustable Aluminum Trekking Poles", "Sports & Outdoors", "Hiking", "AeroPeak", 24.00, 59.99, 0.8),
            # Beauty & Personal Care
            ("BEAU-501", "Botanical Hydrating Facial Serum 50ml", "Beauty & Personal Care", "Skincare", "FloraPure", 16.00, 48.99, 0.1),
            ("BEAU-502", "Ionic Ceramic Hair Dryer 1800W", "Beauty & Personal Care", "Haircare", "FloraPure", 32.00, 79.99, 0.9),
            ("BEAU-503", "Bamboo Charcoal Deep Cleanse Mask", "Beauty & Personal Care", "Skincare", "FloraPure", 8.00, 22.99, 0.15),
            ("BEAU-504", "Organic Shea Butter Body Balm 250ml", "Beauty & Personal Care", "Body", "FloraPure", 9.00, 26.99, 0.3),
        ]
        return [
            {
                "product_id": idx + 1,
                "sku": p[0],
                "product_name": p[1],
                "category": p[2],
                "sub_category": p[3],
                "brand": p[4],
                "unit_cost": p[5],
                "unit_price": p[6],
                "weight_kg": p[7],
                "is_active": True,
            }
            for idx, p in enumerate(products_catalog)
        ]

    def generate_customers(self, count: int = 150) -> List[Dict[str, Any]]:
        first_names = ["James", "Emma", "Liam", "Olivia", "Noah", "Ava", "William", "Sophia", "Lucas", "Mia",
                       "Ethan", "Isabella", "Mason", "Charlotte", "Oliver", "Amelia", "Elijah", "Harper", "Aiden", "Evelyn"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
                      "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas", "Taylor", "Moore", "Jackson", "Martin"]
        cities_states = [
            ("New York", "NY", "10001"), ("Los Angeles", "CA", "90001"), ("Chicago", "IL", "60601"),
            ("Houston", "TX", "77001"), ("Phoenix", "AZ", "85001"), ("Philadelphia", "PA", "19101"),
            ("San Antonio", "TX", "78201"), ("San Diego", "CA", "92101"), ("Dallas", "TX", "75201"),
            ("San Jose", "CA", "95101"), ("Austin", "TX", "78701"), ("Seattle", "WA", "98101")
        ]
        segments = ["Standard", "Premium", "VIP", "Corporate"]

        customers = []
        base_date = datetime.now() - timedelta(days=730)
        for i in range(1, count + 1):
            fn = random.choice(first_names)
            ln = random.choice(last_names)
            city, state, zip_code = random.choice(cities_states)
            reg_date = base_date + timedelta(days=random.randint(0, 700))
            customers.append({
                "customer_id": i,
                "customer_uid": str(uuid.uuid4()),
                "first_name": fn,
                "last_name": ln,
                "email": f"{fn.lower()}.{ln.lower()}{i}@example.com",
                "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                "city": city,
                "state": state,
                "postal_code": zip_code,
                "country": "USA",
                "registration_date": reg_date.strftime("%Y-%m-%d"),
                "customer_segment": random.choices(segments, weights=[0.55, 0.25, 0.15, 0.05])[0],
                "is_active": True,
            })
        return customers

    def generate_orders_and_items(
        self,
        customers: List[Dict[str, Any]],
        stores: List[Dict[str, Any]],
        products: List[Dict[str, Any]],
        order_count: int = 500
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        orders = []
        order_items = []
        payment_methods = ["Credit Card", "Debit Card", "PayPal", "Apple Pay", "Gift Card"]
        order_statuses = ["COMPLETED", "COMPLETED", "COMPLETED", "COMPLETED", "SHIPPED", "PROCESSING", "CANCELLED", "REFUNDED"]

        now = datetime.now()
        start_date = now - timedelta(days=180)
        item_id_counter = 1

        for order_id in range(1, order_count + 1):
            customer = random.choice(customers)
            store = random.choice(stores)
            order_date = start_date + timedelta(
                days=random.randint(0, 179),
                hours=random.randint(8, 21),
                minutes=random.randint(0, 59)
            )
            order_status = random.choice(order_statuses)
            payment_method = random.choice(payment_methods)

            # Choose 1 to 5 distinct products for this order
            n_items = random.randint(1, 4)
            chosen_products = random.sample(products, n_items)

            subtotal = 0.0
            order_items_temp = []

            for prod in chosen_products:
                qty = random.choices([1, 2, 3, 4], weights=[0.7, 0.2, 0.07, 0.03])[0]
                unit_price = float(prod["unit_price"])
                unit_cost = float(prod["unit_cost"])
                # Discount
                discount = round(unit_price * qty * 0.1, 2) if random.random() < 0.25 else 0.0
                total_item_price = round((unit_price * qty) - discount, 2)
                subtotal += total_item_price

                order_items_temp.append({
                    "order_item_id": item_id_counter,
                    "order_id": order_id,
                    "product_id": prod["product_id"],
                    "quantity": qty,
                    "unit_price": unit_price,
                    "unit_cost": unit_cost,
                    "discount_amount": discount,
                    "total_price": total_item_price,
                    "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S")
                })
                item_id_counter += 1

            subtotal = round(subtotal, 2)
            tax_amount = round(subtotal * 0.0825, 2)
            shipping_fee = 0.0 if (subtotal > 75.0 or store["store_type"] != "Online") else 7.99
            total_amount = round(subtotal + tax_amount + shipping_fee, 2)

            orders.append({
                "order_id": order_id,
                "order_number": f"ORD-{order_date.strftime('%Y%m')}-{order_id:05d}",
                "customer_id": customer["customer_id"],
                "store_id": store["store_id"],
                "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
                "order_status": order_status,
                "payment_method": payment_method,
                "subtotal_amount": subtotal,
                "tax_amount": tax_amount,
                "shipping_fee": shipping_fee,
                "discount_amount": 0.0,
                "total_amount": total_amount,
                "created_at": order_date.strftime("%Y-%m-%d %H:%M:%S")
            })
            order_items.extend(order_items_temp)

        return orders, order_items

    def generate_inventory(
        self,
        stores: List[Dict[str, Any]],
        products: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        inventory = []
        inv_id = 1
        today = datetime.now().date()
        for s in stores:
            for p in products:
                # Digital store has higher inventory
                base_stock = 300 if s["store_type"] == "Online" else 50
                stock = random.randint(5, base_stock)
                threshold = 20 if s["store_type"] != "Online" else 50
                restock_date = today - timedelta(days=random.randint(1, 45))
                inventory.append({
                    "inventory_id": inv_id,
                    "store_id": s["store_id"],
                    "product_id": p["product_id"],
                    "stock_on_hand": stock,
                    "reorder_threshold": threshold,
                    "reorder_quantity": 100,
                    "last_restock_date": restock_date.strftime("%Y-%m-%d")
                })
                inv_id += 1
        return inventory
