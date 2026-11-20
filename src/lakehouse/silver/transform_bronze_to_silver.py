"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Silver Transformation Engine (Bronze -> Silver Medallion)
Cleanses, deduplicates, validates data quality, and creates dimensional models (Star Schema).
"""

import os
import uuid
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd

from config.settings import settings
from src.serving.audit_logger import LakehouseAuditLogger
from src.lakehouse.silver.quality_rules import DataQualityValidator

logger = logging.getLogger("retaillake.silver")


class SilverTransformationEngine:
    def __init__(self, run_id: str = None):
        self.run_id = run_id or str(uuid.uuid4())
        self.audit_logger = LakehouseAuditLogger(run_id=self.run_id)
        self.validator = DataQualityValidator(run_id=self.run_id, audit_logger=self.audit_logger)
        os.makedirs(settings.LAKEHOUSE_SILVER_DIR, exist_ok=True)

    def _read_bronze(self, table_name: str) -> pd.DataFrame:
        path = os.path.join(settings.LAKEHOUSE_BRONZE_DIR, table_name, f"{table_name}_part0.parquet")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Bronze table file not found: {path}")
        return pd.read_parquet(path)

    def _save_silver(self, df: pd.DataFrame, table_name: str) -> str:
        target_dir = os.path.join(settings.LAKEHOUSE_SILVER_DIR, table_name)
        os.makedirs(target_dir, exist_ok=True)
        file_path = os.path.join(target_dir, f"{table_name}_part0.parquet")
        df["_silver_processed_at"] = datetime.now().isoformat()
        df["_silver_batch_id"] = self.run_id
        df.to_parquet(file_path, index=False, engine="pyarrow")
        return file_path

    def transform_all(self) -> Dict[str, int]:
        logger.info(f"[{self.run_id}] Executing Silver layer transformations...")

        # 1. Read Bronze tables
        raw_cust = self._read_bronze("customers")
        raw_stores = self._read_bronze("stores")
        raw_prods = self._read_bronze("products")
        raw_orders = self._read_bronze("orders")
        raw_items = self._read_bronze("order_items")
        raw_inv = self._read_bronze("inventory")

        # 2. Quality validations
        self.validator.validate_non_null(raw_cust, "customers", "customer_id")
        self.validator.validate_non_null(raw_orders, "orders", "order_id")
        self.validator.validate_positive_numeric(raw_prods, "products", "unit_price", allow_zero=False)
        self.validator.validate_positive_numeric(raw_orders, "orders", "total_amount", allow_zero=True)
        self.validator.validate_referential_integrity(raw_orders, "customer_id", raw_cust, "customer_id", "orders")

        # 3. Model: dim_customers
        dim_cust = raw_cust.drop_duplicates(subset=["customer_id"]).copy()
        dim_cust["full_name"] = dim_cust["first_name"].str.strip() + " " + dim_cust["last_name"].str.strip()
        dim_cust["email"] = dim_cust["email"].str.strip().str.lower()
        self._save_silver(dim_cust, "dim_customers")

        # 4. Model: dim_stores
        dim_stores = raw_stores.drop_duplicates(subset=["store_id"]).copy()
        self._save_silver(dim_stores, "dim_stores")

        # 5. Model: dim_products
        dim_prods = raw_prods.drop_duplicates(subset=["product_id"]).copy()
        dim_prods["margin_dollars"] = dim_prods["unit_price"] - dim_prods["unit_cost"]
        dim_prods["margin_pct"] = (dim_prods["margin_dollars"] / dim_prods["unit_price"]) * 100
        self._save_silver(dim_prods, "dim_products")

        # 6. Model: fct_orders
        fct_orders = raw_orders.drop_duplicates(subset=["order_id"]).copy()
        fct_orders["order_date"] = pd.to_datetime(fct_orders["order_date"])
        fct_orders["order_year"] = fct_orders["order_date"].dt.year
        fct_orders["order_month"] = fct_orders["order_date"].dt.month
        fct_orders["order_day"] = fct_orders["order_date"].dt.day
        fct_orders["order_year_month"] = fct_orders["order_date"].dt.strftime("%Y-%m")
        fct_orders["order_date_str"] = fct_orders["order_date"].dt.strftime("%Y-%m-%d")
        self._save_silver(fct_orders, "fct_orders")

        # 7. Model: fct_order_items (Enriched with cost & profit)
        fct_items = raw_items.drop_duplicates(subset=["order_item_id"]).copy()
        prod_meta = dim_prods[["product_id", "category", "sub_category"]].copy()
        fct_items = fct_items.merge(prod_meta, on="product_id", how="left")
        fct_items["cogs_total"] = fct_items["quantity"] * fct_items["unit_cost"]
        fct_items["gross_profit"] = fct_items["total_price"] - fct_items["cogs_total"]
        self._save_silver(fct_items, "fct_order_items")

        # 8. Model: fct_inventory
        fct_inventory = raw_inv.drop_duplicates(subset=["store_id", "product_id"]).copy()
        self._save_silver(fct_inventory, "fct_inventory")

        summary = {
            "dim_customers": len(dim_cust),
            "dim_stores": len(dim_stores),
            "dim_products": len(dim_prods),
            "fct_orders": len(fct_orders),
            "fct_order_items": len(fct_items),
            "fct_inventory": len(fct_inventory)
        }

        self.audit_logger.log(
            level="INFO",
            component="SilverTransformation",
            action="transform_silver_complete",
            message=f"Silver layer transformed successfully with models: {list(summary.keys())}"
        )
        return summary


if __name__ == "__main__":
    SilverTransformationEngine().transform_all()
