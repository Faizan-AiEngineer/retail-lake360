"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Gold Layer Aggregations Engine (Silver -> Gold Lakehouse Datamarts)
Produces business-level analytical models: Daily/Monthly Sales, RFM Segments, Category Margins, Store Rankings, Inventory Alerts.
"""

import os
import uuid
import logging
from datetime import datetime
from typing import Dict, Any
import numpy as np
import pandas as pd

from config.settings import settings
from src.serving.audit_logger import LakehouseAuditLogger

logger = logging.getLogger("retaillake.gold")


class GoldAggregationEngine:
    def __init__(self, run_id: str = None):
        self.run_id = run_id or str(uuid.uuid4())
        self.audit_logger = LakehouseAuditLogger(run_id=self.run_id)
        os.makedirs(settings.LAKEHOUSE_GOLD_DIR, exist_ok=True)

    def _read_silver(self, table_name: str) -> pd.DataFrame:
        path = os.path.join(settings.LAKEHOUSE_SILVER_DIR, table_name, f"{table_name}_part0.parquet")
        if not os.path.exists(path):
            raise FileNotFoundError(f"Silver table not found: {path}")
        return pd.read_parquet(path)

    def _save_gold(self, df: pd.DataFrame, table_name: str) -> str:
        target_dir = os.path.join(settings.LAKEHOUSE_GOLD_DIR, table_name)
        os.makedirs(target_dir, exist_ok=True)
        file_path = os.path.join(target_dir, f"{table_name}_part0.parquet")
        out_df = df.copy()
        out_df["_gold_aggregated_at"] = datetime.now().isoformat()
        out_df["_gold_batch_id"] = self.run_id
        out_df.to_parquet(file_path, index=False, engine="pyarrow")
        return file_path

    def compute_all_gold_models(self) -> Dict[str, pd.DataFrame]:
        logger.info(f"[{self.run_id}] Computing Gold analytical datasets...")

        dim_cust = self._read_silver("dim_customers")
        dim_stores = self._read_silver("dim_stores")
        dim_prods = self._read_silver("dim_products")
        fct_orders = self._read_silver("fct_orders")
        fct_items = self._read_silver("fct_order_items")
        fct_inv = self._read_silver("fct_inventory")

        # -------------------------------------------------------------
        # 1. Gold Daily Sales Summary
        # -------------------------------------------------------------
        # Join orders with stores and aggregate order items
        completed_orders = fct_orders[fct_orders["order_status"].isin(["COMPLETED", "SHIPPED"])].copy()

        # Item-level rollup per order
        item_agg = fct_items.groupby("order_id").agg(
            total_units_sold=("quantity", "sum"),
            total_cogs=("cogs_total", "sum")
        ).reset_index()

        orders_enriched = completed_orders.merge(item_agg, on="order_id", how="left")
        orders_enriched["total_units_sold"] = orders_enriched["total_units_sold"].fillna(1).astype(int)
        orders_enriched["total_cogs"] = orders_enriched["total_cogs"].fillna(orders_enriched["subtotal_amount"] * 0.45)

        daily_grouped = orders_enriched.groupby(["order_date_str", "store_id"]).agg(
            total_orders=("order_id", "count"),
            total_units_sold=("total_units_sold", "sum"),
            total_revenue=("total_amount", "sum"),
            total_cogs=("total_cogs", "sum"),
            distinct_customers=("customer_id", "nunique")
        ).reset_index()

        # Merge store metadata
        daily_sales = daily_grouped.merge(
            dim_stores[["store_id", "store_name", "store_type"]],
            on="store_id",
            how="left"
        )
        daily_sales.rename(columns={"order_date_str": "report_date"}, inplace=True)
        daily_sales["gross_profit"] = daily_sales["total_revenue"] - daily_sales["total_cogs"]
        daily_sales["gross_margin_pct"] = np.where(
            daily_sales["total_revenue"] > 0,
            (daily_sales["gross_profit"] / daily_sales["total_revenue"]) * 100,
            0.0
        ).round(2)
        daily_sales["avg_order_value"] = (daily_sales["total_revenue"] / daily_sales["total_orders"]).round(2)
        daily_sales["total_revenue"] = daily_sales["total_revenue"].round(2)
        daily_sales["total_cogs"] = daily_sales["total_cogs"].round(2)
        daily_sales["gross_profit"] = daily_sales["gross_profit"].round(2)

        self._save_gold(daily_sales, "gold_daily_sales_summary")

        # -------------------------------------------------------------
        # 2. Gold Monthly Sales Summary
        # -------------------------------------------------------------
        orders_enriched["year_month"] = pd.to_datetime(orders_enriched["order_date"]).dt.strftime("%Y-%m")
        monthly_grouped = orders_enriched.groupby(["year_month", "store_id"]).agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            total_cogs=("total_cogs", "sum")
        ).reset_index()

        monthly_sales = monthly_grouped.merge(
            dim_stores[["store_id", "store_name"]],
            on="store_id",
            how="left"
        )
        monthly_sales["gross_profit"] = (monthly_sales["total_revenue"] - monthly_sales["total_cogs"]).round(2)
        monthly_sales["gross_margin_pct"] = np.where(
            monthly_sales["total_revenue"] > 0,
            (monthly_sales["gross_profit"] / monthly_sales["total_revenue"]) * 100,
            0.0
        ).round(2)
        monthly_sales["avg_order_value"] = (monthly_sales["total_revenue"] / monthly_sales["total_orders"]).round(2)
        monthly_sales["total_revenue"] = monthly_sales["total_revenue"].round(2)
        monthly_sales["total_cogs"] = monthly_sales["total_cogs"].round(2)

        self._save_gold(monthly_sales, "gold_monthly_sales_summary")

        # -------------------------------------------------------------
        # 3. Gold Customer RFM Segments & CLV
        # -------------------------------------------------------------
        ref_date = pd.to_datetime(fct_orders["order_date"]).max() + pd.Timedelta(days=1)
        cust_orders = completed_orders.groupby("customer_id").agg(
            recency_days=("order_date", lambda d: (ref_date - pd.to_datetime(d).max()).days),
            frequency_orders=("order_id", "count"),
            monetary_total=("total_amount", "sum"),
            first_purchase_date=("order_date", lambda d: pd.to_datetime(d).min().strftime("%Y-%m-%d")),
            last_purchase_date=("order_date", lambda d: pd.to_datetime(d).max().strftime("%Y-%m-%d"))
        ).reset_index()

        # Merge with customer dimension
        rfm_df = dim_cust[["customer_id", "customer_uid", "full_name", "email", "city", "state"]].merge(
            cust_orders, on="customer_id", how="inner"
        )

        # Compute RFM quantile scores (1 to 5)
        rfm_df["r_score"] = pd.qcut(rfm_df["recency_days"], q=5, labels=[5, 4, 3, 2, 1]).astype(int)
        rfm_df["f_score"] = pd.qcut(rfm_df["frequency_orders"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)
        rfm_df["m_score"] = pd.qcut(rfm_df["monetary_total"].rank(method="first"), q=5, labels=[1, 2, 3, 4, 5]).astype(int)

        def assign_rfm_segment(row):
            r, f = row["r_score"], row["f_score"]
            if r >= 4 and f >= 4:
                return "Champions"
            elif r >= 3 and f >= 3:
                return "Loyal Customers"
            elif r >= 4 and f <= 2:
                return "Recent Customers"
            elif r <= 2 and f >= 4:
                return "At Risk"
            elif r <= 2 and f <= 2:
                return "Hibernating"
            elif r == 3 and f <= 2:
                return "Promising"
            else:
                return "Need Attention"

        rfm_df["rfm_segment"] = rfm_df.apply(assign_rfm_segment, axis=1)
        rfm_df["estimated_clv"] = (rfm_df["monetary_total"] * 1.85 + (rfm_df["frequency_orders"] * 45.0)).round(2)
        rfm_df["churn_risk"] = np.where(
            rfm_df["recency_days"] > 90, "High",
            np.where(rfm_df["recency_days"] > 45, "Medium", "Low")
        )
        rfm_df["monetary_total"] = rfm_df["monetary_total"].round(2)

        self._save_gold(rfm_df, "gold_customer_rfm_segments")

        # -------------------------------------------------------------
        # 4. Gold Product Category Profitability
        # -------------------------------------------------------------
        cat_metrics = fct_items.groupby(["category", "sub_category"]).agg(
            total_units_sold=("quantity", "sum"),
            total_revenue=("total_price", "sum"),
            total_cogs=("cogs_total", "sum")
        ).reset_index()

        prod_count = dim_prods.groupby(["category", "sub_category"]).agg(
            product_count=("product_id", "count")
        ).reset_index()

        cat_prof = cat_metrics.merge(prod_count, on=["category", "sub_category"], how="left")
        cat_prof["gross_profit"] = (cat_prof["total_revenue"] - cat_prof["total_cogs"]).round(2)
        cat_prof["gross_margin_pct"] = np.where(
            cat_prof["total_revenue"] > 0,
            (cat_prof["gross_profit"] / cat_prof["total_revenue"]) * 100,
            0.0
        ).round(2)
        cat_prof["total_revenue"] = cat_prof["total_revenue"].round(2)
        cat_prof["total_cogs"] = cat_prof["total_cogs"].round(2)

        self._save_gold(cat_prof, "gold_product_category_profitability")

        # -------------------------------------------------------------
        # 5. Gold Store Performance Analytics
        # -------------------------------------------------------------
        store_orders = orders_enriched.groupby("store_id").agg(
            total_orders=("order_id", "count"),
            total_revenue=("total_amount", "sum"),
            total_cogs=("total_cogs", "sum"),
            active_customers=("customer_id", "nunique")
        ).reset_index()

        store_perf = dim_stores.merge(store_orders, on="store_id", how="left").fillna(0)
        store_perf["gross_profit"] = (store_perf["total_revenue"] - store_perf["total_cogs"]).round(2)
        store_perf["revenue_per_sqft"] = np.where(
            store_perf["sqft_area"] > 0,
            (store_perf["total_revenue"] / store_perf["sqft_area"]),
            0.0
        ).round(2)
        store_perf["total_revenue"] = store_perf["total_revenue"].round(2)
        store_perf["revenue_rank"] = store_perf["total_revenue"].rank(ascending=False, method="min").astype(int)

        gold_stores = store_perf[[
            "store_id", "store_code", "store_name", "store_type", "city", "state",
            "sqft_area", "total_orders", "total_revenue", "gross_profit",
            "revenue_per_sqft", "active_customers", "revenue_rank"
        ]]
        self._save_gold(gold_stores, "gold_store_performance_analytics")

        # -------------------------------------------------------------
        # 6. Gold Inventory Turnover & Restock Alerts
        # -------------------------------------------------------------
        inv_merged = fct_inv.merge(
            dim_stores[["store_id", "store_name"]], on="store_id", how="left"
        ).merge(
            dim_prods[["product_id", "sku", "product_name", "category"]], on="product_id", how="left"
        )

        inv_merged["daily_run_rate"] = np.random.uniform(0.8, 3.5, size=len(inv_merged)).round(2)
        inv_merged["estimated_days_of_stock"] = (
            inv_merged["stock_on_hand"] / inv_merged["daily_run_rate"]
        ).round(1)

        def determine_stock_status(row):
            stock, thresh = row["stock_on_hand"], row["reorder_threshold"]
            if stock <= (thresh * 0.5):
                return "CRITICAL"
            elif stock <= thresh:
                return "WARNING"
            elif stock > (thresh * 4):
                return "OVERSTOCKED"
            return "OPTIMAL"

        inv_merged["stock_status"] = inv_merged.apply(determine_stock_status, axis=1)
        inv_merged["reorder_urgency"] = np.where(
            inv_merged["stock_status"] == "CRITICAL", "IMMEDIATE",
            np.where(inv_merged["stock_status"] == "WARNING", "UPCOMING", "NONE")
        )

        gold_inv = inv_merged[[
            "store_id", "store_name", "product_id", "sku", "product_name", "category",
            "stock_on_hand", "reorder_threshold", "daily_run_rate",
            "estimated_days_of_stock", "stock_status", "reorder_urgency"
        ]]
        self._save_gold(gold_inv, "gold_inventory_turnover_alerts")

        gold_models = {
            "gold_daily_sales_summary": daily_sales,
            "gold_monthly_sales_summary": monthly_sales,
            "gold_customer_rfm_segments": rfm_df,
            "gold_product_category_profitability": cat_prof,
            "gold_store_performance_analytics": gold_stores,
            "gold_inventory_turnover_alerts": gold_inv
        }

        self.audit_logger.log(
            level="INFO",
            component="GoldAggregation",
            action="compute_gold_complete",
            message=f"Gold layer models successfully generated: {list(gold_models.keys())}"
        )
        return gold_models


if __name__ == "__main__":
    GoldAggregationEngine().compute_all_gold_models()
