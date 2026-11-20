"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Executive Analytics Dashboard (Powered by MySQL 8.4 LTS Serving Layer & Streamlit)
"""

import streamlit as st
import pandas as pd

from config.settings import settings
from src.dashboard.queries import DashboardQueryService
from src.dashboard.components.metrics_cards import render_executive_kpis
from src.dashboard.components.charts import (
    create_sales_trend_chart,
    create_store_leaderboard_chart,
    create_rfm_segment_chart,
    create_category_profitability_chart
)

st.set_page_config(
    page_title="RetailLake 360 | Lakehouse & Analytics",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom Styling
st.markdown("""
<style>
    .main-title {
        font-size: 2.2rem;
        font-weight: 700;
        color: #1e3d59;
        margin-bottom: 0px;
    }
    .sub-title {
        font-size: 1.05rem;
        color: #555;
        margin-bottom: 25px;
    }
    .badge-mysql {
        background-color: #00758F;
        color: white;
        padding: 4px 10px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.85rem;
    }
    .badge-parquet {
        background-color: #00A4EF;
        color: white;
        padding: 4px 10px;
        border-radius: 4px;
        font-weight: 600;
        font-size: 0.85rem;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.image("https://img.icons8.com/fluency/96/data-configuration.png", width=64)
st.sidebar.title("RetailLake 360")
st.sidebar.markdown("**Enterprise Lakehouse & Analytics**")
st.sidebar.divider()

st.sidebar.markdown("### 🔌 Database Architecture")
st.sidebar.info(
    f"""
    **RDBMS**: MySQL 8.4 LTS  
    **Driver**: SQLAlchemy + PyMySQL  
    **Serving DB**: `{settings.MYSQL_SERVING_DB}`  
    **Role**: `{settings.MYSQL_ANALYTICS_USER}` (Strictly Read-Only)  
    **Lakehouse Storage**: Parquet (Bronze/Silver/Gold)
    """
)

store_filter = st.sidebar.selectbox(
    "Filter by Store Channel",
    options=["All", "Flagship", "Outlet", "Express", "Online"]
)

st.sidebar.divider()
if st.sidebar.button("🔄 Refresh Lakehouse Data"):
    st.cache_data.clear()
    st.rerun()

# Header
st.markdown('<p class="main-title">🛍️ RetailLake 360 Executive Analytics</p>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-title">Powered by <span class="badge-mysql">MySQL 8.4 LTS</span> serving layer and <span class="badge-parquet">Parquet</span> storage</p>',
    unsafe_allow_html=True
)

query_service = DashboardQueryService()

# Top KPIs
kpis = query_service.get_executive_kpis()
render_executive_kpis(kpis)

st.divider()

# Tab Navigation
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📈 Sales Performance",
    "👥 Customer 360 (RFM)",
    "📦 Product Profitability",
    "🏬 Store Leaderboard",
    "⚠️ Inventory Alerts",
    "⚙️ Lakehouse Observability"
])

# -------------------------------------------------------------
# TAB 1: Sales Performance
# -------------------------------------------------------------
with tab1:
    st.subheader("Daily Sales & Revenue Dynamics")
    sales_df = query_service.get_sales_trends(store_type=store_filter)
    if not sales_df.empty:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.plotly_chart(create_sales_trend_chart(sales_df), use_container_width=True)
        with col2:
            st.markdown("#### 30-Day Totals")
            total_rev_30 = sales_df["revenue"].sum()
            total_profit_30 = sales_df["gross_profit"].sum()
            avg_margin = (total_profit_30 / total_rev_30 * 100) if total_rev_30 > 0 else 0
            st.write(f"**Total Revenue**: ${total_rev_30:,.2f}")
            st.write(f"**Gross Profit**: ${total_profit_30:,.2f}")
            st.write(f"**Average Margin**: {avg_margin:.1f}%")
            st.write(f"**Orders Logged**: {sales_df['orders'].sum():,}")
    else:
        st.info("No daily sales records found for selected filter.")

# -------------------------------------------------------------
# TAB 2: Customer 360 (RFM)
# -------------------------------------------------------------
with tab2:
    st.subheader("Customer Lifetime Value & RFM Segmentation")
    rfm_df = query_service.get_rfm_segments()
    if not rfm_df.empty:
        col1, col2 = st.columns([1, 1])
        with col1:
            st.plotly_chart(create_rfm_segment_chart(rfm_df), use_container_width=True)
        with col2:
            st.markdown("#### Segment Revenue & CLV Metrics")
            st.dataframe(
                rfm_df.rename(columns={
                    "rfm_segment": "Segment",
                    "customer_count": "Customers",
                    "total_segment_spend": "Total Spend ($)",
                    "avg_customer_spend": "Avg Spend ($)",
                    "avg_recency_days": "Avg Recency (Days)",
                    "avg_estimated_clv": "Avg Est. CLV ($)"
                }),
                use_container_width=True,
                hide_index=True
            )

# -------------------------------------------------------------
# TAB 3: Product Profitability
# -------------------------------------------------------------
with tab3:
    st.subheader("Product Category & Sub-Category Margin Analysis")
    cat_df = query_service.get_category_profitability()
    if not cat_df.empty:
        st.plotly_chart(create_category_profitability_chart(cat_df), use_container_width=True)
        st.dataframe(
            cat_df.rename(columns={
                "category": "Category",
                "sub_category": "Sub-Category",
                "total_units_sold": "Units Sold",
                "total_revenue": "Revenue ($)",
                "total_cogs": "COGS ($)",
                "gross_profit": "Gross Profit ($)",
                "gross_margin_pct": "Margin (%)",
                "product_count": "Active Products"
            }),
            use_container_width=True,
            hide_index=True
        )

# -------------------------------------------------------------
# TAB 4: Store Leaderboard
# -------------------------------------------------------------
with tab4:
    st.subheader("Retail Stores Performance & Rankings")
    store_df = query_service.get_store_leaderboard()
    if not store_df.empty:
        col1, col2 = st.columns([2, 1])
        with col1:
            st.plotly_chart(create_store_leaderboard_chart(store_df), use_container_width=True)
        with col2:
            st.markdown("#### Top Stores by Efficiency")
            st.dataframe(
                store_df[["store_name", "store_type", "total_revenue", "revenue_per_sqft", "revenue_rank"]].rename(
                    columns={
                        "store_name": "Store",
                        "store_type": "Format",
                        "total_revenue": "Revenue ($)",
                        "revenue_per_sqft": "$/SqFt",
                        "revenue_rank": "Rank"
                    }
                ),
                use_container_width=True,
                hide_index=True
            )

# -------------------------------------------------------------
# TAB 5: Inventory Alerts
# -------------------------------------------------------------
with tab5:
    st.subheader("Inventory Stockout Risk & Reorder Alerts")
    inv_df = query_service.get_inventory_alerts()
    if not inv_df.empty:
        critical_count = len(inv_df[inv_df["stock_status"] == "CRITICAL"])
        if critical_count > 0:
            st.warning(f"⚠️ **{critical_count} inventory items** are currently at CRITICAL stockout risk and require immediate restock.")
        st.dataframe(
            inv_df.rename(columns={
                "store_name": "Store",
                "product_name": "Product",
                "category": "Category",
                "stock_on_hand": "Stock on Hand",
                "reorder_threshold": "Threshold",
                "estimated_days_of_stock": "Days of Supply",
                "stock_status": "Status",
                "reorder_urgency": "Urgency"
            }),
            use_container_width=True,
            hide_index=True
        )

# -------------------------------------------------------------
# TAB 6: Lakehouse Observability & MySQL Metadata
# -------------------------------------------------------------
with tab6:
    st.subheader("Lakehouse Pipeline Runs & Ingestion Audit")
    st.markdown("Monitored from `retaillake_metadata.pipeline_runs`:")
    pipeline_df = query_service.get_pipeline_runs()
    st.dataframe(
        pipeline_df.rename(columns={
            "run_id": "Run ID",
            "pipeline_name": "Pipeline",
            "trigger_type": "Trigger",
            "start_time": "Start Time",
            "end_time": "End Time",
            "duration_seconds": "Duration (s)",
            "status": "Status",
            "records_ingested": "Ingested",
            "records_transformed": "Transformed",
            "records_published": "Published"
        }),
        use_container_width=True,
        hide_index=True
    )
