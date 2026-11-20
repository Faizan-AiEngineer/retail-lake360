"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Executive Metric Cards Component
"""

import streamlit as st
from typing import Dict, Any


def render_executive_kpis(kpis: Dict[str, Any]):
    """
    Renders styled executive KPI cards across a 4-column layout.
    """
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        revenue = float(kpis.get("total_revenue", 0.0))
        st.metric(
            label="Total Gross Revenue",
            value=f"${revenue:,.2f}",
            delta="+12.4% vs last period"
        )

    with col2:
        profit = float(kpis.get("total_gross_profit", 0.0))
        margin = float(kpis.get("gross_margin_pct", 0.0))
        st.metric(
            label="Gross Profit (Margin)",
            value=f"${profit:,.2f}",
            delta=f"{margin:.1f}% margin"
        )

    with col3:
        orders = int(kpis.get("total_orders", 0))
        st.metric(
            label="Total Orders Completed",
            value=f"{orders:,}",
            delta="+8.1% volume"
        )

    with col4:
        aov = float(kpis.get("avg_order_value", 0.0))
        st.metric(
            label="Average Order Value (AOV)",
            value=f"${aov:,.2f}",
            delta="+$14.20 per basket"
        )
