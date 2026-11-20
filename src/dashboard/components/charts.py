"""
RetailLake 360 Enterprise Data Lakehouse & Analytics Platform
Plotly Interactive Visualizations Component
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


def create_sales_trend_chart(df: pd.DataFrame):
    """Line and area chart showing daily revenue and gross profit over time."""
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["report_date"],
        y=df["revenue"],
        mode="lines+markers",
        name="Daily Revenue ($)",
        line=dict(color="#1f77b4", width=3)
    ))
    fig.add_trace(go.Scatter(
        x=df["report_date"],
        y=df["gross_profit"],
        mode="lines",
        name="Gross Profit ($)",
        line=dict(color="#2ca02c", width=2, dash="dash"),
        fill="tonexty"
    ))
    fig.update_layout(
        title="Revenue & Gross Profit Performance Trend",
        xaxis_title="Report Date",
        yaxis_title="Amount (USD)",
        template="plotly_white",
        hovermode="x unified",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig


def create_store_leaderboard_chart(df: pd.DataFrame):
    """Horizontal bar chart ranking store revenue and profit."""
    df_sorted = df.sort_values("total_revenue", ascending=True)
    fig = go.Figure(go.Bar(
        x=df_sorted["total_revenue"],
        y=df_sorted["store_name"],
        orientation="h",
        marker=dict(
            color=df_sorted["total_revenue"],
            colorscale="Viridis",
            showscale=True
        ),
        text=[f"${x:,.0f}" for x in df_sorted["total_revenue"]],
        textposition="auto"
    ))
    fig.update_layout(
        title="Store Revenue Leaderboard",
        xaxis_title="Total Revenue ($)",
        yaxis_title="Store Location",
        template="plotly_white",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig


def create_rfm_segment_chart(df: pd.DataFrame):
    """Donut chart showing customer distribution by RFM segment."""
    fig = px.pie(
        df,
        names="rfm_segment",
        values="customer_count",
        title="Customer Segmentation Breakdown (RFM)",
        hole=0.45,
        color_discrete_sequence=px.colors.qualitative.Prism
    )
    fig.update_traces(textinfo="percent+label")
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=20, r=20, t=50, b=20)
    )
    return fig


def create_category_profitability_chart(df: pd.DataFrame):
    """Scatter / bubble chart showing Total Revenue vs Gross Profit Margin %."""
    fig = px.scatter(
        df,
        x="total_revenue",
        y="gross_margin_pct",
        size="total_units_sold",
        color="category",
        hover_name="sub_category",
        title="Product Sub-Category Profitability Matrix",
        labels={"total_revenue": "Total Revenue ($)", "gross_margin_pct": "Gross Margin (%)"},
        template="plotly_white"
    )
    fig.update_layout(margin=dict(l=20, r=20, t=50, b=20))
    return fig
