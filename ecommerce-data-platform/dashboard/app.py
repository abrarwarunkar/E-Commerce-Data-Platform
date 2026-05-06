"""
Streamlit dashboard for the E-Commerce Data Platform.
Data Lineage: FastAPI serving layer -> dashboard/app.py -> browser
"""

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

# ── Config ─────────────────────────────────────────────────────────────────

API_BASE = os.getenv("API_BASE_URL", "http://api:8000/api/v1")

st.set_page_config(
    page_title="E-Commerce Analytics",
    page_icon="🛒",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Custom CSS ─────────────────────────────────────────────────────────────

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .stMetric label { font-size: 0.85rem; color: #94a3b8; }
    .stMetric [data-testid="metric-container"] { background: #1e293b; border-radius: 10px; padding: 16px; }
    h1 { background: linear-gradient(90deg, #6366f1, #8b5cf6); -webkit-background-clip: text;
         -webkit-text-fill-color: transparent; }
</style>
""", unsafe_allow_html=True)


# ── Helpers ────────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def fetch(endpoint: str, params: dict = None) -> dict | list | None:
    """Fetch data from the API with caching."""
    try:
        resp = requests.get(f"{API_BASE}{endpoint}", params=params, timeout=10)
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.ConnectionError:
        st.warning("⚠️ API not reachable. Make sure the API service is running.")
        return None
    except Exception as exc:
        st.error(f"API error: {exc}")
        return None


# ── Sidebar ────────────────────────────────────────────────────────────────

with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/shopping-cart.png", width=64)
    st.title("E-Commerce\nAnalytics")
    st.divider()

    days = st.selectbox("📅 Lookback Period", [7, 14, 30, 60, 90], index=2, format_func=lambda x: f"Last {x} days")
    granularity = st.selectbox("⏱️ Trend Granularity", ["hour", "day", "week", "month"], index=1)
    category_filter = st.text_input("🏷️ Category Filter (optional)", "")
    top_n = st.slider("🏆 Top N Products", 5, 50, 10)

    st.divider()
    if st.button("🔄 Refresh Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    st.caption(f"Data refreshes every 60s")


# ── Main Dashboard ─────────────────────────────────────────────────────────

st.title("🛒 E-Commerce Data Platform")
st.caption("Real-time analytics powered by Kafka → Spark → PostgreSQL → dbt")

# ── KPI Row ────────────────────────────────────────────────────────────────

summary = fetch("/metrics/summary", {"days": days})

col1, col2, col3, col4 = st.columns(4)
if summary:
    col1.metric("💰 Total Revenue", f"${summary.get('total_revenue', 0):,.2f}")
    col2.metric("📦 Total Orders", f"{summary.get('total_orders', 0):,}")
    col3.metric("👥 Active Users", f"{summary.get('total_users', 0):,}")
    col4.metric("🛒 Avg Order Value", f"${summary.get('avg_order_value', 0):,.2f}")
else:
    for col in [col1, col2, col3, col4]:
        col.metric("—", "N/A")

st.divider()

# ── Revenue Trends ─────────────────────────────────────────────────────────

col_left, col_right = st.columns([2, 1])

with col_left:
    st.subheader("📈 Revenue Trends")
    trends_data = fetch("/metrics/revenue/trends", {"days": days, "granularity": granularity})
    if trends_data:
        df_trends = pd.DataFrame(trends_data)
        fig = px.area(
            df_trends,
            x="period",
            y="revenue",
            title=f"Revenue ({granularity.capitalize()}ly)",
            color_discrete_sequence=["#6366f1"],
            template="plotly_dark",
        )
        fig.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="",
            yaxis_title="Revenue ($)",
            showlegend=False,
            margin=dict(l=0, r=0, t=40, b=0),
        )
        fig.update_traces(fill="tozeroy", line_color="#6366f1", fillcolor="rgba(99,102,241,0.15)")
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No revenue trend data available.")

with col_right:
    st.subheader("🥧 Category Breakdown")
    cat_data = fetch("/metrics/categories", {"days": days})
    if cat_data:
        df_cat = pd.DataFrame(cat_data)
        fig_pie = px.pie(
            df_cat,
            names="category",
            values="total_revenue",
            template="plotly_dark",
            color_discrete_sequence=px.colors.qualitative.Vivid,
            hole=0.4,
        )
        fig_pie.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=20, b=0),
            legend=dict(orientation="v", x=1, y=0.5),
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No category data available.")

st.divider()

# ── Top Products ───────────────────────────────────────────────────────────

st.subheader("🏆 Top Products by Revenue")
params = {"limit": top_n, "days": days}
if category_filter:
    params["category"] = category_filter

products_data = fetch("/products/top", params)
if products_data and products_data.get("products"):
    df_prod = pd.DataFrame(products_data["products"])
    fig_bar = px.bar(
        df_prod.head(top_n),
        x="total_revenue",
        y="name",
        orientation="h",
        color="category",
        text="total_revenue",
        template="plotly_dark",
        color_discrete_sequence=px.colors.qualitative.Vivid,
    )
    fig_bar.update_traces(texttemplate="$%{text:,.0f}", textposition="outside")
    fig_bar.update_layout(
        plot_bgcolor="rgba(0,0,0,0)",
        paper_bgcolor="rgba(0,0,0,0)",
        xaxis_title="Total Revenue ($)",
        yaxis_title="",
        yaxis=dict(autorange="reversed"),
        margin=dict(l=0, r=60, t=20, b=0),
        showlegend=True,
    )
    st.plotly_chart(fig_bar, use_container_width=True)

    with st.expander("📋 View Raw Data"):
        st.dataframe(
            df_prod[["name", "category", "price", "total_revenue", "total_orders", "total_units_sold"]],
            use_container_width=True,
        )
else:
    st.info("No product data available.")

st.divider()

# ── Active Users ───────────────────────────────────────────────────────────

st.subheader("👥 Most Active Users")
users_data = fetch("/users/activity", {"days": days, "limit": 15})
if users_data and users_data.get("users"):
    df_users = pd.DataFrame(users_data["users"])

    col_u1, col_u2 = st.columns(2)
    with col_u1:
        fig_spend = px.bar(
            df_users.head(10),
            x="name",
            y="total_spend",
            color="total_spend",
            template="plotly_dark",
            color_continuous_scale="Viridis",
            title="Top Users by Spend",
        )
        fig_spend.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            xaxis_title="",
            yaxis_title="Total Spend ($)",
            margin=dict(l=0, r=0, t=40, b=0),
            coloraxis_showscale=False,
        )
        st.plotly_chart(fig_spend, use_container_width=True)

    with col_u2:
        fig_events = px.scatter(
            df_users,
            x="total_events",
            y="total_spend",
            size="total_orders",
            color="total_sessions",
            hover_name="name",
            template="plotly_dark",
            title="Engagement vs Spend",
            color_continuous_scale="Plasma",
        )
        fig_events.update_layout(
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            margin=dict(l=0, r=0, t=40, b=0),
        )
        st.plotly_chart(fig_events, use_container_width=True)

    with st.expander("📋 View User Data"):
        st.dataframe(
            df_users[["name", "email", "location", "total_orders", "total_spend", "total_events"]],
            use_container_width=True,
        )
else:
    st.info("No user activity data available.")

# ── Footer ─────────────────────────────────────────────────────────────────
st.divider()
st.caption("🔧 E-Commerce Data Platform · Kafka + Spark + PostgreSQL + dbt + FastAPI + Streamlit")
