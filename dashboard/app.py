import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ---- Page config ----
st.set_page_config(
    page_title="Customer 360 Analytics",
    page_icon="📊",
    layout="wide",
)

DATA_DIR = Path(__file__).parent / "data"

@st.cache_data
def load_data():
    customer_360 = pd.read_csv(DATA_DIR / "customer_360.csv")
    mrr = pd.read_csv(DATA_DIR / "mrr.csv")
    churn = pd.read_csv(DATA_DIR / "churn.csv")

    # Parse date columns for nicer charts
    if "invoice_month" in mrr.columns:
        mrr["invoice_month"] = pd.to_datetime(mrr["invoice_month"])
    if "churn_month" in churn.columns:
        churn["churn_month"] = pd.to_datetime(churn["churn_month"])

    return customer_360, mrr, churn

customer_360, mrr, churn = load_data()

# header
st.markdown("## 📊 Customer 360 Analytics Dashboard")
st.markdown(
    "End-to-end Databricks → Gold layer exported to power key revenue and churn insights."
)

# ---- High-level KPIs ----
total_revenue = customer_360["total_revenue"].sum()
churn_rate = (customer_360["churn_risk"] == "high").mean() * 100
total_customers = len(customer_360)

kpi1, kpi2, kpi3 = st.columns(3)
kpi1.metric("Total Revenue", f"${total_revenue:,.0f}")
kpi2.metric("Churn Rate", f"{churn_rate:.2f}%")
kpi3.metric("Total Customers", total_customers)

st.markdown("---")

# layout tabs
tab_overview, tab_customers = st.tabs(["📈 Overview", "🧍 Customer 360"])

# overview 
with tab_overview:
    col_left, col_right = st.columns(2)

    # MRR chart
    with col_left:
        st.subheader("📈 Monthly Recurring Revenue (MRR)")
        if not mrr.empty:
            mrr_sorted = mrr.sort_values("invoice_month")
            fig_mrr = px.line(
                mrr_sorted,
                x="invoice_month",
                y="mrr",
                markers=True,
                labels={"invoice_month": "Month", "mrr": "MRR ($)"},
                title="MRR Over Time",
            )
            fig_mrr.update_layout(margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_mrr, use_container_width=True)
        else:
            st.info("No MRR data available.")

    # Churn chart
    with col_right:
        st.subheader("📉 Churned Customers Per Month")
        if not churn.empty:
            churn_sorted = churn.sort_values("churn_month")
            fig_churn = px.bar(
                churn_sorted,
                x="churn_month",
                y="churned_customers",
                labels={"churn_month": "Month", "churned_customers": "Churned Customers"},
                title="Churned Customers Per Month",
            )
            fig_churn.update_layout(margin=dict(l=10, r=10, t=40, b=10))
            st.plotly_chart(fig_churn, use_container_width=True)
        else:
            st.info("No churn data available.")

    st.markdown("### 💰 Revenue by Segment")
    if "segment" in customer_360.columns:
        seg_rev = (
            customer_360.groupby("segment", as_index=False)["total_revenue"]
            .sum()
            .sort_values("total_revenue", ascending=False)
        )
        fig_seg = px.bar(
            seg_rev,
            x="segment",
            y="total_revenue",
            labels={"segment": "Segment", "total_revenue": "Total Revenue ($)"},
            title="Total Revenue by Segment",
        )
        fig_seg.update_layout(margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_seg, use_container_width=True)

#
with tab_customers:
    st.subheader("🧍 Customer 360 View")

    # Filters
    col_f1, col_f2, col_f3 = st.columns(3)

    with col_f1:
        region_choices = ["All"] + sorted(
            customer_360["region"].dropna().unique().tolist()
        )
        region_filter = st.selectbox("Region", region_choices)

    with col_f2:
        segment_choices = ["All"] + sorted(
            customer_360["segment"].dropna().unique().tolist()
        )
        segment_filter = st.selectbox("Segment", segment_choices)

    with col_f3:
        churn_choices = ["All", "low", "high"]
        churn_filter = st.selectbox("Churn Risk", churn_choices)

    # Apply filters
    df = customer_360.copy()
    if region_filter != "All":
        df = df[df["region"] == region_filter]
    if segment_filter != "All":
        df = df[df["segment"] == segment_filter]
    if churn_filter != "All":
        df = df[df["churn_risk"] == churn_filter]

    # Sort customers by revenue descending
    if "total_revenue" in df.columns:
        df = df.sort_values("total_revenue", ascending=False)

    st.markdown(f"**Showing {len(df)} customers**")
    st.dataframe(df, use_container_width=True, height=450)