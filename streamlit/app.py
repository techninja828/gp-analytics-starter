import os
import pandas as pd
import streamlit as st
import altair as alt
from pyathena import connect

AWS_REGION = os.getenv("AWS_REGION", "us-west-2")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "primary")
ATHENA_S3_STAGING_DIR = os.getenv("ATHENA_S3_STAGING_DIR")  # s3://gp-data-project/athena-results/

if not ATHENA_S3_STAGING_DIR:
    st.error("Missing env var ATHENA_S3_STAGING_DIR (e.g., s3://<bucket>/athena-results/)")
    st.stop()

@st.cache_data(ttl=60)
def run_query(sql: str) -> pd.DataFrame:
    conn = connect(
        s3_staging_dir=ATHENA_S3_STAGING_DIR,
        region_name=AWS_REGION,
        work_group=ATHENA_WORKGROUP,
    )
    return pd.read_sql(sql, conn)

st.set_page_config(page_title="GlobalPartners Analytics", layout="wide")
st.title("GlobalPartners Analytics")
st.caption("Athena-backed dashboards")

TAB_METRICS = {
    "Segmentation": [
        "Customers by CLV band and loyalty",
        "Customers by RFM segment and loyalty",
    ],
    "Churn": [
        "Customers flagged by days since last order",
        "Average days between orders and spend change",
    ],
    "Sales Trends": [
        "Monthly revenue and order volume",
    ],
    "Loyalty": [
        "Average order value and lifetime value for loyalty members",
    ],
    "Locations": [
        "Revenue, AOV, repeat rate, and orders per week by location",
    ],
    "Discounts": [
        "Impact of discounts on revenue and orders",
    ],
}


def render_metrics(tab_name: str) -> None:
    """Display a bullet list of metrics for the given tab."""
    metrics = TAB_METRICS.get(tab_name, [])
    if metrics:
        st.markdown("**Metrics**")
        st.markdown("\n".join(f"- {m}" for m in metrics))

tabs = st.tabs([
    "Segmentation","Churn","Sales Trends","Loyalty","Locations","Discounts"
])

with tabs[0]:
    st.subheader("Customer Segmentation (latest day)")
    render_metrics("Segmentation")
    df = run_query("SELECT * FROM default.gp_v_customer_segmentation")
    st.metric("Customers", int(df["customer_count"].sum()))

    clv_chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="clv_band:N",
            y="sum(customer_count):Q",
            color="is_loyalty_member:N",
        )
        .properties(title="Customers by CLV Band and Loyalty")
    )
    st.altair_chart(clv_chart, use_container_width=True)

    rfm_chart = (
        alt.Chart(df)
        .mark_bar()
        .encode(
            x="rfm_segment:N",
            y="sum(customer_count):Q",
            color="is_loyalty_member:N",
        )
        .properties(title="Customers by RFM Segment and Loyalty")
    )
    st.altair_chart(rfm_chart, use_container_width=True)

    st.dataframe(df)

with tabs[1]:
    st.subheader("Churn Risk (latest day)")
    render_metrics("Churn")
    df = run_query("SELECT * FROM default.gp_v_churn_indicators")
    risky = df[df["at_risk"]]
    st.metric("At risk (>=45d)", len(risky))

    def days_style(val):
        if pd.isna(val):
            return ""
        if val > 30:
            return "background-color: #ffcccc"
        if val > 15:
            return "background-color: #fff0b3"
        return ""

    def spend_style(val):
        if pd.isna(val):
            return ""
        if val < -0.5:
            return "background-color: #ffcccc"
        if val < -0.2:
            return "background-color: #fff0b3"
        return ""

    styled = (
        risky.head(200)
        .style.format(
            {"spend_change_pct": "{:.1%}", "avg_days_between_orders": "{:.1f}"}
        )
        .applymap(days_style, subset=["avg_days_between_orders"])
        .applymap(spend_style, subset=["spend_change_pct"])
    )
    st.dataframe(styled)

with tabs[2]:
    st.subheader("Sales Trends (monthly)")
    render_metrics("Sales Trends")
    df = run_query("SELECT * FROM default.gp_v_sales_trends_monthly ORDER BY month")
    st.line_chart(df.set_index('month')[['revenue_net','orders']])
    st.dataframe(df.tail(24))

with tabs[3]:
    st.subheader("Loyalty Impact")
    render_metrics("Loyalty")
    df = run_query("SELECT * FROM default.gp_v_loyalty_impact")
    st.dataframe(df)

with tabs[4]:
    st.subheader("Location Performance")
    render_metrics("Locations")
    df = run_query(
        """
        SELECT p.restaurant_id,
               p.revenue_net,
               p.orders,
               p.aov,
               r.repeat_customer_rate,
               r.orders_per_week
        FROM default.gp_v_location_performance p
        LEFT JOIN default.gp_v_location_retention r
          ON p.restaurant_id = r.restaurant_id
        ORDER BY p.revenue_net DESC
        """
    )
    st.dataframe(df)

with tabs[5]:
    st.subheader("Pricing & Discount Effectiveness")
    render_metrics("Discounts")
    df = run_query("SELECT * FROM default.gp_v_discount_effectiveness")
    st.dataframe(df)
