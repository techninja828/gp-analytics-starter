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

tabs = st.tabs([
    "Segmentation","Churn","Sales Trends","Loyalty","Locations","Discounts"
])

with tabs[0]:
    st.subheader("Customer Segmentation (latest day)")
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
    df = run_query("SELECT * FROM default.gp_v_churn_indicators")
    risky = df[df['at_risk'] == True]
    st.metric("At risk (>=45d)", len(risky))
    st.dataframe(risky.head(200))

with tabs[2]:
    st.subheader("Sales Trends (monthly)")
    df = run_query("SELECT * FROM default.gp_v_sales_trends_monthly ORDER BY month")
    st.line_chart(df.set_index('month')[['revenue_net','orders']])
    st.dataframe(df.tail(24))

with tabs[3]:
    st.subheader("Loyalty Impact")
    df = run_query("SELECT * FROM default.gp_v_loyalty_impact")
    st.dataframe(df)

with tabs[4]:
    st.subheader("Location Performance")
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
    df = run_query("SELECT * FROM default.gp_v_discount_effectiveness")
    st.dataframe(df)
