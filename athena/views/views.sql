
CREATE OR REPLACE VIEW default.gp_v_customer_segmentation AS
WITH latest AS (
  SELECT max(dt) AS max_dt FROM default.gp_analytics_fct_customer_day
),
loy AS (
  SELECT customer_id,
         MAX(CASE WHEN is_loyalty_member THEN 1 ELSE 0 END) = 1 AS is_loyalty_member
  FROM default.gp_analytics_fct_order
  GROUP BY customer_id
),
base AS (
  SELECT
    f.customer_id,
    f.clv_band,
    f.rfm_segment,
    COALESCE(l.is_loyalty_member, FALSE) AS is_loyalty_member
  FROM default.gp_analytics_fct_customer_day f
  JOIN latest ON f.dt = latest.max_dt
  LEFT JOIN loy l ON f.customer_id = l.customer_id
)
SELECT
  clv_band,
  rfm_segment,
  is_loyalty_member,
  COUNT(*) AS customer_count
FROM base
GROUP BY clv_band, rfm_segment, is_loyalty_member;

-- 2) Churn Risk Indicators (latest day, 45d rule)
CREATE OR REPLACE VIEW default.gp_v_churn_indicators AS
WITH latest AS (
  SELECT max(dt) AS max_dt FROM default.gp_analytics_fct_customer_day
)
SELECT
  f.customer_id, f.dt, f.days_since_last_order,
  f.orders_30d, f.revenue_30d, f.orders_90d, f.revenue_90d,
  (f.days_since_last_order >= 45) AS at_risk
FROM default.gp_analytics_fct_customer_day f
JOIN latest ON f.dt = latest.max_dt;

-- 3) Sales Trends & Seasonality (monthly)
CREATE OR REPLACE VIEW default.gp_v_sales_trends_monthly AS
SELECT
  CAST(date_trunc('month', CAST(order_date AS timestamp)) AS date) AS month,
  SUM(revenue_net) AS revenue_net,
  COUNT(*) AS orders,
  SUM(revenue_net) / COUNT(*) AS aov
FROM default.gp_analytics_fct_order
GROUP BY 1
ORDER BY 1;

-- 4) Loyalty Program Impact
CREATE OR REPLACE VIEW default.gp_v_loyalty_impact AS
WITH cust AS (
  SELECT
    is_loyalty_member,
    customer_id,
    COUNT(*) AS orders_per_cust,
    SUM(revenue_net) AS revenue_per_cust
  FROM default.gp_analytics_fct_order
  GROUP BY is_loyalty_member, customer_id
)
SELECT
  is_loyalty_member,
  SUM(revenue_per_cust) AS revenue_net,
  SUM(orders_per_cust)  AS orders,
  SUM(revenue_per_cust) / NULLIF(SUM(orders_per_cust),0) AS aov,
  SUM(CASE WHEN orders_per_cust >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS repeat_rate
FROM cust
GROUP BY is_loyalty_member;

-- 5) Location Performance
CREATE OR REPLACE VIEW default.gp_v_location_performance AS
SELECT
  restaurant_id,
  SUM(revenue_net) AS revenue_net,
  COUNT(*) AS orders,
  SUM(revenue_net) / COUNT(*) AS aov
FROM default.gp_analytics_fct_order
GROUP BY restaurant_id
ORDER BY revenue_net DESC;

-- 6) Pricing & Discount Effectiveness
CREATE OR REPLACE VIEW default.gp_v_discount_effectiveness AS
SELECT
  has_discount,
  SUM(revenue_gross) AS revenue_gross,
  SUM(revenue_net)   AS revenue_net,
  COUNT(*)           AS orders,
  SUM(revenue_net) / NULLIF(SUM(revenue_gross),0) AS net_to_gross_ratio
FROM default.gp_analytics_fct_order
GROUP BY has_discount;

-- 7) Location Retention
CREATE OR REPLACE VIEW default.gp_v_location_retention AS
WITH cust AS (
  SELECT
    restaurant_id,
    customer_id,
    COUNT(*) AS orders_per_cust
  FROM default.gp_analytics_fct_order
  GROUP BY restaurant_id, customer_id
),
orders AS (
  SELECT
    restaurant_id,
    COUNT(*) AS total_orders,
    MIN(CAST(order_date AS DATE)) AS first_order_date,
    MAX(CAST(order_date AS DATE)) AS last_order_date
  FROM default.gp_analytics_fct_order
  GROUP BY restaurant_id
)
SELECT
  o.restaurant_id,
  SUM(CASE WHEN c.orders_per_cust >= 2 THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS repeat_customer_rate,
  o.total_orders * 1.0 / (date_diff('week', o.first_order_date, o.last_order_date) + 1) AS orders_per_week
FROM cust c
JOIN orders o ON c.restaurant_id = o.restaurant_id
GROUP BY o.restaurant_id, o.total_orders, o.first_order_date, o.last_order_date;
