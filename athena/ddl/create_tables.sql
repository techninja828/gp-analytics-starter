
-- Create database
CREATE DATABASE IF NOT EXISTS default;

-- GOLD: fct_order
CREATE EXTERNAL TABLE IF NOT EXISTS default.gp_analytics_fct_order (
  order_id          string,
  customer_id       string,
  restaurant_id     string,
  order_ts          timestamp,
  order_date        date,
  revenue_gross     double,
  option_net        double,
  revenue_net       double,
  total_items       int,
  has_discount      boolean,
  is_loyalty_member boolean
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://gp-data-project/gold/fct_order/';

-- GOLD: fct_customer_day
CREATE EXTERNAL TABLE IF NOT EXISTS default.gp_analytics_fct_customer_day (
  customer_id string,
  orders_to_date int,
  ltv_to_date double,
  aov_to_date double,
  orders_30d int,
  revenue_30d double,
  orders_90d int,
  revenue_90d double,
  days_since_last_order int,
  R int,
  F int,
  M int,
  rfm_segment string,
  clv_band string
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://gp-data-project/gold/fct_customer_day/';

-- SILVER: order_items
CREATE EXTERNAL TABLE IF NOT EXISTS default.gp_silver_order_items (
  order_id string,
  lineitem_id string,
  restaurant_id string,
  customer_id string,
  order_ts timestamp,
  order_date date,
  item_price double,
  item_quantity int,
  is_loyalty_member boolean,
  item_category string,
  item_name string,
  app_name string,
  currency string
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://gp-data-project/silver/order_items/';

-- SILVER: order_item_options
CREATE EXTERNAL TABLE IF NOT EXISTS default.gp_silver_order_item_options (
  order_id string,
  lineitem_id string,
  option_price double,
  option_quantity int,
  option_name string,
  option_group_name string
)
PARTITIONED BY (dt date)
STORED AS PARQUET
LOCATION 's3://gp-data-project/silver/order_item_options/';

-- Discover partitions after writes
MSCK REPAIR TABLE default.gp_analytics_fct_order;
MSCK REPAIR TABLE default.gp_analytics_fct_customer_day;
MSCK REPAIR TABLE default.gp_silver_order_items;
MSCK REPAIR TABLE default.gp_silver_order_item_options;
