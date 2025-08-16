from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

import sys

# --------- Args ---------
args = getResolvedOptions(sys.argv, ["BUCKET", "FROM_DT"])
BUCKET = args["BUCKET"]
FROM_DT = args["FROM_DT"]  # YYYY-MM-DD (matches ingest_dt folder)

bronze_base = f"s3://{BUCKET}/bronze"
gold_base   = f"s3://{BUCKET}/gold"

spark_context = SparkContext.getOrCreate()
glue_context = GlueContext(spark_context)
spark = glue_context.spark_session

# safer overwrites by partition
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# --------- Read bronze (CSV) ---------
oi_path  = f"{bronze_base}/order_items/ingest_dt={FROM_DT}/*"
opt_path = f"{bronze_base}/order_item_options/ingest_dt={FROM_DT}/*"

# If you also staged a date_dim CSV, it is not required for this v1 job.

# Read order_items
order_items = (
    spark.read
    .option("header", "true")
    .csv(oi_path)
)

# Ensure required columns exist
required_cols = ["order_id", "lineitem_id", "restaurant_id", "user_id", "creation_time_utc", "item_price", "item_quantity"]
missing = [c for c in required_cols if c not in order_items.columns]
if missing:
    raise Exception(f"Missing columns in order_items CSV: {missing}")

# Optional loyalty flag
has_loyalty = "is_loyalty_member" in order_items.columns

# Type casts + derived
order_items_t = (
    order_items.select(
        F.col("order_id").cast("string"),
        F.col("lineitem_id").cast("string"),
        F.col("restaurant_id").cast("string"),
        F.col("user_id").cast("string").alias("customer_id"),
        F.to_timestamp("creation_time_utc").alias("order_ts"),
        F.col("item_price").cast("double"),
        F.col("item_quantity").cast("int"),
        (F.col("is_loyalty_member").cast("boolean") if has_loyalty else F.lit(None).cast("boolean")).alias("is_loyalty_member")
    )
    .withColumn("order_date", F.to_date("order_ts"))
)

# Read order_item_options (optional for discounts)
try:
    order_opts = (
        spark.read
        .option("header", "true")
        .csv(opt_path)
    )
    has_opts = True
except Exception:
    has_opts = False

if has_opts and len(order_opts.columns) > 0:
    # best-effort casts
    needed_opt_cols = ["order_id", "lineitem_id", "option_price", "option_quantity"]
    # Add missing columns gracefully if absent
    for c in needed_opt_cols:
        if c not in order_opts.columns:
            order_opts = order_opts.withColumn(c, F.lit(None))

    order_opts_t = (
        order_opts.select(
            F.col("order_id").cast("string"),
            F.col("lineitem_id").cast("string"),
            F.col("option_price").cast("double"),
            F.col("option_quantity").cast("int")
        )
    )
    # per-order aggregates
    opt_order_agg = (
        order_opts_t
        .groupBy("order_id")
        .agg(
            F.sum(F.col("option_price") * F.col("option_quantity")).alias("option_net"),
            F.max(F.when(F.col("option_price") < 0, F.lit(1)).otherwise(F.lit(0))).alias("has_discount_int")
        )
        .withColumn("has_discount", F.col("has_discount_int") == F.lit(1))
        .drop("has_discount_int")
    )
else:
    # no options present
    opt_order_agg = spark.createDataFrame([], T.StructType([
        T.StructField("order_id", T.StringType(), True),
        T.StructField("option_net", T.DoubleType(), True),
        T.StructField("has_discount", T.BooleanType(), True),
    ]))

# --------- Build gold.fct_order ---------
items_order_agg = (
    order_items_t
    .groupBy("order_id", "restaurant_id", "customer_id", "order_date", "is_loyalty_member")
    .agg(
        F.sum(F.col("item_price") * F.col("item_quantity")).alias("revenue_gross"),
        F.sum(F.col("item_quantity")).alias("total_items"),
        F.max("order_ts").alias("order_ts")  # latest ts per order
    )
)

fct_order = (
    items_order_agg
    .join(opt_order_agg, on="order_id", how="left")
    .withColumn("option_net", F.coalesce(F.col("option_net"), F.lit(0.0)))
    .withColumn("has_discount", F.coalesce(F.col("has_discount"), F.lit(False)))
    .withColumn("revenue_net", F.col("revenue_gross") + F.col("option_net"))
    .withColumn("dt", F.col("order_date"))  # partition column
    .select(
        "order_id",
        "customer_id",
        "restaurant_id",
        "order_ts",
        "order_date",
        "revenue_gross",
        "option_net",
        "revenue_net",
        "total_items",
        "has_discount",
        "is_loyalty_member",
        "dt"
    )
)

# --------- Write gold.fct_order (Parquet partitioned by dt) ---------
fct_order_path = f"{gold_base}/fct_order"
(
    fct_order
    .write
    .mode("overwrite")
    .format("parquet")
    .partitionBy("dt")
    .save(fct_order_path)
)

print(f"Wrote gold.fct_order to {fct_order_path}")
