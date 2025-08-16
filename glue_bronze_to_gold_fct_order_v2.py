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

sc = SparkContext.getOrCreate()
gc = GlueContext(sc)
spark = gc.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def normalize_cols(df):
    return df.toDF(*[c.strip().lower().replace(" ", "_") for c in df.columns])

# --------- Read bronze (CSV) ---------
oi_path  = f"{bronze_base}/order_items/ingest_dt={FROM_DT}/*"
opt_path = f"{bronze_base}/order_item_options/ingest_dt={FROM_DT}/*"

order_items_raw = spark.read.option("header", "true").csv(oi_path)
order_items = normalize_cols(order_items_raw)

# Map expected names (handles upper-case headers like ORDER_ID, etc.)
col_map = {
    "order_id": None,
    "lineitem_id": None,
    "restaurant_id": None,
    "user_id": None,
    "creation_time_utc": None,
    "item_price": None,
    "item_quantity": None,
    # loyalty (optional)
    "is_loyalty_member": None
}

for c in order_items.columns:
    if c in ("order_id","lineitem_id","restaurant_id","user_id","creation_time_utc","item_price","item_quantity","is_loyalty_member","is_loyalty"):
        # prefer exact match, otherwise map is_loyalty -> is_loyalty_member
        key = c if c != "is_loyalty" else "is_loyalty_member"
        col_map[key] = c

missing = [k for k,v in col_map.items() if k not in ("is_loyalty_member",) and v is None]
if missing:
    raise Exception(f"Missing required columns in order_items CSV: {missing}. Found: {order_items.columns}")

# Parse timestamp in ISO 8601 with Z (UTC)
order_items_t = (
    order_items
    .withColumn("order_id", F.col(col_map["order_id"]).cast("string"))
    .withColumn("lineitem_id", F.col(col_map["lineitem_id"]).cast("string"))
    .withColumn("restaurant_id", F.col(col_map["restaurant_id"]).cast("string"))
    .withColumn("customer_id", F.col(col_map["user_id"]).cast("string"))
    .withColumn("order_ts", F.to_timestamp(F.col(col_map["creation_time_utc"]), "yyyy-MM-dd'T'HH:mm:ss.SSSX"))
    .withColumn("item_price", F.col(col_map["item_price"]).cast("double"))
    .withColumn("item_quantity", F.col(col_map["item_quantity"]).cast("int"))
    .withColumn("is_loyalty_member",
        F.col(col_map["is_loyalty_member"]).cast("boolean") if col_map["is_loyalty_member"] else F.lit(None).cast("boolean")
    )
    .withColumn("order_date", F.to_date("order_ts"))
    .select("order_id","lineitem_id","restaurant_id","customer_id","order_ts","order_date","item_price","item_quantity","is_loyalty_member")
)

# --------- Options (discounts) ---------
try:
    order_opts_raw = spark.read.option("header","true").csv(opt_path)
    order_opts = normalize_cols(order_opts_raw)
    has_opts = True
except Exception:
    has_opts = False

if has_opts:
    # map expected fields
    omap = {"order_id":None, "lineitem_id":None, "option_price":None, "option_quantity":None}
    for c in order_opts.columns:
        if c in omap:
            omap[c] = c
    # if options missing, build empty
    if any(omap[k] is None for k in ("order_id","lineitem_id","option_price","option_quantity")):
        has_opts = False

if has_opts:
    order_opts_t = (
        order_opts
        .withColumn("order_id", F.col(omap["order_id"]).cast("string"))
        .withColumn("lineitem_id", F.col(omap["lineitem_id"]).cast("string"))
        .withColumn("option_price", F.col(omap["option_price"]).cast("double"))
        .withColumn("option_quantity", F.col(omap["option_quantity"]).cast("int"))
    )
    opt_per_order = (
        order_opts_t.groupBy("order_id")
        .agg(
            F.sum(F.col("option_price")*F.col("option_quantity")).alias("option_net"),
            F.max(F.when(F.col("option_price") < 0, F.lit(1)).otherwise(F.lit(0))).alias("has_discount_int")
        )
        .withColumn("has_discount", F.col("has_discount_int") == F.lit(1))
        .drop("has_discount_int")
    )
else:
    opt_per_order = spark.createDataFrame([], T.StructType([
        T.StructField("order_id", T.StringType(), True),
        T.StructField("option_net", T.DoubleType(), True),
        T.StructField("has_discount", T.BooleanType(), True),
    ]))

# --------- Build gold.fct_order ---------
items_per_order = (
    order_items_t
    .groupBy("order_id","restaurant_id","customer_id","order_date","is_loyalty_member")
    .agg(
        F.sum(F.col("item_price")*F.col("item_quantity")).alias("revenue_gross"),
        F.sum("item_quantity").alias("total_items"),
        F.max("order_ts").alias("order_ts")
    )
)

fct_order = (
    items_per_order
    .join(opt_per_order, on="order_id", how="left")
    .withColumn("option_net", F.coalesce(F.col("option_net"), F.lit(0.0)))
    .withColumn("has_discount", F.coalesce(F.col("has_discount"), F.lit(False)))
    .withColumn("revenue_net", F.col("revenue_gross") + F.col("option_net"))
    .withColumn("dt", F.col("order_date"))
    .select(
        "order_id","customer_id","restaurant_id","order_ts","order_date",
        "revenue_gross","option_net","revenue_net","total_items",
        "has_discount","is_loyalty_member","dt"
    )
)

# --------- Write ---------
out_path = f"{gold_base}/fct_order"
(fct_order.write.mode("overwrite").format("parquet").partitionBy("dt").save(out_path))
print(f"Wrote gold.fct_order to {out_path}")
