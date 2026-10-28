from pyspark.sql.types import (DoubleType, IntegerType, StringType, StructField, StructType,
                               TimestampNTZType)

# Column order = CSV header order.
SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("event_ts", TimestampNTZType()),   # local time, no zone in the file -> BigQuery DATETIME
    StructField("customer_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("campaign_id", IntegerType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("unit_cost", DoubleType()),
    StructField("discount_pct", DoubleType()),
    StructField("gross_amount", DoubleType()),
    StructField("net_amount", DoubleType()),
    StructField("margin_amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("payment_method", StringType()),
    StructField("channel", StringType()),
    StructField("status", StringType()),
])
