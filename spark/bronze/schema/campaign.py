from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

# Column order = CSV header order.
SCHEMA = StructType([
    StructField("campaign_id", IntegerType()),
    StructField("campaign_code", StringType()),
    StructField("campaign_name", StringType()),
    StructField("channel", StringType()),
    StructField("budget_aud", DoubleType()),
    StructField("discount_pct", DoubleType()),
    StructField("start_date", DateType()),
    StructField("is_running", BooleanType()),
    StructField("snapshot_date", DateType()),
])
