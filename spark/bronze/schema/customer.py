from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

# Column order = CSV header order.
SCHEMA = StructType([
    StructField("customer_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("tier", StringType()),
    StructField("credit_limit", DoubleType()),
    StructField("state", StringType()),
    StructField("city", StringType()),
    StructField("postal_code", StringType()),
    StructField("signup_date", DateType()),
    StructField("marketing_opt_in", BooleanType()),
    StructField("is_active", BooleanType()),
    StructField("snapshot_date", DateType()),
])
