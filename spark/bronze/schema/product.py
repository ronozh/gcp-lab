from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType)

# Column order = CSV header order.
SCHEMA = StructType([
    StructField("product_id", IntegerType()),
    StructField("sku", StringType()),
    StructField("brand", StringType()),
    StructField("category", StringType()),
    StructField("subcategory", StringType()),
    StructField("product_name", StringType()),
    StructField("unit_price", DoubleType()),
    StructField("unit_cost", DoubleType()),
    StructField("weight_kg", DoubleType()),
    StructField("launch_date", DateType()),
    StructField("is_available", BooleanType()),
    StructField("snapshot_date", DateType()),
])
