"""Landing CSV files -> bronze Iceberg tables (one table per feed).

Incremental by file name: a file whose path is already in bronze `_src_file` is skipped.
Each file's row count must match its .ctrl file, or the file is not loaded and the job fails.
Each feed is written in one Iceberg commit (all new files, or none).

Args:
  --landing gs://<bucket>    landing bucket
  --batch-id <id>            recorded in _batch_id (the Dataproc batch id)
  --reload <regex>           re-load files whose path matches, replacing their rows atomically
"""
import argparse
import re
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (BooleanType, DateType, DoubleType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

CATALOG = "lake"        # Spark catalog backed by BigQuery metastore (see scripts/run-bronze.sh)
NAMESPACE = "bronze"    # = BigQuery dataset
DOMAIN = "ecommerce"

S, I, D, B, DT, TS = StringType(), IntegerType(), DoubleType(), BooleanType(), DateType(), TimestampType()

# Config: one entry per feed. Column order = CSV header order.
FEEDS = {
    "campaign": [("campaign_id", I), ("campaign_code", S), ("campaign_name", S), ("channel", S),
                 ("budget_aud", D), ("discount_pct", D), ("start_date", DT), ("is_running", B),
                 ("snapshot_date", DT)],
    "customer": [("customer_id", I), ("first_name", S), ("last_name", S), ("email", S), ("tier", S),
                 ("credit_limit", D), ("state", S), ("city", S), ("postal_code", S),
                 ("signup_date", DT), ("marketing_opt_in", B), ("is_active", B), ("snapshot_date", DT)],
    "product": [("product_id", I), ("sku", S), ("brand", S), ("category", S), ("subcategory", S),
                ("product_name", S), ("unit_price", D), ("unit_cost", D), ("weight_kg", D),
                ("launch_date", DT), ("is_available", B), ("snapshot_date", DT)],
    "transaction": [("transaction_id", S), ("event_ts", TS), ("customer_id", I), ("product_id", I),
                    ("campaign_id", I), ("quantity", I), ("unit_price", D), ("unit_cost", D),
                    ("discount_pct", D), ("gross_amount", D), ("net_amount", D), ("margin_amount", D),
                    ("currency", S), ("payment_method", S), ("channel", S), ("status", S)],
}


def list_files(spark, glob):
    """Paths matching a gs:// glob, via the Hadoop GCS connector built into Dataproc."""
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(glob)
    statuses = path.getFileSystem(spark._jsc.hadoopConfiguration()).globStatus(path) or []
    return sorted(s.getPath().toString() for s in statuses)


def relative(path, landing):
    """gs://bucket/ecommerce/x/2026-01-01/f.csv -> ecommerce/x/2026-01-01/f.csv (no project id in data)."""
    return path[len(landing.rstrip("/")) + 1:]


def ingest_feed(spark, feed, landing, batch_id, reload_re):
    table = f"{CATALOG}.{NAMESPACE}.{feed}"
    exists = spark.catalog.tableExists(table)
    files = list_files(spark, f"{landing}/{DOMAIN}/{feed}/*/*.csv")

    if reload_re:
        todo = [f for f in files if re.search(reload_re, relative(f, landing))]
    else:
        loaded = set()
        if exists:
            loaded = {r[0] for r in spark.table(table).select("_src_file").distinct().collect()}
        todo = [f for f in files if relative(f, landing) not in loaded]

    print(f"[{feed}] {len(files)} files in landing, {len(todo)} to load")
    if not todo:
        return True

    schema = StructType([StructField(n, t, True) for n, t in FEEDS[feed]])
    raw = (spark.read.option("header", True).option("mode", "FAILFAST").schema(schema).csv(todo)
           .withColumn("_src_path", F.col("_metadata.file_path")))

    # Row counts per file vs .ctrl record_count.
    actual = {r["_src_path"]: r["n"] for r in raw.groupBy("_src_path").agg(F.count("*").alias("n")).collect()}
    ctrl = {r["file_name"]: int(r["record_count"]) for r in
            spark.read.option("header", True).csv([f[:-4] + ".ctrl" for f in todo]).collect()}
    good, ok = [], True
    for f in todo:
        expected, got = ctrl.get(f.rsplit("/", 1)[1]), actual.get(f, 0)
        if expected == got:
            good.append(f)
        else:
            ok = False
            print(f"[{feed}] SKIP {relative(f, landing)}: rows {got} != ctrl {expected}")
    if not good:
        return ok

    df = (raw.where(F.col("_src_path").isin(good))
          .withColumn("_src_file", F.expr(f"substr(_src_path, {len(landing.rstrip('/')) + 2})"))
          .withColumn("_business_date", F.to_date(F.regexp_extract("_src_file", r"/(\d{4}-\d{2}-\d{2})/", 1)))
          .withColumn("_loaded_at", F.current_timestamp())
          .withColumn("_batch_id", F.lit(batch_id))
          .drop("_src_path"))

    if not exists:
        df.writeTo(table).using("iceberg").partitionedBy(F.col("_business_date")).create()
    elif reload_re:
        # Atomic: delete the old rows of these files and add the new ones in one snapshot.
        files_rel = [relative(f, landing) for f in good]
        df.writeTo(table).overwrite(F.col("_src_file").isin(files_rel))
    else:
        df.writeTo(table).append()
    print(f"[{feed}] loaded {len(good)} files, {sum(actual[f] for f in good)} rows")
    return ok


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--landing", required=True)
    p.add_argument("--batch-id", required=True)
    p.add_argument("--reload")
    args = p.parse_args()

    spark = SparkSession.builder.appName("bronze_ingest").getOrCreate()
    results = [ingest_feed(spark, feed, args.landing, args.batch_id, args.reload) for feed in FEEDS]
    spark.stop()
    sys.exit(0 if all(results) else 1)


if __name__ == "__main__":
    main()
