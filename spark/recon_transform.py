"""Entry point: cell-to-cell recon of one feed for one business date, AWS output vs GCP bronze -> recon.<feed>.

  --source / --feed <name>   e.g. ecommerce / campaign (columns from bronze/<source>/<feed>/schema.py)
  --keys <col,...>           business key(s); _meta_file_name is always added
  --date <yyyy-mm-dd>        business date, taken from the file name on both sides
  --processed gs://<bucket>  AWS output copied by Storage Transfer: <bucket>/<source>/<feed>/<date>/*.parquet
  --batch-id <id>            for the log and the output

GCP side: lake.bronze.<feed> (Iceberg). Output: BigQuery native table recon.<feed>, partitioned by
business_date; a rerun replaces only that date's partition.
"""
import argparse
import logging
import sys

from pyspark.sql import SparkSession, functions as F

from bronze.base import FeedConfig
from recon.compare import FILE_KEY, business_date, compare

log = logging.getLogger("recon")


def main():
    p = argparse.ArgumentParser()
    for arg in ("--source", "--feed", "--keys", "--date", "--processed", "--batch-id"):
        p.add_argument(arg, required=True)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = FeedConfig.load(a.source, a.feed)
    keys = a.keys.split(",")
    columns = [c for c in cfg.schema.fieldNames() if c not in keys]

    spark = (SparkSession.builder.appName(f"recon_{a.feed}")
             .config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate())
    on_date = business_date(F.col(FILE_KEY)) == F.lit(a.date).cast("date")   # columns need the session
    aws = spark.read.parquet(f"{a.processed.rstrip('/')}/{a.source}/{a.feed}/*/*.parquet").filter(on_date)
    gcp = spark.table(f"lake.bronze.{a.feed}").filter(on_date)
    log.info("batch=%s feed=%s date=%s keys=%s columns=%d", a.batch_id, a.feed, a.date, keys, len(columns))

    out = (compare(aws, gcp, keys, columns)
           .withColumn("_recon_batch_id", F.lit(a.batch_id))
           .withColumn("_recon_ts", F.current_timestamp())).cache()
    if out.isEmpty():
        raise ValueError(f"no rows for {a.feed} on {a.date} on either side")

    summary = out.groupBy("_row_status", "_all_eq").count().collect()
    log.info("rows: %s", sorted((r._row_status, r._all_eq, r["count"]) for r in summary))
    diffs = out.select([F.sum((~F.col(f"{c}_is_eq")).cast("int")).alias(c) for c in columns]).first().asDict()
    log.info("unequal cells per column: %s", {c: n for c, n in diffs.items() if n})

    (out.coalesce(1).write.format("bigquery")   # one feed-day is small: one writer, not one per shuffle partition
     .option("writeMethod", "direct")
     .option("partitionField", "business_date").option("partitionType", "DAY")
     .mode("overwrite")                       # dynamic: only this date's partition
     .save(f"recon.{a.feed}"))
    log.info("wrote recon.%s date=%s rows=%d", a.feed, a.date, out.count())
    spark.stop()


if __name__ == "__main__":
    main()
