"""Entry point: one feed's landing CSV files -> its bronze Iceberg table.

  --source <source>                   e.g. ecommerce (folder bronze/<source>/)
  --feed <name>                       e.g. transaction (folder bronze/<source>/<feed>/)
  --mode incremental|full_refresh     default: incremental
  --landing gs://<bucket>             landing bucket
  --batch-id <id>                     recorded in _meta_batch_id (the Dataproc batch id)

The packages ship as lib.zip (--py-files): bronze/base.py (FeedConfig), bronze/ingest.py (common logic),
bronze/<source>/<feed>/config.py + schema.py (one folder per feed).
"""
import argparse
import logging
import sys

from pyspark.sql import SparkSession

from bronze.base import FeedConfig
from bronze.ingest import MODES, ingest


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--source", required=True)
    p.add_argument("--feed", required=True)
    p.add_argument("--mode", choices=MODES, default="incremental")
    p.add_argument("--landing", required=True)
    p.add_argument("--batch-id", required=True)
    a = p.parse_args()
    # Our lines go to stdout, Spark's own to stderr. Serverless ships both to Cloud Logging; filter on "bronze:".
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    cfg = FeedConfig.load(a.source, a.feed)
    spark = SparkSession.builder.appName(f"bronze_ingest_{a.feed}").getOrCreate()
    ingest(spark, cfg, a.landing.rstrip("/"), a.batch_id, a.mode)
    spark.stop()


if __name__ == "__main__":
    main()
