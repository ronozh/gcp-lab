"""Entry point: one gold model = gold/<model>/rules.py: gold.<SOURCE> -> its declarative rules -> gold.<model> (Iceberg).

  --model <name>           e.g. sales_features (gold/sales_features/rules.py: SOURCE, transform, build(df))
  --lake gs://<bucket>     lake bucket (Iceberg warehouse root: <lake>/iceberg)
  --batch-id <id>          for the log

The source (e.g. gold.fct_sales) is written by BigQuery (dbt). Spark reads it from its exported Iceberg metadata,
and is the single writer of gold.<model>. Each run replaces the table.
"""
import argparse
import importlib
import logging
import re
import sys

from pyspark.sql import SparkSession

CATALOG = "lake"
log = logging.getLogger("gold")


def latest_metadata(spark: SparkSession, table_dir: str) -> str:
    """Highest v<N>.metadata.json under <table_dir>/metadata (BigQuery's EXPORT TABLE METADATA writes these)."""
    path = spark._jvm.org.apache.hadoop.fs.Path(f"{table_dir}/metadata/v*.metadata.json")
    files = [s.getPath().toString()
             for s in path.getFileSystem(spark._jsc.hadoopConfiguration()).globStatus(path) or []]
    if not files:
        raise FileNotFoundError(f"no exported metadata under {table_dir}/metadata (run dbt build first)")
    return max(files, key=lambda f: int(re.search(r"/v(\d+)\.metadata\.json$", f).group(1)))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--model", required=True)
    p.add_argument("--lake", required=True)
    p.add_argument("--batch-id", required=True)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    model = importlib.import_module(f"gold.{a.model}.rules")
    target = f"{CATALOG}.gold.{a.model}"

    spark = SparkSession.builder.appName(f"gold_{a.model}").getOrCreate()
    warehouse = f"{a.lake.rstrip('/')}/iceberg"
    source = latest_metadata(spark, f"{warehouse}/gold/{model.SOURCE}")
    log.info("batch=%s model=%s source=gold.%s/%s", a.batch_id, a.model, model.SOURCE, source.rsplit("/", 1)[1])
    log.info("rules:\n%s", model.transform.lineage())

    out = model.build(spark.read.format("iceberg").load(source))
    (out.writeTo(target).using("iceberg")
     .tableProperty("location", f"{warehouse}/gold/{a.model}")
     .createOrReplace())
    log.info("wrote gold.%s rows=%d columns=%d", a.model, spark.table(target).count(), len(out.columns))
    spark.stop()


if __name__ == "__main__":
    main()
