"""Entry point: gold.fct_sales -> declarative business rules -> gold.sales_features (Iceberg, written by Spark).

  --lake gs://<bucket>     lake bucket (Iceberg warehouse root: <lake>/iceberg)
  --batch-id <id>          for the log

gold.fct_sales is written by BigQuery (dbt). Spark reads it from its exported Iceberg metadata,
and is the single writer of gold.sales_features.
"""
import argparse
import logging
import re
import sys

from pyspark.sql import SparkSession

from declarative import sales_features

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
    p.add_argument("--lake", required=True)
    p.add_argument("--batch-id", required=True)
    a = p.parse_args()
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    spark = SparkSession.builder.appName("gold_features").getOrCreate()
    warehouse = f"{a.lake.rstrip('/')}/iceberg"
    source = latest_metadata(spark, f"{warehouse}/gold/fct_sales")
    log.info("batch=%s source=%s", a.batch_id, source.rsplit("/", 1)[1])
    log.info("rules:\n%s", sales_features.transform.lineage())

    out = sales_features.build(spark.read.format("iceberg").load(source))
    (out.writeTo(f"{CATALOG}.gold.sales_features").using("iceberg")
     .tableProperty("location", f"{warehouse}/gold/sales_features")
     .createOrReplace())
    log.info("wrote gold.sales_features rows=%d columns=%d", spark.table(f"{CATALOG}.gold.sales_features").count(),
             len(out.columns))
    spark.stop()


if __name__ == "__main__":
    main()
