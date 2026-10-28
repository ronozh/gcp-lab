"""Common ingest: one feed's landing files -> its bronze Iceberg table, driven by FeedConfig.

Modes:
  incremental   (default) load files not yet in bronze, append
  full_refresh  load every landing file, replace the table in one commit
"""
import logging
import time

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from bronze.base import FeedConfig

CATALOG = "lake"       # Iceberg catalog backed by BigQuery metastore (scripts/run-spark.sh)
NAMESPACE = "bronze"   # = BigQuery dataset
MODES = ("incremental", "full_refresh")

log = logging.getLogger("bronze")


def list_files(spark: SparkSession, glob: str) -> list:
    """Paths matching a gs:// glob, via the Hadoop GCS connector built into Dataproc."""
    path = spark._jvm.org.apache.hadoop.fs.Path(glob)
    paths = [s.getPath() for s in path.getFileSystem(spark._jsc.hadoopConfiguration()).globStatus(path) or []]
    for p in paths:
        # Spark's _metadata.file_path is URL-encoded; a space or % would never match this listing.
        if p.toUri().toString() != p.toString():
            raise ValueError(f"unsupported characters in path: {p}")
    return sorted(p.toString() for p in paths)


def relative(path: str, landing: str) -> str:
    """gs://<bucket>/ecommerce/x/... -> ecommerce/x/... (keeps the project id out of the data)."""
    return path[len(landing) + 1:]


def files_to_load(spark: SparkSession, cfg: FeedConfig, landing: str, table: str, replace: bool) -> list:
    files = list_files(spark, f"{landing}/{cfg.source}/{cfg.name}/{cfg.file_glob}")
    if replace:
        return files
    loaded = {r[0] for r in spark.table(table).select("_meta_file_path").distinct().collect()}
    return [f for f in files if relative(f, landing) not in loaded]


def read(spark: SparkSession, cfg: FeedConfig, files: list) -> DataFrame:
    """Typed rows + Spark's hidden file metadata (`_metadata` struct)."""
    return (spark.read.format(cfg.format).options(**cfg.reader_options()).schema(cfg.schema)
            .load(files)
            .select("*", "_metadata",
                    # Taken right after the read, before any shuffle: increases in file order.
                    F.monotonically_increasing_id().alias("_read_order")))


def add_meta(df: DataFrame, cfg: FeedConfig, landing: str, batch_id: str, mode: str) -> DataFrame:
    """Provenance columns, all prefixed _meta_."""
    m = F.col("_metadata")
    rel_path = F.expr(f"substr(_metadata.file_path, {len(landing) + 2})")
    return (df
            .withColumn("_meta_file_path", rel_path)
            .withColumn("_meta_file_name", m.file_name)
            .withColumn("_meta_file_size", m.file_size)
            .withColumn("_meta_file_modified_ts", m.file_modification_time)
            # Spark has no CSV line numbers: 1..n per file in read order.
            # Exact when a file is read as one split (< spark.sql.files.maxPartitionBytes, 128 MB).
            .withColumn("_meta_record_number",
                        F.row_number().over(Window.partitionBy(m.file_path).orderBy("_read_order")))
            .withColumn("_meta_business_date",
                        F.to_date(F.regexp_extract(rel_path, cfg.business_date_regex, 1)))
            .withColumn("_meta_processed_ts", F.current_timestamp())
            .withColumn("_meta_batch_id", F.lit(batch_id))
            .withColumn("_meta_load_mode", F.lit(mode))
            .drop("_metadata", "_read_order"))


def ingest(spark: SparkSession, cfg: FeedConfig, landing: str, batch_id: str, mode: str) -> None:
    """Load one feed: new files (incremental) or all files (full_refresh)."""
    start = time.time()
    table = f"{CATALOG}.{NAMESPACE}.{cfg.name}"
    replace = mode == "full_refresh" or not spark.catalog.tableExists(table)
    log.info("feed=%s mode=%s batch=%s table=%s", cfg.name, mode, batch_id, table)

    files = files_to_load(spark, cfg, landing, table, replace)
    log.info("feed=%s files_to_load=%d", cfg.name, len(files))
    if not files:
        log.info("feed=%s nothing to do", cfg.name)
        return

    out = add_meta(read(spark, cfg, files), cfg, landing, batch_id, mode).writeTo(table)
    if replace:
        # One commit: readers never see an empty table. (BigQuery time travel only reaches back to here.)
        # Explicit location: <warehouse>/bronze/<feed> (the catalog default would be <warehouse>/bronze.db/<feed>).
        warehouse = spark.conf.get(f"spark.sql.catalog.{CATALOG}.warehouse").rstrip("/")
        (out.using("iceberg").partitionedBy(F.col("_meta_business_date"))
         .tableProperty("location", f"{warehouse}/{NAMESPACE}/{cfg.name}")
         .createOrReplace())
    else:
        out.append()
    log.info("feed=%s loaded files=%d seconds=%.1f", cfg.name, len(files), time.time() - start)
