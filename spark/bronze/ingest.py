"""Common ingest: one feed's landing files -> its bronze Iceberg table, driven by FeedConfig.

Modes:
  incremental   (default) load files not yet in bronze, append
  full_refresh  load every landing file, replace the table in one commit
"""
import logging
import time

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

from bronze.config.feed import FeedConfig

CATALOG = "lake"       # Iceberg catalog backed by BigQuery metastore (scripts/run-bronze.sh)
NAMESPACE = "bronze"   # = BigQuery dataset
MODES = ("incremental", "full_refresh")

log = logging.getLogger("bronze")


def list_files(spark: SparkSession, glob: str) -> list:
    """Paths matching a gs:// glob, via the Hadoop GCS connector built into Dataproc."""
    path = spark._jvm.org.apache.hadoop.fs.Path(glob)
    statuses = path.getFileSystem(spark._jsc.hadoopConfiguration()).globStatus(path) or []
    return sorted(s.getPath().toString() for s in statuses)


def relative(path: str, landing: str) -> str:
    """gs://<bucket>/ecommerce/x/... -> ecommerce/x/... (keeps the project id out of the data)."""
    return path[len(landing) + 1:]


def files_to_load(spark: SparkSession, cfg: FeedConfig, landing: str, table: str, mode: str) -> list:
    files = list_files(spark, f"{landing}/{cfg.domain}/{cfg.name}/{cfg.file_glob}")
    if mode == "full_refresh" or not spark.catalog.tableExists(table):
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


def validate(spark: SparkSession, cfg: FeedConfig, df: DataFrame, files: list) -> list:
    """Rows per file must equal the .ctrl record_count. Returns error messages (empty = all good)."""
    if cfg.ctrl_suffix is None:
        return []
    actual = {r[0]: r[1] for r in df.groupBy("_metadata.file_path").count().collect()}
    ctrl_files = [f.rsplit(".", 1)[0] + cfg.ctrl_suffix for f in files]
    ctrl = {r["file_name"]: int(r["record_count"])
            for r in spark.read.option("header", True).csv(ctrl_files).collect()}
    errors = []
    for f in files:
        expected, got = ctrl.get(f.rsplit("/", 1)[1]), actual.get(f, 0)
        if expected != got:
            errors.append(f"{f.rsplit('/', 1)[1]}: rows {got} != ctrl {expected}")
    return errors


def add_meta(df: DataFrame, cfg: FeedConfig, landing: str, batch_id: str, mode: str) -> DataFrame:
    """Provenance columns, all prefixed _meta_."""
    m = F.col("_metadata")
    rel_path = F.expr(f"substr(_metadata.file_path, {len(landing) + 2})")
    return (df
            .withColumn("_meta_file_path", rel_path)
            .withColumn("_meta_file_name", m.file_name)
            .withColumn("_meta_file_size", m.file_size)
            .withColumn("_meta_file_modified_ts", m.file_modification_time)
            # Best effort: Spark has no CSV line numbers. Exact when a file is read as one split.
            .withColumn("_meta_record_number",
                        F.row_number().over(Window.partitionBy(m.file_path).orderBy("_read_order")))
            .withColumn("_meta_business_date",
                        F.to_date(F.regexp_extract(rel_path, cfg.business_date_regex, 1)))
            .withColumn("_meta_processed_ts", F.current_timestamp())
            .withColumn("_meta_batch_id", F.lit(batch_id))
            .withColumn("_meta_load_mode", F.lit(mode))
            .drop("_metadata", "_read_order"))


def ingest(spark: SparkSession, cfg: FeedConfig, landing: str, batch_id: str, mode: str) -> None:
    """Load one feed. Raises if any file fails validation; then nothing is written."""
    start = time.time()
    table = f"{CATALOG}.{NAMESPACE}.{cfg.name}"
    log.info("feed=%s mode=%s batch=%s table=%s", cfg.name, mode, batch_id, table)
    files = files_to_load(spark, cfg, landing, table, mode)
    log.info("feed=%s files_to_load=%d", cfg.name, len(files))
    if not files:
        log.info("feed=%s nothing to do", cfg.name)
        return

    df = read(spark, cfg, files)
    errors = validate(spark, cfg, df, files)
    if errors:
        for e in errors:
            log.error("feed=%s validation failed: %s", cfg.name, e)
        raise ValueError(f"[{cfg.name}] validation failed, nothing written: {errors}")
    log.info("feed=%s validation passed (row counts = .ctrl)", cfg.name)

    out = add_meta(df, cfg, landing, batch_id, mode).writeTo(table)
    if mode == "full_refresh" or not spark.catalog.tableExists(table):
        # One commit. Old snapshots stay, so time travel to the previous version still works.
        out.using("iceberg").partitionedBy(F.col("_meta_business_date")).createOrReplace()
    else:
        out.append()
    log.info("feed=%s loaded files=%d rows=%d seconds=%.1f",
             cfg.name, len(files), df.count(), time.time() - start)
