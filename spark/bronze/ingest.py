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
    files = list_files(spark, f"{landing}/{cfg.domain}/{cfg.name}/{cfg.file_glob}")
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


def validate(spark: SparkSession, cfg: FeedConfig, landing: str, files: list, counts: dict) -> list:
    """Rows per file must equal the record_count in its control file. Returns errors (empty = good)."""
    if cfg.ctrl_suffix is None:
        return []
    ctrl_of = {f: f.rsplit(".", 1)[0] + cfg.ctrl_suffix for f in files}
    ctrl_glob = cfg.file_glob.rsplit(".", 1)[0] + cfg.ctrl_suffix          # */*.csv -> */*.ctrl
    existing = set(list_files(spark, f"{landing}/{cfg.domain}/{cfg.name}/{ctrl_glob}"))
    expected = {}
    if existing:
        rows = (spark.read.option("header", True).csv(sorted(existing))
                .select("record_count", F.col("_metadata.file_path").alias("path")).collect())
        expected = {r["path"]: int(r["record_count"]) for r in rows}
    errors = []
    for f in files:
        name = f.rsplit("/", 1)[1]
        if ctrl_of[f] not in existing:
            errors.append(f"{name}: missing {cfg.ctrl_suffix}")
        elif expected.get(ctrl_of[f]) != counts.get(f, 0):
            errors.append(f"{name}: rows {counts.get(f, 0)} != ctrl {expected.get(ctrl_of[f])}")
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
    """Load one feed. Raises if any file fails validation; then nothing is written."""
    start = time.time()
    table = f"{CATALOG}.{NAMESPACE}.{cfg.name}"
    replace = mode == "full_refresh" or not spark.catalog.tableExists(table)
    log.info("feed=%s mode=%s batch=%s table=%s", cfg.name, mode, batch_id, table)

    files = files_to_load(spark, cfg, landing, table, replace)
    log.info("feed=%s files_to_load=%d", cfg.name, len(files))
    if not files:
        log.info("feed=%s nothing to do", cfg.name)
        return

    df = read(spark, cfg, files)
    counts = {r[0]: r[1] for r in df.groupBy("_metadata.file_path").count().collect()}
    errors = validate(spark, cfg, landing, files, counts)
    if errors:
        for e in errors:
            log.error("feed=%s validation failed: %s", cfg.name, e)
        raise ValueError(f"[{cfg.name}] validation failed, nothing written: {errors}")
    log.info("feed=%s validation passed (row counts = %s)", cfg.name, cfg.ctrl_suffix)
    for f in files:
        if counts.get(f, 0) == 0:
            log.warning("feed=%s empty file %s: no rows, so it is re-read on every incremental run",
                        cfg.name, f.rsplit("/", 1)[1])

    out = add_meta(df, cfg, landing, batch_id, mode).writeTo(table)
    if replace:
        # One commit. Old snapshots stay, so time travel to the previous version still works.
        # Explicit location: <warehouse>/bronze/<feed> (the catalog default would be <warehouse>/bronze.db/<feed>).
        warehouse = spark.conf.get(f"spark.sql.catalog.{CATALOG}.warehouse").rstrip("/")
        (out.using("iceberg").partitionedBy(F.col("_meta_business_date"))
         .tableProperty("location", f"{warehouse}/{NAMESPACE}/{cfg.name}")
         .createOrReplace())
    else:
        out.append()
    log.info("feed=%s loaded files=%d rows=%d seconds=%.1f",
             cfg.name, len(files), sum(counts.values()), time.time() - start)
