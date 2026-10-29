"""csv-to-parquet: the stand-in for ANZ's legacy pipeline (an independent implementation, like the real one).

raw-<acct>/ecommerce/<feed>/<date>/<name>.csv -> processed-<acct>/ecommerce/<feed>/<date>/<name>.parquet
Typed columns + _meta_file_name (the recon joins on business key + file name).
LEGACY: where this pipeline differs from GCP bronze on purpose, so the recon has something to find.
Triggered by SQS raw-to-lambda (S3 event JSON, raw SNS delivery). pandas + pyarrow come from the AWS layer.
"""
import io
import json
import logging
import os
import urllib.parse

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger()
log.setLevel(logging.INFO)
s3 = boto3.client("s3")

# Column order = CSV header order. Parsed with the same types as GCP bronze (spark/bronze/<source>/<feed>/schema.py).
SCHEMAS = {
    "campaign": pa.schema([
        ("campaign_id", pa.int32()),
        ("campaign_code", pa.string()),
        ("campaign_name", pa.string()),
        ("channel", pa.string()),
        ("budget_aud", pa.float64()),
        ("discount_pct", pa.float64()),
        ("start_date", pa.date32()),
        ("is_running", pa.bool_()),
        ("snapshot_date", pa.date32()),
    ]),
}

# feed -> (change to the parsed rows, output types that change)
LEGACY = {
    "campaign": (
        lambda df: df.assign(
            is_running=df["is_running"].astype("Int32"),   # 1/0 instead of true/false
            discount_pct=df["discount_pct"] * 100,          # a percentage (26.0) instead of a fraction (0.26)
        ),
        {"is_running": pa.int32()},
    ),
}


def convert(body: bytes, schema: pa.Schema) -> pd.DataFrame:
    # Only an empty field is null (as in Spark); pandas would also turn "NA", "null", ... into null.
    df = pd.read_csv(io.BytesIO(body), dtype=str, keep_default_na=False, na_values=[""])
    if list(df.columns) != schema.names:
        raise ValueError(f"header {list(df.columns)} != {schema.names}")
    for f in schema:
        col = df[f.name]
        if pa.types.is_integer(f.type):
            df[f.name] = pd.to_numeric(col).astype("Int32")
        elif pa.types.is_floating(f.type):
            df[f.name] = pd.to_numeric(col)
        elif pa.types.is_date(f.type):
            df[f.name] = pd.to_datetime(col, format="%Y-%m-%d").dt.date
        elif pa.types.is_boolean(f.type):
            df[f.name] = col.str.lower().map({"true": True, "false": False}).astype("boolean")
    return df


def process(bucket: str, key: str) -> None:
    _, feed, *_ = key.split("/")          # ecommerce/<feed>/<date>/<name>.csv
    if feed not in SCHEMAS:
        log.info("skip %s: no schema for feed %s", key, feed)
        return
    df = convert(s3.get_object(Bucket=bucket, Key=key)["Body"].read(), SCHEMAS[feed])
    change, types = LEGACY.get(feed, (lambda d: d, {}))
    df = change(df)
    df["_meta_file_name"] = key.rsplit("/", 1)[1]
    schema = pa.schema([pa.field(f.name, types.get(f.name, f.type)) for f in SCHEMAS[feed]]
                       + [pa.field("_meta_file_name", pa.string())])
    out = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df, schema=schema, preserve_index=False), out)
    out_key = key.removesuffix(".csv") + ".parquet"
    s3.put_object(Bucket=os.environ["OUTPUT_BUCKET"], Key=out_key, Body=out.getvalue())
    log.info("wrote s3://%s/%s rows=%d", os.environ["OUTPUT_BUCKET"], out_key, len(df))


def handler(event, context):
    failures = []
    for msg in event["Records"]:
        try:
            for rec in json.loads(msg["body"]).get("Records", []):   # no Records: S3's test event
                process(rec["s3"]["bucket"]["name"], urllib.parse.unquote_plus(rec["s3"]["object"]["key"]))
        except Exception:
            log.exception("failed: %s", msg["messageId"])
            failures.append({"itemIdentifier": msg["messageId"]})   # retried, then the dead-letter queue
    return {"batchItemFailures": failures}
