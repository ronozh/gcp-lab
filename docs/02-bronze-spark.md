# 02 · Bronze: PySpark on Serverless Spark → Iceberg

**What this phase does:** one feed's raw CSV files in GCS become a typed Iceberg table in `bronze`, readable from BigQuery.

## Mental model in one line

```
run-bronze.sh <feed> [incremental|full_refresh]  =  one Spark batch  =  one feed  ->  bronze.<feed>
```
- **incremental** (default): load the feed's files that aren't in bronze yet, and **append**.
- **full_refresh**: load **all** the feed's files and **replace** the table in one commit.

## The flow

```
data/sample ─ingest/upload.sh─▶ gs://<project-id>-landing/ecommerce/<feed>/<date>/<feed>_<ts>.csv + .ctrl
                                        │
     scripts/run-bronze.sh <feed> ─▶ Serverless Spark batch (runs as spark-runner, 12 vCPU)
                                        │  bronze_ingest.py + lib.zip (bronze/: config, schema, ingest)
                                        ▼
            Iceberg catalog "lake" (BigQuery metastore)
              ├─ data + metadata files ─▶ gs://<project-id>-lake/iceberg/bronze/<feed>/
              └─ table registered in BigQuery dataset `bronze` ─▶ SELECT from BigQuery (read-only)
```

## GCP vs AWS

| GCP | AWS ≈ | Note |
|---|---|---|
| **Serverless Spark batch** (Dataproc Serverless) | EMR Serverless job / Glue job | No cluster. You submit a job; Google starts the machines, runs it, removes them. Billed per use |
| **Runtime version** `2.3` | EMR release label | Spark 3.5, Scala 2.13, Java 17, Python 3.11. **Already includes Iceberg + the BigQuery metastore catalog** |
| **BigQuery metastore catalog** | Glue Data Catalog as Iceberg catalog | Iceberg namespace = BigQuery dataset. Spark writes; BigQuery sees the table |
| **Iceberg table** | Iceberg on S3 | Data files (Parquet) + metadata (snapshots) in the bucket |
| `spark-runner` SA | EMR job execution role | What the job is allowed to touch |

**One writer per table:** Spark owns bronze. BigQuery can only *read* these tables, using **your** credentials (they are plain external tables: no connection, so no row/column security). Gold, written by BigQuery, is where governance lives.

## Code layout: configuration-driven

```
spark/
  bronze_ingest.py          entry point: --feed, --mode
  bronze/
    config/feed.py          FeedConfig dataclass: every option + a sensible default
    config/<feed>.py        CONFIG = FeedConfig.from_dict({"name": …, "schema": <feed>.SCHEMA, …overrides})
    config/__init__.py      FEEDS registry: feed name -> config
    schema/<feed>.py        SCHEMA = StructType([...])
    ingest.py               common logic for every feed
```
- **Add a feed:** add `schema/<feed>.py` + `config/<feed>.py`, and list it in `config/__init__.py`. No ingest code changes.
- **Override an option:** add a key to the dict, e.g. `"delimiter": "|"`. Unknown keys raise an error, so typos are caught. Read options as attributes: `CONFIG.header`, `CONFIG.schema`.
- **Add a config option:** a new `FeedConfig` field *with a default*, so existing configs keep working.

## What `ingest.py` does
1. **Pick files:**
   - `incremental`: landing files whose path isn't in bronze `_meta_file_path` yet;
   - `full_refresh`: all landing files.
2. **Read** with the feed's CSV options and schema (`parse_mode=FAILFAST`). A malformed value fails the write: there's no commit, so the table is unchanged, though stray data files may remain.
3. **Validate** rows per file = `record_count` in its `.ctrl` (a missing `.ctrl` is an error too). If any file fails, the job fails and **nothing is written**.
4. **Add `_meta_*` columns:**

   | Column | From |
   |---|---|
   | `_meta_file_path` | landing path (no bucket, so no project ID in the data) |
   | `_meta_file_name`, `_meta_file_size`, `_meta_file_modified_ts` | Spark's hidden `_metadata` struct |
   | `_meta_record_number` | 1..n per file, in read order. Spark has no CSV line numbers; exact when a file is read as one split (< 128 MB) |
   | `_meta_business_date` | the date folder in the path (also the partition column) |
   | `_meta_processed_ts`, `_meta_batch_id`, `_meta_load_mode` | this run |
5. **Write:**
   - `incremental`: `append()`;
   - `full_refresh` (or first load): `createOrReplace()`. That's one commit, and the previous snapshots are kept, so you can still time-travel back.

## Commands

```bash
ingest/upload.sh 2026-01-01                    # one day (all 4 feeds) -> landing
ingest/upload.sh 2026-01-02 2026-01-08         # a range
scripts/run-bronze.sh transaction              # incremental (code deployed by CI)
scripts/run-bronze.sh transaction full_refresh
LOCAL=1 scripts/run-bronze.sh transaction      # run your local code (dev loop, no push needed)
for f in campaign customer product transaction; do scripts/run-bronze.sh $f; done   # all feeds, one after another
```
CI deploys the code: push a change under `spark/`, and Terraform uploads the entry files + `lib.zip` (packages `bronze/`, `declarative/`). `run-bronze.sh` and `run-gold.sh` both call `scripts/run-spark.sh` (shared Spark settings).

## Verify it yourself

```bash
source bootstrap/.env
gcloud storage ls -r gs://$PROJECT_ID-landing/ecommerce/transaction/       # landed files
gcloud dataproc batches list --region=$REGION --limit=5                   # batch history + state
bq ls bronze                                                              # EXTERNAL tables
bq query --nouse_legacy_sql --location=$REGION \
  'SELECT _meta_business_date, _meta_file_name, _meta_load_mode, COUNT(*) n, MAX(_meta_record_number) last_rec
   FROM bronze.transaction GROUP BY 1,2,3 ORDER BY 1'
cat data/sample/ecommerce/transaction/2026-01-01/*.ctrl                    # compare record_count
gcloud storage ls gs://$PROJECT_ID-lake/iceberg/bronze/transaction/metadata/   # Iceberg snapshots
```

## Try it
- **Idempotency:** run `scripts/run-bronze.sh transaction` twice. The second run logs `files_to_load=0` and `nothing to do`.
- **Full refresh:** run it with `full_refresh`, then query `_meta_load_mode`: every row says `full_refresh`, and the count is unchanged.
- **Bad file:** fake a day 9 whose `.ctrl` count is wrong. The batch fails, and bronze is unchanged.
  ```bash
  L=gs://$PROJECT_ID-landing/ecommerce/transaction
  gcloud storage cp $L/2026-01-08/transaction_20260108235959.csv $L/2026-01-09/transaction_20260109235959.csv
  printf 'file_name,record_count\ntransaction_20260109235959.csv,1\n' | gcloud storage cp - $L/2026-01-09/transaction_20260109235959.ctrl
  scripts/run-bronze.sh transaction          # ERROR bronze: ... rows 1695 != ctrl 1
  gcloud storage rm -r $L/2026-01-09/        # clean up
  ```
- **Logs:** Console → Dataproc → Serverless → Batches → your batch → Output.

## Logs (Cloud Logging ≈ CloudWatch Logs)
The job logs with Python `logging`: logger `bronze`, to stdout. Serverless Spark ships driver output to **Cloud Logging**; nothing is written to local files (the VMs are deleted after each batch).
```
2026-10-27 10:16:09 INFO bronze: feed=campaign files_to_load=8
2026-10-27 10:16:21 INFO bronze: feed=campaign validation passed (row counts = .ctrl)
2026-10-27 10:16:36 INFO bronze: feed=campaign loaded files=8 rows=144 seconds=31.3
```
| Where | How |
|---|---|
| Console → Dataproc → Serverless → Batches → *batch* → **Output** | Full driver output for one batch |
| Console → **Logs Explorer** | Query: `resource.type="cloud_dataproc_batch" "bronze:"` |
| CLI | `gcloud logging read 'resource.type="cloud_dataproc_batch" AND resource.labels.batch_id="<batch>" AND "bronze:"' --order=asc --format='value(jsonPayload.message)'` |

Search for text such as `validation failed`. Python log levels are **not** mapped to Cloud Logging severity, so `severity>=ERROR` finds nothing.

## Why a batch takes ~3 minutes for 3 MB
- **~75 s:** Google provisions the VMs.
- **~40–80 s:** Spark/JVM startup + catalog calls; the data work itself takes seconds.
- **~1–2 min before the next batch can start:** the trial's 12 vCPU quota fits exactly one batch, and 12 vCPU (4-core driver + 2 × 4-core executors) is also Serverless Spark's **minimum** batch size, so batches can't run side by side.

Serverless suits scheduled jobs, not a tight dev loop.

## Gotchas
- **Quota:** a new batch fails with `CPUS_ALL_REGIONS … available 0` until the previous one has released its machines.
- **Timestamps:** the files have local times with no zone, so `event_ts` is `TimestampNTZType` → Iceberg `timestamp` → BigQuery **DATETIME** (as snowflake-lab's TIMESTAMP_NTZ). A plain `TimestampType` would treat them as UTC.
- **Empty file:** a 0-row file passes validation but adds no rows, so it is re-read on every incremental run (logged as a warning).
- **Reserved names:** BigQuery rejects columns starting `_FILE_`, `_TABLE_` or `_PARTITION`; `_meta_file_name` is fine because it starts with `_meta`.
- **Catalog keys:** the runtime's bundled catalog reads `gcp_project` / `gcp_location`. Upstream Iceberg docs say `gcp.bigquery.project-id`, which doesn't work here.
- **No extra jars:** adding upstream Iceberg jars is unnecessary, and the `_2.12` ones are the wrong Scala version for runtime 2.3.
- **No `.db` suffix:** the catalog's default layout is Hive-style `<warehouse>/bronze.db/<feed>`. The job sets each table's `location` to `<warehouse>/bronze/<feed>`, so all layers look alike: `iceberg/bronze|silver|gold/<table>/`.
- **Schema changes `full_refresh` can't do:** changing a column's **type** (BigQuery: `Field … has changed type`) or removing a **partition column**. Drop and reload instead:
  ```bash
  bq rm -f -t bronze.<feed> && gcloud storage rm -r gs://$PROJECT_ID-lake/iceberg/bronze/<feed>
  scripts/run-bronze.sh <feed>               # first load creates the table
  ```
