# 02 · Bronze: PySpark on Serverless Spark → Iceberg

**What this phase does:** raw CSV files in GCS become typed Iceberg tables in `bronze`, loaded incrementally by file name and readable from BigQuery.

## The flow

```
data/sample ─ingest/upload.sh─▶ gs://<project-id>-landing/ecommerce/<feed>/<date>/<feed>_<ts>.csv + .ctrl
                                        │
        scripts/run-bronze.sh ─▶ Serverless Spark batch (runs as spark-runner, 12 vCPU, ~2-3 min)
                                        │  spark/bronze_ingest.py
                                        ▼
            Iceberg catalog "lake" (BigQuery metastore)
              ├─ data + metadata files ─▶ gs://<project-id>-lake/iceberg/bronze.db/<feed>/
              └─ table registered in BigQuery dataset `bronze` ─▶ SELECT from BigQuery (read-only)
```

## Mental model (AWS ≈)

| GCP | AWS ≈ | Note |
|---|---|---|
| **Serverless Spark batch** (Dataproc Serverless) | EMR Serverless job / Glue job | No cluster. You submit a job; Google starts the machines, runs it, removes them. Billed per use |
| **Runtime version** `2.3` | EMR release label | Spark 3.5, Scala 2.13, Java 17. **Already includes Iceberg + the BigQuery metastore catalog** |
| **BigQuery metastore catalog** | Glue Data Catalog as Iceberg catalog | Iceberg namespace = BigQuery dataset. Spark writes; BigQuery sees the table |
| **Iceberg table** | Iceberg on S3 | Data files (Parquet) + metadata (snapshots) in the bucket |
| `spark-runner` SA | EMR job execution role | What the job is allowed to touch |

**One writer per table:** Spark owns bronze. BigQuery can only *read* these tables, using **your** credentials (they are plain external tables: no connection, so no row/column security). Gold, written by BigQuery, is where governance lives.

## What the job does (`spark/bronze_ingest.py`)
1. **Config-driven:** `FEEDS` holds the column names and types for each feed. One job handles all 4 feeds.
2. **Incremental by file name:** it lists `landing/ecommerce/<feed>/*/*.csv`, removes paths already in bronze `_src_file`, and loads the rest.
3. **Validates:** row count per file = `record_count` in its `.ctrl`. A mismatched file is skipped and reported, and the job exits 1.
4. **Adds provenance:** `_src_file` (path in landing), `_business_date` (from the folder), `_loaded_at`, `_batch_id`.
5. **Writes:** creates the table on the first run (partitioned by `_business_date`), then appends. Each feed is **one commit** (all or nothing).
6. **`--reload '<regex>'`:** *known issue: currently fails (see Gotchas); being fixed in the refactor.*

## Commands

```bash
ingest/upload.sh 2026-01-01                    # one day (all 4 feeds) -> landing
ingest/upload.sh 2026-01-02 2026-01-08         # a range
scripts/run-bronze.sh                          # run the job CI deployed to gs://<project-id>-code
JOB=spark/bronze_ingest.py scripts/run-bronze.sh                      # run your local copy (dev loop)
scripts/run-bronze.sh --reload 'transaction/2026-01-03/'              # replace one file's rows
```
CI deploys the job: push a change under `spark/`, and Terraform uploads the new file.

## Verify it yourself

```bash
source bootstrap/.env
gcloud storage ls -r gs://$PROJECT_ID-landing/ecommerce/transaction/       # landed files
gcloud dataproc batches list --region=$REGION --limit=5                   # batch history + state
bq ls bronze                                                              # 4 EXTERNAL tables
bq query --nouse_legacy_sql --location=$REGION \
  'SELECT _business_date, COUNT(*) n FROM bronze.transaction GROUP BY 1 ORDER BY 1'
cat data/sample/ecommerce/transaction/2026-01-01/*.ctrl                    # compare record_count
gcloud storage ls gs://$PROJECT_ID-lake/iceberg/bronze.db/transaction/metadata/   # Iceberg snapshots
```

## Try it
- **Idempotency:** run `scripts/run-bronze.sh` twice. The second run prints `0 to load`.
- **Bad file:** edit a `.ctrl` count, upload a new day, run. That file is skipped and the batch fails, while the other feeds still load.
- **Logs:** Console → Dataproc → Serverless → Batches → your batch → Output. Or `gcloud dataproc batches describe <batch> --region=$REGION`.

## Gotchas
- **Quota:** the free trial allows 12 vCPU, exactly one batch. A new batch fails with `CPUS_ALL_REGIONS … available 0` until the previous one has released its machines (~2 min).
- **Reserved names:** BigQuery rejects columns starting `_FILE_`, `_TABLE_` or `_PARTITION`, hence `_business_date`, not `_file_date`.
- **Catalog keys:** the runtime's bundled catalog reads `gcp_project` / `gcp_location`. Upstream Iceberg docs say `gcp.bigquery.project-id`, which doesn't work here.
- **No extra jars:** adding upstream Iceberg jars is unnecessary, and the `_2.12` ones are the wrong Scala version for runtime 2.3.
- **Reload by filter fails:** Iceberg's `overwrite(filter)` deletes whole data files, and string stats are truncated, so it can't prove a file matches `_src_file = …`. Fix (refactor): partition by source file and replace partitions.
