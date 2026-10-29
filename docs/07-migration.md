# 07 · Migration demo: AWS (the "ANZ" side) → GCP, run in parallel, reconciled

**What this phase does:** simulates moving one feed (`campaign`) from an AWS estate to this lakehouse. The AWS pipeline keeps running. Its raw files are copied to GCP as they land, and GCP's output is compared with AWS's output, cell by cell.

Built in three steps:
1. **AWS side** (done): raw bucket, events, and a Lambda as the "legacy pipeline".
2. **Integration** (done): Storage Transfer Service copies raw and processed files to GCS, event-driven and keyless.
3. **Recon** (done): GCP's bronze vs AWS's Parquet, cell by cell, for one business date, triggered by hand.

## Mental model

```
s3://raw-<acct>/ecommerce/campaign/<date>/*.csv
   │ S3 event (*.csv)
   ▼
 SNS raw-events ──┬──▶ SQS raw-to-gcp     ──▶ Storage Transfer → gs://<project>-landing/ecommerce/… → Eventarc → bronze_campaign
                  └──▶ SQS raw-to-lambda  ──▶ Lambda csv-to-parquet
                                                 ▼
s3://processed-<acct>/ecommerce/campaign/<date>/*.parquet
   │ S3 event (*.parquet)
   ▼
 SQS processed-to-gcp ──▶ Storage Transfer → gs://<project>-processed/ecommerce/…
```

Why SNS for raw but not for processed:
- Raw has **two consumers** (GCP and the Lambda). S3 allows one target per event and prefix, and one SQS message goes to one consumer. So S3 → SNS fans out, and each consumer gets **its own queue**.
- Processed has one consumer, so S3 → SQS directly.
- Raw delivery: each queue receives the S3 event JSON unchanged.
- Interview point: adding GCP as a second consumer of an existing bucket usually means re-wiring its events to a fan-out.

## AWS side (`aws/terraform/`)

| Resource | Purpose |
|---|---|
| S3 `raw-<acct>`, `processed-<acct>` | Versioned, private, TLS only. The account id keeps the names unique without committing it |
| SNS `raw-events` | Fan-out of raw `*.csv` events under `ecommerce/` |
| SQS `raw-to-gcp`, `raw-to-lambda`, `processed-to-gcp` | One queue per consumer; each has a `-dlq` (after 3 failed receives) |
| Lambda `csv-to-parquet` | The "legacy pipeline": CSV → typed Parquet + `_meta_file_name`. pandas/pyarrow from AWS's managed layer, nothing to package |

The Lambda is written separately from the Spark code, like a real legacy system:
- Same types as GCP bronze.
- Only an empty field is null, as in Spark. pandas would also null `NA` and `null`.
- Two **planted legacy differences** (`LEGACY` in the Lambda), for the recon to find:
  - `is_running` is `1`/`0` (int), not `true`/`false`.
  - `discount_pct` is a percentage (`26.0`), not a fraction (`0.26`).
- Failed messages are retried, then go to the dead-letter queue (`ReportBatchItemFailures`).

Applied **locally** with your SSO login, not by CI: in a real migration, this side belongs to ANZ. Its state is in the GCP state bucket, under prefix `aws`.

## Bridge: Storage Transfer Service (`aws/terraform/bridge.tf`)

| GCP | AWS ≈ | Here |
|---|---|---|
| **Storage Transfer Service (STS)** | DataSync / S3 replication | Copies new S3 objects to GCS. Event-driven: it reads S3 events from an SQS queue, so there's no schedule |
| **STS service agent** `project-<number>@storage-transfer-service…` | a service principal | Google's identity for STS in this project. It assumes the AWS role |

| Job | From | To | Then |
|---|---|---|---|
| `raw` | `s3://raw-<acct>/ecommerce/` via `raw-to-gcp` | `gs://<project>-landing/ecommerce/` | the existing Eventarc → function → `bronze_<feed>` DAG |
| `processed` | `s3://processed-<acct>/ecommerce/` via `processed-to-gcp` | `gs://<project>-processed/ecommerce/` | input for the recon (step 3) |

**Keyless in both directions:**
- The AWS role `gcp-storage-transfer` trusts `accounts.google.com` with condition `sub = <the STS agent's subject id>` (web identity federation, the mirror image of GitHub → GCP WIF).
- It can read both buckets and consume only the two `-to-gcp` queues.
- On GCP, the agent has `objectAdmin` + `legacyBucketReader` on the two sink buckets only.

**Why this stack and not the GCP one:** the AWS role needs Google's subject id, and the STS jobs need the AWS role and queue ARNs. One stack avoids passing ids between stacks, and the AWS account id stays out of CI. The GCP stack only adds the `processed` bucket and the API.

The same layout on both clouds (`ecommerce/<feed>/<date>/<file>`) means nothing downstream changes: a file copied by STS looks exactly like one uploaded by hand.

## How to use

```bash
aws sso login --profile dpivoted
scripts/aws.sh apply
scripts/aws.sh upload campaign 2026-01-01
scripts/aws.sh ls
```

`apply` creates 38 resources (the GCP stack must be applied first: it creates the `processed` bucket and enables the API). After the upload, `ls` shows the CSV under `raw` and, within seconds, the `.parquet` (18 rows) under `processed`.

Check the copies on GCP:

```bash
gcloud storage ls -r "gs://$PROJECT_ID-landing/ecommerce/campaign/**" "gs://$PROJECT_ID-processed/ecommerce/**"
gcloud transfer operations list --job-names=$(gcloud transfer jobs list --format='value(name)' | paste -sd, -) --format='table(metadata.transferJobName.basename(), metadata.status, metadata.counters.objectsCopiedToSink)'
```

Each event-driven job has one long-running operation: it stays `IN_PROGRESS` and its `OBJECTS_COPIED_TO_SINK` grows with each event.

Tested:
- The CSV reached GCS landing 9 s after the S3 upload; the Parquet reached `processed` 26 s after.
- The landed CSV triggered `bronze_campaign` like any other file: it recreated `bronze.campaign` (18 rows, 1 file).
- All queues and dead-letter queues were empty afterwards.

## Recon: cell by cell (`spark/recon/compare.py`, `recon_transform.py`)

```
gs://<project>-processed/ecommerce/campaign/*/*.parquet  (AWS)  ─┐  filter: business date in the file name
lake.bronze.campaign                                     (GCP)  ─┤  = --date
                                                                 ▼
   FULL OUTER JOIN on campaign_id_aws = campaign_id_gcp, same _meta_file_name
                                                                 ▼
             BigQuery native recon.campaign, partitioned by business_date
```

Output: one row per key, with three columns per business column:

| Column | Meaning |
|---|---|
| `business_date`, `_meta_file_name` | the file, and its date |
| `_row_status` | `both`, `aws_only` or `gcp_only` |
| `_all_eq` | the row is on both sides and every cell is equal |
| `<col>_aws`, `<col>_gcp`, `<col>_is_eq` | for each of the 9 columns, the key `campaign_id` included. Null-safe (`<=>`): null = null is equal. False when the row is on one side only |
| `_recon_batch_id`, `_recon_ts` | which run wrote it |

Design:
- **Generic.** Columns come from the feed's `schema.py`; keys from `--keys`. Another feed needs only a new YAML.
- **Strict when types differ.** If a column's type differs between the sides, the values are compared as text: `1` vs `true` is unequal. Spark alone would convert the boolean to a number and call them equal.
- **Business date from the file name** (`campaign_20260101235959.csv`) on both sides, so folders can differ between clouds.
- **Fails loudly** if a key isn't unique on either side, or if neither side has rows for the date.
- **Re-runnable.** Each run replaces only its date's partition (`recon.campaign$20260101`), so dates can be reconciled in any order.
- **BigQuery native** (Spark BigQuery connector, indirect write: Parquet staged in the code bucket, then a load job). A native table gets BigQuery's governance and is cheap to query.

Run it from your terminal (the Spark batch runs in GCP, ~3 min):

```bash
scripts/run-recon.sh campaign 2026-01-02
```

`LOCAL=1` in front runs your local, unpushed code. Optional third argument: keys (default `<feed>_id`).

Or from the Airflow UI:

```bash
scripts/airflow.sh ui
```

Then trigger `recon_campaign` with `date` = `2026-01-02`. Without a date, Airflow rejects the trigger (`Invalid input for param date`).

Check in BigQuery:

```sql
SELECT
  business_date,
  _row_status,
  _all_eq,
  COUNT(*) AS n_rows
FROM recon.campaign
GROUP BY business_date, _row_status, _all_eq
ORDER BY business_date, _row_status;
```

Expect, for 2026-01-01: `both`, `false`, 18 (every row has the two planted differences).

```sql
SELECT
  business_date,
  COUNTIF(NOT campaign_id_is_eq) AS campaign_id,
  COUNTIF(NOT campaign_code_is_eq) AS campaign_code,
  COUNTIF(NOT campaign_name_is_eq) AS campaign_name,
  COUNTIF(NOT channel_is_eq) AS channel,
  COUNTIF(NOT budget_aud_is_eq) AS budget_aud,
  COUNTIF(NOT discount_pct_is_eq) AS discount_pct,
  COUNTIF(NOT start_date_is_eq) AS start_date,
  COUNTIF(NOT is_running_is_eq) AS is_running,
  COUNTIF(NOT snapshot_date_is_eq) AS snapshot_date
FROM recon.campaign
GROUP BY business_date
ORDER BY business_date;
```

This counts unequal cells per column. Expect 18 for `discount_pct` and `is_running`, 0 for the others.

```sql
SELECT
  business_date,
  campaign_id_aws,
  campaign_id_gcp,
  _row_status,
  is_running_aws,
  is_running_gcp,
  discount_pct_aws,
  discount_pct_gcp
FROM recon.campaign
WHERE NOT _all_eq
ORDER BY business_date, campaign_id_aws;
```

This lists the rows that differ, side by side: `0` vs `false`, `26.0` vs `0.26`.

Tested (2026-01-01):
- Identical logic on both sides: 18 rows `both`, all equal. A re-run replaced the partition (still 18 rows, one batch id).
- With the legacy differences: 18 rows, `_all_eq = false`; unequal cells only in `discount_pct` (18) and `is_running` (18).

Check the Lambda:

```bash
aws logs tail /aws/lambda/csv-to-parquet --since 10m --profile dpivoted
```

Expect `wrote s3://processed-<acct>/ecommerce/campaign/2026-01-01/campaign_20260101235959.parquet rows=18`.

Tear down: `scripts/aws.sh destroy` (buckets are `force_destroy`).

## Gotchas
- **A recon schema change needs the table dropped** (`bq rm -t recon.campaign`): a partition overwrite can't change column types.
- **Spark BigQuery connector, partitioned writes.** The direct write ignores `partitionField` (the table is created unpartitioned). `partitionOverwriteMode=dynamic` fails once the table exists ("Incompatible table partitioning specification"). What works: indirect write + `datePartition=<yyyymmdd>` + overwrite.
- **Event-driven STS only sees new events.** Objects already in S3 before the job existed need a one-off batch run.
- **S3 sends `s3:TestEvent`** to a target when its notification is configured. Consumers must skip messages without `Records`.
- **Reading an SQS message counts as a receive.** Peeking at a queue three times sends its messages to the DLQ (`maxReceiveCount = 3`). Use `ApproximateNumberOfMessages`, or purge after testing.
