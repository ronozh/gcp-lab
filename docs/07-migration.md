# 07 · Migration demo: AWS (the "ANZ" side) → GCP, run in parallel, reconciled

**What this phase does:** simulates moving one feed (`campaign`) from an AWS estate to this lakehouse. The AWS pipeline keeps running. Its raw files are copied to GCP as they land, and GCP's output is compared with AWS's output, cell by cell.

Built in three steps:
1. **AWS side** (done): raw bucket, events, and a Lambda as the "legacy pipeline".
2. **Integration**: Storage Transfer Service copies raw and processed files to GCS, event-driven and keyless.
3. **End to end + recon**: GCP's bronze vs AWS's Parquet, cell by cell.

## Mental model

```
s3://raw-<acct>/ecommerce/campaign/<date>/*.csv
   │ S3 event (*.csv)
   ▼
 SNS raw-events ──┬──▶ SQS raw-to-gcp     ──▶ (step 2) Storage Transfer → gs://<project>-landing/ecommerce/…
                  └──▶ SQS raw-to-lambda  ──▶ Lambda csv-to-parquet
                                                 ▼
s3://processed-<acct>/ecommerce/campaign/<date>/*.parquet
   │ S3 event (*.parquet)
   ▼
 SQS processed-to-gcp ──▶ (step 2) Storage Transfer → gs://<project>-processed/ecommerce/…
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
- `legacy_quirk = true` (Terraform variable) rounds `budget_aud` to the nearest 1000: a planted logic difference for the recon to find.
- Failed messages are retried, then go to the dead-letter queue (`ReportBatchItemFailures`).

Applied **locally** with your SSO login, not by CI: in a real migration, this side belongs to ANZ. Its state is in the GCP state bucket, under prefix `aws`.

## How to use

```bash
aws sso login --profile dpivoted
scripts/aws.sh apply
scripts/aws.sh upload campaign 2026-01-01
scripts/aws.sh ls
```

`apply` creates 30 resources. After the upload, `ls` shows the CSV under `raw` and, within seconds, the `.parquet` (18 rows) under `processed`. `raw-to-gcp` and `processed-to-gcp` each hold one message, waiting for step 2.

Check the Lambda:

```bash
aws logs tail /aws/lambda/csv-to-parquet --since 10m --profile dpivoted
```

Expect `wrote s3://processed-<acct>/ecommerce/campaign/2026-01-01/campaign_20260101235959.parquet rows=18`.

Tear down: `scripts/aws.sh destroy` (buckets are `force_destroy`).

## Gotchas
- **S3 sends `s3:TestEvent`** to a target when its notification is configured. Consumers must skip messages without `Records`.
- **Reading an SQS message counts as a receive.** Peeking at a queue three times sends its messages to the DLQ (`maxReceiveCount = 3`). Use `ApproximateNumberOfMessages`, or purge after testing.
