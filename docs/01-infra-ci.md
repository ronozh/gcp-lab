# 01 · Infrastructure by CI (Terraform)

**What this phase does:** all lab resources come from `terraform/`. You edit, push to `main`, and CI applies.

## The flow

```
edit terraform/*.tf ─▶ git push (main) ─▶ GitHub Actions "terraform"
   validate (fmt, validate) ─▶ dev: WIF login ─▶ plan ─▶ apply (only if changes) ─▶ state in gs://<project-id>-tfstate
```
CI logs show only counts and resource types (public repo). To see the full plan, run it locally. **Don't `apply` locally: CI owns apply.**
```bash
gcloud auth application-default login                          # once: credentials for Terraform/dbt
cd terraform
cp backend.hcl.example backend.hcl; cp terraform.tfvars.example terraform.tfvars   # fill in
terraform init -backend-config=backend.hcl && terraform plan
```

## Mental model (AWS ≈)
- **Network:** a GCP VPC is **global**, with **regional** subnets (AWS: regional VPC, AZ subnets). Every project gets a `default` network. Spark uses its subnet in `australia-southeast1`.
- **BigQuery connection:** a Google-managed service account that BigQuery *acts as* to read and write GCS (≈ the IAM role Athena / Redshift Spectrum assumes). The **default connection** is a per-region project setting.
- **Dataset** ≈ schema / Glue database. **Bucket** ≈ S3 bucket.

## What exists and why

| File | Resource | GCP name | Why |
|---|---|---|---|
| `apis.tf` | `google_project_service` × 14 | dataproc, bigquery, bigqueryconnection, biglake, datacatalog, bigquerydatapolicy; phase 6: run, sqladmin, secretmanager, cloudfunctions, cloudbuild, artifactregistry, eventarc, pubsub | Turn on the services the lab uses |
| `storage.tf` | 3 buckets | `<project-id>-landing` (versioned), `-lake`, `-code` | Raw files / Iceberg files / Spark code |
| `bigquery.tf` | 3 datasets | `bronze`, `silver`, `gold` | One per layer |
| `bigquery.tf` | connection `lake` + bucket IAM | `bqcx-…` Google-managed SA | Lets BigQuery write gold Iceberg files to `lake` |
| `bigquery.tf` | `google_bigquery_job` | `ALTER PROJECT … default_cloud_resource_connection_id='lake'` | dbt always uses the *default* connection |
| `iam.tf` | SA `spark-runner` + roles | `dataproc.worker` (project), `bigquery.dataEditor` (bronze) | Identity of Spark jobs (≈ EMR job role). `dataproc.worker` also gives read/write on all project buckets |
| `spark.tf` | bucket object | `gs://<project-id>-code/spark/bronze_ingest.py` | CI deploys the Spark job (phase 2) |
| `airflow.tf`, `trigger.tf` | Airflow on Cloud Run + Cloud SQL; function + Eventarc trigger | — | Orchestration (phase 6): see [06-orchestration](06-orchestration.md) |
| `network.tf` | `removed {}` | — | Hands the subnet back to bootstrap without deleting it |

## Key ideas
- **APIs first:** resources `depends_on` the API resources, so the first apply doesn't race.
- **Idempotent:** a second run shows `No changes.`. That's the check that code = reality.
- **Some things aren't Terraform resources:** the project-default connection has no resource, so it's a one-off DDL job. Terraform won't notice if someone changes it by hand.

## Verify it yourself

```bash
source bootstrap/.env
gcloud storage ls --project=$PROJECT_ID                         # 4 buckets
bq ls --project_id=$PROJECT_ID                                  # bronze, gold, silver
bq show --connection $PROJECT_ID.$REGION.lake                   # serviceAccountId = bqcx-…
gcloud compute networks subnets describe default --region=$REGION --project=$PROJECT_ID \
  --format="value(privateIpGoogleAccess)"                       # True
bq query --project_id=$PROJECT_ID --nouse_legacy_sql --location=$REGION \
  "SELECT option_name, option_value FROM \`region-$REGION\`.INFORMATION_SCHEMA.PROJECT_OPTIONS"
```

## Try it
Add `labels = { owner = "me" }` to a bucket in `storage.tf`, push, and watch CI show `update: 1 (google_storage_bucket)`. Revert it the same way.

## Gotchas
- BigQuery jobs can't be deleted, and job IDs are unique forever, so the DDL job uses a random ID suffix (`random_id`). It re-runs only if its SQL or the connection changes.
- `force_destroy = true` on the buckets lets `terraform destroy` delete them while they still contain files (lab only).
