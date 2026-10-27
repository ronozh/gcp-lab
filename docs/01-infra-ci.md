# 01 · Infrastructure by CI (Terraform)

**What this phase does:** all lab resources come from `terraform/`. You edit, push to `main`, and CI applies.

## The flow

```
edit terraform/*.tf ─▶ git push (main) ─▶ GitHub Actions "terraform"
   validate (fmt, validate) ─▶ dev: WIF login ─▶ plan ─▶ apply (only if changes) ─▶ state in gs://<project-id>-tfstate
```
CI logs show only counts and resource types (public repo). To see the full plan, run it locally:
```bash
cd terraform && terraform init -backend-config=backend.hcl && terraform plan    # needs: gcloud auth application-default login
```

## What exists and why

| File | Resource | GCP name | Why (AWS ≈) |
|---|---|---|---|
| `apis.tf` | `google_project_service` × 7 | compute, dataproc, bigquery, bigqueryconnection, biglake, datacatalog, bigquerydatapolicy | Turn on the services the lab uses |
| `network.tf` | `google_compute_subnetwork` (imported) | `default` subnet, Private Google Access on | Spark workers have no public IP; PGA lets them reach GCS/BigQuery (≈ VPC endpoints) |
| `storage.tf` | 3 buckets | `<project-id>-landing` (versioned), `-lake`, `-code` | Raw files / Iceberg files / Spark code (≈ S3) |
| `bigquery.tf` | 3 datasets | `bronze`, `silver`, `gold` | Layers. Dataset ≈ schema (≈ Glue database) |
| `bigquery.tf` | connection `lake` + bucket IAM | `bqcx-…` Google-managed SA | Lets BigQuery write Iceberg files to `lake` (≈ Lake Formation data-access role) |
| `bigquery.tf` | `google_bigquery_job` | `ALTER PROJECT … default_cloud_resource_connection_id='lake'` | dbt always uses the *default* connection |
| `iam.tf` | SA `spark-runner` + roles | `dataproc.worker` (project), `bigquery.dataEditor` (bronze) | Identity of Spark jobs (≈ EMR job role). `dataproc.worker` also gives read/write on all project buckets |

## Key ideas
- **Import vs create:** the `default` subnet came with the project. An `import {}` block adopts it into Terraform; then Terraform changes only `private_ip_google_access`.
- **APIs first:** resources `depends_on` the API resources, so the first apply doesn't race.
- **Idempotent:** a second run shows `No changes.`. That's the check that code = reality.

## Verify it yourself

```bash
source bootstrap/.env
gcloud storage ls --project=$PROJECT_ID                         # 4 buckets
bq ls --project_id=$PROJECT_ID                                  # bronze, gold, silver
bq show --connection $PROJECT_ID.$REGION.lake                   # serviceAccountId = bqcx-…
gcloud compute networks subnets describe default --region=$REGION --project=$PROJECT_ID \
  --format="value(privateIpGoogleAccess)"                       # True
bq query --nouse_legacy_sql --location=$REGION \
  'SELECT option_name, option_value FROM `region-australia-southeast1`.INFORMATION_SCHEMA.PROJECT_OPTIONS'
```

## Try it
Add a label to a bucket in `storage.tf`, then push and watch CI show `update: 1 (google_storage_bucket)`. Revert it the same way.

## Gotchas
- `google_bigquery_job` runs once; to re-run it, change its `job_id`.
- `force_destroy = true` on the buckets lets `terraform destroy` delete them while they still contain files (lab only).
