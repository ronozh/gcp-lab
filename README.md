# gcp-lab

A medallion lakehouse on GCP, built from code. Sibling of [snowflake-lab](https://github.com/ronozh/snowflake-lab), using the same 4 feeds.

```
CSV ─▶ GCS landing ─PySpark (Serverless Spark)─▶ bronze (Iceberg) ─dbt + BigQuery─▶ silver (views) ─▶ gold (Iceberg)
Terraform ◀─ GitHub Actions (Workload Identity Federation, no keys)
```

**Learn how it works:** [docs/](docs/README.md)

## Security (public repo)
- No keys, state, tfvars or project-specific config in git. Local config is in gitignored `.env` / `*.tfvars` / `backend.hcl`; CI gets it from `dev` environment secrets.
- CI logs in to GCP via WIF, restricted to this repo on `main`.
- Actions allowlist + SHA pinning, read-only default token, `dev` environment deploys from `main` only.
