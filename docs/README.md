# gcp-lab docs

A lakehouse on GCP, built from code. Each doc explains one part: how it works, the commands, and how to test it. Read them in order.

```
CSV files ─▶ GCS landing ─Spark─▶ bronze (Iceberg) ─dbt/BigQuery─▶ silver (views) ─▶ gold (Iceberg)
```

| Doc | Topic |
|---|---|
| [00-bootstrap](00-bootstrap.md) | Owner setup once; GitHub → GCP without keys (WIF) |
| [01-infra-ci](01-infra-ci.md) | Terraform via GitHub Actions; what each resource is for |
| [02-bronze-spark](02-bronze-spark.md) | PySpark on Serverless Spark → Iceberg; incremental loads |
| 03-dbt *(coming)* | dbt + BigQuery: silver views, gold Iceberg |
| 04-governance *(coming)* | Policy tags, masking, row access (≈ Lake Formation) |
