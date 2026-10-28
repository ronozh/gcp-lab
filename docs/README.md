# gcp-lab docs

A lakehouse on GCP, built from code. Each doc explains one part: how it works, the commands, and how to test it. Read them in order.

```
CSV files ─▶ GCS landing ─Spark─▶ bronze ─dbt/BigQuery─▶ silver ─▶ gold      (all Iceberg in gs://<lake>/iceberg/<layer>/)
```

| Doc | Topic |
|---|---|
| [00-bootstrap](00-bootstrap.md) | Owner setup once; GitHub → GCP without keys (WIF) |
| [01-infra-ci](01-infra-ci.md) | Terraform via GitHub Actions; what each resource is for |
| [02-bronze-spark](02-bronze-spark.md) | PySpark on Serverless Spark → Iceberg; incremental loads |
| [03-dbt](03-dbt.md) | dbt + BigQuery: silver + gold as BigQuery-managed Iceberg |
| [04-governance](04-governance.md) | Policy tags (column security), row access policies (≈ Lake Formation) |
| [05-declarative](05-declarative.md) | Declarative business rules in PySpark: one rule = one function = one column |
| [06-orchestration](06-orchestration.md) | Event-driven, config-driven Airflow 3 on Cloud Run: a file lands → its bronze DAG runs |

**Live demo:** [demo.md](demo.md): a new file flows landing → bronze → silver → gold.
