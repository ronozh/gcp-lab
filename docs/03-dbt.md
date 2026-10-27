# 03 · dbt + BigQuery: silver and gold

**What this phase does:** SQL models in BigQuery turn bronze into clean **silver** tables and one enriched **gold** table, `fct_sales`, stored as Iceberg. A full build takes ~16 s.

## The flow

```
bronze.<feed>  (Iceberg, written by Spark; BigQuery reads)
     │  dbt (runs on your laptop; BigQuery does the work)
     ▼
silver.campaign / customer / product   native BigQuery tables: current state (latest file)
silver.transaction                     native BigQuery table: all days (latest file per day)
     │
     ▼
gold.fct_sales                         BigQuery-managed Iceberg table → files in gs://<project-id>-lake/iceberg/gold/
```

## GCP vs AWS

| GCP | AWS ≈ | Note |
|---|---|---|
| **BigQuery** | Redshift Serverless / Athena | Serverless SQL engine and warehouse. You pay per TB scanned (on-demand) |
| **Dataset** (`silver`, `gold`) | Schema / Glue database | Where tables live; has a location (`australia-southeast1`) |
| **Native table** | Redshift table | BigQuery's own storage format |
| **BigQuery-managed Iceberg table** | Iceberg table written by Athena | BigQuery writes Parquet to **your** bucket via the `lake` connection, but keeps the Iceberg metadata **inside BigQuery**. `EXPORT TABLE METADATA` writes it to the bucket so other engines can read it (Athena + Glue can read immediately; here you must export) |
| **ADC** (`gcloud auth application-default login`) | `aws sso login` credentials file | What dbt, Terraform and SDKs use locally |

**Three table types in this lab:**

| Table | Written by | Stored | Read by |
|---|---|---|---|
| bronze | Spark | Iceberg in `gs://…-lake/iceberg/` | BigQuery (read-only) |
| silver | BigQuery (dbt) | BigQuery native storage | BigQuery |
| gold | BigQuery (dbt) | Iceberg in `gs://…-lake/iceberg/gold/` | BigQuery; other engines after `EXPORT TABLE METADATA` (a dbt post-hook runs it on every build) |

## The models

| Model | Logic |
|---|---|
| `silver.campaign`, `customer`, `product` | Daily full snapshots → keep only the **latest file**: current state, one row per id |
| `silver.transaction` | All days; for each business date, rows from the latest file (a resent day replaces the old one) |
| `gold.fct_sales` | Each transaction + customer (name, email, tier, state, city), product (sku, brand, category) and campaign (name, channel) |

**Tests** (`dbt build` runs them right after each model):
- `unique` + `not_null` on every id;
- `relationships`: every fact customer/product exists in silver;
- `not_null` on `fct_sales.customer_state` (phase 4 filters rows on it).

## Files

```
dbt/
  dbt_project.yml       silver → +schema silver, table · gold → +schema gold, table, +catalog_name lake_iceberg,
                        +base_location_root iceberg, +post-hook EXPORT TABLE METADATA
  profiles.yml          BigQuery, method oauth (your ADC); project/region from env (bootstrap/.env)
  catalogs.yml          lake_iceberg = biglake_metastore, Iceberg/Parquet in gs://<project-id>-lake
  macros/generate_schema_name.sql   dataset = +schema exactly (silver, gold)
  models/sources.yml    bronze.* tables
  models/silver/*.sql, models/gold/fct_sales.sql
scripts/run-dbt.sh      loads bootstrap/.env, creates the venv if missing, runs dbt
```

## Commands

```bash
brew install uv                            # once: Python env manager (venv + installs)
gcloud auth application-default login     # once
scripts/run-dbt.sh debug                  # connection check
scripts/run-dbt.sh build                  # run models + tests in dependency order
scripts/run-dbt.sh build -s gold          # just gold (+ its tests)
scripts/run-dbt.sh docs generate && scripts/run-dbt.sh docs serve   # lineage graph in the browser
```

## Verify it yourself

```bash
source bootstrap/.env
bq ls silver; bq ls gold                                   # gold.fct_sales Type = TABLE
bq show --format=prettyjson gold.fct_sales | jq '.biglakeConfiguration'   # Iceberg: storageUri, PARQUET
gcloud storage ls -r gs://$PROJECT_ID-lake/iceberg/gold/fct_sales/        # data/*.parquet + exported metadata/ (v<n>.metadata.json, manifests)
bq query --nouse_legacy_sql --location=$REGION \
  'SELECT order_date, COUNT(*) n, ROUND(SUM(net_amount),2) net FROM gold.fct_sales GROUP BY 1 ORDER BY 1'
bq query --nouse_legacy_sql --dry_run \
  'SELECT * FROM silver.transaction'                       # bytes it would scan (cost check), no charge
                                                           # (Iceberg tables report a 0-byte lower bound)
```

## Try it
- **Change the logic:** add `t.unit_cost` to `fct_sales.sql`, run `scripts/run-dbt.sh build -s fct_sales`, and query it.
- **Break a test:** add `- name: campaign_id\n        data_tests: [not_null]` to `gold.yml` (most sales have no campaign), build, and watch it fail. Then revert.

## Gotchas
- dbt always creates Iceberg tables `WITH CONNECTION DEFAULT`, so Terraform sets `lake` as the project's default connection.
- Each build is `CREATE OR REPLACE`. Old Parquet files of a replaced managed-Iceberg table may stay in the bucket (Google doesn't garbage-collect them); clean with `gcloud storage rm` if they pile up.
- Managed Iceberg limits: no clone/copy/rename/`TRUNCATE`; no JSON, GEOGRAPHY, INTERVAL or BIGNUMERIC columns; one mutating DML statement at a time.
