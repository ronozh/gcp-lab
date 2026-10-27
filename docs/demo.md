# Demo: a new file flows landing → bronze → silver → gold

**Starting state:** `transaction` has days 1–7 loaded. Day 8 sits in `data/sample`, not yet in landing.

Terminal commands run from the repo root. SQL runs in **BigQuery Studio** (console.cloud.google.com/bigquery → select your project → **+** new query tab → paste → **Run**).

## Sign in (once per day, opens a browser)
```bash
gcloud auth login
```
```bash
gcloud auth application-default login
```
```bash
cd ~/code/dpivoted/gcp-lab && source bootstrap/.env
```
```bash
gcloud config set project $PROJECT_ID
```
```bash
gcloud auth application-default set-quota-project $PROJECT_ID
```
- `gcloud auth login` signs in the `gcloud`, `bq` and `gsutil` CLIs (≈ `aws sso login`).
- `application-default login` creates the credentials that dbt, Terraform and Spark libraries use (Application Default Credentials, ADC).
- In the console, sign in with the same Google account.

## 0 · Before

```sql
SELECT
  COUNT(*)                 AS row_count,
  MAX(_meta_business_date) AS last_day
FROM bronze.transaction;
```
Expect 9919 rows, last_day 2026-01-07.

## 1 · A new file lands (≈ an upstream system drops a daily file)
```bash
ingest/upload.sh 2026-01-08
```
Day 8 of all 4 feeds lands in `gs://<project-id>-landing/ecommerce/<feed>/2026-01-08/` (Console: Cloud Storage → Buckets → `…-landing`).

## 2 · Bronze: incremental PySpark on Serverless Spark (~3 min)
```bash
scripts/run-bronze.sh transaction
```
Expect `files_to_load=1` (only the new file), then `loaded files=1`. If another batch is running, the script prints `waiting: …` and starts by itself.

While it runs, show Console → **Dataproc → Serverless → Batches** (logs), or talk through `spark/bronze/` (config-driven: FeedConfig + schema per feed).

## 3 · Silver + gold: dbt on BigQuery (~20 s)
```bash
scripts/run-dbt.sh build
```
5 models and 13 tests pass. The build also re-applies the policy tag and row policies and exports Iceberg metadata.

## 4 · Gold business rules: declarative PySpark (~3 min)
```bash
scripts/test-spark.sh -s
```
```bash
scripts/run-spark-declarative.sh
```
The first command is optional: 5 s locally, it runs the tests and prints the rule lineage table. The second reads `gold.fct_sales`, applies the 8 registered rules and writes `gold.sales_features`.

## 5 · Show the result

**The new day flowed through every layer:**
```sql
SELECT 'bronze' AS layer, COUNT(*) AS row_count, MAX(_meta_business_date) AS last_day
FROM bronze.transaction
UNION ALL
SELECT 'silver', COUNT(*), MAX(_meta_business_date)
FROM silver.transaction
UNION ALL
SELECT 'gold', COUNT(*), MAX(order_date)
FROM gold.fct_sales
UNION ALL
SELECT 'gold features', COUNT(*), MAX(order_date)
FROM gold.sales_features
ORDER BY layer;
```
Expect 11614 rows in each, last day 2026-01-08.

**Time travel: bronze before vs after today's load:**
```sql
SELECT 'now' AS version, COUNT(*) AS row_count, MAX(_meta_business_date) AS last_day
FROM bronze.transaction
UNION ALL
SELECT 'before the load', COUNT(*), MAX(_meta_business_date)
FROM bronze.transaction
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE);
```
Expect now 11614 / 2026-01-08, and before the load 9919 / 2026-01-07. Each incremental load is a new Iceberg snapshot. Set `INTERVAL` to a moment between the reset and step 2. Fallback: the same query on `silver.transaction`.

**Provenance: which file and batch each row came from:**
```sql
SELECT
  _meta_business_date,
  _meta_file_name,
  _meta_batch_id,
  _meta_load_mode,
  COUNT(*)                 AS row_count,
  MAX(_meta_record_number) AS last_record
FROM bronze.transaction
GROUP BY 1, 2, 3, 4
ORDER BY _meta_business_date;
```

**Daily sales (gold):**
```sql
SELECT
  order_date,
  COUNT(*)                    AS orders,
  ROUND(SUM(net_amount), 2)   AS net_revenue,
  ROUND(SUM(margin_amount), 2) AS margin
FROM gold.fct_sales
WHERE status = 'completed'
GROUP BY order_date
ORDER BY order_date;
```

**Declarative business rules (gold features):**
```sql
SELECT
  spend_band,
  COUNT(*)                   AS transactions,
  COUNTIF(is_first_order)    AS first_orders,
  ROUND(AVG(margin_pct), 3)  AS avg_margin_pct
FROM gold.sales_features
GROUP BY spend_band
ORDER BY spend_band;
```

**Governance: as you (owner), PII is denied:**
```sql
SELECT customer_email
FROM gold.fct_sales
LIMIT 1;
```
Fails: *Access Denied … policy tag "classification : pii"*. The same table without PII works:
```sql
SELECT * EXCEPT (customer_email, customer_first_name, customer_last_name)
FROM gold.fct_sales
LIMIT 10;
```

**Governance as each identity** (terminal):
```bash
scripts/test-governance.sh
```
`lab-admin` sees PII; the analysts are denied; `lab-analyst-nsw` sees NSW rows only.

**Idempotent: run bronze again:**
```bash
scripts/run-bronze.sh transaction
```
Expect `files_to_load=0`, `nothing to do`.

**All layers are Iceberg in one bucket:**
```bash
gcloud storage ls gs://$PROJECT_ID-lake/iceberg/*/
```
`bronze/`, `silver/`, `gold/` (Console: Cloud Storage → `…-lake` → `iceberg/`).

## Reset (to run the demo again)
```bash
gcloud storage rm "gs://$PROJECT_ID-landing/ecommerce/transaction/2026-01-08/*"
```
```bash
scripts/run-bronze.sh transaction full_refresh && scripts/run-dbt.sh build && scripts/run-spark-declarative.sh
```
