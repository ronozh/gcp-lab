# Demo: a new file flows landing → bronze → silver → gold

**Starting state:** `transaction` has days 1–7 loaded. Day 8 sits in `data/sample`, not yet in landing.

```bash
cd ~/code/dpivoted/gcp-lab && source bootstrap/.env
```

## 0 · Before
```bash
bq query --nouse_legacy_sql 'SELECT COUNT(*) n, MAX(_meta_business_date) last_day FROM bronze.transaction'
```
Expect: 9919 rows, last_day 2026-01-07.

## 1 · A new file lands (≈ an upstream system drops a daily file)
```bash
ingest/upload.sh 2026-01-08
```
Day 8 of all 4 feeds lands in `gs://<project-id>-landing/ecommerce/<feed>/2026-01-08/`.

## 2 · Bronze: incremental PySpark on Serverless Spark (~3 min)
```bash
scripts/run-bronze.sh transaction
```
Expect `files_to_load=1` (only the new file), `validation passed`, then `loaded files=1 rows=1695`. If another batch is running, the script prints `waiting: …` and starts by itself.

While it runs: Console → Dataproc → Serverless → Batches (logs), or talk through `spark/bronze/` (config-driven: FeedConfig + schema per feed).

## 3 · Silver + gold: dbt on BigQuery (~20 s)
```bash
scripts/run-dbt.sh build
```
5 models and 13 tests pass. The build also re-applies policy tags and row policies and exports Iceberg metadata.

## 4 · Gold business rules: declarative PySpark (~3 min)
```bash
scripts/test-spark.sh -s
```
```bash
scripts/run-gold.sh
```
The first command is optional: 5 s locally, it runs the tests and prints the rule lineage table. The second reads `gold.fct_sales`, applies the 8 registered rules and writes `gold.sales_features`.

## 5 · Show the result
```bash
bq query --nouse_legacy_sql 'SELECT "bronze" t, COUNT(*) n, MAX(_meta_business_date) last_day FROM bronze.transaction UNION ALL SELECT "gold", COUNT(*), MAX(order_date) FROM gold.fct_sales UNION ALL SELECT "features", COUNT(*), MAX(order_date) FROM gold.sales_features'
```
Expect 11614 rows everywhere, last day 2026-01-08.

```bash
bq query --nouse_legacy_sql 'SELECT order_date, COUNT(*) n, ROUND(SUM(net_amount),2) net FROM gold.fct_sales GROUP BY 1 ORDER BY 1'
```
```bash
scripts/run-bronze.sh transaction
```
Run again: `files_to_load=0`, `nothing to do` (idempotent).

```bash
scripts/test-governance.sh
```
Who sees PII, and which rows.

```bash
gcloud storage ls gs://$PROJECT_ID-lake/iceberg/*/
```
`bronze/`, `silver/`, `gold/`: all Iceberg in one bucket.

## Reset (to run the demo again)
```bash
gcloud storage rm "gs://$PROJECT_ID-landing/ecommerce/transaction/2026-01-08/*"
scripts/run-bronze.sh transaction full_refresh && scripts/run-dbt.sh build && scripts/run-gold.sh
```
