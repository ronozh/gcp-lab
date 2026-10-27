# 05 · Declarative business rules (PySpark)

**The problem:** business-layer logic changes often. Over years it becomes one huge SQL statement or Spark job: spaghetti, hard to change, hard to test.

**The idea:** **one business rule = one small function = one output column.** You register it with a decorator. The framework runs the rules stage by stage and selects the results.

```python
transform = Transform()

@transform.register()                               # stage 1 (default); column name = function name
def is_completed():
    """Only completed orders count as revenue."""
    return F.col("status") == "completed"

@transform.register(col_name="order_hour")          # explicit column name
def hour_of_order():
    return F.hour("event_ts")

@transform.register(stage=2)                        # window rule; uses the stage-1 column is_completed
def customer_daily_spend():
    day = Window.partitionBy("customer_id", "order_date")
    return F.sum(F.when(F.col("is_completed"), F.col("net_amount")).otherwise(0)).over(day)

result = transform.run(df, keep=["transaction_id", "order_date", "customer_id"])
```

## Mental model
- A **rule** returns a Spark `Column`: a direct mapping, an expression, or a window function.
- **`stage`** defaults to 1, so most rules don't mention it. Set `stage=2`, `3`, … only when a rule needs columns from an earlier stage. Rules within one stage run together, so their order doesn't matter.
- **`run()`** adds each stage's columns together (`withColumns`), then selects `keep` + all derived columns.
- **`lineage()`** prints stage | column | docstring: living documentation of the business rules.

**Why it helps:** each change is scoped to one function, so it's easy to review, test and change; the rules are separate from the plumbing; and it's safe, because adding a rule doesn't touch the others.
**Not for:** grain changes (`groupBy` to one row per group). Do those before or after the transform.

## In this lab

```
gold.fct_sales (BigQuery/dbt, Iceberg) ──Spark reads exported metadata──▶ sales_features rules ──▶ gold.sales_features (Spark, Iceberg)
```

| File | What |
|---|---|
| `spark/declarative/transform.py` | The framework (~30 lines) |
| `spark/declarative/sales_features.py` | 8 rules: stage 1 row-level (4), stage 2 window (2), stage 3 built on stage 2 (2) |
| `spark/gold_features.py` | Job: read `fct_sales` → rules → write `gold.sales_features` |
| `spark/tests/test_transform.py` | Tests on tiny in-memory data, no GCP |

## Commands

```bash
scripts/test-spark.sh -s          # local tests + prints lineage (~5 s; needs Java 17 + uv)
scripts/run-spark-declarative.sh               # Serverless Spark: writes gold.sales_features (~3 min)
bq query --nouse_legacy_sql \
  'SELECT spend_band, COUNT(*) n, COUNTIF(is_first_order) first_orders FROM gold.sales_features GROUP BY 1'
```

## Try it
Add a rule to `sales_features.py`, e.g. `is_weekend` = `F.dayofweek("order_date").isin(1, 7)` (stage 1, no argument needed), run `scripts/test-spark.sh`, then `scripts/run-spark-declarative.sh`. No other code changes.

## Gotchas
- **Schema changes (tested):** adding or removing a rule adds or removes the column on the next run, through CI → deployed job. A *type* change needs `bq rm -f -t gold.sales_features`, then a rerun (the table is fully derived, so nothing is lost).
- **No BigQuery time travel across runs:** each run is `createOrReplace`, so `FOR SYSTEM_TIME AS OF` before the last run gives "no snapshot active". Schema history lives in the Iceberg metadata files.
- Dataproc runs Spark **3.5.3**; a newer local 3.5.x can have extra functions. `F.nullif` worked locally but failed on Dataproc, so keep rules to well-established functions.
