# 05 · Declarative business rules (PySpark)

**The problem:** business-layer logic changes often. Over years it becomes one huge SQL statement or Spark job: spaghetti, hard to change, hard to test.

**The idea:** **one business rule = one small function = one output column.** You register it with a decorator. The framework runs the rules in `seq` order and selects the results.

```python
transform = Transform()

@transform.register(seq=1)                          # column name = function name
def is_completed():
    """Only completed orders count as revenue."""
    return F.col("status") == "completed"

@transform.register(seq=1, col_name="order_hour")   # explicit column name
def hour_of_order():
    return F.hour("event_ts")

@transform.register(seq=2)                          # window rule; can use seq-1 columns
def customer_daily_spend():
    day = Window.partitionBy("customer_id", "order_date")
    return F.sum(F.when(F.col("is_completed"), F.col("net_amount")).otherwise(0)).over(day)

result = transform.run(df, keep=["transaction_id", "order_date", "customer_id"])
```

## Mental model
- A **rule** returns a Spark `Column`: a direct mapping, an expression, or a window function.
- **`seq`** is the order. Rules with a higher `seq` can use columns from a lower one; rules with the same `seq` run together.
- **`run()`** adds each seq's columns (`withColumns`), then selects `keep` + all derived columns.
- **`lineage()`** prints seq | column | docstring: living documentation of the business rules.

**Why it helps:** each change is scoped to one function, so it's easy to review, test and change; the rules are separate from the plumbing; and it's safe, because adding a rule doesn't touch the others.
**Not for:** grain changes (`groupBy` to one row per group). Do those before or after the transform.

## In this lab

```
gold.fct_sales (BigQuery/dbt, Iceberg) ──Spark reads exported metadata──▶ sales_features rules ──▶ gold.sales_features (Spark, Iceberg)
```

| File | What |
|---|---|
| `spark/declarative/transform.py` | The framework (~30 lines) |
| `spark/declarative/sales_features.py` | 8 rules: row-level (seq 1), window (seq 2), built on seq 2 (seq 3) |
| `spark/gold_features.py` | Job: read `fct_sales` → rules → write `gold.sales_features` |
| `spark/tests/test_transform.py` | Tests on tiny in-memory data, no GCP |

## Commands

```bash
scripts/test-spark.sh -s          # local tests + prints lineage (~5 s; needs Java 17 + uv)
scripts/run-gold.sh               # Serverless Spark: writes gold.sales_features (~3 min)
bq query --nouse_legacy_sql \
  'SELECT spend_band, COUNT(*) n, COUNTIF(is_first_order) first_orders FROM gold.sales_features GROUP BY 1'
```

## Try it
Add a rule to `sales_features.py`, e.g. `is_weekend` = `F.dayofweek("order_date").isin(1, 7)` at `seq=1`, run `scripts/test-spark.sh`, then `scripts/run-gold.sh`. No other code changes.

## Gotchas
- Dataproc runs Spark **3.5.3**; a newer local 3.5.x can have extra functions. `F.nullif` worked locally but failed on Dataproc, so keep rules to well-established functions.
