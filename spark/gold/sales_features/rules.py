"""Business rules for gold.sales_features: one function per output column.

Input: gold.fct_sales (one row per transaction). Output: one row per transaction, KEYS + derived columns.
To change a rule, change its function. To add one, add a function. Nothing else moves.
"""
from pyspark.sql import DataFrame, Window, functions as F

from gold.declarative import Transform

SOURCE = "fct_sales"   # input: gold.<SOURCE>; output: gold.<this module's name>
KEYS = ("transaction_id", "order_date", "customer_id")

transform = Transform()


# --- stage 1 (default): row-level rules, only input columns --------------------------------------

@transform.register()
def is_completed():
    """Only completed orders count as revenue."""
    return F.col("status") == "completed"


@transform.register()
def margin_pct():
    """Margin as a share of net amount (null when net is 0)."""
    return F.when(F.col("net_amount") != 0, F.round(F.col("margin_amount") / F.col("net_amount"), 4))


@transform.register()
def is_discounted():
    """Any discount applied."""
    return F.col("discount_pct") > 0


@transform.register(col_name="order_hour")
def hour_of_order():
    """Hour of day the order was placed (0-23)."""
    return F.hour("event_ts")


# --- stage 2: window rules (use stage-1 columns) -------------------------------------------------

@transform.register(stage=2)
def customer_daily_spend():
    """Completed net spend of this customer on this day."""
    day = Window.partitionBy("customer_id", "order_date")
    return F.round(F.sum(F.when(F.col("is_completed"), F.col("net_amount")).otherwise(0)).over(day), 2)


@transform.register(stage=2)
def customer_order_number():
    """1 = the customer's first order in the data, 2 = second, ..."""
    history = Window.partitionBy("customer_id").orderBy("event_ts", "transaction_id")
    return F.row_number().over(history)


# --- stage 3: rules built on stage 2 -------------------------------------------------------------

@transform.register(stage=3)
def is_first_order():
    """The customer's first order."""
    return F.col("customer_order_number") == 1


@transform.register(stage=3)
def spend_band():
    """Daily spend band: high >= 1000, medium >= 200, else low."""
    spend = F.col("customer_daily_spend")
    return F.when(spend >= 1000, "high").when(spend >= 200, "medium").otherwise("low")


def build(fct_sales: DataFrame) -> DataFrame:
    return transform.run(fct_sales, keep=KEYS)
