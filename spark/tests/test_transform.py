"""Local tests: tiny in-memory DataFrames, no GCP. Run: scripts/test-spark.sh"""
from datetime import date, datetime

import pytest
from pyspark.sql import SparkSession, Window, functions as F

from declarative import sales_features
from declarative.transform import Transform


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


def test_default_name_explicit_name_and_seq_chaining(spark):
    t = Transform()

    @t.register(seq=2, col_name="doubled_total")    # registered first, runs second
    def double_it():
        return F.col("total") * 2

    @t.register(seq=1)
    def total():
        return F.col("a") + F.col("b")

    out = t.run(spark.createDataFrame([(1, 2)], "a int, b int"), keep=["a"])
    assert out.columns == ["a", "total", "doubled_total"]
    assert out.first().asDict() == {"a": 1, "total": 3, "doubled_total": 6}


def test_window_rule(spark):
    t = Transform()

    @t.register(seq=1)
    def group_sum():
        return F.sum("v").over(Window.partitionBy("g"))

    rows = t.run(spark.createDataFrame([("x", 1), ("x", 2), ("y", 5)], "g string, v int"), keep=["g"]).collect()
    assert sorted((r.g, r.group_sum) for r in rows) == [("x", 3), ("x", 3), ("y", 5)]


def test_sales_features_rules(spark):
    cols = ("transaction_id string, event_ts timestamp, order_date date, customer_id int, status string, "
            "net_amount double, margin_amount double, discount_pct double")
    d = date(2026, 1, 1)
    df = spark.createDataFrame([
        ("t1", datetime(2026, 1, 1, 9), d, 7, "completed", 900.0, 300.0, 0.0),
        ("t2", datetime(2026, 1, 1, 15), d, 7, "completed", 200.0, 50.0, 0.1),
        ("t3", datetime(2026, 1, 1, 16), d, 7, "cancelled", 999.0, 0.0, 0.0),
    ], cols)
    rows = {r.transaction_id: r for r in sales_features.build(df).collect()}

    assert rows["t1"].is_first_order and not rows["t2"].is_first_order
    assert rows["t2"].order_hour == 15 and rows["t2"].is_discounted
    assert rows["t1"].margin_pct == round(300 / 900, 4)
    assert rows["t3"].customer_daily_spend == 1100.0   # cancelled order excluded
    assert {r.spend_band for r in rows.values()} == {"high"}
    print("\n" + sales_features.transform.lineage())
