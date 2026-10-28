"""Declarative column transformations: one business rule = one small registered function.

    transform = Transform()

    @transform.register()                           # stage 1 (default); column name = function name
    def line_total():
        return F.col("quantity") * F.col("unit_price")

    @transform.register(stage=2, col_name="day_rank")   # stage 2 can use stage-1 columns
    def rank_in_day():
        return F.row_number().over(Window.partitionBy("order_date").orderBy(F.desc("line_total")))

    result = transform.run(df, keep=["transaction_id"])   # keep columns + derived columns
"""
from itertools import groupby

from pyspark.sql import DataFrame


class Transform:
    def __init__(self):
        self.rules = []   # (stage, col_name, fn)

    def register(self, stage=1, col_name=None):
        def decorator(fn):
            self.rules.append((stage, col_name or fn.__name__, fn))
            return fn
        return decorator

    def run(self, df: DataFrame, keep=()) -> DataFrame:
        rules = sorted(self.rules, key=lambda r: r[0])
        for _, same_stage in groupby(rules, key=lambda r: r[0]):
            # Rules in one stage are added together, so they can only use columns from earlier stages.
            df = df.withColumns({name: fn() for _, name, fn in same_stage})
        return df.select(*keep, *[name for _, name, _ in rules])

    def lineage(self) -> str:
        return "\n".join(f"{stage} | {name:<24} | {(fn.__doc__ or '').strip()}"
                         for stage, name, fn in sorted(self.rules, key=lambda r: r[0]))
