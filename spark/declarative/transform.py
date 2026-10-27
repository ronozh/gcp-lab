"""Declarative column transformations: one business rule = one small registered function.

    transform = Transform()

    @transform.register(seq=1)                      # column name = function name
    def line_total():
        return F.col("quantity") * F.col("unit_price")

    @transform.register(seq=2, col_name="day_rank") # explicit name; may use earlier columns
    def rank_in_day():
        return F.row_number().over(Window.partitionBy("order_date").orderBy(F.desc("line_total")))

    result = transform.run(df, keep=["transaction_id"])   # keep columns + derived columns
"""
from pyspark.sql import DataFrame


class Transform:
    def __init__(self):
        self.rules = []   # (seq, col_name, fn)

    def register(self, seq, col_name=None):
        """seq is unique: rules run one by one in seq order."""
        def decorator(fn):
            if any(s == seq for s, _, _ in self.rules):
                raise ValueError(f"seq {seq} already used")
            self.rules.append((seq, col_name or fn.__name__, fn))
            return fn
        return decorator

    def run(self, df: DataFrame, keep=()) -> DataFrame:
        rules = sorted(self.rules, key=lambda r: r[0])
        for _, name, fn in rules:          # one by one: each rule can use every earlier column
            df = df.withColumn(name, fn())
        return df.select(*keep, *[name for _, name, _ in rules])

    def lineage(self) -> str:
        return "\n".join(f"{seq} | {name:<24} | {(fn.__doc__ or '').strip()}"
                         for seq, name, fn in sorted(self.rules, key=lambda r: r[0]))
