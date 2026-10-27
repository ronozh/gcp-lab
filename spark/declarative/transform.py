"""Declarative column transformations: one business rule = one small registered function.

    transform = Transform()

    @transform.register(seq=1)                      # column name = function name
    def line_total():
        return F.col("quantity") * F.col("unit_price")

    @transform.register(seq=2, col_name="day_rank") # explicit column name
    def rank_in_day():                              # may use columns from lower seqs
        return F.row_number().over(Window.partitionBy("order_date").orderBy(F.desc("line_total")))

    result = transform.run(df, keep=["transaction_id"])   # keep columns + derived columns, in seq order

Rules return a Spark Column: a direct mapping, an expression, or a window function.
Grain changes (groupBy) don't fit a per-row column: do them before or after the Transform.
"""
import re
from dataclasses import dataclass
from itertools import groupby
from typing import Callable, Optional

from pyspark.sql import Column, DataFrame

# Spark accepts almost any column name, but BigQuery/Iceberg tables don't (e.g. "derived/column2").
VALID_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Rule:
    col_name: str
    seq: int
    fn: Callable[[], Column]

    @property
    def doc(self) -> str:
        return (self.fn.__doc__ or "").strip().split("\n")[0]


class Transform:
    def __init__(self):
        self._rules = {}   # col_name -> Rule

    def register(self, seq: int, col_name: Optional[str] = None):
        """Decorator: register fn as the rule for one output column."""
        def decorator(fn: Callable[[], Column]) -> Callable[[], Column]:
            name = col_name or fn.__name__
            if not VALID_NAME.match(name):
                raise ValueError(f"column {name!r}: use letters, digits, _ (BigQuery/Iceberg reject others)")
            if name in self._rules:
                raise ValueError(f"column {name!r} already registered by {self._rules[name].fn.__name__}()")
            self._rules[name] = Rule(name, seq, fn)
            return fn
        return decorator

    @property
    def rules(self) -> list:
        """Rules in run order: by seq, then registration order."""
        return sorted(self._rules.values(), key=lambda r: r.seq)

    def run(self, df: DataFrame, keep: tuple = ()) -> DataFrame:
        """Apply rules seq by seq (one select per seq), then return `keep` + derived columns."""
        for _, same_seq in groupby(self.rules, key=lambda r: r.seq):
            # Rules with the same seq are computed together, so they can't see each other's output.
            df = df.withColumns({r.col_name: r.fn() for r in same_seq})
        return df.select(*keep, *[r.col_name for r in self.rules])

    def lineage(self) -> str:
        """Run plan as a table: seq | column | function | first docstring line."""
        rows = [f"{r.seq:>3} | {r.col_name:<28} | {r.fn.__name__:<28} | {r.doc}" for r in self.rules]
        return "\n".join([f"{'seq':>3} | {'column':<28} | {'function':<28} | rule", "-" * 100, *rows])
