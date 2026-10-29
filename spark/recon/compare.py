"""Cell-to-cell comparison of two outputs of the same feed: AWS (legacy) vs GCP (new).

FULL OUTER JOIN on business key(s) + _meta_file_name. For every other column c:
  c_aws, c_gcp, c_is_eq   (null-safe: null = null is equal; false when the row is on one side only)
Plus _row_status (both / aws_only / gcp_only) and _all_eq (row on both sides, every cell equal).
"""
from functools import reduce

from pyspark.sql import Column, DataFrame, functions as F

FILE_KEY = "_meta_file_name"
FILE_DATE = r"_(\d{8})\d{6}\.[a-z]+$"   # <feed>_<yyyymmdd><hhmmss>.csv -> business date


def business_date(file_name: Column) -> Column:
    """The business date in a file name (null if it has none)."""
    return F.to_date(F.regexp_extract(file_name, FILE_DATE, 1), "yyyyMMdd")


def check_unique(df: DataFrame, keys: list, side: str) -> None:
    dupes = df.groupBy(*keys).count().filter("count > 1").limit(5).collect()
    if dupes:
        raise ValueError(f"{side}: keys {keys} not unique, e.g. {[r.asDict() for r in dupes]}")


def compare(aws: DataFrame, gcp: DataFrame, keys: list, columns: list) -> DataFrame:
    keys = [*keys, FILE_KEY]
    for df, side in ((aws, "aws"), (gcp, "gcp")):
        check_unique(df, keys, side)

    def side(df, name):
        return df.select(*keys, *[F.col(c).alias(f"{c}_{name}") for c in columns], F.lit(True).alias(f"_in_{name}"))

    joined = side(aws, "aws").join(side(gcp, "gcp"), on=keys, how="full")
    both = F.col("_in_aws").isNotNull() & F.col("_in_gcp").isNotNull()
    eq = {c: F.when(both, F.col(f"{c}_aws").eqNullSafe(F.col(f"{c}_gcp"))).otherwise(False) for c in columns}
    cells = [x for c in columns for x in (F.col(f"{c}_aws"), F.col(f"{c}_gcp"), eq[c].alias(f"{c}_is_eq"))]
    return joined.select(
        business_date(F.col(FILE_KEY)).alias("business_date"),
        *keys,
        F.when(both, "both").when(F.col("_in_aws").isNotNull(), "aws_only").otherwise("gcp_only").alias("_row_status"),
        reduce(lambda x, y: x & y, eq.values(), both).alias("_all_eq"),
        *cells,
    )
