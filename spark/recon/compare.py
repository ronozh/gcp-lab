"""Cell-to-cell comparison of two outputs of the same feed: AWS (legacy) vs GCP (new).

FULL OUTER JOIN on <key>_aws = <key>_gcp (each business key) and the same _meta_file_name.
For every column c, keys included: c_aws, c_gcp, c_is_eq
  null-safe: null = null is equal; false when the row is on one side only.
  strict: if c's type differs between the sides, the values are compared as text, so 1 != true
  (Spark would coerce and call them equal).
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
    for df, name in ((aws, "aws"), (gcp, "gcp")):
        check_unique(df, [*keys, FILE_KEY], name)
    cols = [*keys, *columns]

    def side(df, name):
        return df.select(*[F.col(c).alias(f"{c}_{name}") for c in [*cols, FILE_KEY]], F.lit(True).alias(f"_in_{name}"))

    on = [F.col(f"{k}_aws") == F.col(f"{k}_gcp") for k in [*keys, FILE_KEY]]
    joined = side(aws, "aws").join(side(gcp, "gcp"), on=reduce(lambda x, y: x & y, on), how="full")
    both = F.col("_in_aws").isNotNull() & F.col("_in_gcp").isNotNull()

    def cell_eq(c):
        a, g = F.col(f"{c}_aws"), F.col(f"{c}_gcp")
        if aws.schema[c].dataType != gcp.schema[c].dataType:
            a, g = a.cast("string"), g.cast("string")
        return F.when(both, a.eqNullSafe(g)).otherwise(False)

    eq = {c: cell_eq(c) for c in cols}
    file_name = F.coalesce(F.col(f"{FILE_KEY}_aws"), F.col(f"{FILE_KEY}_gcp"))
    return joined.select(
        business_date(file_name).alias("business_date"),
        file_name.alias(FILE_KEY),
        F.when(both, "both").when(F.col("_in_aws").isNotNull(), "aws_only").otherwise("gcp_only").alias("_row_status"),
        reduce(lambda x, y: x & y, eq.values(), both).alias("_all_eq"),
        *[x for c in cols for x in (F.col(f"{c}_aws"), F.col(f"{c}_gcp"), eq[c].alias(f"{c}_is_eq"))],
    )
