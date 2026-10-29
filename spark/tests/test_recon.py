"""Local tests for the cell-to-cell recon. Run: scripts/test-spark.sh"""
from datetime import date

import pytest
from pyspark.sql import SparkSession

from recon.compare import compare


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.master("local[1]").config("spark.ui.enabled", "false").getOrCreate()
    yield s
    s.stop()


SCHEMA = "id int, name string, amount double, _meta_file_name string"
F1 = "campaign_20260101235959.csv"


def test_compare_cells_rows_and_nulls(spark):
    aws = spark.createDataFrame([(1, "a", 1.0, F1), (2, None, 2.0, F1), (3, "c", 3.0, F1)], SCHEMA)
    gcp = spark.createDataFrame([(1, "a", 1.0, F1), (2, None, 2.5, F1), (4, "d", 4.0, F1)], SCHEMA)
    out = compare(aws, gcp, ["id"], ["name", "amount"])
    rows = {r.id_aws or r.id_gcp: r for r in out.collect()}

    assert out.columns[:4] == ["business_date", "_meta_file_name", "_row_status", "_all_eq"]
    assert out.columns[4:7] == ["id_aws", "id_gcp", "id_is_eq"]
    assert rows[1]._row_status == "both" and rows[1]._all_eq and rows[1].id_is_eq
    assert rows[1].business_date == date(2026, 1, 1)
    assert rows[2].name_is_eq                      # null = null
    assert not rows[2].amount_is_eq and (rows[2].amount_aws, rows[2].amount_gcp) == (2.0, 2.5)
    assert not rows[2]._all_eq
    assert (rows[3]._row_status, rows[3].id_gcp, rows[3].id_is_eq, rows[3]._all_eq) == ("aws_only", None, False, False)
    assert rows[4]._row_status == "gcp_only" and rows[4].name_aws is None


def test_compare_is_strict_when_types_differ(spark):
    aws = spark.createDataFrame([(1, 1, F1)], "id int, flag int, _meta_file_name string")
    gcp = spark.createDataFrame([(1, True, F1)], "id int, flag boolean, _meta_file_name string")
    r = compare(aws, gcp, ["id"], ["flag"]).first()
    assert (r.flag_aws, r.flag_gcp, r.flag_is_eq) == (1, True, False)   # Spark alone would say 1 = true


def test_compare_rejects_duplicate_keys(spark):
    aws = spark.createDataFrame([(1, "a", 1.0, F1), (1, "b", 1.0, F1)], SCHEMA)
    gcp = spark.createDataFrame([(1, "a", 1.0, F1)], SCHEMA)
    with pytest.raises(ValueError, match="aws: keys"):
        compare(aws, gcp, ["id"], ["name", "amount"])
