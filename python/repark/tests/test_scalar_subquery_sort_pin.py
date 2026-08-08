"""DF 54.1 regression pin: ORDER BY must survive an uncorrelated scalar subquery.

DataFusion 54.1's new default-on `enable_physical_uncorrelated_scalar_subquery` physical
path (`ScalarSubqueryExec` plan wrapping) drops the query's top-level Sort — found by the
seeded fuzzer (banked repros fuzz-42-1 / fuzz-42-2, 2026-08-01). repark-session forces the
flag OFF; these pins are the re-enable done-signal: they must stay green when the flag is
flipped back after the upstream fix.
"""

from __future__ import annotations

from repark.session import ReparkSession


def _session() -> ReparkSession:
    return ReparkSession.builder.getOrCreate()


def test_order_by_desc_survives_scalar_subquery_filter() -> None:
    spark = _session()
    spark.createDataFrame([(0, -6642), (4, -4769)], ["id", "b"]).createOrReplaceTempView(
        "sub_sort_t0"
    )
    rows = (
        spark.sql("SELECT id FROM sub_sort_t0 WHERE id < (SELECT 99) ORDER BY b DESC")
        .to_arrow()
        .to_pylist()
    )
    assert rows == [{"id": 4}, {"id": 0}]


def test_multi_key_order_survives_aggregate_scalar_subquery() -> None:
    """The fuzz-42-1 shape: COUNT subquery in WHERE + multi-key ORDER BY with NULLS."""
    spark = _session()
    spark.createDataFrame(
        [(0, 0, -6642), (1, 4, -4769)], ["row_id", "id", "b"]
    ).createOrReplaceTempView("sub_sort_t1")
    spark.createDataFrame([(6,), (1,)], ["id"]).createOrReplaceTempView("sub_sort_t2")
    rows = (
        spark.sql(
            "SELECT id FROM sub_sort_t1 "
            "WHERE row_id < (SELECT COUNT(id) FROM sub_sort_t2) "
            "ORDER BY b DESC NULLS FIRST, id ASC NULLS FIRST, row_id ASC NULLS LAST"
        )
        .to_arrow()
        .to_pylist()
    )
    assert rows == [{"id": 4}, {"id": 0}]


def test_dataframe_order_by_survives_scalar_subquery_filter() -> None:
    """Same guarantee through the DataFrame API sort path."""
    from repark.functions import col

    spark = _session()
    spark.createDataFrame([(0, -6642), (4, -4769)], ["id", "b"]).createOrReplaceTempView(
        "sub_sort_t3"
    )
    base = spark.sql("SELECT id, b FROM sub_sort_t3 WHERE id < (SELECT 99)")
    rows = base.orderBy(col("b").desc()).select("id").to_arrow().to_pylist()
    assert rows == [{"id": 4}, {"id": 0}]
