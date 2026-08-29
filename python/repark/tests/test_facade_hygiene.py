"""R-FACADE-HYGIENE pins: listTables CDF hide, weakref GC bound, fillna, dropDuplicates,
OOS errors.
"""

from __future__ import annotations

import gc

import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-facade-hygiene").getOrCreate()
    yield session
    session.stop()


def test_list_tables_hides_repark_cdf_views(spark: ReparkSession) -> None:
    spark.register_memory_catalog("hygiene_cat", "/tmp/repark-hygiene-wh")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS hygiene_cat.ns")
    spark.catalog.setCurrentCatalog("hygiene_cat")
    spark.catalog.setCurrentDatabase("ns")
    _ = spark.createDataFrame([(1,)], ["x"])
    names = [table.name for table in spark.catalog.listTables()]
    assert not any(name.startswith("__repark_cdf_") for name in names)


def test_cdf_views_bounded_after_gc(spark: ReparkSession) -> None:
    def _cdf_count() -> int:
        return sum(
            1 for table in spark.catalog.listTables() if table.name.startswith("__repark_cdf_")
        )

    # Hidden from listTables — use information_schema directly for growth pin.
    def _raw_cdf_count() -> int:
        rows = (
            spark.sql(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name LIKE '__repark_cdf_%'"
            )
            .to_arrow()
            .to_pylist()
        )
        return len(rows)

    spark._ensure_information_schema()
    baseline = _raw_cdf_count()
    holders = [spark.createDataFrame([(index,)], ["x"]) for index in range(20)]
    mid = _raw_cdf_count()
    assert mid >= baseline
    del holders
    gc.collect()
    gc.collect()
    after = _raw_cdf_count()
    # Bounded growth: not exact-zero (greylight B7); should not keep all 20 forever.
    assert after <= baseline + 5, (baseline, mid, after)


def test_fillna_dict_one_projection(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(None, None), (1, None)], ["a", "b"])
    filled = frame.fillna({"a": 0, "b": 0})
    rows = filled.to_arrow().to_pylist()
    assert rows[0]["a"] == 0 and rows[0]["b"] == 0


def test_drop_duplicates_subset(spark: ReparkSession) -> None:
    frame = spark.createDataFrame([(1, "a"), (1, "b"), (2, "c")], ["k", "v"])
    out = frame.dropDuplicates(["k"]).to_arrow().to_pylist()
    assert len(out) == 2
    keys = sorted(row["k"] for row in out)
    assert keys == [1, 2]


def test_oos_named_errors(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS x")
    with pytest.raises(UnsupportedOperationException, match="rdd"):
        _ = frame.rdd
    with pytest.raises(UnsupportedOperationException, match="writeStream"):
        _ = frame.writeStream
    with pytest.raises(UnsupportedOperationException, match="foreach"):
        _ = frame.foreach
    with pytest.raises(UnsupportedOperationException, match="registerTempTable"):
        spark.registerTempTable("t", frame)
    with pytest.raises(UnsupportedOperationException, match="pandas_api"):
        _ = spark.pandas_api
