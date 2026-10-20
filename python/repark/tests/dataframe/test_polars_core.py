"""R-POLARS-CORE — polars-style API over repark plans (importorskip real polars)."""

from __future__ import annotations

import pytest

import repark.spark.polars as rp
from repark import ReparkSession
from repark.errors import PySparkTypeError
from repark.spark.functions import col
from repark.spark.functions import sum as sum_

pl = pytest.importorskip("polars")


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-polars-core").getOrCreate()
    yield session
    session.stop()


def test_pl_accessor_and_collect(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id, 10 AS v UNION ALL SELECT 2, 20")
    out = frame.pl.select("id", "v").filter(col("id") > 1).collect()
    assert isinstance(out, pl.DataFrame)
    assert out.to_dicts() == [{"id": 2, "v": 20}]


def test_with_columns_sort_group(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS g, 10 AS v UNION ALL SELECT 1, 20 UNION ALL SELECT 2, 5")
    result = (
        frame.pl.with_columns(v2=col("v") * rp.lit(2))
        .sort("v", descending=True)
        .group_by("g")
        .agg(sum_("v").alias("sv"))
        .collect()
        .sort("g")
    )
    assert result.to_dicts() == [{"g": 1, "sv": 30}, {"g": 2, "sv": 5}]


def test_join_unique_drop_nulls(spark: ReparkSession) -> None:
    left = spark.sql("SELECT 1 AS id, 'a' AS x UNION ALL SELECT 2, 'b'")
    right = spark.sql("SELECT 1 AS id, 9 AS y")
    joined = left.pl.join(right.pl, on="id", how="inner").collect()
    assert joined.to_dicts() == [{"id": 1, "x": "a", "y": 9}]
    uniq = spark.sql("SELECT 1 AS id UNION ALL SELECT 1 UNION ALL SELECT 2").pl.unique().collect()
    assert sorted(uniq["id"].to_list()) == [1, 2]
    dropped = (
        spark.sql("SELECT 1 AS id, CAST(NULL AS INT) AS v UNION ALL SELECT 2, 3")
        .pl.drop_nulls()
        .collect()
    )
    assert dropped.to_dicts() == [{"id": 2, "v": 3}]


def test_spark_roundtrip_and_reject_real_pl_expr(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id")
    assert frame.pl.spark is frame or frame.pl.spark.count() == 1
    assert frame.pl.lazy() is not None
    real = pl.col("id")
    with pytest.raises(PySparkTypeError, match="polars"):
        frame.pl.filter(real)  # type: ignore[arg-type]


def test_collect_import_message_without_polars(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    import builtins

    real_import = builtins.__import__

    def guarded(name: str, *args: object, **kwargs: object) -> object:
        if name == "polars" or name.startswith("polars."):
            raise ImportError("forced missing polars")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", guarded)
    frame = spark.sql("SELECT 1 AS id")
    with pytest.raises(ImportError, match="pip install"):
        frame.pl.collect()
