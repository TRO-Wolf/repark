"""R-FN-BATCH4 — aggregates / stats / hash / id census pins."""

from __future__ import annotations

from decimal import Decimal

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import UnsupportedOperationException
from repark.spark.functions import (
    approx_percentile,
    bit_and,
    bit_or,
    bit_xor,
    collect_list,
    corr,
    covar_pop,
    covar_samp,
    first,
    input_file_name,
    kurtosis,
    last,
    median,
    mode,
    monotonically_increasing_id,
    percentile_approx,
    rand,
    sha2,
    skewness,
    spark_partition_id,
    stddev,
    stddev_pop,
    var_pop,
    variance,
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fn-batch4").getOrCreate()
    yield session
    session.stop()


def test_stats_aggregates(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT * FROM (VALUES (1.0), (2.0), (3.0)) t(x)")
    table = frame.agg(
        stddev("x").alias("sd"),
        stddev_pop("x").alias("sdp"),
        variance("x").alias("var"),
        var_pop("x").alias("vp"),
        median("x").alias("med"),
        corr("x", "x").alias("cr"),
        covar_pop("x", "x").alias("cp"),
        covar_samp("x", "x").alias("cs"),
        first("x").alias("f"),
        last("x").alias("l"),
        collect_list("x").alias("cl"),
    ).to_arrow()
    row = table.to_pylist()[0]
    # VALUES (1.0),(2.0),(3.0) are DECIMAL(2,1). stddev/corr stay float; median
    # and collect_list follow the decimal column.
    assert abs(row["sd"] - 1.0) < 1e-9
    assert abs(row["cr"] - 1.0) < 1e-9
    assert row["med"] == Decimal("2.0")
    assert sorted(row["cl"]) == [Decimal("1.0"), Decimal("2.0"), Decimal("3.0")]
    assert pa.types.is_floating(table.schema.field("sd").type)
    assert table.schema.field("med").type == pa.decimal128(2, 1)


def test_bit_aggregates(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT * FROM (VALUES (1), (2), (3)) t(x)")
    row = (
        frame.agg(bit_and("x").alias("a"), bit_or("x").alias("o"), bit_xor("x").alias("x_or"))
        .to_arrow()
        .to_pylist()[0]
    )
    assert row["a"] == 0
    assert row["o"] == 3


def test_sha2_256(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 'a' AS s")
    table = frame.select(sha2("s", 256).alias("h")).to_arrow()
    assert table.num_rows == 1


def test_rand_returns_float(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS x")
    table = frame.select(rand().alias("r")).to_arrow()
    val = table.to_pylist()[0]["r"]
    assert 0.0 <= float(val) <= 1.0


def test_percentile_approx_scalar_bounds(spark: ReparkSession) -> None:
    """percentile_approx / approx_percentile lower to t-digest; bounds-window oracle.

    Fixture sorted exact p50 neighbor window on {1,2,3} is [1,3] (exact median 2.0).
    Never pin exact cross-engine equality vs Spark GK.
    """
    frame = spark.sql("SELECT * FROM (VALUES (1.0), (2.0), (3.0)) t(x)")
    row = (
        frame.agg(
            percentile_approx("x", 0.5).alias("p50"),
            approx_percentile("x", 0.5).alias("ap50"),
            percentile_approx("x", 0.5, accuracy=10000).alias("p50_acc"),
        )
        .to_arrow()
        .to_pylist()[0]
    )
    for key in ("p50", "ap50", "p50_acc"):
        value = float(row[key])
        assert 1.0 <= value <= 3.0, f"{key}={value} outside exact-quantile neighbor window"
    # accuracy accepted-and-ignored: still returns a finite percentile (divergence pin).
    assert row["p50_acc"] is not None


def test_percentile_approx_sql_aliases(spark: ReparkSession) -> None:
    """SQL Spark names percentile_approx / approx_percentile resolve via UDAF aliases."""
    for name in ("percentile_approx", "approx_percentile", "approx_percentile_cont"):
        row = spark.sql(
            f"SELECT {name}(x, 0.5) AS m FROM (VALUES (1.0), (2.0), (3.0)) t(x)"
        ).collect()[0]
        value = float(row.asDict()["m"])
        assert 1.0 <= value <= 3.0


def test_percentile_approx_array_stop(spark: ReparkSession) -> None:
    """Array-of-percentages form is loud STOP (scalar engine only)."""
    with pytest.raises(UnsupportedOperationException, match="array_of_percentages"):
        percentile_approx("x", [0.25, 0.5, 0.75])


def test_percentile_approx_bool_percentage_rejected(spark: ReparkSession) -> None:
    """bool is not a valid percentage type (bool subclasses int)."""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match="percentage"):
        percentile_approx("x", True)  # type: ignore[arg-type]


def test_percentile_approx_sql_third_arg_is_centroids(spark: ReparkSession) -> None:
    """SQL 3rd arg is t-digest centroids (not facade-ignored GK accuracy).

    On a fixed 1..200 fixture, centroids=2 must be allowed to diverge from the
    default/high-centroid path within the global bounds window [1, 200].
    """
    values_sql = " UNION ALL ".join(f"SELECT {index}.0 AS x" for index in range(1, 201))
    row = (
        spark.sql(
            f"SELECT percentile_approx(x, 0.5) AS p_default, "
            f"percentile_approx(x, 0.5, 2) AS p_c2, "
            f"percentile_approx(x, 0.5, 10000) AS p_c10k "
            f"FROM ({values_sql})"
        )
        .collect()[0]
        .asDict()
    )
    for key in ("p_default", "p_c2", "p_c10k"):
        value = float(row[key])
        assert 1.0 <= value <= 200.0, f"{key}={value} outside fixture bounds"
    # centroids=2 is a coarser t-digest; it may equal by chance, so only finite + in-window
    # is required.
    assert row["p_c2"] is not None


def test_approx_percentile_double_interpolation_divergence_is_pinned(spark: ReparkSession) -> None:
    """FN-APPROXPCT-1: repark interpolates to DOUBLE where Spark is exact and BIGINT."""
    frame = spark.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("a", None), ("b", 4), ("b", 6)],
        ["k", "v"],
    )
    table = frame.select(approx_percentile("v", 0.5).alias("p")).to_arrow()
    assert table.to_pylist()[0]["p"] == 3.0
    assert pa.types.is_float64(table.schema.field("p").type)
    grouped = frame.groupBy("k").agg(approx_percentile("v", 0.5).alias("p")).collect()
    assert sorted((row["k"], row["p"]) for row in grouped) == [("a", 2.0), ("b", 5.0)]


def test_batch4_loud_unsupported(spark: ReparkSession) -> None:
    with pytest.raises(UnsupportedOperationException, match="skewness"):
        skewness("x")
    with pytest.raises(UnsupportedOperationException, match="kurtosis"):
        kurtosis("x")
    # percentile_approx / approx_percentile ship (see test_percentile_approx_scalar_bounds);
    # the rest stay loud-unsupported.
    with pytest.raises(UnsupportedOperationException, match="mode"):
        mode("x")
    # FNP-3: sha1 / crc32 / xxhash64 ship (datafusion-spark hash kernels). Behavior:
    # test_fnp3_destubbed.py.
    # G2: randn is live (XORShift Gaussian); keep other loud stubs.
    with pytest.raises(UnsupportedOperationException, match="monotonically_increasing_id"):
        monotonically_increasing_id()
    with pytest.raises(UnsupportedOperationException, match="spark_partition_id"):
        spark_partition_id()
    with pytest.raises(UnsupportedOperationException, match="input_file_name"):
        input_file_name()
    with pytest.raises(UnsupportedOperationException, match="256"):
        sha2("s", 512)
