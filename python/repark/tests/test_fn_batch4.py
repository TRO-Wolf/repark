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


def test_sha2_facade_hex_string_matches_spark(spark: ReparkSession) -> None:
    """FN-SHA2-1: facade sha2 is lowercase hex STRING. pins: fn-fix-1-registry-rows/C-003"""
    hello = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    sha512 = (
        "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca7"
        "2323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
    )
    sha224 = "ea09ae9cc6768c50fcee903ed054556e5bfc8347907f12598aa24193"
    sha384 = (
        "59e1748777448c69de6b800d7a33bbfb9ff1b463e44354c3553bcdb9c666fa90"
        "125a3c79f90397bdf5f6a13de828684f"
    )
    frame = spark.sql("SELECT 'hello' AS s")
    for bits, want in (
        (0, hello),
        (224, sha224),
        (256, hello),
        (384, sha384),
        (512, sha512),
    ):
        table = frame.select(sha2("s", bits).alias("h")).to_arrow()
        assert table.column("h").to_pylist()[0] == want, bits
        assert pa.types.is_string(table.schema.field("h").type)
    door = spark.sql("SELECT sha2('hello', 256) AS h, sha2('hello', 0) AS z").to_arrow()
    assert door.column("h").to_pylist()[0] == hello
    assert door.column("z").to_pylist()[0] == hello
    from repark.errors import PySparkValueError

    with pytest.raises(PySparkValueError, match="VALUE_NOT_ALLOWED"):
        sha2("s", 128)


def test_rand_returns_float(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS x")
    table = frame.select(rand().alias("r")).to_arrow()
    val = table.to_pylist()[0]["r"]
    assert 0.0 <= float(val) <= 1.0


def test_percentile_approx_scalar_bounds(spark: ReparkSession) -> None:
    """Discrete p50 of {1,2,3} is 2. pins: fn-fix-1-registry-rows/C-003"""
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
        assert float(row[key]) == 2.0, f"{key}={row[key]}"


def test_percentile_approx_sql_aliases(spark: ReparkSession) -> None:
    """SQL Spark names resolve; discrete names answer 2. pins: fn-fix-1-registry-rows/C-003"""
    for name in ("percentile_approx", "approx_percentile"):
        row = spark.sql(
            f"SELECT {name}(x, 0.5) AS m FROM (VALUES (1.0), (2.0), (3.0)) t(x)"
        ).collect()[0]
        assert float(row.asDict()["m"]) == 2.0
    row = spark.sql(
        "SELECT approx_percentile_cont(x, 0.5) AS m FROM (VALUES (1.0), (2.0), (3.0)) t(x)"
    ).collect()[0]
    value = float(row.asDict()["m"])
    assert 1.0 <= value <= 3.0


def test_percentile_approx_array_of_percentages(spark: ReparkSession) -> None:
    """Array-of-percentages discrete values. pins: fn-fix-1-registry-rows/C-003"""
    frame = spark.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("b", 4), ("b", 6)],
        ["k", "v"],
    )
    grouped = frame.groupBy("k").agg(percentile_approx("v", [0.25, 0.5, 0.75]).alias("p")).collect()
    assert sorted((row["k"], row["p"]) for row in grouped) == [("a", [1, 2, 3]), ("b", [4, 4, 6])]


def test_percentile_approx_bool_percentage_rejected(spark: ReparkSession) -> None:
    """bool is not a valid percentage type (bool subclasses int)."""
    from repark.errors import PySparkTypeError

    with pytest.raises(PySparkTypeError, match="percentage"):
        percentile_approx("x", True)  # type: ignore[arg-type]


def test_percentile_approx_sql_third_arg_does_not_change_discrete_p50(
    spark: ReparkSession,
) -> None:
    """FN-APPROXPCT-ACC-1: accuracy 2 is 100.0; Spark 1.0. pins: fn-fix-1-registry-rows/C-003"""
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
    assert float(row["p_default"]) == 100.0
    assert float(row["p_c10k"]) == 100.0
    assert (float(row["p_c2"]), 1.0) == (100.0, 1.0)


def test_approx_percentile_discrete_bigint_matches_spark(spark: ReparkSession) -> None:
    """FN-APPROXPCT-1: discrete data value, BIGINT. pins: fn-fix-1-registry-rows/C-003"""
    frame = spark.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("a", None), ("b", 4), ("b", 6)],
        ["k", "v"],
    )
    table = frame.select(approx_percentile("v", 0.5).alias("p")).to_arrow()
    assert table.to_pylist()[0]["p"] == 3
    assert pa.types.is_int64(table.schema.field("p").type)
    grouped = frame.groupBy("k").agg(approx_percentile("v", 0.5).alias("p")).collect()
    assert sorted((row["k"], row["p"]) for row in grouped) == [("a", 2), ("b", 4)]
    percentile_table = frame.select(percentile_approx("v", 0.5).alias("p")).to_arrow()
    assert percentile_table.to_pylist()[0]["p"] == 3
    assert pa.types.is_int64(percentile_table.schema.field("p").type)
    percentile_grouped = frame.groupBy("k").agg(percentile_approx("v", 0.5).alias("p")).collect()
    assert sorted((row["k"], row["p"]) for row in percentile_grouped) == [("a", 2), ("b", 4)]
    array_table = frame.select(approx_percentile("v", [0.0, 0.5, 1.0]).alias("p")).to_arrow()
    assert array_table.to_pylist()[0]["p"] == [1, 3, 6]


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
