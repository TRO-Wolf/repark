"""Grouped avg/try_avg answer pins plus the many-groups cost probe (PERF-AGG-AVG-1)."""

from __future__ import annotations

import statistics
import time
from decimal import Decimal
from pathlib import Path

import _live_parity as live_parity
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import repark
from repark.spark import SparkSession
from repark.spark import functions as F  # noqa: N812
from repark_parity import assert_frames_equal

RATIO_BOUND = 2.5
MANY_GROUPS = 200_000
MANY_ROWS_PER_GROUP = 10

SQL_INT_GLOBAL = "SELECT avg(v) AS a FROM (VALUES (1), (2), (3), (NULL), (5)) AS t(v)"

SQL_FLOAT_GLOBAL = (
    "SELECT avg(v) AS a FROM (VALUES (CAST(1.5 AS DOUBLE)), (CAST(NULL AS DOUBLE)), "
    "(CAST(2.5 AS DOUBLE)), (CAST(-0.5 AS DOUBLE))) AS t(v)"
)

SQL_GROUPED_SMALL = (
    "SELECT k, avg(v) AS a FROM (VALUES "
    "('a', CAST(1.0 AS DOUBLE)), ('a', CAST(3.0 AS DOUBLE)), "
    "('b', CAST(NULL AS DOUBLE)), ('b', CAST(4.0 AS DOUBLE)), "
    "('c', CAST(NULL AS DOUBLE))) AS t(k, v) GROUP BY k ORDER BY k"
)

SQL_GROUPED_SMALL_A = (
    "SELECT avg(v) AS a FROM (VALUES "
    "('a', CAST(1.0 AS DOUBLE)), ('a', CAST(3.0 AS DOUBLE)), "
    "('b', CAST(NULL AS DOUBLE)), ('b', CAST(4.0 AS DOUBLE)), "
    "('c', CAST(NULL AS DOUBLE))) AS t(k, v) GROUP BY k"
)

SQL_DECIMAL_GROUPED = (
    "SELECT k, avg(v) AS a FROM (VALUES "
    "(CAST(1 AS INT), CAST('1.10' AS DECIMAL(10, 2))), "
    "(CAST(1 AS INT), CAST('2.20' AS DECIMAL(10, 2))), "
    "(CAST(2 AS INT), CAST('3.30' AS DECIMAL(10, 2))), "
    "(CAST(2 AS INT), CAST(NULL AS DECIMAL(10, 2)))) "
    "AS t(k, v) GROUP BY k ORDER BY k"
)

SQL_DECIMAL_GROUPED_A = (
    "SELECT avg(v) AS a FROM (VALUES "
    "(CAST(1 AS INT), CAST('1.10' AS DECIMAL(10, 2))), "
    "(CAST(1 AS INT), CAST('2.20' AS DECIMAL(10, 2))), "
    "(CAST(2 AS INT), CAST('3.30' AS DECIMAL(10, 2))), "
    "(CAST(2 AS INT), CAST(NULL AS DECIMAL(10, 2)))) "
    "AS t(k, v) GROUP BY k"
)

SQL_DECIMAL_GLOBAL = (
    "SELECT avg(v) AS a FROM (VALUES "
    "(CAST('1.10' AS DECIMAL(10, 2))), (CAST('2.20' AS DECIMAL(10, 2)))) AS t(v)"
)

SQL_TRY_AVG_OVERFLOW = (
    "SELECT try_avg(v) AS a FROM (VALUES "
    "(CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0))), "
    "(CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0)))) AS t(v)"
)

SQL_AVG_OVERFLOW = (
    "SELECT avg(v) AS a FROM (VALUES "
    "(CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0))), "
    "(CAST(99999999999999999999999999999999999999 AS DECIMAL(38, 0)))) AS t(v)"
)

SQL_EMPTY_GLOBAL = "SELECT avg(v) AS a FROM (VALUES (CAST(1.0 AS DOUBLE))) AS t(v) WHERE false"

SQL_ALL_NULL_GROUP = (
    "SELECT k, avg(v) AS a FROM (VALUES "
    "('a', CAST(NULL AS DOUBLE)), ('a', CAST(NULL AS DOUBLE)), "
    "('b', CAST(1.0 AS DOUBLE))) AS t(k, v) GROUP BY k ORDER BY k"
)

SQL_ALL_NULL_GROUP_A = (
    "SELECT avg(v) AS a FROM (VALUES "
    "('a', CAST(NULL AS DOUBLE)), ('a', CAST(NULL AS DOUBLE)), "
    "('b', CAST(1.0 AS DOUBLE))) AS t(k, v) GROUP BY k"
)

SQL_WINDOW_SLIDING = (
    "SELECT id, avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a "
    "FROM (VALUES (CAST(1 AS INT), CAST(1.0 AS DOUBLE)), "
    "(CAST(2 AS INT), CAST(2.0 AS DOUBLE)), "
    "(CAST(3 AS INT), CAST(NULL AS DOUBLE)), (CAST(4 AS INT), CAST(4.0 AS DOUBLE))) "
    "AS t(id, v) ORDER BY id"
)

SQL_WINDOW_SLIDING_A = (
    "SELECT avg(v) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS a "
    "FROM (VALUES (CAST(1 AS INT), CAST(1.0 AS DOUBLE)), "
    "(CAST(2 AS INT), CAST(2.0 AS DOUBLE)), "
    "(CAST(3 AS INT), CAST(NULL AS DOUBLE)), (CAST(4 AS INT), CAST(4.0 AS DOUBLE))) "
    "AS t(id, v)"
)

SQL_AVG_DISTINCT = "SELECT avg(DISTINCT v) AS a FROM (VALUES (1.0), (2.0), (2.0)) AS t(v)"

SQL_AVG_DISTINCT_INT = "SELECT avg(DISTINCT v) AS a FROM (VALUES (1), (2), (2)) AS t(v)"

SQL_MULTI_DISTINCT = (
    "SELECT avg(DISTINCT a) AS x, sum(DISTINCT b) AS y FROM (VALUES (1, 10), (2, 10)) AS t(a, b)"
)

SQL_GROUPED_MULTI_DISTINCT = (
    "SELECT k, avg(DISTINCT a) AS x, sum(DISTINCT b) AS y FROM "
    "(VALUES (1, 1, 10), (1, 2, 10), (2, 1, 10)) AS t(k, a, b) GROUP BY k"
)

SQL_GROUPED_AVG_NULL = "SELECT k, avg(v) AS a FROM (VALUES (1, NULL)) AS t(k, v) GROUP BY k"

SQL_DECIMAL_SUMWRAP_GROUPED_TRY = (
    "SELECT k, try_avg(v) AS a FROM (VALUES "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('40282366920938463463374607431768211459' AS DECIMAL(38, 0)))) "
    "AS t(k, v) GROUP BY k"
)

SQL_DECIMAL_SUMWRAP_GROUPED = (
    "SELECT k, avg(v) AS a FROM (VALUES "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('40282366920938463463374607431768211459' AS DECIMAL(38, 0)))) "
    "AS t(k, v) GROUP BY k"
)

SQL_DECIMAL_SUMWRAP_GLOBAL = (
    "SELECT avg(v) AS a FROM (VALUES "
    "(CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST('40282366920938463463374607431768211459' AS DECIMAL(38, 0)))) AS t(v)"
)

SQL_DECIMAL_SUMWRAP_NONZERO_GROUPED_TRY = (
    "SELECT k, try_avg(v) AS a FROM (VALUES "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('40282366920938463463374607431768611459' AS DECIMAL(38, 0)))) "
    "AS t(k, v) GROUP BY k"
)

SQL_DECIMAL_SUMWRAP_NONZERO_GROUPED = (
    "SELECT k, avg(v) AS a FROM (VALUES "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('40282366920938463463374607431768611459' AS DECIMAL(38, 0)))) "
    "AS t(k, v) GROUP BY k"
)

SQL_DECIMAL_SUMWRAP_NONZERO_FRAME = (
    "SELECT k, v FROM (VALUES "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(1, CAST('40282366920938463463374607431768611459' AS DECIMAL(38, 0)))) "
    "AS t(k, v)"
)

SQL_DECIMAL_SUMWRAP_WINDOW_TRY = (
    "SELECT id, try_avg(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING "
    "AND CURRENT ROW) AS a FROM (VALUES "
    "(CAST(1 AS INT), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST(2 AS INT), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST(3 AS INT), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST(4 AS INT), CAST('40282366920938463463374607431768211459' AS DECIMAL(38, 0)))) "
    "AS t(id, v) ORDER BY id"
)

SQL_DECIMAL_SUMWRAP_WINDOW = (
    "SELECT id, avg(v) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING "
    "AND CURRENT ROW) AS a FROM (VALUES "
    "(CAST(1 AS INT), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST(2 AS INT), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST(3 AS INT), CAST('99999999999999999999999999999999999999' AS DECIMAL(38, 0))), "
    "(CAST(4 AS INT), CAST('40282366920938463463374607431768211459' AS DECIMAL(38, 0)))) "
    "AS t(id, v) ORDER BY id"
)

SQL_FLOAT_DRIFT_GROUPED = (
    "SELECT k, avg(v) AS a FROM (VALUES (1, CAST(1e16 AS DOUBLE)), "
    + ", ".join(["(1, CAST(1.0 AS DOUBLE))"] * 64)
    + ") AS t(k, v) GROUP BY k"
)

SQL_FLOAT_DRIFT_GROUPED_A = (
    "SELECT avg(v) AS a FROM (VALUES (1, CAST(1e16 AS DOUBLE)), "
    + ", ".join(["(1, CAST(1.0 AS DOUBLE))"] * 64)
    + ") AS t(k, v) GROUP BY k"
)

SPARK_FLOAT_DRIFT_GROUPED = 153846153846154.34

SQL_MANY_CHECKSUM = (
    "SELECT count(*) AS groups, sum(a) AS checksum FROM "
    "(SELECT k, avg(v) AS a FROM many_groups GROUP BY k) t"
)

SQL_MANY_SAMPLE = "SELECT k, avg(v) AS a FROM many_groups GROUP BY k ORDER BY k LIMIT 3"

SQL_MANY_AVG = "SELECT k, avg(v) AS a FROM many_groups GROUP BY k"

SQL_MANY_AVG_A = "SELECT avg(v) AS a FROM many_groups GROUP BY k"

SQL_MANY_SUM = "SELECT k, sum(v) AS s FROM many_groups GROUP BY k"


def _session() -> SparkSession:
    """A facade session of this module's own, per the suite's build-your-own convention."""
    return SparkSession.builder.appName("pytest-perf-agg-avg-1").getOrCreate()


def _sig(table: pa.Table) -> list[tuple[str, str, bool]]:
    """The (name, arrow-type, nullable) schema signature of one result table."""
    return [(field.name, str(field.type), field.nullable) for field in table.schema]


def _write_many_groups(path: Path) -> None:
    """Write the deterministic 1e5-group parquet fixture with periodic NULLs."""
    keys: list[int] = []
    values: list[float | None] = []
    for key in range(MANY_GROUPS):
        for row in range(MANY_ROWS_PER_GROUP):
            keys.append(key)
            ordinal = key * MANY_ROWS_PER_GROUP + row
            if ordinal % 11 == 0:
                values.append(None)
            else:
                values.append(float((key * 31 + row * 7) % 64) * 0.5)
    table = pa.table({"k": pa.array(keys, type=pa.int32()), "v": pa.array(values)})
    pq.write_table(table, path)


def _many_groups_view(spark: SparkSession, path: Path) -> None:
    """Register the many-groups parquet fixture as the shared temp view."""
    spark.read.parquet(str(path)).createOrReplaceTempView("many_groups")


def _median_ms(spark: SparkSession, sql: str, iterations: int = 3) -> float:
    """Median wall ms of one SQL string rebuilt and executed per iteration."""
    spark.sql(sql).toArrow()
    samples: list[float] = []
    for _ in range(iterations):
        start = time.perf_counter()
        spark.sql(sql).toArrow()
        samples.append((time.perf_counter() - start) * 1000.0)
    return statistics.median(samples)


def test_avg_int_global_value_and_type() -> None:
    """Int avg with a NULL answers 2.75 as a nullable double."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_INT_GLOBAL).toArrow()
    assert table.column("a").to_pylist() == [2.75]
    assert _sig(table) == [("a", "double", True)]


def test_avg_float_global_with_nulls() -> None:
    """Float avg skips NULLs and answers the exact double quotient."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_FLOAT_GLOBAL).toArrow()
    assert table.column("a").to_pylist() == [3.5 / 3]
    assert _sig(table) == [("a", "double", True)]


@pytest.mark.parametrize(
    "cast",
    ["TINYINT", "SMALLINT", "INT", "BIGINT", "FLOAT", "DOUBLE"],
)
def test_avg_input_widths_coerce_to_double(cast: str) -> None:
    """Every served input width averages to the same nullable double."""
    """pins: perf-agg-avg-1/C-003."""
    table = (
        _session()
        .sql(f"SELECT avg(CAST(v AS {cast})) AS a FROM (VALUES (1), (3), (5)) AS t(v)")
        .toArrow()
    )
    assert table.column("a").to_pylist() == [3.0]
    assert _sig(table) == [("a", "double", True)]


def test_avg_grouped_small_value_and_type() -> None:
    """Small grouped avg answers per key with NULL-only keys NULL."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_GROUPED_SMALL).toArrow()
    assert table.column("k").to_pylist() == ["a", "b", "c"]
    assert table.column("a").to_pylist() == [2.0, 4.0, None]
    assert _sig(table) == [("k", "string", True), ("a", "double", True)]


def test_avg_grouped_small_dataframe_door() -> None:
    """The DataFrame groupBy door answers the same grouped avgs."""
    """pins: perf-agg-avg-1/C-003."""
    spark = _session()
    frame = spark.sql("SELECT k, v FROM (VALUES ('a', 1.0), ('a', 3.0), ('b', 4.0)) AS t(k, v)")
    table = frame.groupBy("k").agg(F.avg("v").alias("a")).orderBy("k").toArrow()
    assert table.column("a").to_pylist() == [2.0, 4.0]


def test_avg_global_ansi_door() -> None:
    """The native ANSI door answers the same global avg."""
    """pins: perf-agg-avg-1/C-003."""
    table = repark.sql("SELECT avg(v) AS a FROM (VALUES (1.0), (3.0)) AS t(v)").to_arrow()
    assert table.column("a").to_pylist() == [2.0]
    assert str(table.schema.field("a").type) == "double"


def test_avg_decimal128_grouped_type_and_values() -> None:
    """Decimal(10, 2) grouped avg answers decimal(14, 6) with exact values."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_DECIMAL_GROUPED).toArrow()
    assert _sig(table) == [
        ("k", "int32", True),
        ("a", "decimal128(14, 6)", True),
    ]
    assert table.column("a").to_pylist() == [Decimal("1.650000"), Decimal("3.300000")]


def test_avg_decimal128_global_type_and_value() -> None:
    """Decimal(10, 2) global avg answers decimal(14, 6) 1.650000."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_DECIMAL_GLOBAL).toArrow()
    assert _sig(table) == [("a", "decimal128(14, 6)", True)]
    assert table.column("a").to_pylist() == [Decimal("1.650000")]


@pytest.mark.parametrize(
    ("precision", "expected"),
    [(5, "decimal128(9, 6)"), (10, "decimal128(14, 6)")],
)
def test_avg_decimal_casts_share_values(precision: int, expected: str) -> None:
    """Narrow decimal casts average to the same values at Spark's widened scale."""
    """pins: perf-agg-avg-1/C-003."""
    spark = _session()
    narrow = spark.sql(
        f"SELECT avg(v) AS a FROM (VALUES (CAST('1.10' AS DECIMAL({precision}, 2))), "
        f"(CAST('2.20' AS DECIMAL({precision}, 2)))) AS t(v)"
    ).toArrow()
    wide = spark.sql(SQL_DECIMAL_GLOBAL).toArrow()
    assert narrow.column("a").to_pylist() == wide.column("a").to_pylist()
    assert str(narrow.schema.field("a").type) == expected


def test_try_avg_overflow_is_null() -> None:
    """Decimal try_avg overflow answers NULL at decimal(38, 4)."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_TRY_AVG_OVERFLOW).toArrow()
    assert table.column("a").to_pylist() == [None]
    assert _sig(table) == [("a", "decimal128(38, 4)", True)]


def test_avg_overflow_raises() -> None:
    """Decimal plain-avg overflow raises instead of answering NULL."""
    """pins: perf-agg-avg-1/C-003."""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException, match="Arithmetic Overflow"):
        _session().sql(SQL_AVG_OVERFLOW).toArrow()


def test_avg_empty_input_is_null() -> None:
    """Global avg over no rows answers one NULL double row."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_EMPTY_GLOBAL).toArrow()
    assert table.column("a").to_pylist() == [None]
    assert _sig(table) == [("a", "double", True)]


def test_avg_all_null_group_is_null() -> None:
    """An all-NULL group averages NULL while its sibling answers."""
    """pins: perf-agg-avg-1/C-003."""
    table = _session().sql(SQL_ALL_NULL_GROUP).toArrow()
    assert table.column("a").to_pylist() == [None, 1.0]
    assert _sig(table) == [("k", "string", True), ("a", "double", True)]


def test_avg_single_distinct_answers() -> None:
    """Single avg(DISTINCT) answers through the optimizer's dedup rewrite."""
    """pins: perf-agg-avg-1/C-001."""
    spark = _session()
    decimal = spark.sql(SQL_AVG_DISTINCT).toArrow()
    assert decimal.column("a").to_pylist() == [Decimal("1.50000")]
    assert _sig(decimal) == [("a", "decimal128(6, 5)", True)]
    ints = spark.sql(SQL_AVG_DISTINCT_INT).toArrow()
    assert ints.column("a").to_pylist() == [1.5]
    assert _sig(ints) == [("a", "double", True)]


def test_avg_multi_distinct_refuses() -> None:
    """Multi-column distinct avg keeps its loud not-implemented refusal."""
    """pins: perf-agg-avg-1/C-001."""
    from repark.errors import UnsupportedOperationException

    with pytest.raises(UnsupportedOperationException, match="DistinctAvgAccumulator"):
        _session().sql(SQL_MULTI_DISTINCT).toArrow()


def test_avg_grouped_multi_distinct_refuses_bare() -> None:
    """Grouped multi-distinct refuses as a bare PySparkException."""
    """pins: perf-agg-avg-1/C-001."""
    from repark.errors import PySparkException

    with pytest.raises(PySparkException, match="DistinctAvgAccumulator") as caught:
        _session().sql(SQL_GROUPED_MULTI_DISTINCT).toArrow()
    assert type(caught.value) is PySparkException


def test_avg_grouped_float_drift_within_spark() -> None:
    """Grouped 1e16-plus-ones avg pins repark's bits within 1e-12 of Spark's."""
    """pins: perf-agg-avg-1/C-003; types-1/C-001 (VALUES k is int32 on Spark)."""
    table = _session().sql(SQL_FLOAT_DRIFT_GROUPED).toArrow()
    assert table.column("a").to_pylist() == [153846153846153.84]
    assert _sig(table) == [("k", "int32", True), ("a", "double", True)]
    assert table.column("a").to_pylist() == pytest.approx([SPARK_FLOAT_DRIFT_GROUPED], rel=1e-12)


def test_avg_decimal_sumwrap_records_divergence() -> None:
    """Sum-wrap fixtures answer the wrapped quotient; Spark NULLs and raises."""
    """pins: perf-agg-avg-1/C-003; types-1/C-001 (VALUES k is int32 on Spark)."""
    from repark.errors import PySparkException

    spark = _session()
    grouped_try = spark.sql(SQL_DECIMAL_SUMWRAP_GROUPED_TRY).toArrow()
    assert grouped_try.column("a").to_pylist() == [Decimal("0.0000")]
    assert _sig(grouped_try) == [("k", "int32", True), ("a", "decimal128(38, 4)", True)]
    grouped = spark.sql(SQL_DECIMAL_SUMWRAP_GROUPED).toArrow()
    assert grouped.column("a").to_pylist() == [Decimal("0.0000")]
    native_grouped = repark.sql(SQL_DECIMAL_SUMWRAP_GROUPED).to_arrow()
    assert native_grouped.column("a").to_pylist() == [Decimal("0.0000")]
    native_global = repark.sql(SQL_DECIMAL_SUMWRAP_GLOBAL).to_arrow()
    assert native_global.column("a").to_pylist() == [Decimal("0.0000")]
    assert _sig(native_global) == [("a", "decimal128(38, 4)", True)]
    nonzero_try = spark.sql(SQL_DECIMAL_SUMWRAP_NONZERO_GROUPED_TRY).toArrow()
    assert nonzero_try.column("a").to_pylist() == [Decimal("100000.0000")]
    assert _sig(nonzero_try) == [("k", "int32", True), ("a", "decimal128(38, 4)", True)]
    nonzero = spark.sql(SQL_DECIMAL_SUMWRAP_NONZERO_GROUPED).toArrow()
    assert nonzero.column("a").to_pylist() == [Decimal("100000.0000")]
    native_nonzero = repark.sql(SQL_DECIMAL_SUMWRAP_NONZERO_GROUPED).to_arrow()
    assert native_nonzero.column("a").to_pylist() == [Decimal("100000.0000")]
    frame = spark.sql(SQL_DECIMAL_SUMWRAP_NONZERO_FRAME)
    frame_try = frame.groupBy("k").agg(F.try_avg("v").alias("a")).toArrow()
    assert frame_try.column("a").to_pylist() == [Decimal("100000.0000")]
    frame_avg = frame.groupBy("k").agg(F.avg("v").alias("a")).toArrow()
    assert frame_avg.column("a").to_pylist() == [Decimal("100000.0000")]
    window_try = spark.sql(SQL_DECIMAL_SUMWRAP_WINDOW_TRY).toArrow()
    assert window_try.column("a").to_pylist() == [None, None, None, Decimal("0.0000")]
    assert _sig(window_try) == [("id", "int32", True), ("a", "decimal128(38, 4)", True)]
    with pytest.raises(PySparkException, match="Arithmetic Overflow"):
        spark.sql(SQL_DECIMAL_SUMWRAP_WINDOW).toArrow()


def test_avg_grouped_null_refuses_naming_groups_accumulator() -> None:
    """Grouped avg over NULL refuses naming the groups accumulator pair."""
    """pins: perf-agg-avg-1/C-001."""
    from repark.errors import UnsupportedOperationException

    with pytest.raises(
        UnsupportedOperationException, match=r"AvgGroupsAccumulator for \(Null --> Float64\)"
    ):
        _session().sql(SQL_GROUPED_AVG_NULL).toArrow()


def test_window_frame_avg_control() -> None:
    """Sliding-frame avg still answers through the retract path."""
    """pins: perf-agg-avg-1/C-002."""
    table = _session().sql(SQL_WINDOW_SLIDING).toArrow()
    assert table.column("a").to_pylist() == [1.0, 1.5, 2.0, 4.0]
    assert _sig(table) == [("id", "int32", True), ("a", "double", True)]


def test_many_groups_answers_match_pinned_checksum(tmp_path: Path) -> None:
    """2e5 grouped avgs match the engine-computed checksum and head rows."""
    """pins: perf-agg-avg-1/C-003, C-005."""
    spark = _session()
    fixture = tmp_path / "many_groups.parquet"
    _write_many_groups(fixture)
    _many_groups_view(spark, fixture)
    checksum = spark.sql(SQL_MANY_CHECKSUM).toArrow()
    assert checksum.column("groups").to_pylist() == [MANY_GROUPS]
    assert checksum.column("checksum").to_pylist() == pytest.approx([3150001.7499999637], rel=1e-9)
    sample = spark.sql(SQL_MANY_SAMPLE).toArrow()
    assert sample.column("k").to_pylist() == [0, 1, 2]
    assert sample.column("a").to_pylist() == [17.5, 14.833333333333334, 19.27777777777778]


def test_many_groups_avg_costs_like_sum(tmp_path: Path) -> None:
    """Grouped avg costs no more than RATIO_BOUND times grouped sum."""
    """pins: perf-agg-avg-1/C-001, C-004."""
    spark = (
        SparkSession.builder.appName("pytest-perf-agg-avg-1-probe")
        .config("repark.target.partitions", 1)
        .getOrCreate()
    )
    fixture = tmp_path / "many_groups.parquet"
    _write_many_groups(fixture)
    _many_groups_view(spark, fixture)
    avg_ms = _median_ms(spark, SQL_MANY_AVG)
    sum_ms = _median_ms(spark, SQL_MANY_SUM)
    assert avg_ms <= RATIO_BOUND * sum_ms, f"avg {avg_ms:.1f} ms vs sum {sum_ms:.1f} ms"


def test_live_grouped_avg_matches_spark(spark_engine: live_parity.Engine) -> None:
    """Live small grouped avgs equal PySpark value and type."""
    """pins: perf-agg-avg-1/C-003."""
    spark = _session()
    for sql in (
        SQL_GROUPED_SMALL_A,
        SQL_DECIMAL_GROUPED_A,
        SQL_ALL_NULL_GROUP_A,
        SQL_WINDOW_SLIDING_A,
    ):
        mine = spark.sql(sql).toArrow()
        theirs = spark_engine.arrow_of(spark_engine.session.sql(sql))
        assert_frames_equal(mine, theirs)


def test_live_many_groups_avg_matches_spark(
    spark_engine: live_parity.Engine, tmp_path: Path
) -> None:
    """Live 2e5 grouped avgs equal PySpark value and type."""
    """pins: perf-agg-avg-1/C-003."""
    spark = _session()
    fixture = tmp_path / "many_groups.parquet"
    _write_many_groups(fixture)
    _many_groups_view(spark, fixture)
    spark_engine.session.read.parquet(str(fixture)).createOrReplaceTempView("many_groups")
    mine = spark.sql(SQL_MANY_AVG_A).toArrow()
    theirs = spark_engine.arrow_of(spark_engine.session.sql(SQL_MANY_AVG_A))
    assert_frames_equal(mine, theirs)


def test_live_grouped_float_drift_within_spark(spark_engine: live_parity.Engine) -> None:
    """Live grouped 1e16-plus-ones avg stays within 1e-12 of PySpark."""
    """pins: perf-agg-avg-1/C-003."""
    mine = _session().sql(SQL_FLOAT_DRIFT_GROUPED_A).toArrow()
    theirs = spark_engine.arrow_of(spark_engine.session.sql(SQL_FLOAT_DRIFT_GROUPED_A))
    assert mine.column("a").to_pylist() == pytest.approx(theirs.column("a").to_pylist(), rel=1e-12)


def test_live_try_avg_overflow_matches_spark(spark_engine: live_parity.Engine) -> None:
    """Live try_avg overflow is NULL on both engines with the same type."""
    """pins: perf-agg-avg-1/C-003."""
    mine = _session().sql(SQL_TRY_AVG_OVERFLOW).toArrow()
    theirs = spark_engine.arrow_of(spark_engine.session.sql(SQL_TRY_AVG_OVERFLOW))
    assert_frames_equal(mine, theirs)


def test_live_avg_overflow_raises_on_both(spark_engine: live_parity.Engine) -> None:
    """Live plain-avg overflow raises on both engines."""
    """pins: perf-agg-avg-1/C-003."""
    from pyspark.errors import ArithmeticException as SparkArithmeticException

    from repark.errors import PySparkException

    with pytest.raises(PySparkException, match="Arithmetic Overflow"):
        _session().sql(SQL_AVG_OVERFLOW).toArrow()
    with pytest.raises(SparkArithmeticException, match="NUMERIC_VALUE_OUT_OF_RANGE"):
        spark_engine.session.sql(SQL_AVG_OVERFLOW).toArrow()


def test_live_avg_distinct_single_answers(spark_engine: live_parity.Engine) -> None:
    """Live single avg(DISTINCT) answers Spark-equal on both input shapes."""
    """pins: perf-agg-avg-1/C-001."""
    spark = _session()
    for sql in (SQL_AVG_DISTINCT, SQL_AVG_DISTINCT_INT):
        mine = spark.sql(sql).toArrow()
        theirs = spark_engine.arrow_of(spark_engine.session.sql(sql))
        assert_frames_equal(mine, theirs)


def test_live_avg_multi_distinct_refusal_while_spark_answers(
    spark_engine: live_parity.Engine,
) -> None:
    """Live multi-distinct refuses here while Spark answers both aggregates."""
    """pins: perf-agg-avg-1/C-001."""
    from repark.errors import UnsupportedOperationException

    with pytest.raises(UnsupportedOperationException, match="DistinctAvgAccumulator"):
        _session().sql(SQL_MULTI_DISTINCT).toArrow()
    theirs = spark_engine.arrow_of(spark_engine.session.sql(SQL_MULTI_DISTINCT))
    assert theirs.column("x").to_pylist() == [1.5]
    assert theirs.column("y").to_pylist() == [10]
