"""PERF-APPROXPCT-1 sketch pins. pins: perf-approxpct-1/C-002, C-003, C-004, C-005, C-006"""

from __future__ import annotations

from decimal import Decimal
from typing import Any, Final

import _live_parity as lp
import pyarrow as pa
import pytest

SEQ200_DOUBLE_SQL: Final[str] = " UNION ALL ".join(
    f"SELECT CAST({index} AS DOUBLE) AS x" for index in range(1, 201)
)

SEQ200_INT_SQL: Final[str] = " UNION ALL ".join(f"SELECT {index} AS x" for index in range(1, 201))

NULLS_DOUBLE_SQL: Final[str] = (
    "SELECT * FROM (VALUES (CAST(1.0 AS DOUBLE)), (CAST(2.0 AS DOUBLE)), "
    "(CAST(3.0 AS DOUBLE)), (CAST(NULL AS DOUBLE)), (CAST(4.0 AS DOUBLE)), "
    "(CAST(6.0 AS DOUBLE))) t(x)"
)

GROUPED_SQL: Final[str] = (
    "SELECT * FROM (VALUES ('a', CAST(1 AS BIGINT)), ('a', CAST(2 AS BIGINT)), "
    "('a', CAST(3 AS BIGINT)), ('a', CAST(NULL AS BIGINT)), ('b', CAST(4 AS BIGINT)), "
    "('b', CAST(6 AS BIGINT))) t(k, v)"
)

DUPES_DOUBLE_SQL: Final[str] = " UNION ALL ".join(
    ["SELECT CAST(5.0 AS DOUBLE) AS x"] * 500
    + ["SELECT CAST(1.0 AS DOUBLE) AS x"] * 250
    + ["SELECT CAST(9.0 AS DOUBLE) AS x"] * 250
)

SKEW_DOUBLE_SQL: Final[str] = " UNION ALL ".join(
    [f"SELECT CAST({index} AS DOUBLE) AS x" for index in range(1, 201)]
    + ["SELECT CAST(1000000000 AS DOUBLE) AS x"]
)

SINGLE_DOUBLE_VIEW: Final[str] = "approxpct_1_single_double"
SINGLE_INT_VIEW: Final[str] = "approxpct_1_single_int"
SINGLE_DEC_VIEW: Final[str] = "approxpct_1_single_dec"
SINGLE_DUPES_VIEW: Final[str] = "approxpct_1_single_dupes"

FRAC_DOUBLE_SQL: Final[str] = (
    "SELECT * FROM (VALUES (CAST(0.1 AS DOUBLE)), (CAST(0.2 AS DOUBLE)), "
    "(CAST(0.3 AS DOUBLE)), (CAST(0.4 AS DOUBLE)), (CAST(0.9 AS DOUBLE))) t(x)"
)

DEC_FIVE_SQL: Final[str] = (
    "SELECT CAST(x AS DECIMAL(10, 2)) AS d FROM (VALUES (1.10), (2.20), (3.30), "
    "(4.40), (5.50)) t(x)"
)

DECBIG_SQL: Final[str] = " UNION ALL ".join(
    f"SELECT CAST({index}.25 AS DECIMAL(10, 2)) AS d" for index in range(1, 201)
)

NEG_DOUBLE_SQL: Final[str] = (
    "SELECT * FROM (VALUES (CAST(-5.0 AS DOUBLE)), (CAST(-1.0 AS DOUBLE)), "
    "(CAST(0.0 AS DOUBLE)), (CAST(0.0 AS DOUBLE)), (CAST(3.0 AS DOUBLE))) t(x)"
)

SAME_DOUBLE_SQL: Final[str] = " UNION ALL ".join(["SELECT CAST(7.0 AS DOUBLE) AS x"] * 1000)

FLOAT_SQL: Final[str] = (
    "SELECT * FROM (VALUES (CAST(1.5 AS FLOAT)), (CAST(2.5 AS FLOAT)), (CAST(3.5 AS FLOAT))) t(x)"
)

MATRIX: Final[tuple[tuple[str, str, str, Any], ...]] = (
    ("seq50_default", SEQ200_DOUBLE_SQL, "percentile_approx(x, 0.5)", 100.0),
    ("seq50_acc2", SEQ200_DOUBLE_SQL, "percentile_approx(x, 0.5, 2)", 1.0),
    (
        "seq_array_default",
        SEQ200_DOUBLE_SQL,
        "percentile_approx(x, array(0.0, 0.5, 1.0))",
        [1.0, 100.0, 200.0],
    ),
    (
        "seq_array_acc2",
        SEQ200_DOUBLE_SQL,
        "percentile_approx(x, array(0.0, 0.5, 1.0), 2)",
        [1.0, 1.0, 200.0],
    ),
    ("nulls_default", NULLS_DOUBLE_SQL, "percentile_approx(x, 0.5)", 3.0),
    ("nulls_acc2", NULLS_DOUBLE_SQL, "percentile_approx(x, 0.5, 2)", 1.0),
    ("dupes_default", DUPES_DOUBLE_SQL, "percentile_approx(x, 0.5)", 5.0),
    ("dupes_acc2", DUPES_DOUBLE_SQL, "percentile_approx(x, 0.5, 2)", 1.0),
    ("skew_default", SKEW_DOUBLE_SQL, "percentile_approx(x, 0.5)", 101.0),
    ("skew_acc2", SKEW_DOUBLE_SQL, "percentile_approx(x, 0.5, 2)", 1.0),
    ("skew99_acc2", SKEW_DOUBLE_SQL, "percentile_approx(x, 0.99, 2)", 1e9),
    ("frac_default", FRAC_DOUBLE_SQL, "percentile_approx(x, 0.5)", 0.3),
    ("frac_acc2", FRAC_DOUBLE_SQL, "percentile_approx(x, 0.5, 2)", 0.1),
    ("dec_default", DEC_FIVE_SQL, "percentile_approx(d, 0.5)", Decimal("3.30")),
    ("dec_acc2", DEC_FIVE_SQL, "percentile_approx(d, 0.5, 2)", Decimal("1.10")),
    ("decbig_default", DECBIG_SQL, "percentile_approx(d, 0.5)", Decimal("100.25")),
    ("decbig_acc2", DECBIG_SQL, "percentile_approx(d, 0.5, 2)", Decimal("1.25")),
    ("neg_default", NEG_DOUBLE_SQL, "percentile_approx(x, 0.5)", 0.0),
    ("neg_acc2", NEG_DOUBLE_SQL, "percentile_approx(x, 0.5, 2)", -5.0),
    ("same_default", SAME_DOUBLE_SQL, "percentile_approx(x, 0.5)", 7.0),
    ("same_acc2", SAME_DOUBLE_SQL, "percentile_approx(x, 0.5, 2)", 7.0),
    ("float_default", FLOAT_SQL, "percentile_approx(x, 0.5)", 2.5),
    ("float_acc2", FLOAT_SQL, "percentile_approx(x, 0.5, 2)", 1.5),
    ("alias_acc2", SEQ200_DOUBLE_SQL, "approx_percentile(x, 0.5, 2)", 1.0),
    (
        "null_elem",
        SEQ200_DOUBLE_SQL,
        "percentile_approx(x, array(0.5, CAST(NULL AS DOUBLE)))",
        [100.0, 1.0],
    ),
)

MATRIX_INT: Final[tuple[tuple[str, str, str, Any], ...]] = (
    ("iseq50_default", SEQ200_INT_SQL, "percentile_approx(x, 0.5)", 100),
    ("iseq50_acc2", SEQ200_INT_SQL, "percentile_approx(x, 0.5, 2)", 1),
    (
        "iseq_array_acc2",
        SEQ200_INT_SQL,
        "percentile_approx(x, array(0.25, 0.5, 0.75), 2)",
        [1, 1, 200],
    ),
)

SINGLE_MATRIX: Final[tuple[tuple[str, str, str, Any], ...]] = (
    ("single50_acc100", SINGLE_DOUBLE_VIEW, "percentile_approx(x, 0.5, 100)", 99.0),
    ("single50_acc10", SINGLE_DOUBLE_VIEW, "percentile_approx(x, 0.5, 10)", 90.0),
    ("single25_acc10", SINGLE_DOUBLE_VIEW, "percentile_approx(x, 0.25, 10)", 43.0),
    ("single75_acc10", SINGLE_DOUBLE_VIEW, "percentile_approx(x, 0.75, 10)", 135.0),
    ("single10_acc10", SINGLE_DOUBLE_VIEW, "percentile_approx(x, 0.1, 10)", 1.0),
    ("single90_acc10", SINGLE_DOUBLE_VIEW, "percentile_approx(x, 0.9, 10)", 200.0),
    (
        "single_array_acc10",
        SINGLE_DOUBLE_VIEW,
        "percentile_approx(x, array(0.25, 0.5, 0.75), 10)",
        [43.0, 90.0, 135.0],
    ),
    ("single_int_acc100", SINGLE_INT_VIEW, "percentile_approx(x, 0.5, 100)", 99),
    ("single_int_acc10", SINGLE_INT_VIEW, "percentile_approx(x, 0.5, 10)", 90),
    ("single_dec_acc100", SINGLE_DEC_VIEW, "percentile_approx(d, 0.5, 100)", Decimal("99.25")),
    ("single_dec_acc10", SINGLE_DEC_VIEW, "percentile_approx(d, 0.5, 10)", Decimal("90.25")),
    ("single_dupes_acc10", SINGLE_DUPES_VIEW, "percentile_approx(x, 0.5, 10)", 5.0),
    (
        "single_dupes_array_acc10",
        SINGLE_DUPES_VIEW,
        "percentile_approx(x, array(0.25, 0.5, 0.75), 10)",
        [1.0, 5.0, 5.0],
    ),
)

GROUPED_MATRIX: Final[tuple[tuple[str, str, Any], ...]] = (
    ("grouped_default", "percentile_approx(v, 0.5)", (("a", 2), ("b", 4))),
    ("grouped_acc10", "percentile_approx(v, 0.5, 10)", (("a", 2), ("b", 4))),
    ("grouped_acc2", "percentile_approx(v, 0.5, 2)", (("a", 1), ("b", 4))),
    (
        "grouped_array_acc2",
        "percentile_approx(v, array(0.0, 0.5, 1.0), 2)",
        (("a", [1, 1, 3]), ("b", [4, 4, 6])),
    ),
)

SKETCH_INDICES: Final[tuple[int, ...]] = (0, 49, 99, 149, 199)

SKETCH_GOLDEN: Final[dict[str, tuple[Any, ...]]] = {
    "sketch_default": (1.0, 25.0, 50.0, 100.0, 150.0),
    "sketch_acc10": (1.0, 23.0, 48.0, 98.0, 148.0),
    "sketch_acc2": (1.0, 1.0, 1.0, 51.0, 101.0),
}

WALL_ROWS: Final[int] = 1_000_000
WALL_BAR_SECONDS: Final[float] = 120.0


@pytest.fixture
def repark_engine() -> Any:
    """A fresh repark engine per test (the suite clears the active session between tests)."""
    return lp.build_repark_engine()


def _scalar(engine: Any, query: str) -> Any:
    """Run a single-cell SQL query on an engine and return the value."""
    return engine.session.sql(query).collect()[0][0]


def _pairs(engine: Any, query: str) -> list[tuple[Any, Any]]:
    """Run a grouped two-column SQL query on an engine and return sorted rows."""
    rows = engine.session.sql(query).collect()
    return sorted((row[0], row[1]) for row in rows)


def _column(engine: Any, query: str) -> tuple[Any, ...]:
    """Run a single-column SQL query on an engine and return every value."""
    return tuple(row[0] for row in engine.session.sql(query).collect())


def _single_views(session: Any) -> None:
    """Register the one-partition scan fixtures both engines read identically."""
    session.range(1, 201).repartition(1).selectExpr(
        "CAST(id AS DOUBLE) AS x"
    ).createOrReplaceTempView(SINGLE_DOUBLE_VIEW)
    session.range(1, 201).repartition(1).selectExpr("id AS x").createOrReplaceTempView(
        SINGLE_INT_VIEW
    )
    session.range(1, 201).repartition(1).selectExpr(
        "CAST(CAST(id AS DECIMAL(10, 2)) + CAST(0.25 AS DECIMAL(10, 2)) AS DECIMAL(10, 2)) AS d"
    ).createOrReplaceTempView(SINGLE_DEC_VIEW)
    session.range(1, 1001).repartition(1).selectExpr(
        "CASE WHEN id <= 500 THEN CAST(5.0 AS DOUBLE) "
        "WHEN id <= 750 THEN CAST(1.0 AS DOUBLE) "
        "ELSE CAST(9.0 AS DOUBLE) END AS x"
    ).createOrReplaceTempView(SINGLE_DUPES_VIEW)


@pytest.mark.parametrize(
    ("label", "fixture", "call", "expected"), MATRIX, ids=[cell[0] for cell in MATRIX]
)
def test_accuracy_matrix_matches_spark(
    repark_engine: Any, label: str, fixture: str, call: str, expected: Any
) -> None:
    """C-002: every matrix cell answers the recorded live-Spark 4.1.2 value."""
    value = _scalar(repark_engine, f"SELECT {call} AS p FROM ({fixture})")
    assert value == expected, f"{label}: {value!r} != {expected!r}"


@pytest.mark.parametrize(
    ("label", "fixture", "call", "expected"), MATRIX_INT, ids=[cell[0] for cell in MATRIX_INT]
)
def test_integer_matrix_matches_spark(
    repark_engine: Any, label: str, fixture: str, call: str, expected: Any
) -> None:
    """C-002: the integer matrix answers the recorded live-Spark 4.1.2 values."""
    value = _scalar(repark_engine, f"SELECT {call} AS p FROM ({fixture})")
    assert value == expected, f"{label}: {value!r} != {expected!r}"


@pytest.mark.parametrize(
    ("label", "call", "expected"), GROUPED_MATRIX, ids=[cell[0] for cell in GROUPED_MATRIX]
)
def test_grouped_matrix_matches_spark(
    repark_engine: Any, label: str, call: str, expected: Any
) -> None:
    """C-002: grouped cells answer the recorded live-Spark 4.1.2 rows."""
    rows = _pairs(repark_engine, f"SELECT k, {call} AS p FROM ({GROUPED_SQL}) GROUP BY k")
    assert rows == sorted(expected), f"{label}: {rows!r} != {sorted(expected)!r}"


@pytest.mark.parametrize(
    ("label", "view", "call", "expected"), SINGLE_MATRIX, ids=[cell[0] for cell in SINGLE_MATRIX]
)
def test_single_partition_scan_matches_spark(
    repark_engine: Any, label: str, view: str, call: str, expected: Any
) -> None:
    """C-002: one-partition scan cells answer the recorded live-Spark 4.1.2 values."""
    _single_views(repark_engine.session)
    value = _scalar(repark_engine, f"SELECT {call} AS p FROM {view}")
    assert value == expected, f"{label}: {value!r} != {expected!r}"


def test_dataframe_door_honours_accuracy(repark_engine: Any) -> None:
    """C-002: F.percentile_approx threads accuracy on scalar, array and grouped forms."""
    from repark.spark import functions as spark_functions

    frame = repark_engine.session.range(1, 201)
    select = spark_functions.percentile_approx
    default = frame.select(select("id", 0.5).alias("p")).collect()[0][0]
    assert default == 100
    coarse = frame.select(select("id", 0.5, accuracy=2).alias("p")).collect()[0][0]
    assert coarse == 1
    arrayed = frame.select(select("id", [0.0, 0.5, 1.0], accuracy=2).alias("p")).collect()[0][0]
    assert list(arrayed) == [1, 1, 200]
    aliased = frame.select(spark_functions.approx_percentile("id", 0.5, accuracy=2).alias("p"))
    assert aliased.collect()[0][0] == 1
    grouped = repark_engine.session.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("b", 4), ("b", 6)], ["k", "v"]
    )
    cells = grouped.groupBy("k").agg(select("v", 0.5, accuracy=2).alias("p")).collect()
    assert sorted((row["k"], row["p"]) for row in cells) == [("a", 1), ("b", 4)]


def test_sliding_frame_honours_accuracy_per_frame(repark_engine: Any) -> None:
    """C-003: the 100-row frame answers the recorded Spark column at default, 10 and 2."""
    frame = repark_engine.session.range(1, 201).selectExpr("CAST(id AS DOUBLE) AS x", "id AS k")
    frame.createOrReplaceTempView("approxpct_1_sketch")
    clause = "ORDER BY k ROWS BETWEEN 99 PRECEDING AND CURRENT ROW"
    for call, label in (
        ("percentile_approx(x, 0.5)", "sketch_default"),
        ("percentile_approx(x, 0.5, 10)", "sketch_acc10"),
        ("percentile_approx(x, 0.5, 2)", "sketch_acc2"),
    ):
        column = _column(
            repark_engine, f"SELECT {call} OVER ({clause}) AS w FROM approxpct_1_sketch ORDER BY k"
        )
        sampled = tuple(column[index] for index in SKETCH_INDICES)
        assert sampled == SKETCH_GOLDEN[label], label


def test_empty_and_all_null_answer_null(repark_engine: Any) -> None:
    """C-002: empty input, all-NULL input and an empty percentage array answer NULL."""
    empty = _scalar(
        repark_engine, "SELECT percentile_approx(x, 0.5) AS p FROM (SELECT 1.0 AS x WHERE 1 = 0)"
    )
    assert empty is None
    all_null = _scalar(
        repark_engine,
        "SELECT percentile_approx(x, 0.5) AS p FROM (SELECT CAST(NULL AS DOUBLE) AS x "
        "FROM (VALUES (1), (2)) t(y))",
    )
    assert all_null is None
    no_percentages = _scalar(
        repark_engine, f"SELECT percentile_approx(x, array()) AS p FROM ({SEQ200_DOUBLE_SQL})"
    )
    assert no_percentages is None


@pytest.mark.parametrize("accuracy", ["0", "-3", "NULL", "2147483648"])
def test_invalid_accuracy_raises(repark_engine: Any, accuracy: str) -> None:
    """C-002: an out-of-range or NULL accuracy fails loudly on the SQL door."""
    with pytest.raises(Exception, match="accuracy"):
        query = f"SELECT percentile_approx(x, 0.5, {accuracy}) AS p FROM ({SEQ200_DOUBLE_SQL})"
        _scalar(repark_engine, query)


@pytest.mark.parametrize("percentage", ["1.5", "-0.5", "NULL"])
def test_out_of_range_percentage_raises(repark_engine: Any, percentage: str) -> None:
    """C-002: an out-of-range or NULL percentage fails loudly on the SQL door."""
    with pytest.raises(Exception, match="percentage"):
        query = f"SELECT percentile_approx(x, {percentage}) AS p FROM ({SEQ200_DOUBLE_SQL})"
        _scalar(repark_engine, query)


def test_answer_types_follow_the_column(repark_engine: Any) -> None:
    """C-002: the answer carries the column Arrow type on the Arrow path."""
    session = repark_engine.session
    double_table = session.sql(f"SELECT percentile_approx(x, 0.5) AS p FROM ({SEQ200_DOUBLE_SQL})")
    assert pa.types.is_float64(double_table.to_arrow().schema.field("p").type)
    int_table = session.sql(f"SELECT percentile_approx(x, 0.5) AS p FROM ({SEQ200_INT_SQL})")
    assert pa.types.is_integer(int_table.to_arrow().schema.field("p").type)
    dec_table = session.sql(f"SELECT percentile_approx(d, 0.5) AS p FROM ({DECBIG_SQL})")
    dec_type = dec_table.to_arrow().schema.field("p").type
    assert pa.types.is_decimal128(dec_type)
    assert (dec_type.precision, dec_type.scale) == (10, 2)
    float_table = session.sql(f"SELECT percentile_approx(x, 0.5) AS p FROM ({FLOAT_SQL})")
    assert pa.types.is_float32(float_table.to_arrow().schema.field("p").type)


def test_million_row_wall_stays_within_bar(repark_engine: Any) -> None:
    """C-004: the sketch answers 1e6 rows inside the bar (release only; debug skips)."""
    import time

    import repark._native as native

    if native.__debug_assertions__:
        pytest.skip("wall pins run on release modules only")
    frame = repark_engine.session.range(1, WALL_ROWS + 1)
    started = time.monotonic()
    value = frame.selectExpr("percentile_approx(id, 0.5) AS p").collect()[0][0]
    elapsed = time.monotonic() - started
    assert value == 500_000
    assert elapsed < WALL_BAR_SECONDS, f"{elapsed:.1f}s over the {WALL_BAR_SECONDS:.0f}s bar"


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_reproduces_the_matrix(spark_engine: Any) -> None:
    """C-005: live PySpark 4.1.2 still answers every pinned matrix cell."""
    for label, fixture, call, expected in (*MATRIX, *MATRIX_INT):
        value = _scalar(spark_engine, f"SELECT {call} AS p FROM ({fixture})")
        assert value == expected, f"live {label}: {value!r} != {expected!r}"
    for label, call, expected in GROUPED_MATRIX:
        rows = _pairs(spark_engine, f"SELECT k, {call} AS p FROM ({GROUPED_SQL}) GROUP BY k")
        assert rows == sorted(expected), f"live {label}: {rows!r} != {sorted(expected)!r}"


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_reproduces_the_single_cells(spark_engine: Any) -> None:
    """C-005: live PySpark 4.1.2 still answers every pinned one-partition scan cell."""
    _single_views(spark_engine.session)
    for label, view, call, expected in SINGLE_MATRIX:
        value = _scalar(spark_engine, f"SELECT {call} AS p FROM {view}")
        assert value == expected, f"live {label}: {value!r} != {expected!r}"


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_reproduces_the_frame_column(spark_engine: Any, repark_engine: Any) -> None:
    """C-005: live PySpark 4.1.2 still answers the three pinned frame columns."""
    frame = spark_engine.session.range(1, 201).selectExpr("CAST(id AS DOUBLE) AS x", "id AS k")
    frame.createOrReplaceTempView("approxpct_1_sketch_oracle")
    mine = repark_engine.session.range(1, 201).selectExpr("CAST(id AS DOUBLE) AS x", "id AS k")
    mine.createOrReplaceTempView("approxpct_1_sketch_mine")
    clause = "ORDER BY k ROWS BETWEEN 99 PRECEDING AND CURRENT ROW"
    for call, label in (
        ("percentile_approx(x, 0.5)", "sketch_default"),
        ("percentile_approx(x, 0.5, 10)", "sketch_acc10"),
        ("percentile_approx(x, 0.5, 2)", "sketch_acc2"),
    ):
        oracle = _column(
            spark_engine,
            f"SELECT {call} OVER ({clause}) AS w FROM approxpct_1_sketch_oracle ORDER BY k",
        )
        column = _column(
            repark_engine,
            f"SELECT {call} OVER ({clause}) AS w FROM approxpct_1_sketch_mine ORDER BY k",
        )
        assert column == oracle, f"live full {label}"
        sampled = tuple(column[index] for index in SKETCH_INDICES)
        assert sampled == SKETCH_GOLDEN[label], f"live {label}"


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_spark_reproduces_the_edge_cells(spark_engine: Any) -> None:
    """C-005: live PySpark 4.1.2 still answers the NULL-element, empty and NULL cells."""
    null_elem = _scalar(
        spark_engine,
        "SELECT percentile_approx(x, array(0.5, CAST(NULL AS DOUBLE))) AS p "
        f"FROM ({SEQ200_DOUBLE_SQL})",
    )
    assert list(null_elem) == [100.0, 1.0]
    empty_array = _scalar(
        spark_engine, f"SELECT percentile_approx(x, array()) AS p FROM ({SEQ200_DOUBLE_SQL})"
    )
    assert empty_array is None
    empty = _scalar(
        spark_engine, "SELECT percentile_approx(x, 0.5) AS p FROM (SELECT 1.0 AS x WHERE 1 = 0)"
    )
    assert empty is None
