"""Adversarial Spark-parity corpus for the raw ``spark.sql()`` passthrough (AR-WG-SQL, C-AR-005).

The production audit (``task/audit-2026-07-10.md`` findings #1, #4, #5, #6, #15) executed the
built engine and proved the passthrough leaked raw DataFusion semantics: ``5/2`` truncated to
``2``, ORDER BY put NULLs on the wrong end (changing *rows* under LIMIT), ``arr[0]`` was NULL
(1-based), ``substr`` broke on position 0 / negative positions, and ``element_at`` failed on
every array. Each case here pins the divergence class on the exact input that was live-proven
wrong, straight through ``spark.sql()`` — no DataFrame-API mediation.

Goldens are hand-computed from Spark's documented semantics (Spark's default **non-ANSI** mode:
divide/modulo-by-zero → NULL, invalid array index → NULL). Real pyspark goldens are still not
recordable in this environment (Connect-only pyspark + Java 11 — tracked in ``task/todo.md``).

Known divergence NOT yet pinned here (parity backlog): a runtime ``CAST`` of a non-numeric or
out-of-range string to a numeric type — e.g. ``CAST('abc' AS INT)`` — **raises** in repark today
(the WG-3 error-taxonomy pins in ``test_errors.py`` codify that), while Spark non-ANSI returns
**NULL**. It is a backlog entry (``docs/spark-sql-iceberg-parity.md`` §7 "Known Spark-parity
divergences"; audit F-BR-6), not a corpus row, because the corpus is green-only and repark does
not match Spark on this class yet — a future CAST-parity unit updates those pins to assert NULL.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark_parity import assert_frames_equal


@pytest.fixture
def spark() -> ReparkSession:
    """A default session (PySpark ``SparkSession.builder.getOrCreate()``)."""
    return ReparkSession.builder.appName("pytest-sql-passthrough").getOrCreate()


# ==================================================================================================
# C-AR-001 — integer `/` is always-double division (the audit's S0: `SELECT 5/2` returned 2)
# ==================================================================================================


def test_integer_division_is_double(spark: ReparkSession) -> None:
    result = spark.sql("SELECT 5/2 AS a, 7/2 AS b, -7/2 AS c")
    golden = pa.table(
        {
            "a": pa.array([2.5], pa.float64()),
            "b": pa.array([3.5], pa.float64()),
            "c": pa.array([-3.5], pa.float64()),
        }
    )
    assert_frames_equal(result.to_arrow(), golden)


def test_division_and_modulo_by_zero_are_null(spark: ReparkSession) -> None:
    # Spark non-ANSI: ANY division/modulo by zero is NULL — DataFusion errors on int/0 and
    # yields inf on float/0.0, both live-proven divergences (#16).
    table = spark.sql(
        "SELECT 1/0 AS a, 1.0/0.0 AS b, 5 % 0 AS c, 5.0 % 0.0 AS d, "
        "CAST(1 AS DOUBLE)/CAST(0 AS DOUBLE) AS e"
    ).to_arrow()
    assert [column.to_pylist() for column in table.columns] == [[None]] * 5
    assert pa.types.is_float64(table.schema.field("a").type)  # int/int is DOUBLE even for NULL


def test_division_by_zero_in_a_column_divisor(spark: ReparkSession) -> None:
    # The divisor is a column, not a literal — the guard must be runtime, not const-folding.
    result = spark.sql("SELECT a / b AS d FROM (VALUES (1, 0), (9, 3)) AS t(a, b) ORDER BY a")
    golden = pa.table({"d": pa.array([None, 3.0], pa.float64())})
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)


def test_decimal_division_stays_decimal(spark: ReparkSession) -> None:
    # The int→double promotion must not touch decimals (Spark keeps decimal ÷ decimal in
    # decimal; the precision/scale rules are DataFusion's for now — flagged in the charter).
    table = spark.sql(
        "SELECT CAST(1.00 AS DECIMAL(10,2)) / CAST(3.00 AS DECIMAL(10,2)) AS d"
    ).to_arrow()
    assert pa.types.is_decimal(table.schema.field("d").type)


# ==================================================================================================
# C-AR-002 — ORDER BY default null placement (audit #4: inverted, changes rows under LIMIT)
# ==================================================================================================


def test_order_by_asc_defaults_nulls_first(spark: ReparkSession) -> None:
    result = spark.sql("SELECT v FROM (VALUES (2), (NULL), (1)) AS t(v) ORDER BY v")
    golden = pa.table({"v": pa.array([None, 1, 2], pa.int64())})
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)


def test_order_by_desc_defaults_nulls_last(spark: ReparkSession) -> None:
    result = spark.sql("SELECT v FROM (VALUES (2), (NULL), (1)) AS t(v) ORDER BY v DESC")
    golden = pa.table({"v": pa.array([2, 1, None], pa.int64())})
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)


def test_order_by_explicit_null_placement_is_honoured(spark: ReparkSession) -> None:
    result = spark.sql("SELECT v FROM (VALUES (2), (NULL), (1)) AS t(v) ORDER BY v ASC NULLS LAST")
    golden = pa.table({"v": pa.array([1, 2, None], pa.int64())})
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)


def test_order_by_limit_returns_spark_rows(spark: ReparkSession) -> None:
    # The data-changing case: under LIMIT the placement decides WHICH rows survive.
    result = spark.sql("SELECT v FROM (VALUES (2), (NULL), (1)) AS t(v) ORDER BY v LIMIT 2")
    golden = pa.table({"v": pa.array([None, 1], pa.int64())})
    assert_frames_equal(result.to_arrow(), golden, order_sensitive=True)


def test_window_order_by_defaults_nulls_first(spark: ReparkSession) -> None:
    # The default reaches OVER (ORDER BY …): the NULL row takes row_number 1 (Spark).
    table = spark.sql(
        "SELECT rn FROM (SELECT v, row_number() OVER (ORDER BY v) AS rn "
        "FROM (VALUES (2), (NULL), (1)) AS t(v)) WHERE v IS NULL"
    ).to_arrow()
    assert table.column("rn").to_pylist() == [1]


# ==================================================================================================
# C-AR-003 — `[]` is 0-based; `element_at` is 1-based and works on arrays (audit #5, #15)
# ==================================================================================================


def test_array_subscript_is_zero_based(spark: ReparkSession) -> None:
    # The audit's live proof: array(10,20,30)[0] was NULL and [1] was 10 (1-based DataFusion).
    table = spark.sql(
        "SELECT array(10,20,30)[0] AS a, array(10,20,30)[1] AS b, array(10,20,30)[2] AS c, "
        "array(10,20,30)[3] AS oob, array(10,20,30)[-1] AS neg"
    ).to_arrow()
    assert [column.to_pylist() for column in table.columns] == [[10], [20], [30], [None], [None]]


def test_element_at_array_is_one_based(spark: ReparkSession) -> None:
    # Previously every one of these failed coercion (element_at resolved to map_extract).
    table = spark.sql(
        "SELECT element_at(array(10,20,30), 1) AS first, "
        "element_at(array(10,20,30), -1) AS last, element_at(array(10,20,30), 4) AS oob"
    ).to_arrow()
    assert [column.to_pylist() for column in table.columns] == [[10], [30], [None]]


def test_element_at_zero_index_raises(spark: ReparkSession) -> None:
    with pytest.raises(RuntimeError, match="index 0"):
        spark.sql("SELECT element_at(array(10,20,30), 0)").to_arrow()


def test_element_at_map_returns_plain_value(spark: ReparkSession) -> None:
    table = spark.sql(
        "SELECT element_at(map(['a','b'], [1,2]), 'b') AS hit, "
        "element_at(map(['a','b'], [1,2]), 'z') AS miss"
    ).to_arrow()
    assert table.column("hit").to_pylist() == [2]
    assert table.column("miss").to_pylist() == [None]


# ==================================================================================================
# C-AR-004 — substr/substring position edge cases (audit #6)
# ==================================================================================================


def test_substr_spark_edge_positions(spark: ReparkSession) -> None:
    # The audit's exact cases first: pos 0 acts as 1 ('hel' not 'he'); negative pos counts from
    # the end ('ll' not ''); a window reaching before the start clips instead of emptying.
    table = spark.sql(
        "SELECT substr('hello', 0, 3) AS zero_pos, substring('hello', -3, 2) AS neg_pos, "
        "substring('hello', -7, 3) AS clipped, substr('hello', 2) AS to_end, "
        "substr('hello', 9, 3) AS past_end, substring('hello', 1, 0) AS len_zero, "
        "substr('hello', 1, -1) AS len_negative"
    ).to_arrow()
    expected = [["hel"], ["ll"], ["h"], ["ello"], [""], [""], [""]]
    assert [column.to_pylist() for column in table.columns] == expected


def test_substr_null_and_multibyte(spark: ReparkSession) -> None:
    table = spark.sql(
        "SELECT substr(CAST(NULL AS STRING), 1, 2) AS null_str, "
        "substring('héllo', 2, 3) AS multibyte"
    ).to_arrow()
    assert table.column("null_str").to_pylist() == [None]
    assert table.column("multibyte").to_pylist() == ["éll"]  # characters, not bytes


# ==================================================================================================
# Divergence corpus x entry points — every divergence class above, pinned through EVERY user
# entry point on the Arrow path (value AND type). Added 2026-07-13 after the F.expr regression:
# `F.expr("5/2")` handed off a pre-analysis Int64 label over Float64 buffers, so `collect()`
# returned 2.5's bit pattern as an int while the identical string through `spark.sql()` was
# correct (and `show()` looked right on both). One green entry point is not parity — a new user
# entry point that evaluates SQL expressions must join this matrix.
# ==================================================================================================

# (expression, expected value, expected Arrow type or None when the value alone is the pin)
DIVERGENCE_CORPUS: list[tuple[str, object, pa.DataType | None]] = [
    ("5/2", 2.5, pa.float64()),
    ("1/0", None, pa.float64()),
    ("7 % 0", None, None),
    ("substr('hello', 0, 3)", "hel", None),
    ("substr('hello', -3)", "llo", None),
    ("element_at(array(10, 20, 30), 1)", 10, None),
    ("(array(10, 20, 30))[0]", 10, None),
]


def _via_spark_sql(spark: ReparkSession, expression: str) -> pa.Table:
    """Entry point 1: the raw-SQL passthrough."""
    return spark.sql(f"SELECT ({expression}) AS x").to_arrow()


def _via_f_expr(spark: ReparkSession, expression: str) -> pa.Table:
    """Entry point 2: an F.expr column applied to a DataFrame (the handoff path)."""
    import repark.functions as F  # noqa: N812 — PySpark idiomatic alias

    return spark.sql("SELECT 1 AS dummy").select(F.expr(expression).alias("x")).to_arrow()


@pytest.mark.parametrize("entry_point", [_via_spark_sql, _via_f_expr], ids=["spark.sql", "F.expr"])
@pytest.mark.parametrize(
    ("expression", "expected", "expected_type"),
    DIVERGENCE_CORPUS,
    ids=[case[0] for case in DIVERGENCE_CORPUS],
)
def test_divergence_corpus_on_the_arrow_path(
    spark: ReparkSession,
    entry_point: object,
    expression: str,
    expected: object,
    expected_type: pa.DataType | None,
) -> None:
    table = entry_point(spark, expression)  # type: ignore[operator]
    assert table.column("x").to_pylist() == [expected]
    if expected_type is not None:
        assert table.schema.field("x").type == expected_type
