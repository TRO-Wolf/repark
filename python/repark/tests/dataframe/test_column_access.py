"""Group G1 — column-access sugar: ``df.x``, ``df["x"]``, ``-col``.

Goldens recorded from live PySpark 4.1.2
(``JAVA_HOME=/usr/lib/jvm/zulu-17-amd64``, ``SPARK_LOCAL_IP=127.0.0.1``, ANSI on). Routine
tests are JVM-free and pin those recorded behaviours.

Probe matrix (verbatim intent vs live 4.1.2)::

    df = spark.createDataFrame([(1, 10), (2, 20)], ["x", "y"])
    # G1a __getattr__
    type(df.x)                         # → Column
    df.missing                         # → PySparkAttributeError / ATTRIBUTE_NOT_SUPPORTED
                                       #   (message names `missing`)
    df2 = spark.createDataFrame([(1,)], ["count"])
    df2.count                          # → bound method (method wins over column name)
    df.X                               # → ATTRIBUTE_NOT_SUPPORTED (case-sensitive)
    # G1b __getitem__
    type(df["x"])                      # → Column
    df[0]                              # → first column; df[-1] works
    df[99]                             # → IndexError
    df[df.x > 1]                       # → filter
    df[["x", "y"]] / df[("x", "y")]    # → select
    df["missing"]                      # → AnalysisException at access time (eager)
    df["X"] with column ``x``          # → Column (case-insensitive; caseSensitive=false)
    # G1c __neg__
    df.select(-df.x).columns           # → ['negative(x)']
    str(-df.x)                         # → Column<'negative(x)'>
    df.agg(F.sum(-df.x)).columns       # → ['sum(negative(x))']
    values negate ints; double → negative(negative(x))

repark raises :class:`AttributeError` (no ``PySparkAttributeError``) with the same
``[ATTRIBUTE_NOT_SUPPORTED]`` message; missing ``df["col"]`` raises
:class:`~repark.errors.AnalysisException` naming the column. ``df["X"]`` resolves
case-insensitively to the canonical schema name (Spark analyzer default); ``df.X`` stays
case-sensitive (PySpark attr surface).
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from repark import Column, ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-column-access").getOrCreate()
    yield session
    session.stop()


def _source(spark: ReparkSession) -> object:
    return spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["x", "y"])


# ---- G1a: DataFrame.__getattr__ ----


def test_getattr_returns_column(spark: ReparkSession) -> None:
    """``df.x`` → Column (live PySpark 4.1.2)."""
    df = _source(spark)
    column = df.x
    assert isinstance(column, Column)
    assert column.spark_display_part() == "x"
    table = df.select(df.x).to_arrow()
    assert table.column_names == ["x"]
    assert table.to_pydict() == {"x": [1, 2, 3]}


def test_getattr_missing_raises_attribute_error_naming_column(spark: ReparkSession) -> None:
    """Missing attr → AttributeError with Spark ``[ATTRIBUTE_NOT_SUPPORTED]`` text + name."""
    df = _source(spark)
    with pytest.raises(AttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED") as caught:
        _ = df.missing
    message = str(caught.value)
    assert "missing" in message
    assert "`missing`" in message or "Attribute `missing`" in message


def test_getattr_method_precedence_over_column_named_count(spark: ReparkSession) -> None:
    """Column named ``count`` does not shadow ``DataFrame.count`` (Python attr lookup)."""
    df = spark.createDataFrame([(1,), (2,)], ["count"])
    # Method still wins — never hits __getattr__.
    assert callable(df.count)
    assert df.count() == 2
    # Column still reachable via getitem / col.
    assert df["count"].spark_display_part() == "count"
    assert df.select(df["count"]).to_arrow().to_pydict() == {"count": [1, 2]}


def test_getattr_case_sensitive(spark: ReparkSession) -> None:
    """``df.X`` fails when the column is ``x`` (case-sensitive attr surface)."""
    df = _source(spark)
    with pytest.raises(AttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED"):
        _ = df.X
    assert isinstance(df.x, Column)


def test_getattr_underscore_column(spark: ReparkSession) -> None:
    """Column ``_x`` via ``df._x``; missing ``_x`` raises ATTRIBUTE_NOT_SUPPORTED."""
    df = spark.createDataFrame([(7,)], ["_x"])
    assert isinstance(df._x, Column)
    assert df.select(df._x).to_arrow().to_pydict() == {"_x": [7]}

    plain = _source(spark)
    with pytest.raises(AttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED"):
        _ = plain._x


def test_getattr_dunder_resolved_on_type(spark: ReparkSession) -> None:
    """Existing type dunders like ``__class__`` resolve on the type (never hit ``__getattr__``)."""
    df = _source(spark)
    assert df.__class__ is type(df)
    assert isinstance(df, type(df))
    # Missing dunder falls through membership check (Spark classic is membership-only too).
    with pytest.raises(AttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED") as caught:
        _ = df.__copy__  # type: ignore[attr-defined]
    assert "`__copy__`" in str(caught.value)


def test_getattr_getitem_repr_str(spark: ReparkSession) -> None:
    """``repr``/``str`` on getattr/getitem entry points (not only ``-col``)."""
    df = _source(spark)
    assert repr(df.x) == "Column<'x'>"
    assert str(df["y"]) == "Column<'y'>"
    assert repr(df["x"]) == "Column<'x'>"
    assert str(df.y) == "Column<'y'>"


# ---- G1b: DataFrame.__getitem__ ----


def test_getitem_str_returns_column(spark: ReparkSession) -> None:
    """``df["x"]`` → Column."""
    df = _source(spark)
    column = df["x"]
    assert isinstance(column, Column)
    assert column.spark_display_part() == "x"
    assert df.select(df["x"]).to_arrow().to_pydict() == {"x": [1, 2, 3]}


def test_getitem_str_case_insensitive_resolves_canonical(spark: ReparkSession) -> None:
    """``df["X"]`` succeeds when the column is ``x`` (Spark CI default).

    Live 4.1.2 (caseSensitive=false): the analyzer resolver accepts the differently-cased key
    and the REQUESTED spelling becomes the output name; display matches ``F.col("X")`` so
    compounds stay clean. ``df.X`` stays case-sensitive AttributeError (G1a).
    """
    df = _source(spark)
    column = df["X"]
    assert isinstance(column, Column)
    assert column.spark_display_part() == "X"
    assert repr(column) == "Column<'X'>"
    table = df.select(df["X"]).to_arrow()
    assert table.column_names == ["X"]
    assert table.to_pydict() == {"X": [1, 2, 3]}
    # Compound projection names match F.col path (live Spark 4.1.2).
    assert df.select(df["X"] + 1).columns == ["(X + 1)"]
    with pytest.raises(AttributeError, match=r"ATTRIBUTE_NOT_SUPPORTED"):
        _ = df.X
    # Engine select path keeps the requested spelling too; values still come
    # from schema column ``x``.
    via_select = df.select("X").to_arrow()
    assert via_select.column_names == ["X"]
    assert via_select.to_pydict() == {"X": [1, 2, 3]}


def test_getitem_str_case_ambiguous_raises(spark: ReparkSession) -> None:
    """Multiple case-insensitive matches (exact miss) → AnalysisException naming ambiguity.

    With schema ``Foo`` and ``foo``, spelling ``FOO`` exact-misses both but casefolds to both.
    Exact keys still prefer the exact schema name.
    """
    df = spark.createDataFrame([(1, 2)], ["Foo", "foo"])
    with pytest.raises(AnalysisException, match=r"ambiguous") as caught:
        _ = df["FOO"]
    message = str(caught.value)
    assert "Foo" in message and "foo" in message
    # Exact match still wins (prefer exact before CI fan-out).
    assert df["Foo"].spark_display_part() == "Foo"
    assert df["foo"].spark_display_part() == "foo"


def test_getitem_int_positional(spark: ReparkSession) -> None:
    """``df[0]`` / ``df[-1]`` → column by position; OOB → IndexError."""
    df = _source(spark)
    assert df[0].spark_display_part() == "x"
    assert df[1].spark_display_part() == "y"
    assert df[-1].spark_display_part() == "y"
    assert df.select(df[0]).to_arrow().to_pydict() == {"x": [1, 2, 3]}
    with pytest.raises(IndexError):
        _ = df[99]
    with pytest.raises(IndexError):
        _ = df[-99]


def test_getitem_column_filters(spark: ReparkSession) -> None:
    """``df[df.x > 1]`` → filter (same as ``filter``); values + schema (type/nullability)."""
    df = _source(spark)
    filtered = df[df.x > 1].orderBy("x").to_arrow()
    expected = pa.table(
        {"x": [2, 3], "y": [20, 30]},
        schema=pa.schema(
            [
                pa.field("x", pa.int64(), nullable=True),
                pa.field("y", pa.int64(), nullable=True),
            ]
        ),
    )
    assert filtered.column_names == expected.column_names
    assert filtered.to_pydict() == expected.to_pydict()
    # Live schema equality (type/nullability pinned).
    assert filtered.schema.equals(expected.schema)
    # Same as explicit filter.
    via_filter = df.filter(df.x > 1).orderBy("x").to_arrow().to_pydict()
    assert filtered.to_pydict() == via_filter


def test_getitem_list_and_tuple_select(spark: ReparkSession) -> None:
    """``df[["x", "y"]]`` / ``df[("x", "y")]`` → select (values + schema type/nullability)."""
    df = _source(spark)
    from_list = df[["x", "y"]].to_arrow()
    from_tuple = df[("x", "y")].to_arrow()
    expected = pa.table(
        {"x": [1, 2, 3], "y": [10, 20, 30]},
        schema=pa.schema(
            [
                pa.field("x", pa.int64(), nullable=True),
                pa.field("y", pa.int64(), nullable=True),
            ]
        ),
    )
    assert from_list.column_names == ["x", "y"]
    assert from_tuple.column_names == ["x", "y"]
    assert from_list.to_pydict() == expected.to_pydict()
    assert from_tuple.to_pydict() == from_list.to_pydict()
    # Schema equality on list/tuple getitem→select (sibling of filter pin).
    assert from_list.schema.equals(expected.schema)
    assert from_tuple.schema.equals(expected.schema)
    # Single-name list is still select (one column).
    assert df[["y"]].to_arrow().column_names == ["y"]


def test_getitem_missing_str_raises_analysis_exception(spark: ReparkSession) -> None:
    """``df["missing"]`` → AnalysisException naming the column (eager, Spark-like).

    Type identity ``repark.errors.AnalysisException`` + ``RuntimeError`` hierarchy.
    """
    df = _source(spark)
    with pytest.raises(AnalysisException, match=r"missing") as caught:
        _ = df["missing"]
    assert "missing" in str(caught.value)
    assert type(caught.value) is AnalysisException
    assert isinstance(caught.value, RuntimeError)
    assert AnalysisException.__module__ == "repark.errors"
    assert caught.value.__class__.__module__ == "repark.errors"


def test_getitem_rejects_unsupported_key_type(spark: ReparkSession) -> None:
    """Non str/int/Column/list/tuple → TypeError."""
    df = _source(spark)
    with pytest.raises(TypeError, match=r"str|int|Column|list|tuple"):
        _ = df[1.5]  # type: ignore[index]


def test_held_dataframe_column_access_raises_after_stop() -> None:
    """G1 entry points gate after ``session.stop()``.

    Prefer-stop over TypeError for unsupported keys — first-line ``_ensure_alive`` preserves
    that ordering.
    """
    from repark import session as session_module

    session_module._reset_active_session_for_tests()
    session = ReparkSession.builder.appName("g1-stop-pins").getOrCreate()
    frame = session.createDataFrame([(1, 10), (2, 20)], ["x", "y"])
    # Mint the predicate before stop: post-stop ``frame.x`` would raise on getattr and never
    # reach the getitem Column→filter arm.
    predicate = frame.x > 0
    session.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame.x
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame["x"]
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame[0]
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame[predicate]
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame[["x"]]
    with pytest.raises(RuntimeError, match="stopped"):
        _ = frame[1.5]  # type: ignore[index]
    session_module._reset_active_session_for_tests()


# ---- G1c: Column.__neg__ ----


def test_neg_select_column_name_and_values(spark: ReparkSession) -> None:
    """``df.select(-df.x).columns == ['negative(x)']``; values negate; int64 + nullable."""
    df = spark.createDataFrame([(1,), (-3,), (0,)], ["x"])
    out = df.select(-df.x).to_arrow()
    assert out.column_names == ["negative(x)"]
    assert out.to_pydict() == {"negative(x)": [-1, 3, 0]}
    # Exact width + nullability (not family-only is_integer).
    field = out.schema.field(0)
    assert field.type == pa.int64()
    assert field.nullable is True


def test_neg_null_rows_preserve_null(spark: ReparkSession) -> None:
    """``__neg__`` keeps NULL rows under ``negative(x)`` (not positives-only)."""
    df = spark.createDataFrame([(1,), (None,), (0,), (-2,)], ["x"])
    out = df.select(-df.x).to_arrow()
    assert out.column_names == ["negative(x)"]
    assert out.to_pydict() == {"negative(x)": [-1, None, 0, 2]}
    field = out.schema.field(0)
    assert field.type == pa.int64()
    assert field.nullable is True


def test_neg_float_values_and_type(spark: ReparkSession) -> None:
    """Float ``-df.x`` pins values, float64, nullability, null rows."""
    df = spark.createDataFrame([(1.5,), (None,), (-2.0,)], ["x"])
    out = df.select(-df.x).to_arrow()
    assert out.column_names == ["negative(x)"]
    assert out.to_pydict() == {"negative(x)": [-1.5, None, 2.0]}
    field = out.schema.field(0)
    assert field.type == pa.float64()
    assert field.nullable is True


def test_neg_str_and_repr_match_pyspark(spark: ReparkSession) -> None:
    """``str(-df.x)`` / ``repr`` → ``Column<'negative(x)'>`` (live PySpark 4.1.2)."""
    df = _source(spark)
    negated = -df.x
    assert str(negated) == "Column<'negative(x)'>"
    assert repr(negated) == "Column<'negative(x)'>"
    assert negated.spark_display_part() == "negative(x)"


def test_neg_agg_sum_display_name(spark: ReparkSession) -> None:
    """``df.agg(F.sum(-df.x)).columns == ['sum(negative(x))']`` + value pin."""
    df = spark.createDataFrame([(1,), (2,), (-4,)], ["x"])
    aggregated = df.agg(F.sum(-df.x))
    assert aggregated.columns == ["sum(negative(x))"]
    table = aggregated.to_arrow()
    assert table.column_names == ["sum(negative(x))"]
    assert table.to_pydict() == {"sum(negative(x))": [1]}


def test_neg_double_negation_display_and_values(spark: ReparkSession) -> None:
    """Double unary minus: ``negative(negative(x))`` restores values."""
    df = spark.createDataFrame([(5,), (-2,)], ["x"])
    once = -df.x
    double = -once
    assert double.spark_display_part() == "negative(negative(x))"
    out = df.select(double).to_arrow()
    assert out.column_names == ["negative(negative(x))"]
    assert out.to_pydict() == {"negative(negative(x))": [5, -2]}


def test_neg_composes_with_binary_ops_in_agg_name(spark: ReparkSession) -> None:
    """Nested ``sum(negative((x + 1)))`` via ``-(df.x + 1)`` — display + values."""
    df = _source(spark)
    aggregated = df.agg(F.sum(-(df.x + 1)))
    assert aggregated.columns == ["sum(negative((x + 1)))"]
    table = aggregated.to_arrow()
    assert table.column_names == ["sum(negative((x + 1)))"]
    assert table.to_pydict() == {"sum(negative((x + 1)))": [-9]}


def test_getitem_case_insensitive_keeps_requested_spelling(spark: ReparkSession) -> None:
    """Live-oracle pin: ``df["B"]`` on column ``b`` names the output ``B``.

    The analyzer resolves case-insensitively but the REQUESTED spelling wins in the output
    schema; exact-case access keeps the canonical name.
    """
    df = spark.createDataFrame([(1, 2)], ["a", "b"])
    assert df.select(df["B"]).columns == ["B"]
    assert df.select(df["b"]).columns == ["b"]
    assert df.select(df["B"]).collect()[0][0] == 2


def test_copy_protocols_do_not_recurse(spark: ReparkSession) -> None:
    """A half-built instance's attribute miss must not recurse.

    copy.copy creates the object before filling ``__dict__``; ``_inner`` misses re-entered
    ``__getattr__`` unguarded. Any outcome except RecursionError is acceptable — PySpark
    raises from its JVM handle here, so no parity pin.
    """
    import copy

    df = spark.createDataFrame([(1,)], ["a"])
    try:
        copy.copy(df)
    except RecursionError:  # pragma: no cover — the defect class under pin
        raise AssertionError("copy.copy(df) recursed through __getattr__") from None
    except Exception:
        pass
