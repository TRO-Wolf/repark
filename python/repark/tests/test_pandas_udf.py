"""U7 scalar pandas_udf: facade projection-rewrite over the mapInArrow bridge."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pandas as pd
import pyarrow as pa
import pytest

from repark import SparkSession
from repark.dataframe import DataFrame
from repark.errors import (
    AnalysisException,
    PySparkException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.functions import (
    PandasUDFColumn,
    PandasUDFType,
    bucket,
    col,
    current_timestamp,
    explode,
    lit,
    months,
    pandas_udf,
    years,
)
from repark.functions import (
    sum as f_sum,
)
from repark.types import (
    CharType,
    IntegerType,
    LongType,
    StringType,
    TimestampNTZType,
    TimestampType,
    VarcharType,
)


@pytest.fixture
def spark() -> Iterator[SparkSession]:
    session = SparkSession.builder.master("local[1]").appName("test-pandas-udf").getOrCreate()
    yield session
    session.stop()


def _rows(table: pa.Table) -> list[dict[str, Any]]:
    return table.to_pylist()


def _multiset(rows: list[dict[str, Any]]) -> list[tuple[tuple[str, Any], ...]]:
    def cell(value: Any) -> Any:
        if value is None:
            return ("null",)
        if isinstance(value, float) and value != value:  # NaN
            return ("nan",)
        return ("v", value)

    packed = [tuple(sorted((key, cell(val)) for key, val in row.items())) for row in rows]
    return sorted(packed)


@pandas_udf("long")
def double_long(series: pd.Series) -> pd.Series:
    return series.astype("int64") * 2


@pandas_udf(LongType())
def add_long(left: pd.Series, right: pd.Series) -> pd.Series:
    return left.astype("int64") + right.astype("int64")


@pandas_udf("string")
def upper_str(series: pd.Series) -> pd.Series:
    return series.str.upper()


def test_pandas_udf_select_values(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.select(double_long(col("x")).alias("y"))
    assert _rows(out.to_arrow()) == [{"y": 2}, {"y": 4}, {"y": 6}]
    assert out.schema.fields[0].dataType.simpleString() in {"bigint", "long"}


def test_pandas_udf_with_column_values(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, "a"), (2, "b")], "x INT, s STRING")
    out = frame.withColumn("y", double_long("x"))
    assert _multiset(_rows(out.to_arrow())) == _multiset(
        [
            {"x": 1, "s": "a", "y": 2},
            {"x": 2, "s": "b", "y": 4},
        ]
    )


def test_pandas_udf_select_keeps_sibling_columns(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 10), (2, 20)], "id INT, x INT")
    out = frame.select(col("id"), double_long(col("x")).alias("y"))
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"id": 1, "y": 20}, {"id": 2, "y": 40}])


def test_pandas_udf_multi_udf_one_pass(
    spark: SparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Multi-UDF select shares one mapInArrow bridge (one-pass per batch, documented).

    MUTATION (octo C1-Q-004): a hollow ``>= 1`` call count still passes under N sequential
    bridges. Pin mapInArrow call count == 1 and both UDF bodies fire in that single pass.
    """
    frame = spark.createDataFrame([(1, "ab"), (2, "cd")], "x INT, s STRING")
    calls: list[str] = []
    map_in_arrow_calls: list[int] = []
    real_map_in_arrow = DataFrame.mapInArrow

    def _counting_map_in_arrow(
        self: DataFrame,
        func: Any,
        schema: Any,
    ) -> DataFrame:
        map_in_arrow_calls.append(1)
        return real_map_in_arrow(self, func, schema)

    monkeypatch.setattr(DataFrame, "mapInArrow", _counting_map_in_arrow)

    @pandas_udf("long")
    def tracked_double(series: pd.Series) -> pd.Series:
        calls.append("double")
        return series.astype("int64") * 2

    @pandas_udf("string")
    def tracked_upper(series: pd.Series) -> pd.Series:
        calls.append("upper")
        return series.str.upper()

    out = frame.select(
        tracked_double(col("x")).alias("y"),
        tracked_upper(col("s")).alias("t"),
    )
    # Plan-time: multi-UDF rewrite must open exactly one mapInArrow bridge (not N).
    assert map_in_arrow_calls == [1]
    rows = _rows(out.to_arrow())
    assert _multiset(rows) == _multiset([{"y": 2, "t": "AB"}, {"y": 4, "t": "CD"}])
    # Small frame → one batch; both UDFs run exactly once in that shared pass.
    assert calls.count("double") == 1
    assert calls.count("upper") == 1
    assert calls == ["double", "upper"]


def test_pandas_udf_multi_arg(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1, 10), (2, 20)], "a INT, b INT")
    out = frame.select(add_long(col("a"), col("b")).alias("s"))
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"s": 11}, {"s": 22}])


def test_pandas_udf_null_handling(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (None,), (3,)], "x INT")

    @pandas_udf("long")
    def null_safe_double(series: pd.Series) -> pd.Series:
        return series.astype("Int64") * 2

    out = frame.select(null_safe_double(col("x")).alias("y"))
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"y": 2}, {"y": None}, {"y": 6}])


def test_pandas_udf_null_int_series_mul_without_hand_cast(spark: SparkSession) -> None:
    """Null INT inputs stay integer-nullable so the common ``series * 2`` path works.

    MUTATION (octo C1-Q-003): bare ``Array.to_pandas()`` demotes null ints to float64;
    users must hand-cast ``Int64``. After fix, ``series * 2`` with nulls yields long.
    """
    frame = spark.createDataFrame([(1,), (None,), (3,)], "x INT")

    @pandas_udf("long")
    def naive_double(series: pd.Series) -> pd.Series:
        # No astype("Int64") — relies on bridge not demoting null ints to float64.
        assert str(series.dtype) in {"Int32", "Int64", "int32", "int64", "int32[pyarrow]"}
        return series * 2

    out = frame.select(naive_double(col("x")).alias("y")).to_arrow()
    assert out.schema.field("y").type == pa.int64()
    assert _multiset(_rows(out)) == _multiset([{"y": 2}, {"y": None}, {"y": 6}])


def test_pandas_udf_type_coercion_int_to_long(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.select(double_long(col("x")).alias("y")).to_arrow()
    # Declared long → Arrow int64 (not int32).
    assert out.schema.field("y").type == pa.int64()
    assert out.column("y").to_pylist() == [2, 4]


def test_pandas_udf_string_values(spark: SparkSession) -> None:
    frame = spark.createDataFrame([("hi",), ("Yo",)], "s STRING")
    out = frame.select(upper_str(col("s")).alias("u"))
    assert _rows(out.to_arrow()) == [{"u": "HI"}, {"u": "YO"}]


def test_pandas_udf_lazy_until_action(spark: SparkSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """select/withColumn is plan-only — no UDF body and no intermediate to_arrow (octo C6-Q-001).

    MUTATION: restore ``intermediate.limit(0).to_arrow()`` for pass-through schema → this pin
    sees a ``to_arrow`` call during select and fails red. UDF-call-only was hollow for the
    STOP claim that intermediate is plan-only (no row pull until action).
    """
    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    calls: list[int] = []
    to_arrow_calls: list[str] = []

    @pandas_udf("long")
    def counted(series: pd.Series) -> pd.Series:
        calls.append(len(series))
        return series.astype("int64") * 2

    real_to_arrow = DataFrame.to_arrow

    def tracking_to_arrow(self: DataFrame, *args: Any, **kwargs: Any) -> Any:
        to_arrow_calls.append("to_arrow")
        return real_to_arrow(self, *args, **kwargs)

    monkeypatch.setattr(DataFrame, "to_arrow", tracking_to_arrow)

    # withColumn pass-through path also needed physical types historically via limit(0).
    out = frame.withColumn("y", counted(col("x")))
    # schema / columns must not run the UDF (mapInArrow placeholder contract).
    assert out.columns == ["x", "y"]
    _ = out.schema
    assert calls == []
    assert to_arrow_calls == [], (
        "select/withColumn must not call to_arrow (plan-only intermediate; no limit(0) action); "
        f"got {to_arrow_calls}"
    )
    # Action may call to_arrow — clear the plan-time pin before asserting values.
    monkeypatch.setattr(DataFrame, "to_arrow", real_to_arrow)
    assert _rows(out.to_arrow()) == [{"x": 1, "y": 2}, {"x": 2, "y": 4}]
    assert calls  # action ran the UDF


def test_pandas_udf_bridge_defers_pandas_import() -> None:
    """pandas import lives inside the mapInArrow callback, not at select entry (octo C6-Q-001).

    MUTATION: move ``import pandas as pd`` back to the top of ``_select_with_pandas_udfs``
    (before the callback) → this pin fails red.
    """
    import inspect

    source = inspect.getsource(DataFrame._select_with_pandas_udfs)
    before_callback, separator, after_callback = source.partition("def _arrow_pandas_udf_func")
    assert separator, "expected mapInArrow callback in _select_with_pandas_udfs"
    assert "import pandas" not in before_callback, (
        "pandas must not be imported at select/withColumn plan time"
    )
    assert "import pandas" in after_callback, (
        "pandas import must live inside the mapInArrow action callback"
    )


def test_pandas_udf_rerun_on_action(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,)], "x INT")
    calls: list[int] = []

    @pandas_udf("long")
    def counted(series: pd.Series) -> pd.Series:
        calls.append(1)
        return series.astype("int64") * 2

    out = frame.select(counted(col("x")).alias("y"))
    assert _rows(out.to_arrow()) == [{"y": 2}]
    assert _rows(out.to_arrow()) == [{"y": 2}]
    assert len(calls) == 2


def test_pandas_udf_cache_pins_once(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,)], "x INT")
    calls: list[int] = []

    @pandas_udf("long")
    def counted(series: pd.Series) -> pd.Series:
        calls.append(1)
        return series.astype("int64") * 2

    out = frame.select(counted(col("x")).alias("y")).cache()
    assert _rows(out.to_arrow()) == [{"y": 2}]
    assert _rows(out.to_arrow()) == [{"y": 2}]
    assert len(calls) == 1


def test_pandas_udf_empty_input(spark: SparkSession) -> None:
    frame = spark.createDataFrame([], "x INT")
    calls: list[int] = []

    @pandas_udf("long")
    def counted(series: pd.Series) -> pd.Series:
        calls.append(len(series))
        return series.astype("int64") * 2

    out = frame.select(counted(col("x")).alias("y"))
    assert _rows(out.to_arrow()) == []
    assert calls == []


def test_pandas_udf_user_error_surfaces(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,)], "x INT")

    @pandas_udf("long")
    def boom(series: pd.Series) -> pd.Series:
        raise ValueError("udf-boom")

    out = frame.select(boom(col("x")).alias("y"))
    with pytest.raises(PySparkException, match="udf-boom") as caught:
        out.to_arrow()
    assert "ValueError" in str(caught.value)
    assert "Traceback" in str(caught.value) or "boom" in str(caught.value)


def test_pandas_udf_wrong_length_loud(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,), (2,)], "x INT")

    @pandas_udf("long")
    def short(series: pd.Series) -> pd.Series:
        return pd.Series([1], dtype="int64")

    out = frame.select(short(col("x")).alias("y"))
    with pytest.raises(PySparkException, match="expected 2"):
        out.to_arrow()


def test_pandas_udf_composition_refused(spark: SparkSession) -> None:
    """Composition limit: every Column-parity dunder/method refuses UOE (not TypeError).

    octo C5-Q-001: pin beyond +/>/cast so deleting ``__mul__`` / ``.over`` / reflected
    ops cannot stay green. octo C5-Q-002: ``__neg__`` / ``__pow__`` / ``__rpow__`` /
    ``__rmod__`` / ``__rand__`` / ``__ror__`` must raise UnsupportedOperationException
    (M5-class composition seed), not bare TypeError from a missing dunder.
    octo C7-Q-002: Column methods ``isNull`` / ``between`` / ``when`` / ``asc`` /
    ``__contains__`` / string preds / bitwise must raise UOE, not AttributeError.
    """
    marker = double_long(col("x"))
    match = r"mid-expression|projection-rewrite"

    # Binary arithmetic (+ reflected via scalar left so PandasUDFColumn.__r* runs —
    # lit(1)+marker hits Column.__add__ first and never reaches the marker).
    # C5-Q-001 mutates if __mul__/__mod__/__rmod__ deleted.
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker + lit(1)
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = 1 + marker
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker - 1
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = 1 - marker
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker * 2
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = 2 * marker
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker / 2
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = 2 / marker
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker % 2
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = 2 % marker

    # Power + unary (were TypeError before C5-Q-002).
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker**2
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = 2**marker
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = -marker

    # Comparisons (representative set + remaining equality).
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker > 0
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker >= 0
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker < 0
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker <= 0
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker == 0
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker != 0

    # Logical (+ reflected; reflected were TypeError before C5-Q-002).
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker & True
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = True & marker
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = marker | False
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = False | marker
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = ~marker

    # Methods (deleting .cast must stay red — C5-Q-001).
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.cast("double")
    # M6: .over is a real surface for GROUPED_AGG (see window pins). SCALAR marker +
    # non-WindowSpec → TypeError; SCALAR + real WindowSpec → AnalysisException (not GROUPED_AGG).
    with pytest.raises(PySparkTypeError, match=r"WindowSpec"):
        marker.over(object())
    from repark.window import Window

    with pytest.raises(AnalysisException, match=r"GROUPED_AGG|functionType"):
        marker.over(Window.partitionBy("x"))

    # Column-parity methods (AttributeError before C7-Q-002).
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.isNull()
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.is_null()
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.isNotNull()
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.is_not_null()
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.between(0, 10)
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.eqNullSafe(1)
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.when(lit(True), lit(1))
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.otherwise(lit(0))
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.asc()
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.desc()
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.contains("x")
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.startswith("x")
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.endswith("x")
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.like("%x%")
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.ilike("%x%")
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.rlike("x")
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.bitwiseAND(1)
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.bitwiseOR(1)
    with pytest.raises(UnsupportedOperationException, match=match):
        marker.bitwiseXOR(1)
    with pytest.raises(UnsupportedOperationException, match=match):
        _ = "x" in marker  # type: ignore[operator]


def test_pandas_udf_grouped_map_window_loud() -> None:
    """GROUPED_MAP / window remain loud M6 seeds; SCALAR_ITER + GROUPED_AGG build (M5).

    octo C7-L-001 / C8-Q-001: functionType-first routes must not fall through to SCALAR.
    M5 implements SCALAR_ITER + GROUPED_AGG; only GROUPED_MAP (and window tag) stay refused.
    """
    with pytest.raises(UnsupportedOperationException, match="M6-class"):

        @pandas_udf("long", functionType="GROUPED_MAP")
        def grouped_map(series: pd.Series) -> pd.Series:
            return series

    with pytest.raises(UnsupportedOperationException, match="M6-class"):

        @pandas_udf(PandasUDFType.GROUPED_MAP, "long")
        def grouped_map_ft_first(series: pd.Series) -> pd.Series:
            return series

    with pytest.raises(UnsupportedOperationException, match="M6-class"):

        @pandas_udf("GROUPED_MAP", "long")
        def grouped_map_string_first(series: pd.Series) -> pd.Series:
            return series

    with pytest.raises(UnsupportedOperationException, match="M6-class"):

        @pandas_udf("long", functionType="WINDOW")
        def window_form(series: pd.Series) -> pd.Series:
            return series

    # M5 supported forms build (not UOE).
    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def iter_form(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for series in batches:
            yield series * 2

    assert callable(iter_form)

    @pandas_udf("double", functionType=PandasUDFType.GROUPED_AGG)
    def grouped_agg(series: pd.Series) -> float:
        return float(series.mean())

    assert callable(grouped_agg)

    @pandas_udf(PandasUDFType.GROUPED_AGG, "long")
    def grouped_agg_ft_first(series: pd.Series) -> int:
        return int(series.sum())

    assert callable(grouped_agg_ft_first)

    @pandas_udf(PandasUDFType.SCALAR_ITER, returnType="long")
    def iter_ft_first_kw(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        yield from batches

    assert callable(iter_ft_first_kw)

    @pandas_udf("GROUPED_AGG", "long")
    def grouped_agg_string_first(series: pd.Series) -> int:
        return len(series)

    assert callable(grouped_agg_string_first)

    @pandas_udf("SCALAR_ITER", "long")
    def iter_string_first(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        yield from batches

    assert callable(iter_string_first)

    # SCALAR-first + returnType still builds (valid reverse form after C7-L-001 route).
    @pandas_udf(PandasUDFType.SCALAR, "long")
    def scalar_ft_first(series: pd.Series) -> pd.Series:
        return series * 2

    assert callable(scalar_ft_first)

    @pandas_udf("SCALAR", "long")
    def scalar_string_ft_first(series: pd.Series) -> pd.Series:
        return series * 2

    assert callable(scalar_string_ft_first)

    @pandas_udf("scalar", returnType="long")
    def scalar_string_ft_first_kw(series: pd.Series) -> pd.Series:
        return series * 2

    assert callable(scalar_string_ft_first_kw)


def test_pandas_udf_decorator_forms() -> None:
    @pandas_udf(returnType=IntegerType())
    def as_int(series: pd.Series) -> pd.Series:
        return series.astype("int32")

    assert callable(as_int)
    direct = pandas_udf(lambda s: s * 2, "long")
    assert callable(direct)

    with pytest.raises(PySparkTypeError):
        pandas_udf(lambda s: s)  # type: ignore[call-arg]


def test_pandas_udf_default_output_name(spark: SparkSession) -> None:
    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.select(double_long(col("x")))
    assert out.columns == ["double_long(x)"]
    assert _rows(out.to_arrow()) == [{"double_long(x)": 2}]


def test_pandas_udf_expression_input(spark: SparkSession) -> None:
    """Non-NamedExpression inputs are intermediate-projected then UDF'd (still lazy)."""
    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.select(double_long(col("x") + lit(1)).alias("y"))
    assert _rows(out.to_arrow()) == [{"y": 4}, {"y": 6}]


def test_pandas_udf_export_on_functions_all() -> None:
    import repark.functions as functions

    assert "pandas_udf" in functions.__all__
    assert "PandasUDFType" in functions.__all__
    assert functions.pandas_udf is pandas_udf


def test_pandas_udf_sql_functions_alias() -> None:
    from repark.sql import functions as sql_functions

    assert sql_functions.pandas_udf is pandas_udf
    assert sql_functions.PandasUDFType is PandasUDFType


def test_pandas_udf_requires_column_args() -> None:
    with pytest.raises(PySparkTypeError, match="at least one column"):
        double_long()  # type: ignore[call-arg]


def test_pandas_udf_downstream_filter_after_materialize(spark: SparkSession) -> None:
    """After select, the bridge output is a normal mapInArrow frame — filter works."""
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.select(double_long(col("x")).alias("y")).filter("y > 3")
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"y": 4}, {"y": 6}])


def test_pandas_udf_return_type_datatype_object(spark: SparkSession) -> None:
    @pandas_udf(StringType())
    def as_str(series: pd.Series) -> pd.Series:
        return series.astype(str)

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    out = frame.select(as_str(col("x")).alias("s"))
    assert _rows(out.to_arrow()) == [{"s": "1"}, {"s": "2"}]


def test_pandas_udf_column_bool_refused() -> None:
    """PandasUDFColumn has no truth value (octo C1-Q-001).

    MUTATION: drop ``__bool__`` → ``if marker`` / ``marker and col`` fail-open True.
    """
    marker = double_long(col("x"))
    with pytest.raises(PySparkValueError, match="Cannot convert column into bool"):
        bool(marker)
    with pytest.raises(PySparkValueError, match="Cannot convert column into bool"):
        _ = marker and col("y")
    with pytest.raises(PySparkValueError, match="Cannot convert column into bool"):
        _ = marker or col("y")
    with pytest.raises(PySparkValueError, match="Cannot convert column into bool"):
        if marker:  # pragma: no cover — raise IS the assertion
            pass


def test_pandas_udf_dual_return_type_positionals_loud() -> None:
    """``@pandas_udf(\"long\", \"double\")`` must not silently drop the first type (C1-Q-002).

    MUTATION: restore keyword fall-through that uses only the second positional as
    returnType → this pin goes red (decorator succeeds and return type is double).
    """
    with pytest.raises(PySparkTypeError, match=r"functionType|second positional|two returnType"):

        @pandas_udf("long", "double")  # type: ignore[call-overload]
        def bad(series: pd.Series) -> pd.Series:
            return series


def test_pandas_udf_unsupported_return_type_no_string_fallback() -> None:
    """variant/interval/time and garbage DDL refuse — never fail-open to string (C1-SEC-001)."""
    for bad in ("variant", "interval", "time", "time(6)", "calendarinterval", "not_a_type"):
        with pytest.raises((PySparkTypeError, UnsupportedOperationException)):

            @pandas_udf(bad)
            def bad_ret(series: pd.Series) -> pd.Series:
                return series.astype(str)


def test_pandas_udf_struct_field_list_return_type_refused() -> None:
    """Field-list DDL is StructType and must refuse like ``struct<…>`` (octo C1-SEC-002).

    MUTATION: only ``startswith('struct')`` → ``@pandas_udf('a int, b string')`` succeeds
    and yields a StructType return shape via fromDDL.
    """
    for ddl in ("a int, b string", "a: int", "a: int, b: string"):
        with pytest.raises(UnsupportedOperationException, match=r"struct|M5-class|scalar only"):

            @pandas_udf(ddl)
            def bad_struct(series: pd.Series) -> pd.Series:
                return series


def test_pandas_udf_passthrough_preserves_narrow_arrow_types(spark: SparkSession) -> None:
    """withColumn/select pass-through keeps FLOAT/SMALLINT/BINARY widths (octo C1-L-001).

    MUTATION: rebuild pass-through schema from collapsed ``logical_schema`` type_keys
    (float32→double, i16→int, binary→string) → mapInArrow type-check fails or widens.
    """
    frame = spark.createDataFrame(
        [(1, 1.5, 2, b"ab")],
        "x INT, f FLOAT, s SMALLINT, b BINARY",
    )
    out = frame.withColumn("y", double_long("x"))
    table = out.to_arrow()
    assert table.schema.field("f").type == pa.float32()
    assert table.schema.field("s").type == pa.int16()
    assert table.schema.field("b").type == pa.binary()
    assert table.schema.field("y").type == pa.int64()
    assert _multiset(_rows(table)) == _multiset([{"x": 1, "f": 1.5, "s": 2, "b": b"ab", "y": 2}])


def test_pandas_udf_generator_input_refused(spark: SparkSession) -> None:
    """UDF inputs must not be explode/posexplode (octo C1-L-002).

    MUTATION: skip generator check on UDF inputs → projects array placeholder without
    unnest and returns wrong cardinality/values.
    """
    frame = spark.createDataFrame([([1, 2],)], "arr ARRAY<INT>")
    with pytest.raises(AnalysisException, match=r"generator|explode"):
        frame.select(double_long(explode(col("arr"))).alias("y"))


def test_pandas_udf_partition_transform_input_refused(spark: SparkSession) -> None:
    """UDF inputs must not be years/months/days/hours/bucket (octo C2-Q-001 / C2-L-001).

    MUTATION: skip ``_reject_partition_transform`` on UDF inputs → intermediate projects
    ``literal(None)`` and the UDF silently receives an all-null Series (wrong results).
    """
    frame = spark.createDataFrame([(1,)], "x INT")
    for bad_input in (years("x"), months("x"), bucket(4, "x")):
        with pytest.raises(
            AnalysisException,
            match=r"PARTITION_TRANSFORM|partitionedBy",
        ):
            frame.select(double_long(bad_input).alias("y"))


def test_pandas_udf_aggregate_input_refused(spark: SparkSession) -> None:
    """UDF inputs must not be sticky aggregates (octo C2-Q-002 / C1-L-002 half).

    MUTATION: delete the ``_is_aggregate`` UDF-input refuse branch → suite stays green
    without this pin; with it, the refuse is mutation-proof.
    """
    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    with pytest.raises(AnalysisException, match=r"aggregate"):
        frame.select(double_long(f_sum("x")).alias("y"))


def test_pandas_udf_mix_with_aggregate_refused(spark: SparkSession) -> None:
    """Cannot mix pandas_udf with aggregate siblings in one select (octo C2-Q-002).

    MUTATION: delete the aggregate sibling refuse → suite stays green without this pin.
    """
    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    with pytest.raises(AnalysisException, match=r"aggregate"):
        frame.select(double_long(col("x")).alias("y"), f_sum("x").alias("s"))


def test_pandas_udf_mix_with_explode_refused(spark: SparkSession) -> None:
    """Cannot mix pandas_udf with explode/posexplode siblings (octo C2-Q-002).

    MUTATION: delete the generator sibling refuse → suite stays green without this pin.
    """
    frame = spark.createDataFrame([([1, 2], 3)], "arr ARRAY<INT>, x INT")
    with pytest.raises(AnalysisException, match=r"generator|explode"):
        frame.select(double_long(col("x")).alias("y"), explode(col("arr")).alias("e"))


def test_pandas_udf_nested_unsupported_return_type_no_string_fallback() -> None:
    """Nested variant/interval/time leaves refuse — not only top-level (octo C2-SEC-001).

    MUTATION: only top-level ``pa.string()`` check → ``array<variant>`` /
    ``map<string,time>`` / ``array<struct<a:variant>>`` succeed and fail-open to
    list/map of string.
    """
    nested_bad = (
        "array<variant>",
        "array<interval>",
        "array<time>",
        "map<string,time>",
        "map<string,interval>",
        "array<struct<a:variant>>",
        "array<struct<a:interval>>",
    )
    for bad in nested_bad:
        with pytest.raises((PySparkTypeError, UnsupportedOperationException)):

            @pandas_udf(bad)
            def bad_nested(series: pd.Series) -> pd.Series:
                return series


def test_pandas_udf_passthrough_preserves_timestamp_timezone(
    spark: SparkSession,
) -> None:
    """Pass-through ``current_timestamp()`` keeps ``timestamp[us, tz=UTC]`` (octo C2-L-002).

    MUTATION: rebuild pass-through expected Arrow via ``_arrow_type_to_repark`` +
    ``_coerce_map_in_arrow_schema`` → timezone dropped; mapInArrow validation fails
    (expected timestamp[us], got timestamp[us, tz=UTC]).
    """
    frame = spark.createDataFrame([(1,)], "x INT")
    out = frame.select(current_timestamp().alias("ts"), double_long(col("x")).alias("y"))
    table = out.to_arrow()
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("y").type == pa.int64()
    assert len(table) == 1
    assert table.column("y").to_pylist() == [2]


def test_pandas_udf_null_bool_series_without_object_demotion(spark: SparkSession) -> None:
    """Null BOOLEAN inputs use pandas BooleanDtype — not object demotion (octo C3-Q-001).

    MUTATION: delete ``BooleanDtype`` branch in ``_arrow_array_to_pandas_series`` → bare
    ``Array.to_pandas()`` yields object dtype; ``~series`` fails or loses null semantics
    and this pin goes red.
    """
    frame = spark.createDataFrame([(True,), (None,), (False,)], "flag BOOLEAN")

    @pandas_udf("boolean")
    def invert_flag(series: pd.Series) -> pd.Series:
        # No hand cast — relies on bridge mapping bool nulls to BooleanDtype.
        assert str(series.dtype) in {"boolean", "bool", "bool[pyarrow]"}
        return ~series

    out = frame.select(invert_flag(col("flag")).alias("y")).to_arrow()
    assert out.schema.field("y").type == pa.bool_()
    assert _multiset(_rows(out)) == _multiset([{"y": False}, {"y": None}, {"y": True}])


def test_pandas_udf_null_float_series_nullable_dtype(spark: SparkSession) -> None:
    """Null FLOAT/DOUBLE inputs use pandas Float*Dtype (octo C3-Q-001).

    MUTATION: delete ``Float32Dtype`` / ``Float64Dtype`` branches → suite stays green
    without this pin (bare float null→NaN often still arithmetic-works); with it, the
    nullable-dtype mapper is mutation-proof for both float widths.
    """
    frame = spark.createDataFrame(
        [(1.5, 2.5), (None, None), (3.0, 4.0)],
        "f FLOAT, d DOUBLE",
    )

    @pandas_udf("float")
    def double_f32(series: pd.Series) -> pd.Series:
        assert str(series.dtype) in {"Float32", "float32", "float32[pyarrow]"}
        return series * 2.0

    @pandas_udf("double")
    def double_f64(series: pd.Series) -> pd.Series:
        assert str(series.dtype) in {"Float64", "float64", "double[pyarrow]"}
        return series * 2.0

    out = frame.select(
        double_f32(col("f")).alias("yf"),
        double_f64(col("d")).alias("yd"),
    ).to_arrow()
    assert out.schema.field("yf").type == pa.float32()
    assert out.schema.field("yd").type == pa.float64()
    assert _multiset(_rows(out)) == _multiset(
        [
            {"yf": 3.0, "yd": 5.0},
            {"yf": None, "yd": None},
            {"yf": 6.0, "yd": 8.0},
        ]
    )


def test_pandas_udf_return_none_loud(spark: SparkSession) -> None:
    """User func returning ``None`` must raise (got None) — not coerce (octo C3-Q-002).

    MUTATION: delete ``if result is None`` refuse → ``pd.Series(None)`` is length-0;
    the subsequent length check raises ``returned 0 values; expected 1`` (different
    message). For a **0-row** batch the length matches and the bridge can emit empty
    output without the explicit None refuse — pin requires the ``got None`` wording.
    """

    @pandas_udf("long")
    def returns_none(series: pd.Series) -> pd.Series:
        return None  # type: ignore[return-value]

    # 1-row is the critical path: length-mismatch alone is not the None refuse contract.
    frame = spark.createDataFrame([(1,)], "x INT")
    with pytest.raises(PySparkException, match=r"got None"):
        frame.select(returns_none(col("x")).alias("y")).to_arrow()


def test_pandas_udf_return_non_series_loud(spark: SparkSession) -> None:
    """Non-Series returns refuse ``must return a pandas.Series`` (octo C7-Q-001).

    MUTATION: restore ``pd.Series(result)`` coerce for non-Series → ``return "abc"`` on a
    3-row batch yields length-3 character-split Series (``a``/``b``/``c``) that passes
    the length check and silently emits wrong values. dict/set same class. Pin requires
    the Series refuse (not length-mismatch or silent success).
    """
    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")

    @pandas_udf("string")
    def returns_str(series: pd.Series) -> pd.Series:
        return "abc"  # type: ignore[return-value]

    with pytest.raises(PySparkException, match=r"must return a pandas\.Series"):
        frame.select(returns_str(col("x")).alias("y")).to_arrow()

    @pandas_udf("long")
    def returns_dict(series: pd.Series) -> pd.Series:
        # Three keys → pd.Series(dict) length 3 would pass the length check if coerced.
        return {0: 10, 1: 20, 2: 30}  # type: ignore[return-value]

    with pytest.raises(PySparkException, match=r"must return a pandas\.Series"):
        frame.select(returns_dict(col("x")).alias("y")).to_arrow()

    @pandas_udf("long")
    def returns_list(series: pd.Series) -> pd.Series:
        # list is also non-Series — refuse (Series→Series contract; no coerce whitelist).
        return [1, 2, 3]  # type: ignore[return-value]

    with pytest.raises(PySparkException, match=r"must return a pandas\.Series"):
        frame.select(returns_list(col("x")).alias("y")).to_arrow()


def test_pandas_udf_hostile_return_type_sql_refused(spark: SparkSession) -> None:
    """Hostile PandasUDFColumn / post-build ``_return_type_sql`` mutation refuse (C3-SEC-001).

    MUTATION: bridge uses bare ``_sql_type_to_arrow`` without
    ``_normalize_pandas_udf_return_type_sql`` / ``_pandas_udf_arrow_type_for_return`` →
    ``variant`` fail-opens to ``pa.string()`` and select/action succeeds as string.
    """
    frame = spark.createDataFrame([(1,)], "x INT")

    def as_str(series: pd.Series) -> pd.Series:
        return series.astype(str)

    # Public marker constructor with unvalidated fail-open type.
    with pytest.raises((PySparkTypeError, UnsupportedOperationException)):
        PandasUDFColumn(as_str, "variant", [col("x")], "evil")

    with pytest.raises((PySparkTypeError, UnsupportedOperationException)):
        PandasUDFColumn(as_str, "array<variant>", [col("x")], "evil_nested")

    # Post-construction mutation of the slot attribute — bridge must revalidate.
    marker = double_long(col("x")).alias("y")
    marker._return_type_sql = "variant"
    with pytest.raises((PySparkTypeError, UnsupportedOperationException, PySparkException)):
        frame.select(marker).to_arrow()

    marker_nested = double_long(col("x")).alias("y")
    marker_nested._return_type_sql = "array<interval>"
    with pytest.raises((PySparkTypeError, UnsupportedOperationException, PySparkException)):
        frame.select(marker_nested).to_arrow()


def test_pandas_udf_return_type_schema_preserves_logical_identity(
    spark: SparkSession,
) -> None:
    """Declared returnType identity survives into DataFrame.schema (octo C4-Q-001).

    MUTATION: ``_normalize_pandas_udf_return_type_sql`` stores ``_data_type_to_sql_type``
    (engine tokens) → ``timestamp_ntz`` collapses to ``TIMESTAMP`` (TimestampType) and
    ``varchar(n)`` / ``char(n)`` collapse to ``STRING`` (StringType); schema loses the
    declared Spark type. Pin logical simpleString + class on both lazy schema and after
    action; marker ``_return_type_sql`` must also keep the logical spelling.
    """
    frame = spark.createDataFrame([(1,)], "x INT")

    @pandas_udf(TimestampNTZType())
    def as_ntz(series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, unit="s", utc=False)

    @pandas_udf("timestamp_ntz")
    def as_ntz_ddl(series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, unit="s", utc=False)

    @pandas_udf(VarcharType(10))
    def as_varchar(series: pd.Series) -> pd.Series:
        return series.astype(str)

    @pandas_udf("varchar(8)")
    def as_varchar_ddl(series: pd.Series) -> pd.Series:
        return series.astype(str)

    @pandas_udf(CharType(4))
    def as_char(series: pd.Series) -> pd.Series:
        return series.astype(str)

    @pandas_udf(TimestampType())
    def as_ts(series: pd.Series) -> pd.Series:
        return pd.to_datetime(series, unit="s", utc=True)

    cases: list[tuple[Any, type, str]] = [
        (as_ntz, TimestampNTZType, "timestamp_ntz"),
        (as_ntz_ddl, TimestampNTZType, "timestamp_ntz"),
        (as_varchar, VarcharType, "varchar(10)"),
        (as_varchar_ddl, VarcharType, "varchar(8)"),
        (as_char, CharType, "char(4)"),
        (as_ts, TimestampType, "timestamp"),
    ]
    for udf_fn, expected_cls, expected_simple in cases:
        marker = udf_fn(col("x")).alias("y")
        assert marker._return_type_sql == expected_simple
        out = frame.select(marker)
        field = out.schema.fields[0]
        assert isinstance(field.dataType, expected_cls), (
            f"lazy schema type for {expected_simple}: "
            f"got {type(field.dataType).__name__} simple={field.dataType.simpleString()!r}"
        )
        assert field.dataType.simpleString() == expected_simple
        # Action path rebuilds schema through the same normalize → fromDDL bridge.
        _ = out.to_arrow()
        field_after = out.schema.fields[0]
        assert isinstance(field_after.dataType, expected_cls)
        assert field_after.dataType.simpleString() == expected_simple
        if expected_cls is VarcharType:
            assert field_after.dataType.length == int(expected_simple.split("(")[1].rstrip(")"))
        if expected_cls is CharType:
            assert field_after.dataType.length == int(expected_simple.split("(")[1].rstrip(")"))


# ---------------------------------------------------------------------------
# M5 — SCALAR_ITER + pure GROUPED_AGG
# ---------------------------------------------------------------------------


def test_pandas_udf_scalar_iter_basic(spark: SparkSession) -> None:
    """Iterator[Series] → Iterator[Series] batch-iterator adapter (M5 READY bar)."""

    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def double_iter(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for series in batches:
            yield series.astype("int64") * 2

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.select(double_iter(col("x")).alias("y"))
    assert out.columns == ["y"]
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"y": 2}, {"y": 4}, {"y": 6}])


def test_pandas_udf_scalar_iter_multi_arg(spark: SparkSession) -> None:
    """Multi-arg SCALAR_ITER yields Iterator[tuple[Series, …]]."""

    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def add_iter(batches: Iterator[tuple[pd.Series, pd.Series]]) -> Iterator[pd.Series]:
        for left, right in batches:
            yield left.astype("int64") + right.astype("int64")

    frame = spark.createDataFrame([(1, 10), (2, 20)], "a INT, b INT")
    out = frame.select(add_iter(col("a"), col("b")).alias("s"))
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"s": 11}, {"s": 22}])


def test_pandas_udf_scalar_iter_with_pass_through(spark: SparkSession) -> None:
    """SCALAR_ITER + pass-through sibling in one select."""

    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def plus_one(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for series in batches:
            yield series.astype("int64") + 1

    frame = spark.createDataFrame([(1, "a"), (2, "b")], "x INT, k STRING")
    out = frame.select(col("k"), plus_one("x").alias("y"))
    assert _multiset(_rows(out.to_arrow())) == _multiset([{"k": "a", "y": 2}, {"k": "b", "y": 3}])


def test_pandas_udf_scalar_iter_wrong_batch_count_loud(spark: SparkSession) -> None:
    """SCALAR_ITER must yield one Series per input batch."""

    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def drop_all(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        list(batches)
        return
        yield  # pragma: no cover — make this a generator

    frame = spark.createDataFrame([(1,), (2,)], "x INT")
    with pytest.raises(PySparkException, match=r"SCALAR_ITER|expected"):
        frame.select(drop_all("x").alias("y")).to_arrow()


def test_pandas_udf_scalar_iter_dual_udf_independent_streams(spark: SparkSession) -> None:
    """Two SCALAR_ITER markers share batch buffer; each gets a full iterator (octo M5 C5)."""

    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def plus_one(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for series in batches:
            yield series.astype("int64") + 1

    @pandas_udf("long", PandasUDFType.SCALAR_ITER)
    def times_two(batches: Iterator[pd.Series]) -> Iterator[pd.Series]:
        for series in batches:
            yield series.astype("int64") * 2

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.select(plus_one("x").alias("a"), times_two("x").alias("b")).to_arrow()
    assert _multiset(_rows(out)) == _multiset(
        [
            {"a": 2, "b": 2},
            {"a": 3, "b": 4},
            {"a": 4, "b": 6},
        ]
    )


def test_pandas_udf_grouped_agg_pure(spark: SparkSession) -> None:
    """Pure GROUPED_AGG pandas_udf over applyInPandas machinery (M5 READY bar)."""

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [("a", 1.0), ("a", 3.0), ("b", 10.0), ("b", 20.0)],
        "k STRING, v DOUBLE",
    )
    out = frame.groupBy("k").agg(mean_udf("v").alias("m"))
    rows = _rows(out.to_arrow())
    by_key = {row["k"]: row["m"] for row in rows}
    assert by_key["a"] == pytest.approx(2.0)
    assert by_key["b"] == pytest.approx(15.0)


def test_pandas_udf_grouped_agg_global(spark: SparkSession) -> None:
    """Global groupBy() + GROUPED_AGG = one group."""

    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def sum_udf(series: pd.Series) -> int:
        return int(series.astype("int64").sum())

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.groupBy().agg(sum_udf("x").alias("s"))
    assert _rows(out.to_arrow()) == [{"s": 6}]


def test_pandas_udf_grouped_agg_large_group_stitch(spark: SparkSession) -> None:
    """Multi-batch single group stitches via applyInPandas (octo M5 C7).

    MUTATION: per-batch regroup without stitch → count under-reports (<25000).
    """

    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def count_udf(series: pd.Series) -> int:
        return len(series)

    rows = [(1, index) for index in range(25_000)]
    frame = spark.createDataFrame(rows, "k INT, v INT")
    out = frame.groupBy("k").agg(count_udf("v").alias("n"))
    assert _rows(out.to_arrow()) == [{"k": 1, "n": 25_000}]


def test_pandas_udf_grouped_agg_cube_rollup_refuse(spark: SparkSession) -> None:
    """GROUPED_AGG after cube/rollup is loud (no applyInPandas path) (octo M5 C6)."""

    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def sum_udf(series: pd.Series) -> int:
        return int(series.astype("int64").sum())

    frame = spark.createDataFrame([("a", 1), ("b", 2)], "k STRING, v INT")
    with pytest.raises(AnalysisException, match=r"cube|rollup|grouping"):
        frame.cube("k").agg(sum_udf("v").alias("s"))
    with pytest.raises(AnalysisException, match=r"cube|rollup|grouping"):
        frame.rollup("k").agg(sum_udf("v").alias("s"))


def test_pandas_udf_grouped_agg_mixed_builtin(spark: SparkSession) -> None:
    """M6: mixed UDF + builtin agg via two-pass plan-built join on group keys.

    Order-independent: UDF-first and builtin-first both compose (octo M5 C3 → M6 ship).
    MUTATION: Python-merge of collected groups → would still pass value pins but violates
    the plan-built join contract (engine join, not multiset merge in the facade).
    """

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [("a", 1.0), ("a", 3.0), ("b", 10.0), ("b", 20.0)],
        "k STRING, v DOUBLE",
    )
    out_udf_first = frame.groupBy("k").agg(mean_udf("v").alias("m"), f_sum("v").alias("s"))
    out_builtin_first = frame.groupBy("k").agg(f_sum("v").alias("s"), mean_udf("v").alias("m"))
    for out in (out_udf_first, out_builtin_first):
        rows = {row["k"]: row for row in out.to_arrow().to_pylist()}
        assert rows["a"]["m"] == pytest.approx(2.0)
        assert rows["a"]["s"] == pytest.approx(4.0)
        assert rows["b"]["m"] == pytest.approx(15.0)
        assert rows["b"]["s"] == pytest.approx(30.0)
    # Column order: keys first, then aggregates in caller order.
    assert out_udf_first.columns == ["k", "m", "s"]
    assert out_builtin_first.columns == ["k", "s", "m"]


def test_pandas_udf_grouped_agg_mixed_global(spark: SparkSession) -> None:
    """M6: global groupBy() mixed UDF+builtin via plan-built crossJoin of single-row sides."""

    @pandas_udf("long", PandasUDFType.GROUPED_AGG)
    def sum_udf(series: pd.Series) -> int:
        return int(series.astype("int64").sum())

    frame = spark.createDataFrame([(1,), (2,), (3,)], "x INT")
    out = frame.groupBy().agg(sum_udf("x").alias("u"), f_sum("x").alias("s"))
    rows = out.to_arrow().to_pylist()
    assert len(rows) == 1
    assert rows[0]["u"] == 6
    assert rows[0]["s"] == pytest.approx(6.0)


def test_pandas_udf_grouped_agg_mixed_null_group_keys(spark: SparkSession) -> None:
    """M6 octo C1: NULL group keys must survive mixed UDF+builtin join (null-safe equi-join).

    MUTATION: name-list ``join(on=keys, how='inner')`` uses SQL ``=`` so ``NULL = NULL`` is
    unknown → the null group is silently dropped while pure UDF / pure builtin both keep it.
    """

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [(None, 1.0), (None, 3.0), ("a", 10.0), ("a", 30.0)],
        "k STRING, v DOUBLE",
    )
    pure = {
        row["k"]: row["m"] for row in frame.groupBy("k").agg(mean_udf("v").alias("m")).collect()
    }
    mixed_rows = frame.groupBy("k").agg(mean_udf("v").alias("m"), f_sum("v").alias("s")).collect()
    mixed = {row["k"]: row for row in mixed_rows}
    assert set(mixed) == set(pure) == {None, "a"}
    assert mixed[None]["m"] == pytest.approx(2.0)
    assert mixed[None]["s"] == pytest.approx(4.0)
    assert mixed["a"]["m"] == pytest.approx(20.0)
    assert mixed["a"]["s"] == pytest.approx(40.0)


def test_pandas_udf_grouped_agg_in_select_refuse(spark: SparkSession) -> None:
    """GROUPED_AGG markers cannot appear in select/withColumn."""

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame([(1.0,), (2.0,)], "v DOUBLE")
    with pytest.raises(AnalysisException, match=r"GROUPED_AGG|groupBy"):
        frame.select(mean_udf("v").alias("m"))


def test_pandas_udf_scalar_in_groupby_agg_refuse(spark: SparkSession) -> None:
    """SCALAR pandas_udf in groupBy().agg is refused (needs GROUPED_AGG)."""

    frame = spark.createDataFrame([("a", 1), ("a", 2)], "k STRING, v INT")
    with pytest.raises(AnalysisException, match=r"GROUPED_AGG"):
        frame.groupBy("k").agg(double_long("v").alias("d"))


def test_pandas_udf_type_constants_match_spark() -> None:
    """PandasUDFType int values match PySpark 4.1.2 (oracle surface)."""
    assert PandasUDFType.SCALAR == 200
    assert PandasUDFType.GROUPED_MAP == 201
    assert PandasUDFType.GROUPED_AGG == 202
    assert PandasUDFType.SCALAR_ITER == 204


def test_pandas_udf_window_function_type_loud_refuse() -> None:
    """functionType=WINDOW tag remains loud — use GROUPED_AGG + .over (M6).

    MUTATION: ``WINDOW`` not recognized as functionType-like → positional
    ``@pandas_udf("long", "WINDOW")`` hits dual-returnType refuse (misleading) instead
    of UOE naming window / GROUPED_MAP seeds.
    """
    with pytest.raises(UnsupportedOperationException, match=r"WINDOW|window|M6-class|GROUPED_MAP"):

        @pandas_udf("long", "WINDOW")
        def window_pos(series: pd.Series) -> pd.Series:
            return series

    with pytest.raises(UnsupportedOperationException, match=r"WINDOW|window|M6-class|GROUPED_MAP"):

        @pandas_udf("long", functionType="WINDOW")
        def window_kw(series: pd.Series) -> pd.Series:
            return series

    with pytest.raises(UnsupportedOperationException, match=r"GROUPED_MAP|M6-class"):

        @pandas_udf("long", PandasUDFType.GROUPED_MAP)
        def gmap(series: pd.Series) -> pd.Series:
            return series


def test_pandas_udf_window_partition_unbounded(spark: SparkSession) -> None:
    """M6: GROUPED_AGG.over(Window.partitionBy) unbounded whole-partition form.

    Plan-built: groupBy(partition).agg(udf) join back on keys (not Python merge).
    """
    from repark.window import Window

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [("a", 1.0), ("a", 3.0), ("b", 10.0), ("b", 30.0)],
        "k STRING, v DOUBLE",
    )
    window = Window.partitionBy("k")
    out = frame.select("k", "v", mean_udf("v").over(window).alias("m"))
    rows = out.to_arrow().to_pylist()
    by_key = {}
    for row in rows:
        by_key.setdefault(row["k"], []).append(row["m"])
    assert by_key["a"] == pytest.approx([2.0, 2.0])
    assert by_key["b"] == pytest.approx([20.0, 20.0])
    # withColumn path
    with_col = frame.withColumn("m", mean_udf("v").over(window))
    assert "m" in with_col.columns
    wc_rows = with_col.to_arrow().to_pylist()
    a_means = [row["m"] for row in wc_rows if row["k"] == "a"]
    assert a_means == pytest.approx([2.0, 2.0])


def test_pandas_udf_window_order_by_default_frame(spark: SparkSession) -> None:
    """M7: orderBy → default ROWS UNBOUNDED PRECEDING … CURRENT ROW (running mean).

    Values ordered 1, 3, 5 under key a → running means 1.0, 2.0, 3.0 (not whole-partition 3.0).
    """
    from repark.window import Window

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [("a", 1.0), ("a", 3.0), ("a", 5.0), ("b", 10.0), ("b", 30.0)],
        "k STRING, v DOUBLE",
    )
    window = Window.partitionBy("k").orderBy("v")
    out = frame.select("k", "v", mean_udf("v").over(window).alias("m"))
    rows = sorted(out.to_arrow().to_pylist(), key=lambda row: (row["k"], row["v"]))
    a_means = [row["m"] for row in rows if row["k"] == "a"]
    b_means = [row["m"] for row in rows if row["k"] == "b"]
    assert a_means == pytest.approx([1.0, 2.0, 3.0])
    assert b_means == pytest.approx([10.0, 20.0])


def test_pandas_udf_window_rows_between_duck_typed(spark: SparkSession) -> None:
    """M7: duck-typed _frame_start/_frame_end (G2 rowsBetween seam) → rolling window.

    Uses a WindowSpec subclass so slots can carry frame bounds without editing window.py.
    Frame rowsBetween(-1, 0): mean of previous+current.
    """
    from repark.window import Window, WindowSpec

    class _WindowSpecWithRows(WindowSpec):
        """Test-only G2-shaped frame carrier (slots-safe subclass)."""

        __slots__ = ("_frame_end", "_frame_start", "_frame_type")

        def __init__(
            self,
            partition_columns: list[Any],
            order_columns: list[Any],
            *,
            frame_start: int | None,
            frame_end: int | None,
        ) -> None:
            super().__init__(partition_columns, order_columns)
            self._frame_start = frame_start
            self._frame_end = frame_end
            self._frame_type = "rows"

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [("a", 1.0), ("a", 3.0), ("a", 5.0)],
        "k STRING, v DOUBLE",
    )
    base = Window.partitionBy("k").orderBy("v")
    window = _WindowSpecWithRows(
        list(base._partition_columns),
        list(base._order_columns),
        frame_start=-1,
        frame_end=0,
    )
    out = frame.select("k", "v", mean_udf("v").over(window).alias("m"))
    rows = sorted(out.to_arrow().to_pylist(), key=lambda row: row["v"])
    # v=1 → [1] mean 1; v=3 → [1,3] mean 2; v=5 → [3,5] mean 4
    assert [row["m"] for row in rows] == pytest.approx([1.0, 2.0, 4.0])


def test_pandas_udf_window_select_alias_overwrites_source_column(spark: SparkSession) -> None:
    """M6 octo C2: ``select("v", mean.over(...).alias("v"))`` must keep the window value.

    MUTATION: first-wins ``seen`` set drops the UDF out name → source ``v`` values leak
    through; or null-safe join prefers left for non-keys so even last-wins final_names
    still project source ``v`` when both sides carry the name.
    """
    from repark.window import Window

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [("a", 1.0), ("a", 3.0), ("b", 10.0)],
        "k STRING, v DOUBLE",
    )
    window = Window.partitionBy("k")
    out = frame.select("v", mean_udf("v").over(window).alias("v"))
    assert out.columns == ["v"]
    values = sorted(row["v"] for row in out.to_arrow().to_pylist())
    # Two rows for "a" (mean 2.0) + one for "b" (mean 10.0) — not the raw 1/3/10.
    assert values == pytest.approx([2.0, 2.0, 10.0])
    # Explicit source + overwrite in one select.
    both = frame.select("k", "v", mean_udf("v").over(window).alias("v"))
    assert both.columns == ["k", "v"]
    by_key = {}
    for row in both.to_arrow().to_pylist():
        by_key.setdefault(row["k"], []).append(row["v"])
    assert by_key["a"] == pytest.approx([2.0, 2.0])
    assert by_key["b"] == pytest.approx([10.0])


def test_pandas_udf_window_null_partition_keys(spark: SparkSession) -> None:
    """M6 octo C1: NULL partition keys keep source rows and share the null-group mean.

    MUTATION: name-list equi-join drops ``NULL = NULL`` partitions → only non-null keys
    survive (silently wrong multiset / missing rows).
    """
    from repark.window import Window

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_udf(series: pd.Series) -> float:
        return float(series.mean())

    frame = spark.createDataFrame(
        [(None, 1.0), (None, 3.0), ("a", 10.0), ("a", 30.0)],
        "k STRING, v DOUBLE",
    )
    window = Window.partitionBy("k")
    rows = frame.select("k", "v", mean_udf("v").over(window).alias("m")).to_arrow().to_pylist()
    assert len(rows) == 4
    by_key: dict[Any, list[float]] = {}
    for row in rows:
        by_key.setdefault(row["k"], []).append(row["m"])
    assert by_key[None] == pytest.approx([2.0, 2.0])
    assert by_key["a"] == pytest.approx([20.0, 20.0])

    # Multi-key: all-null composite partition must also match.
    multi = spark.createDataFrame(
        [
            (None, None, 1.0),
            (None, None, 3.0),
            ("a", None, 5.0),
            ("a", None, 7.0),
        ],
        "k STRING, g STRING, v DOUBLE",
    )
    multi_window = Window.partitionBy("k", "g")
    multi_rows = (
        multi.select("k", "g", "v", mean_udf("v").over(multi_window).alias("m"))
        .to_arrow()
        .to_pylist()
    )
    assert len(multi_rows) == 4
    null_means = [row["m"] for row in multi_rows if row["k"] is None and row["g"] is None]
    assert null_means == pytest.approx([2.0, 2.0])
    a_none_means = [row["m"] for row in multi_rows if row["k"] == "a" and row["g"] is None]
    assert a_none_means == pytest.approx([6.0, 6.0])


def test_pandas_udf_grouped_agg_hostile_return_type_sql_refused(spark: SparkSession) -> None:
    """Post-build ``_return_type_sql`` mutation must not fail-open (octo M5 C1).

    MUTATION: GROUPED_AGG builds schema via bare ``DataType.fromDDL`` without
    ``_normalize_pandas_udf_return_type_sql`` / ``_pandas_udf_arrow_type_for_return`` →
    ``variant`` fail-opens (string path) and ``groupBy().agg`` succeeds as string.
    """

    @pandas_udf("string", PandasUDFType.GROUPED_AGG)
    def as_str(series: pd.Series) -> str:
        return "ok"

    frame = spark.createDataFrame([("a", 1.0)], "k STRING, v DOUBLE")
    marker = as_str("v").alias("m")
    marker._return_type_sql = "variant"
    with pytest.raises((PySparkTypeError, UnsupportedOperationException, PySparkException)):
        frame.groupBy("k").agg(marker).to_arrow()

    marker_nested = as_str("v").alias("m")
    marker_nested._return_type_sql = "array<interval>"
    with pytest.raises((PySparkTypeError, UnsupportedOperationException, PySparkException)):
        frame.groupBy("k").agg(marker_nested).to_arrow()


def test_pandas_udf_grouped_agg_multi_key_and_multi_arg(spark: SparkSession) -> None:
    """Pure GROUPED_AGG multi-key + multi-arg (bridge reuse pin)."""

    @pandas_udf("double", PandasUDFType.GROUPED_AGG)
    def mean_prod(left: pd.Series, right: pd.Series) -> float:
        return float((left.astype("float64") * right.astype("float64")).mean())

    frame = spark.createDataFrame(
        [
            (1, "a", 2.0, 3.0),
            (1, "a", 4.0, 5.0),
            (1, "b", 1.0, 1.0),
        ],
        "g1 INT, g2 STRING, x DOUBLE, y DOUBLE",
    )
    out = frame.groupBy("g1", "g2").agg(mean_prod("x", "y").alias("m"))
    by_key = {(row["g1"], row["g2"]): row["m"] for row in _rows(out.to_arrow())}
    assert by_key[(1, "a")] == pytest.approx(13.0)  # mean(6, 20)
    assert by_key[(1, "b")] == pytest.approx(1.0)
