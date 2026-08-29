"""E1 error-class harvest pins — true-EC mechanisms (check_error class + parameter keys).

Apache rows gated here: PySparkRuntimeError hierarchy, interval constructors, facade
pre-checks (bucket/greatest/from_*/schema_of_*/json_tuple), alias(metadata=), Column
__getitem__, native exception surface shim.
"""

from __future__ import annotations

import pytest

from repark import _native
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkException,
    PySparkNotImplementedError,
    PySparkRuntimeError,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark.column import Column
from repark.spark.session import ReparkSession
from repark.spark.types import (
    DayTimeIntervalType,
    StructType,
    YearMonthIntervalType,
)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-e1-errorclass").getOrCreate()
    yield session
    session.stop()


# Family A — PySparkRuntimeError under repark tree + interval constructors


def test_pyspark_runtime_error_under_repark_tree() -> None:
    assert issubclass(PySparkRuntimeError, PySparkException)
    assert issubclass(PySparkRuntimeError, RuntimeError)
    error = PySparkRuntimeError(
        errorClass="INVALID_INTERVAL_CASTING",
        messageParameters={"start_field": "None", "end_field": "3"},
    )
    assert isinstance(error, PySparkException)
    assert error.getCondition() == "INVALID_INTERVAL_CASTING"
    assert error.getErrorClass() == "INVALID_INTERVAL_CASTING"
    assert error.getMessageParameters() == {
        "start_field": "None",
        "end_field": "3",
    }
    assert error.getQueryContext() == []


def test_pyspark_not_implemented_error_under_repark_tree() -> None:
    assert issubclass(PySparkNotImplementedError, PySparkException)
    assert issubclass(PySparkNotImplementedError, NotImplementedError)
    error = PySparkNotImplementedError(errorClass="NOT_IMPLEMENTED", messageParameters={})
    assert isinstance(error, PySparkException)
    assert error.getCondition() == "NOT_IMPLEMENTED"


def test_daytime_interval_type_constructor_invalid_raises() -> None:
    assert DayTimeIntervalType().simpleString() == "interval day to second"
    assert DayTimeIntervalType(DayTimeIntervalType.DAY).simpleString() == "interval day"
    assert (
        DayTimeIntervalType(DayTimeIntervalType.HOUR, DayTimeIntervalType.SECOND).simpleString()
        == "interval hour to second"
    )

    with pytest.raises(PySparkRuntimeError) as caught:
        DayTimeIntervalType(endField=DayTimeIntervalType.SECOND)
    assert caught.value.getCondition() == "INVALID_INTERVAL_CASTING"
    assert caught.value.getMessageParameters() == {
        "start_field": "None",
        "end_field": "3",
    }
    assert isinstance(caught.value, PySparkException)

    with pytest.raises(PySparkRuntimeError) as caught_bad:
        DayTimeIntervalType(123)
    assert caught_bad.value.getMessageParameters() == {
        "start_field": "123",
        "end_field": "123",
    }

    with pytest.raises(PySparkRuntimeError) as caught_end:
        DayTimeIntervalType(DayTimeIntervalType.DAY, 321)
    assert caught_end.value.getMessageParameters() == {
        "start_field": "0",
        "end_field": "321",
    }


def test_yearmonth_interval_type_constructor_invalid_raises() -> None:
    assert YearMonthIntervalType().simpleString() == "interval year to month"
    assert YearMonthIntervalType(YearMonthIntervalType.YEAR).simpleString() == "interval year"

    with pytest.raises(PySparkRuntimeError) as caught:
        YearMonthIntervalType(endField=3)
    assert caught.value.getCondition() == "INVALID_INTERVAL_CASTING"
    assert caught.value.getMessageParameters() == {
        "start_field": "None",
        "end_field": "3",
    }
    assert isinstance(caught.value, PySparkException)

    # Mutation-proof both field memberships (octo C7-Q-001): endField=3 alone stays green if
    # only start_field is checked. Mirror DayTime pins.
    with pytest.raises(PySparkRuntimeError) as caught_bad:
        YearMonthIntervalType(123)
    assert caught_bad.value.getCondition() == "INVALID_INTERVAL_CASTING"
    assert caught_bad.value.getMessageParameters() == {
        "start_field": "123",
        "end_field": "123",
    }

    with pytest.raises(PySparkRuntimeError) as caught_end:
        YearMonthIntervalType(YearMonthIntervalType.YEAR, 321)
    assert caught_end.value.getCondition() == "INVALID_INTERVAL_CASTING"
    assert caught_end.value.getMessageParameters() == {
        "start_field": "0",
        "end_field": "321",
    }


def test_tree_string_includes_ansi_intervals() -> None:
    schema = (
        StructType()
        .add("ym_interval_1", YearMonthIntervalType())
        .add("dt_interval_1", DayTimeIntervalType())
    )
    lines = schema.treeString().split("\n")
    assert " |-- ym_interval_1: interval year to month (nullable = true)" in lines
    assert " |-- dt_interval_1: interval day to second (nullable = true)" in lines


# Family E — native exception surface shim (check_error degrades cleanly)


def test_native_exception_surface_shim_methods() -> None:
    for exception_type in (
        _native.PySparkException,
        _native.AnalysisException,
        _native.ParseException,
        _native.UnsupportedOperationException,
        _native.IllegalArgumentException,
    ):
        error = exception_type("native diagnostic")
        assert error.getCondition() is None
        assert error.getErrorClass() is None
        assert error.getMessageParameters() is None
        assert error.getQueryContext() == []
        # Identity re-export
        assert exception_type is getattr(
            __import__("repark.errors", fromlist=[exception_type.__name__]),
            exception_type.__name__,
        )


def test_engine_analysis_error_has_surface_methods(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SELECT * FROM __no_such_table__")
    assert caught.value.getCondition() is None
    assert caught.value.getQueryContext() == []


# Family B — facade pre-checks


def test_bucket_bad_num_buckets_raises_not_column_or_int() -> None:
    with pytest.raises(PySparkTypeError) as caught:
        F.bucket("5", "id")  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_COLUMN_OR_INT"
    assert caught.value.getMessageParameters() == {
        "arg_name": "numBuckets",
        "arg_type": "str",
    }


def test_greatest_one_column_raises_wrong_num_columns(spark: ReparkSession) -> None:
    frame = spark.range(10)
    with pytest.raises(PySparkValueError) as caught:
        F.greatest(frame.id)
    assert caught.value.getCondition() == "WRONG_NUM_COLUMNS"
    assert caught.value.getMessageParameters() == {
        "func_name": "greatest",
        "num_cols": "2",
    }


def test_least_one_column_raises_wrong_num_columns(spark: ReparkSession) -> None:
    frame = spark.range(10)
    with pytest.raises(PySparkValueError) as caught:
        F.least(frame.id)
    assert caught.value.getCondition() == "WRONG_NUM_COLUMNS"
    assert caught.value.getMessageParameters() == {
        "func_name": "least",
        "num_cols": "2",
    }


def test_from_csv_bad_schema_raises_not_column_or_str(spark: ReparkSession) -> None:
    frame = spark.range(10)
    with pytest.raises(PySparkTypeError) as caught:
        F.from_csv(frame.id, 1)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_COLUMN_OR_STR"
    assert caught.value.getMessageParameters() == {
        "arg_name": "schema",
        "arg_type": "int",
    }


def test_from_xml_bad_schema_raises_not_column_or_str_or_struct(spark: ReparkSession) -> None:
    frame = spark.range(10)
    with pytest.raises(PySparkTypeError) as caught:
        F.from_xml(frame.id, 1)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_COLUMN_OR_STR_OR_STRUCT"
    assert caught.value.getMessageParameters() == {
        "arg_name": "schema",
        "arg_type": "int",
    }


@pytest.mark.parametrize(
    ("function_name", "arg_name"),
    [
        ("schema_of_csv", "csv"),
        ("schema_of_json", "json"),
        ("schema_of_xml", "xml"),
    ],
)
def test_schema_of_star_bad_arg_raises_not_column_or_str(function_name: str, arg_name: str) -> None:
    function = getattr(F, function_name)
    with pytest.raises(PySparkTypeError) as caught:
        function(1)
    assert caught.value.getCondition() == "NOT_COLUMN_OR_STR"
    assert caught.value.getMessageParameters() == {
        "arg_name": arg_name,
        "arg_type": "int",
    }


def test_json_tuple_empty_fields_raises_cannot_be_empty(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(
        [
            ("1", """{"f1": "value1", "f2": "value2"}"""),
        ],
        ("key", "jstring"),
    )
    with pytest.raises(PySparkValueError, match="At least one field must be specified"):
        frame.select(F.json_tuple(frame.jstring))
    # Also pin structured keys for check_error bar.
    with pytest.raises(PySparkValueError) as caught:
        F.json_tuple(frame.jstring)
    assert caught.value.getCondition() == "CANNOT_BE_EMPTY"
    assert caught.value.getMessageParameters() == {"item": "field"}


def test_raise_error_none_raises_not_column_or_str() -> None:
    with pytest.raises(PySparkTypeError) as caught:
        F.raise_error(None)  # type: ignore[arg-type]
    assert caught.value.getCondition() == "NOT_COLUMN_OR_STR"
    assert caught.value.getMessageParameters() == {
        "arg_name": "errMsg",
        "arg_type": "NoneType",
    }


# Family C / D — Column.alias metadata + __getitem__


def test_alias_metadata_multi_name_raises_only_allowed_for_single_column(
    spark: ReparkSession,
) -> None:
    with pytest.raises(PySparkValueError) as caught:
        spark.range(1).id.alias("a", "b", metadata={})
    assert caught.value.getCondition() == "ONLY_ALLOWED_FOR_SINGLE_COLUMN"
    assert caught.value.getMessageParameters() == {"arg_name": "metadata"}


def test_alias_metadata_single_name_accepted(spark: ReparkSession) -> None:
    # metadata is accepted (ignored on engine path); must not TypeError.
    column = spark.range(1).id.alias("x", metadata={"max": 99})
    assert isinstance(column, Column)


def test_column_getitem_returns_column_and_rejects_step() -> None:
    from repark.spark.functions import col

    assert isinstance(col("foo")[1:3], Column)
    assert isinstance(col("foo")[0], Column)
    assert isinstance(col("foo")["bar"], Column)
    with pytest.raises(PySparkValueError) as caught:
        _ = col("foo")[0:10:2]
    assert caught.value.getCondition() == "SLICE_WITH_STEP"
    assert caught.value.getMessageParameters() == {}
    # Dual-base: still a ValueError for pre-existing except ValueError handlers.
    assert isinstance(caught.value, ValueError)


def test_column_getitem_open_bound_slice_no_invented_defaults() -> None:
    """octo C3-L-001: open-bound slices must not invent start=1 / length=start.

    Apache classic passes ``slice.start`` / ``slice.stop`` straight to ``substr``; ``None``
    bounds type-error. RePark must not silently lower ``col[:n]`` → ``substr(1,n)``,
    ``col[i:]`` → ``substr(i,i)``, or ``col[:]`` → ``substr(1,1)``.
    """
    from repark.spark.functions import col

    # col[:3] → substr(None, 3) → type(None) != type(int)
    with pytest.raises(PySparkTypeError) as open_stop:
        _ = col("foo")[:3]
    assert open_stop.value.getCondition() == "NOT_SAME_TYPE"
    assert open_stop.value.getMessageParameters() == {
        "arg_name1": "startPos",
        "arg_name2": "length",
        "arg_type1": "NoneType",
        "arg_type2": "int",
    }

    # col[5:] → substr(5, None)
    with pytest.raises(PySparkTypeError) as open_start:
        _ = col("foo")[5:]
    assert open_start.value.getCondition() == "NOT_SAME_TYPE"
    assert open_start.value.getMessageParameters() == {
        "arg_name1": "startPos",
        "arg_name2": "length",
        "arg_type1": "int",
        "arg_type2": "NoneType",
    }

    # col[:] → substr(None, None) → same type, not int/Column
    with pytest.raises(PySparkTypeError) as open_both:
        _ = col("foo")[:]
    assert open_both.value.getCondition() == "NOT_COLUMN_OR_INT"
    assert open_both.value.getMessageParameters() == {
        "arg_name": "startPos",
        "arg_type": "NoneType",
    }

    # Closed int bounds still build a Column (plan-time; no silent wrong defaults).
    closed = col("foo")[1:3]
    assert isinstance(closed, Column)
    assert "substr" in closed.spark_display_part()
    assert "1" in closed.spark_display_part() and "3" in closed.spark_display_part()


def test_column_getitem_slice_substr_spark_semantics(spark: ReparkSession) -> None:
    """octo C7-L-001: closed getitem slice must use Spark substr, not DF built-in.

    Classic ``Column.__getitem__(slice)`` → ``substr(start, stop)`` with Spark position rules
    (0 acts as 1; negatives from end); the DF ``substring`` 3-arg arm would return ``'he'``
    for ``'hello'[0:3]`` where Apache/SQL returns ``'hel'``. Pin value + type on the Arrow path.
    """
    frame = spark.createDataFrame([("hello",)], ["s"])
    # pos 0 ≡ 1, length 3 → 'hel' (DF built-in → 'he')
    zero_pos = frame.select(frame.s[0:3].alias("v")).collect()[0][0]
    assert zero_pos == "hel"
    assert isinstance(zero_pos, str)
    # positive 1-based window
    assert frame.select(frame.s[1:3].alias("v")).collect()[0][0] == "hel"
    assert frame.select(frame.s[2:3].alias("v")).collect()[0][0] == "ell"
    # negative start (count from end): substr(-3, 2) on 'hello' → 'll'
    assert frame.select(frame.s[-3:2].alias("v")).collect()[0][0] == "ll"
    # negative start clipped before string start: substr(-7, 3) → 'h'
    assert frame.select(frame.s[-7:3].alias("v")).collect()[0][0] == "h"


def test_column_getitem_int_extracts_element_not_slice(spark: ReparkSession) -> None:
    """octo C1-L-001 / C1-Q-002: int getitem is element extract (not array_slice / fail-open)."""
    frame = spark.createDataFrame([([10, 20, 30],)], ["arr"])
    rows = frame.select(frame.arr[0].alias("v"), frame.arr[2].alias("w")).collect()
    assert rows[0][0] == 10
    assert rows[0][1] == 30
    assert not isinstance(rows[0][0], list)


def test_column_getitem_str_field_native_not_parent(spark: ReparkSession) -> None:
    """octo C1-L-002: string getitem evaluates field, not the parent struct/map value."""
    frame = spark.createDataFrame([(1,)], ["id"]).selectExpr("named_struct('a', id) as s")
    rows = frame.select(frame.s["a"].alias("v")).collect()
    assert rows[0][0] == 1


def test_column_getitem_map_str_key_extracts_value(spark: ReparkSession) -> None:
    """octo C4-Q-001: map string getitem extracts value (not parent map / unpinned claim).

    Apache ``test_field_accessor`` asserts ``df.select(df.d["k"]).first()[0] == "v"`` on
    ``createDataFrame([Row(..., d={"k": "v"})])``.
    """
    from repark import Row

    frame = spark.createDataFrame([Row(d={"k": "v"})])
    parent = frame.select(frame.d).collect()[0][0]
    extracted = frame.select(frame.d["k"].alias("v")).collect()[0][0]
    assert extracted == "v"
    # Fail-open to the parent map/list-of-pairs must not pass.
    assert extracted != parent
    assert not isinstance(extracted, (list, dict, tuple))


def test_column_getitem_column_key_not_parent(spark: ReparkSession) -> None:
    """octo C2-L-001: Column-key getitem extracts, never returns the parent container."""
    frame = spark.sql("SELECT array(10, 20, 30) AS arr, map(0, 100, 1, 200) AS m, 0 AS id")
    array_rows = frame.select(frame.arr[F.lit(0)].alias("v")).collect()
    assert array_rows[0][0] == 10
    assert not isinstance(array_rows[0][0], list)

    map_rows = frame.select(frame.m[F.col("id")].alias("v")).collect()
    assert map_rows[0][0] == 100
    assert not isinstance(map_rows[0][0], (list, dict, tuple))


def test_column_getitem_non_int_non_str_not_parent(spark: ReparkSession) -> None:
    """octo C2-L-001: bool/float/None keys must not fail-open to the parent array values."""
    frame = spark.sql("SELECT array(10, 20, 30) AS arr")
    parent = frame.select(frame.arr).collect()[0][0]
    assert parent == [10, 20, 30]

    for key in (True, 1.5, None):
        try:
            value = frame.select(frame.arr[key].alias("v")).collect()[0][0]
        except Exception:
            # Fail-loud is acceptable; fail-open to parent is not.
            continue
        assert value != parent, f"arr[{key!r}] must not return the parent array"


def test_column_getitem_str_sql_expr_quotes_hostile_ident() -> None:
    """octo C1-SEC-001: string keys are double-quoted in free-SQL; injection cannot widen."""
    from repark.spark.functions import col

    hostile = col("id")["id OR true"]
    sql = hostile.sql_expr_part()
    assert sql == '("id")."id OR true"'
    assert " OR " not in sql.replace('"id OR true"', "")

    quoted_quote = col("s")['a"b']
    assert quoted_quote.sql_expr_part() == '("s")."a""b"'


def test_column_iter_raises_not_iterable() -> None:
    from repark.spark.functions import col

    with pytest.raises(PySparkTypeError) as caught:
        for _item in col("foo"):
            break
    assert caught.value.getCondition() == "NOT_ITERABLE"
    assert caught.value.getMessageParameters() == {"objectName": "Column"}


def test_parse_exception_still_analysis(spark: ReparkSession) -> None:
    # Taxonomy regression: parse errors remain AnalysisException.
    with pytest.raises(ParseException):
        spark.sql("SELECT * FROM")
    with pytest.raises(AnalysisException):
        spark.sql("SELECT * FROM")
    assert issubclass(IllegalArgumentException, PySparkException)
    assert issubclass(UnsupportedOperationException, PySparkException)
