"""PERF-FACADE-CDF-1 — the column-wise createDataFrame path equals the legacy path.

pins: perf-facade-cdf-1/C-002, C-003, C-004, C-007, C-009

Oracle: the legacy row-wise path, kept callable as
``create_dataframe_rows._arrow_table_from_raw_tuples_legacy``. Every case runs both
dispatchers on the same input and compares Arrow field types, Arrow values and
``collect()`` by value and by ``(type name, repr)`` signature.
"""

from __future__ import annotations

import datetime
import decimal
from collections.abc import Iterator
from typing import Any

import _live_parity as lp
import pyarrow as pa
import pytest

import repark.spark.session.create_dataframe_rows as rows_module
from repark import ReparkSession
from repark.spark.row import Row
from repark.spark.session import create_dataframe_columns as columns_module


@pytest.fixture
def cdf_session() -> Iterator[ReparkSession]:
    """One repark session per case, stopped afterwards."""
    session = ReparkSession.builder.appName("pytest-perf-facade-cdf-1").getOrCreate()
    yield session
    session.stop()


def _cell_signature(value: object) -> tuple[str, str]:
    """The ``(type name, repr)`` pair that makes a silently retyped cell red."""
    return (type(value).__name__, repr(value))


def _row_signature(row: Row) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...]]:
    """Field names plus every cell signature of one collected row."""
    return (tuple(row.__fields__), tuple(_cell_signature(value) for value in row))


def _arrow_field_signature(frame: Any) -> list[tuple[str, str, bool]]:
    """The Arrow-path type pin: name, type string and nullability per field."""
    return [(field.name, str(field.type), field.nullable) for field in frame.to_arrow().schema]


def _frames_old_new(
    monkeypatch: pytest.MonkeyPatch, session: Any, data: Any, schema: Any
) -> tuple[Any, Any]:
    """Build one ``createDataFrame`` on the legacy path and on the shipped path."""
    shipped = rows_module._arrow_table_from_raw_tuples
    monkeypatch.setattr(
        rows_module, "_arrow_table_from_raw_tuples", rows_module._arrow_table_from_raw_tuples_legacy
    )
    old_frame = session.createDataFrame(data, schema)
    monkeypatch.setattr(rows_module, "_arrow_table_from_raw_tuples", shipped)
    new_frame = session.createDataFrame(data, schema)
    return old_frame, new_frame


def _pylist_signature(frame: Any) -> list[str]:
    """One repr per Arrow row, so NaN cells compare instead of never matching."""
    return [repr(row) for row in frame.to_arrow().to_pylist()]


def _assert_frames_equal(old_frame: Any, new_frame: Any) -> None:
    """Arrow types, Arrow values and collected rows agree on both paths."""
    assert _arrow_field_signature(old_frame) == _arrow_field_signature(new_frame)
    assert _pylist_signature(old_frame) == _pylist_signature(new_frame)
    old_rows = old_frame.collect()
    new_rows = new_frame.collect()
    assert [_row_signature(row) for row in old_rows] == [_row_signature(row) for row in new_rows]


def _assert_create_equal(
    monkeypatch: pytest.MonkeyPatch, session: Any, data: Any, schema: Any
) -> None:
    """One input, both dispatchers, every observable equal."""
    old_frame, new_frame = _frames_old_new(monkeypatch, session, data, schema)
    _assert_frames_equal(old_frame, new_frame)


def _assert_refusals_equal(
    monkeypatch: pytest.MonkeyPatch, session: Any, data: Any, schema: Any
) -> None:
    """Both dispatchers refuse with the same exception type and exact text."""
    shipped = rows_module._arrow_table_from_raw_tuples
    monkeypatch.setattr(
        rows_module, "_arrow_table_from_raw_tuples", rows_module._arrow_table_from_raw_tuples_legacy
    )
    try:
        session.createDataFrame(data, schema)
        old_error: BaseException | None = None
    except Exception as error:
        old_error = error
    monkeypatch.setattr(rows_module, "_arrow_table_from_raw_tuples", shipped)
    try:
        session.createDataFrame(data, schema)
        new_error: BaseException | None = None
    except Exception as error:
        new_error = error
    assert old_error is not None
    assert new_error is not None
    assert type(new_error) is type(old_error)
    assert str(new_error) == str(old_error)


def _assert_new_refusal_text(
    monkeypatch: pytest.MonkeyPatch,
    session: Any,
    data: Any,
    schema: Any,
    error_type: type[BaseException],
    text: str,
) -> None:
    """The shipped path refuses with one exact error while the legacy path differs."""
    shipped = rows_module._arrow_table_from_raw_tuples
    monkeypatch.setattr(rows_module, "_arrow_table_from_raw_tuples", shipped)
    try:
        session.createDataFrame(data, schema)
        new_error: BaseException | None = None
    except Exception as error:
        new_error = error
    monkeypatch.setattr(
        rows_module, "_arrow_table_from_raw_tuples", rows_module._arrow_table_from_raw_tuples_legacy
    )
    try:
        session.createDataFrame(data, schema)
        old_error: BaseException | None = None
    except Exception as error:
        old_error = error
    assert new_error is not None
    assert type(new_error) is error_type
    assert str(new_error) == text
    assert old_error is not None
    assert str(old_error) != text


def test_shipped_dispatcher_is_the_column_wise_path() -> None:
    """The rows module answers through the new dispatcher unless a test swaps it."""
    assert rows_module._arrow_table_from_raw_tuples is columns_module._arrow_table_from_raw_tuples


def test_scalar_matrix_with_none_in_every_column(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Ints, floats, strings, bools and bytes with Nones scattered stay equal."""
    data = [
        (1, 1.5, "a", True, b"x", None),
        (None, None, None, None, None, None),
        (-7, -0.0, "", False, b"", "tail"),
        (2**62, 1e300, "héllo", True, b"\x00\xff", "mix"),
        (None, 2.5, "z", None, None, None),
    ]
    _assert_create_equal(monkeypatch, cdf_session, data, ["i", "f", "s", "b", "bin", "n"])


def test_whole_none_column_stays_varchar(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An all-None column infers VARCHAR on both paths."""
    _assert_create_equal(monkeypatch, cdf_session, [(1, None), (2, None)], ["id", "n"])


def test_bytearray_and_memoryview_columns(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Single-kind binary columns and a mixed binary column stay BINARY."""
    _assert_create_equal(
        monkeypatch,
        cdf_session,
        [(bytearray(b"a"), memoryview(b"b"), b"c"), (None, None, None)],
        ["arr", "view", "raw"],
    )
    _assert_create_equal(monkeypatch, cdf_session, [(b"a",), (bytearray(b"b"),)], ["mixed"])


def test_date_and_datetime_columns(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """DATE, naive TIMESTAMP and tz-aware TIMESTAMP columns stay equal."""
    aware = datetime.datetime(2024, 3, 15, 5, 30, tzinfo=datetime.UTC)
    shifted = datetime.datetime(
        2024, 3, 15, 7, 30, tzinfo=datetime.timezone(datetime.timedelta(hours=2))
    )
    data = [
        (datetime.date(2024, 1, 2), datetime.datetime(2024, 1, 2, 3, 4, 5), aware),
        (None, None, None),
        (datetime.date(1999, 12, 31), datetime.datetime(2020, 6, 7, 8, 9, 10, 123456), shifted),
    ]
    _assert_create_equal(monkeypatch, cdf_session, data, ["d", "ts", "tz"])


def test_decimal_column_at_several_scales(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Decimals keep DECIMAL(38, 18) with scale-sensitive repr equality."""
    data = [
        (decimal.Decimal("1.2"),),
        (decimal.Decimal("1.23000"),),
        (decimal.Decimal("0"),),
        (decimal.Decimal("-999.99"),),
        (decimal.Decimal("12345678.123456789012345678"),),
        (None,),
    ]
    _assert_create_equal(monkeypatch, cdf_session, data, ["dec"])


def test_all_nan_column_witnesses_double(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An all-NaN column is DOUBLE, not VARCHAR, on both paths."""
    _assert_create_equal(monkeypatch, cdf_session, [(float("nan"),), (float("nan"),)], ["x"])


def test_all_nat_column_witnesses_timestamp(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An all-NaT column is TIMESTAMP on both paths."""
    pandas = pytest.importorskip("pandas")
    _assert_create_equal(monkeypatch, cdf_session, [(pandas.NaT,), (pandas.NaT,)], ["t"])


def test_numpy_scalar_column(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """Numpy scalar cells unwrap to the same Arrow column on both paths."""
    numpy = pytest.importorskip("numpy")
    data = [
        (numpy.int64(5), numpy.float64(1.5), numpy.bool_(True)),
        (numpy.int32(-3), numpy.float32(0.5), numpy.bool_(False)),
        (None, None, None),
    ]
    _assert_create_equal(monkeypatch, cdf_session, data, ["i", "f", "b"])


def test_int64_extremes(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """The int64 minimum and maximum round-trip on both paths."""
    data = [(2**63 - 1,), (-(2**63),), (0,), (None,)]
    _assert_create_equal(monkeypatch, cdf_session, data, ["id"])


def test_long_double_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An int column with one float refuses CANNOT_MERGE_TYPE on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(1,), (2.5,)], ["x"])


def test_long_decimal_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An int column with one Decimal refuses CANNOT_MERGE_TYPE on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(1,), (decimal.Decimal("2.5"),)], ["x"])


def test_decimal_double_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A Decimal column with one float refuses CANNOT_MERGE_TYPE on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(decimal.Decimal("1.5"),), (2.0,)], ["x"])


def test_double_boolean_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A float column with one bool refuses CANNOT_MERGE_TYPE on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(1.5,), (True,)], ["x"])


def test_long_boolean_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An int column with one bool refuses CANNOT_MERGE_TYPE on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(1,), (False,)], ["x"])


def test_timestamp_long_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A timestamp column with one int refuses CANNOT_MERGE_TYPE on both paths."""
    stamp = datetime.datetime(2024, 1, 2, 3, 4, 5)
    _assert_refusals_equal(monkeypatch, cdf_session, [(stamp,), (7,)], ["x"])


def test_date_long_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A date column with one int refuses CANNOT_MERGE_TYPE on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(datetime.date(2024, 1, 2),), (7,)], ["x"])


def test_date_timestamp_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A date column with one datetime refuses CANNOT_MERGE_TYPE on both paths."""
    data = [(datetime.date(2024, 1, 2),), (datetime.datetime(2024, 1, 2, 3, 4, 5),)]
    _assert_refusals_equal(monkeypatch, cdf_session, data, ["x"])


def test_infinite_float_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Positive and negative infinity refuse on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(float("inf"),)], ["x"])
    _assert_refusals_equal(monkeypatch, cdf_session, [(1.0,), (float("-inf"),)], ["x"])


def test_complex_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A complex cell refuses on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(complex(1, 2),)], ["x"])


def test_unsupported_array_typecode_refuses_with_field_name(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An unsupported array.array typecode names the column on both paths."""
    import array

    _assert_refusals_equal(monkeypatch, cdf_session, [(array.array("q", [1]),)], ["x"])


def test_duplicate_schema_names_refuse_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Duplicate column names refuse on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(1, 2)], ["x", "x"])


def test_ragged_rows_refuse_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Rows of unequal width refuse on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(1, 2), (3,)], ["a", "b"])


def test_timedelta_cell_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A Timedelta cell refuses on both paths."""
    pandas = pytest.importorskip("pandas")
    _assert_refusals_equal(monkeypatch, cdf_session, [(pandas.Timedelta("1 day"),)], ["x"])


def test_numpy_timedelta64_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A numpy.timedelta64 cell refuses on both paths."""
    numpy = pytest.importorskip("numpy")
    _assert_refusals_equal(monkeypatch, cdf_session, [(numpy.timedelta64(5, "ns"),)], ["x"])


def test_decimal_magnitude_overflow_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A Decimal past 1e20 refuses the envelope on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(decimal.Decimal("1" + "0" * 21),)], ["x"])


def test_decimal_scale_overflow_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A Decimal with 19 fractional digits refuses the envelope on both paths."""
    _assert_refusals_equal(
        monkeypatch, cdf_session, [(decimal.Decimal("1.1234567890123456789"),)], ["x"]
    )


def test_decimal_nan_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A non-finite Decimal refuses on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(decimal.Decimal("NaN"),)], ["x"])


def test_int_beyond_int64_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An int past the int64 maximum refuses the Arrow build on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(2**63,), (1,)], ["x"])


def test_int_string_mix_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An int column with one string refuses the Arrow build on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [(1,), ("nope",)], ["x"])


def test_list_element_long_double_merge_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A list column mixing int and float elements refuses on both paths."""
    _assert_refusals_equal(monkeypatch, cdf_session, [([1, 2.5],)], ["x"])


def test_multi_failure_decimal_envelope_reports_fast_column_value(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Same-row envelope violations in a slow and a fast column report the fast value."""
    from repark.errors import PySparkValueError

    data = [
        (decimal.Decimal("1" + "0" * 21), decimal.Decimal("2" + "0" * 21)),
        ("pad", decimal.Decimal("1.5")),
    ]
    _assert_new_refusal_text(
        monkeypatch,
        cdf_session,
        data,
        ["c0", "c1"],
        PySparkValueError,
        "createDataFrame Decimal value 2000000000000000000000 exceeds DECIMAL(38, 18) "
        "magnitude (|value| must be < 10**20)",
    )


def test_multi_failure_cross_column_reports_build_phase_error(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """A slow-column inference error behind the envelope phase reports its build refusal."""
    from repark.errors import PySparkTypeError
    from repark.spark.ml.linalg import SparseVector

    data = [
        ([1.0, 2.0], decimal.Decimal("1.5")),
        (True, SparseVector(4, [1, 3], [5.0, 6.0])),
        (-1, datetime.date(2024, 1, 2)),
    ]
    _assert_new_refusal_text(
        monkeypatch,
        cdf_session,
        data,
        ["c0", "c1"],
        PySparkTypeError,
        "createDataFrame column 'c0': expected dense float list of width 2, got bool",
    )


def test_list_rows_and_inferred_names(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """List rows with no schema infer positional names on both paths."""
    _assert_create_equal(monkeypatch, cdf_session, [[1, "a"], [2, "b"]], None)


def test_namedtuple_rows_reorder_by_name(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Namedtuple rows bind a reordered schema by name on both paths."""
    import collections

    Point = collections.namedtuple("Point", ["x", "y"])
    data = [Point(1, "a"), Point(2, "b")]
    _assert_create_equal(monkeypatch, cdf_session, data, ["y", "x"])


def test_dict_key_union(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """Dict rows union keys with null fill on both paths."""
    data = [{"c": 1, "a": 2}, {"b": 3.5, "a": 4}, {"d": "x", "c": 6}]
    _assert_create_equal(monkeypatch, cdf_session, data, None)


def test_row_rows_with_strict_bind(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Row rows bind by name on both paths."""
    data = [Row(a=1, b="x"), Row(a=2, b="y")]
    _assert_create_equal(monkeypatch, cdf_session, data, ["a", "b"])


def test_row_key_mismatch_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Row rows with unequal keys refuse on both paths."""
    data = [Row(a=1, b="x"), Row(a=2)]
    _assert_refusals_equal(monkeypatch, cdf_session, data, None)


def test_scalar_cells_with_double_type(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Scalar cells under a bare DoubleType stay equal on both paths."""
    from repark.spark.types import DoubleType

    _assert_create_equal(monkeypatch, cdf_session, [0.5, 1.5, None], DoubleType())


def test_struct_type_schema_keeps_int32(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """An explicit StructType keeps declared widths on both paths."""
    from repark.spark.types import IntegerType, StringType, StructField, StructType

    schema = StructType(
        [StructField("id", IntegerType(), True), StructField("s", StringType(), True)]
    )
    _assert_create_equal(monkeypatch, cdf_session, [(1, "a"), (None, None)], schema)


def test_ddl_string_schema(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """A DDL string schema parses to the same frame on both paths."""
    _assert_create_equal(monkeypatch, cdf_session, [(1, "a"), (2, None)], "id INT, s STRING")


def test_empty_list_with_name_schema(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Zero rows with a name schema give VARCHAR nulls on both paths."""
    _assert_create_equal(monkeypatch, cdf_session, [], ["a", "b"])


def test_empty_list_with_struct_type_schema(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Zero rows with a StructType keep declared types on both paths."""
    from repark.spark.types import IntegerType, StructField, StructType

    schema = StructType([StructField("id", IntegerType(), True)])
    _assert_create_equal(monkeypatch, cdf_session, [], schema)


def test_nested_list_and_struct_cells(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """List, tuple and Row cells keep nested types on both paths."""
    data = [
        ([1, 2], (3, "x"), Row(n=1, t="a")),
        ([], (4, "y"), Row(n=2, t="b")),
        (None, None, None),
    ]
    _assert_create_equal(monkeypatch, cdf_session, data, ["arr", "tpl", "row"])


def _dict_session(value: str) -> ReparkSession:
    """A session with the nested-dict-as-struct conf set to ``value``."""
    return (
        ReparkSession.builder.appName(f"pytest-perf-facade-cdf-1-dict-{value}")
        .config("spark.sql.pyspark.inferNestedDictAsStruct.enabled", value)
        .getOrCreate()
    )


def _column_type(frame: Any, name: str) -> Any:
    """The Arrow type of one named column, proving which conf took effect."""
    return frame.to_arrow().schema.field(name).type


def test_dict_cells_as_struct_and_as_map(monkeypatch: pytest.MonkeyPatch) -> None:
    """Dict cells follow the struct/map conf identically on both paths."""
    data = [({"b": 1, "a": "x"},), ({"b": 2},), (None,)]
    struct_session = _dict_session("true")
    try:
        _assert_create_equal(monkeypatch, struct_session, data, ["d"])
        struct_type = _column_type(struct_session.createDataFrame(data, ["d"]), "d")
        assert pa.types.is_struct(struct_type)
    finally:
        struct_session.stop()
    map_session = _dict_session("false")
    try:
        _assert_create_equal(monkeypatch, map_session, data, ["d"])
        map_type = _column_type(map_session.createDataFrame(data, ["d"]), "d")
        assert pa.types.is_map(map_type)
    finally:
        map_session.stop()


def test_list_of_dict_cells(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """A list-of-dict column keeps its element type on both paths."""
    data = [([{"a": 1}, {"a": 2, "b": "x"}],), ([],), (None,)]
    _assert_create_equal(monkeypatch, cdf_session, data, ["d"])


def test_dict_cell_with_null_key_refuses_with_same_text(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A dict cell with a null key refuses on both paths."""
    struct_session = _dict_session("true")
    try:
        _assert_refusals_equal(monkeypatch, struct_session, [({None: 1},)], ["d"])
    finally:
        struct_session.stop()


def _legacy_coerce_session(value: str) -> ReparkSession:
    """A session with the legacy first-element conf set to ``value``."""
    return (
        ReparkSession.builder.appName(f"pytest-perf-facade-cdf-1-legacy-{value}")
        .config("spark.sql.pyspark.legacy.inferArrayTypeFromFirstElement.enabled", value)
        .getOrCreate()
    )


def test_legacy_first_element_coerce(monkeypatch: pytest.MonkeyPatch) -> None:
    """Nested lists follow the legacy first-element conf on both paths."""
    data = [([{"a": 1}],), ([{"b": "x"}],)]
    session = _legacy_coerce_session("true")
    try:
        _assert_create_equal(monkeypatch, session, data, ["d"])
        first_only = _column_type(session.createDataFrame(data, ["d"]), "d").value_type
        assert [field.name for field in first_only] == ["a"]
    finally:
        session.stop()
    session = _legacy_coerce_session("false")
    try:
        _assert_create_equal(monkeypatch, session, data, ["d"])
        merged = _column_type(session.createDataFrame(data, ["d"]), "d").value_type
        assert [field.name for field in merged] == ["a", "b"]
    finally:
        session.stop()


def _timestamp_type_session(value: str) -> ReparkSession:
    """A session with ``spark.sql.timestampType`` set to ``value``."""
    return (
        ReparkSession.builder.appName(f"pytest-perf-facade-cdf-1-tstype-{value}")
        .config("spark.sql.timestampType", value)
        .getOrCreate()
    )


def test_datetime_column_under_both_timestamp_types(monkeypatch: pytest.MonkeyPatch) -> None:
    """Naive datetimes follow the LTZ/NTZ default identically on both paths."""
    data = [(datetime.datetime(2024, 1, 2, 3, 4, 5),), (None,)]
    session = _timestamp_type_session("TIMESTAMP_LTZ")
    try:
        _assert_create_equal(monkeypatch, session, data, ["ts"])
        ltz_type = _column_type(session.createDataFrame(data, ["ts"]), "ts")
        assert pa.types.is_timestamp(ltz_type)
        assert ltz_type.tz == "UTC"
    finally:
        session.stop()
    session = _timestamp_type_session("TIMESTAMP_NTZ")
    try:
        _assert_create_equal(monkeypatch, session, data, ["ts"])
        ntz_type = _column_type(session.createDataFrame(data, ["ts"]), "ts")
        assert pa.types.is_timestamp(ntz_type)
        assert ntz_type.tz is None
    finally:
        session.stop()


def test_ml_vector_cells(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """Dense and sparse ML vectors keep their shapes on both paths."""
    from repark.spark.ml.linalg import DenseVector, SparseVector

    dense = [(DenseVector([1.0, 2.0]),), (DenseVector([3.0, 4.0]),)]
    _assert_create_equal(monkeypatch, cdf_session, dense, ["v"])
    sparse = [(SparseVector(4, [1, 3], [5.0, 6.0]),), (None,)]
    _assert_create_equal(monkeypatch, cdf_session, sparse, ["v"])


def test_supported_array_typecodes(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Supported array.array typecodes become lists on both paths."""
    import array

    data = [(array.array("d", [1.5, 2.5]), array.array("l", [1, 2])), (None, None)]
    _assert_create_equal(monkeypatch, cdf_session, data, ["f", "i"])


def test_time_cells_become_strings(
    monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession
) -> None:
    """Time-of-day cells stringify on both paths."""
    data = [(datetime.time(1, 2, 3),), (None,)]
    _assert_create_equal(monkeypatch, cdf_session, data, ["t"])


def test_single_row_frame(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """One row with mixed scalars stays equal on both paths."""
    data = [(1, 2.5, "x", True, None)]
    _assert_create_equal(monkeypatch, cdf_session, data, ["i", "f", "s", "b", "n"])


def _wide_rows(count: int) -> list[tuple[Any, ...]]:
    """Deterministic mixed-scalar rows with Nones in every column."""
    rows: list[tuple[Any, ...]] = []
    for index in range(count):
        rows.append(
            (
                None if index % 7 == 0 else index,
                None if index % 11 == 0 else index * 0.5,
                None if index % 13 == 0 else f"s{index % 97}",
                None if index % 5 == 0 else index % 2 == 0,
                None if index % 17 == 0 else index % 251,
            )
        )
    return rows


def test_ten_thousand_rows(monkeypatch: pytest.MonkeyPatch, cdf_session: ReparkSession) -> None:
    """Ten thousand mixed rows with Nones stay equal on both paths."""
    _assert_create_equal(monkeypatch, cdf_session, _wide_rows(10_000), ["i", "f", "s", "b", "j"])


def _spark_field_signature(frame: Any) -> list[tuple[str, str, bool]]:
    """The Spark-type pin: name, simpleString and nullability per field."""
    return [
        (field.name, field.dataType.simpleString(), field.nullable) for field in frame.schema.fields
    ]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_scalar_matrix_matches_pyspark(
    spark_engine: lp.Engine, cdf_session: ReparkSession
) -> None:
    """The scalar matrix answers schema and rows like live PySpark 4.1.2."""
    assert spark_engine.session.version == "4.1.2"
    data = [
        (1, 1.5, "a", True, None),
        (None, None, None, None, None),
        (-7, -0.0, "", False, "tail"),
    ]
    names = ["i", "f", "s", "b", "n"]
    spark_frame = spark_engine.session.createDataFrame(data, names)
    repark_frame = cdf_session.createDataFrame(data, names)
    assert _spark_field_signature(repark_frame) == _spark_field_signature(spark_frame)
    spark_rows = [tuple(row) for row in spark_frame.collect()]
    repark_rows = [tuple(row) for row in repark_frame.collect()]
    assert repark_rows == spark_rows
