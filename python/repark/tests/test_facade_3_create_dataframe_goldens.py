"""FACADE-3 createDataFrame inference goldens against the committed JSON file."""

from __future__ import annotations

import collections
import datetime
import json
import os
import pickle
from collections.abc import Callable, Iterator
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from repark import ReparkSession
from repark.spark.row import Row
from repark.spark.types import (
    ArrayType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
)

RECORD_ENV = "REPARK_FACADE_3_RECORD_GOLDENS"
GOLDEN_PATH = Path(__file__).with_name("facade_3_create_dataframe_goldens.json")
DICT_AS_STRUCT_CONF = "spark.sql.pyspark.inferNestedDictAsStruct.enabled"
BASE_DATE = datetime.date(2024, 1, 2)
BASE_TS_NAIVE = datetime.datetime(2024, 1, 2, 3, 4, 5)
BASE_TS_UTC = datetime.datetime(2024, 1, 2, 3, 4, 5, tzinfo=ZoneInfo("UTC"))
BASE_TS_NY = datetime.datetime(2024, 1, 2, 8, 4, 5, tzinfo=ZoneInfo("America/New_York"))
NT = collections.namedtuple("NT", ["a", "b"])

Case = Callable[[ReparkSession], Any]


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """Session for FACADE-3 createDataFrame goldens (UTC, LTZ timestamps)."""
    session = (
        ReparkSession.builder.appName("facade-3-create-dataframe")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.timestampType", "TIMESTAMP_LTZ")
        .getOrCreate()
    )
    try:
        yield session
    finally:
        session.stop()


def _running_in_ci() -> bool:
    """True when GitHub Actions or generic CI is set."""
    return os.environ.get("CI") == "true" or os.environ.get("GITHUB_ACTIONS") == "true"


def _record_requested() -> bool:
    """True when the explicit record environment variable is `1`."""
    return os.environ.get(RECORD_ENV, "") == "1"


def _assert_record_mode_allowed() -> None:
    """Refuse record mode when CI is set so a golden cannot be rewritten in CI."""
    if _record_requested() and _running_in_ci():
        raise AssertionError(f"{RECORD_ENV} is set while CI is set; refusing to rewrite goldens")


def _canonical_json(payload: dict[str, dict[str, object]]) -> str:
    """Stable JSON bytes used both to record and to compare."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _struct_schema() -> StructType:
    """The explicit seven-column StructType used by ``sch_struct`` cases."""
    return StructType(
        [
            StructField("i", LongType(), True),
            StructField("f", DoubleType(), True),
            StructField("s", StringType(), True),
            StructField("b", BooleanType(), True),
            StructField("d", DateType(), True),
            StructField("t", TimestampType(), True),
            StructField("dc", DecimalType(38, 18), True),
        ]
    )


def _nested_struct_schema() -> StructType:
    """A StructType carrying struct, array, and map fields for nested schema cases."""
    return StructType(
        [
            StructField("st", StructType([StructField("x", IntegerType(), True)]), True),
            StructField("arr", ArrayType(IntegerType(), True), True),
            StructField("m", MapType(StringType(), IntegerType(), True), True),
        ]
    )


def _dict_as_map(session: ReparkSession, data: Any, schema: Any = None) -> Any:
    """createDataFrame under ``inferNestedDictAsStruct=false`` (dict cells stay maps)."""
    session.conf.set(DICT_AS_STRUCT_CONF, False)
    try:
        return (
            session.createDataFrame(data)
            if schema is None
            else session.createDataFrame(data, schema)
        )
    finally:
        session.conf.set(DICT_AS_STRUCT_CONF, True)


def _pandas_frame() -> Any:
    """Small typed pandas frame for the dispatch control cases."""
    pd = pytest.importorskip("pandas")
    import pyarrow as pa

    return pd.DataFrame(
        {
            "i": pd.Series([1, 2, 3], dtype="int64"),
            "f": pd.Series([1.5, 2.5, 3.5], dtype="float64"),
            "s": pd.Series(["a", "b", "c"], dtype="string"),
            "b": pd.Series([True, False, True], dtype="bool"),
            "d": pd.Series([BASE_DATE] * 3, dtype=pd.ArrowDtype(pa.date32())),
            "t": pd.Series([BASE_TS_NAIVE] * 3, dtype="datetime64[us]"),
            "dc": pd.Series(
                [Decimal("1.50"), Decimal("2.25"), Decimal("3.75")],
                dtype=pd.ArrowDtype(pa.decimal128(38, 18)),
            ),
        }
    )


def _polars_frame() -> Any:
    """Small typed polars frame for the dispatch control cases."""
    pl = pytest.importorskip("polars")

    return pl.DataFrame(
        {
            "i": [1, 2, 3],
            "f": [1.5, 2.5, 3.5],
            "s": ["a", "b", "c"],
            "b": [True, False, True],
            "d": [BASE_DATE] * 3,
            "t": [BASE_TS_NAIVE] * 3,
            "dc": [Decimal("1.50"), Decimal("2.25"), Decimal("3.75")],
        }
    )


def _put(out: dict[str, Case], case_id: str, thunk: Case) -> None:
    """Insert one case; duplicate ids are a builder bug."""
    if case_id in out:
        raise AssertionError(f"duplicate case id {case_id}")
    out[case_id] = thunk


def _tuple_cases(out: dict[str, Case]) -> None:
    """Inferred list-of-tuples shapes, merge refusals, and null positions."""
    cases: dict[str, Any] = {
        "tup_scalars": [(1, 1.5, "x", True)],
        "tup_seven": [
            (7, 7.5, "s7", True, BASE_DATE, BASE_TS_NAIVE, Decimal("7.50")),
            (8, 8.5, "s8", False, BASE_DATE, BASE_TS_UTC, Decimal("8.25")),
        ],
        "tup_none_lead": [(None, "a"), (1, "b")],
        "tup_none_mid": [(1, None), (2, 3)],
        "tup_none_tail": [(1,), (None,)],
        "tup_all_none": [(None, None), (None, None)],
        "tup_int_string_merge": [(1,), ("x",)],
        "tup_string_int_merge": [("x",), (1,)],
        "tup_int_float_merge": [(1,), (2.5,)],
        "tup_float_int_merge": [(2.5,), (1,)],
        "tup_int_bool_merge": [(1,), (True,)],
        "tup_bool_int_merge": [(True,), (1,)],
        "tup_int_decimal_merge": [(1,), (Decimal("2.5"),)],
        "tup_float_bool_merge": [(1.5,), (True,)],
        "tup_decimal_float_merge": [(Decimal("1.5"),), (2.5,)],
        "tup_date_timestamp_merge": [(BASE_DATE,), (BASE_TS_NAIVE,)],
        "tup_timestamp_date_merge": [(BASE_TS_NAIVE,), (BASE_DATE,)],
        "tup_timestamp_int_merge": [(BASE_TS_NAIVE,), (5,)],
        "tup_float_inf": [(float("inf"),)],
        "tup_float_neg_inf": [(float("-inf"),)],
        "tup_float_nan": [(float("nan"),), (1.5,)],
        "tup_ragged": [(1, 2), (3,)],
        "tup_hetero_row_element": [(1,), "x"],
        "tup_list_rows": [[1, "a"], [2, "b"]],
        "tup_empty_row": [(), ()],
        "tup_large_int": [(2**70,)],
        "tup_date_only": [(BASE_DATE,), (BASE_DATE + datetime.timedelta(days=1),)],
        "tup_time_only": [(datetime.time(3, 4, 5),)],
        "tup_bool_only": [(True,), (False,)],
        "tup_none_date": [(None,), (BASE_DATE,)],
        "tup_none_bool": [(None,), (True,)],
        "tup_none_float": [(None,), (1.5,)],
        "tup_none_bytes": [(None,), (b"x",)],
        "tup_none_decimal": [(None,), (Decimal("1.5"),)],
        "tup_none_timestamp": [(None,), (BASE_TS_NAIVE,)],
        "tup_bytes_only": [(b"ab",), (b"cd",)],
        "tup_bytearray_only": [(bytearray(b"ab"),)],
        "tup_memoryview_only": [(memoryview(b"ab"),)],
        "tup_mixed_bytes_bytearray": [(b"ab",), (bytearray(b"cd"),)],
    }
    for case_id, data in cases.items():
        _put(out, case_id, lambda session, data=data: session.createDataFrame(data))
    _put(
        out,
        "tup_namedtuple",
        lambda session: session.createDataFrame([NT(1, "a"), NT(2, "b")]),
    )
    _put(
        out,
        "tup_namedtuple_schema_reorder",
        lambda session: session.createDataFrame([NT(1, "a")], ["b", "a"]),
    )


def _row_cases(out: dict[str, Case]) -> None:
    """List-of-Row shapes including nested Row inside Row."""
    _put(
        out,
        "row_basic",
        lambda session: session.createDataFrame([Row(a=1, b="x"), Row(a=2, b="y")]),
    )
    _put(
        out,
        "row_nested",
        lambda session: session.createDataFrame(
            [Row(a=1, inner=Row(x=2, y="s")), Row(a=3, inner=Row(x=4, y="t"))]
        ),
    )
    _put(
        out,
        "row_deep_nested",
        lambda session: session.createDataFrame([Row(inner=Row(inner=Row(v=1)))]),
    )
    _put(out, "row_none_cell", lambda session: session.createDataFrame([Row(a=None, b=1)]))
    _put(
        out,
        "row_schema_reorder",
        lambda session: session.createDataFrame([Row(a=1, b=2)], ["b", "a"]),
    )
    _put(
        out,
        "row_schema_rename",
        lambda session: session.createDataFrame([Row(a=1, b=2)], ["x", "y"]),
    )
    _put(
        out,
        "row_missing_field",
        lambda session: session.createDataFrame([Row(a=1), Row(a=1, b=2)]),
    )
    _put(
        out,
        "row_schema_partial",
        lambda session: session.createDataFrame([Row(a=1, b=2)], ["a", "c"]),
    )
    _put(
        out,
        "row_schema_struct",
        lambda session: session.createDataFrame(
            [Row(a=1, b="x")],
            StructType(
                [StructField("a", IntegerType(), True), StructField("b", StringType(), True)]
            ),
        ),
    )
    _put(
        out,
        "row_schema_ddl",
        lambda session: session.createDataFrame([Row(a=1, b="x")], "a INT, b STRING"),
    )
    _put(out, "row_positional", lambda session: session.createDataFrame([Row(1, 2)]))
    _put(
        out,
        "row_hetero_element",
        lambda session: session.createDataFrame([Row(a=1), (2,)]),
    )
    _put(
        out,
        "row_in_tuple_cell",
        lambda session: session.createDataFrame([(Row(x=1, y=2), 5)]),
    )
    _put(
        out,
        "row_decimal",
        lambda session: session.createDataFrame([Row(a=Decimal("1.25")), Row(a=Decimal("2.5"))]),
    )


def _dict_cases(out: dict[str, Case]) -> None:
    """List-of-dicts key-union and dict-cell conf variants."""
    _put(
        out,
        "dict_basic",
        lambda session: session.createDataFrame([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
    )
    _put(
        out,
        "dict_key_union",
        lambda session: session.createDataFrame(
            [{"c": 1, "a": 2}, {"b": 3, "a": 4}, {"d": 5, "c": 6}]
        ),
    )
    _put(
        out,
        "dict_missing_null_fill",
        lambda session: session.createDataFrame([{"a": 1}, {"b": 2}]),
    )
    _put(out, "dict_none_value", lambda session: session.createDataFrame([{"a": None, "b": 1}]))
    _put(
        out,
        "dict_all_none_then_value",
        lambda session: session.createDataFrame([{"a": None}, {"a": 1}]),
    )
    _put(
        out,
        "dict_nested_struct_conf",
        lambda session: session.createDataFrame(
            [{"m": {"x": 1, "y": "s"}}, {"m": {"x": 2, "z": True}}]
        ),
    )
    _put(
        out,
        "dict_nested_map_conf",
        lambda session: _dict_as_map(
            session, [{"m": {"x": 1, "y": "s"}}, {"m": {"x": 2, "z": True}}]
        ),
    )
    _put(
        out,
        "dict_nested_map_conf_uniform",
        lambda session: _dict_as_map(session, [{"m": {"x": 1, "y": 2}}]),
    )
    _put(
        out,
        "dict_nested_empty",
        lambda session: session.createDataFrame([{"m": {}}, {"m": {"a": 1}}]),
    )
    _put(
        out,
        "dict_nested_none_values",
        lambda session: session.createDataFrame([{"m": {"a": None}}, {"m": {"a": 1}}]),
    )
    _put(out, "dict_non_str_key", lambda session: session.createDataFrame([{1: "x"}]))
    _put(
        out,
        "dict_hetero_element",
        lambda session: session.createDataFrame([{"a": 1}, (2,)]),
    )
    _put(
        out,
        "dict_schema_struct",
        lambda session: session.createDataFrame(
            [{"a": 1}, {"a": 2, "b": "x"}],
            StructType(
                [StructField("a", IntegerType(), True), StructField("b", StringType(), True)]
            ),
        ),
    )
    _put(
        out,
        "dict_schema_ddl",
        lambda session: session.createDataFrame([{"a": 1, "b": "x"}], "a INT, b STRING"),
    )
    _put(
        out,
        "dict_schema_reorder",
        lambda session: session.createDataFrame([{"a": 1, "b": 2}], ["b", "a"]),
    )
    _put(
        out,
        "dict_schema_rename",
        lambda session: session.createDataFrame([{"a": 1, "b": 2}], ["x", "y"]),
    )
    _put(
        out,
        "dict_schema_partial",
        lambda session: session.createDataFrame([{"a": 1, "b": 2}], ["a", "c"]),
    )
    _put(out, "dict_sorted_order", lambda session: session.createDataFrame([{"z": 1, "a": 2}]))


def _nested_cases(out: dict[str, Case]) -> None:
    """Array / map / struct / Row cells inside tuple rows."""
    cells: dict[str, Any] = {
        "nest_list_int": [([1, 2, 3],)],
        "nest_list_float_dense": [([1.0, 2.0],)],
        "nest_list_mixed_width": [([1.0, 2.0],), ([1.0],)],
        "nest_list_of_dict": [([{"a": 1}, {"b": 2}],)],
        "nest_list_of_list": [([[1, 2], [3]],)],
        "nest_list_none_elements": [([None, 1],)],
        "nest_list_empty_then_value": [([],), ([1],)],
        "nest_list_all_empty": [([],), ([],)],
        "nest_tuple_cell": [((1, "a"),)],
        "nest_tuple_mixed": [((1, 2.5, "s"),)],
        "nest_tuple_empty": [((),)],
        "nest_dict_cell_struct": [({"a": 1, "b": "x"},)],
        "nest_sparse_vector": [({"size": 3, "indices": [0, 2], "values": [1.0, 2.5]},)],
        "nest_row_cell": [(Row(x=1, y=2),)],
        "nest_list_of_row": [([Row(x=1), Row(x=2)],)],
        "nest_map_int_keys": [({1: "a", 2: "b"},)],
        "nest_list_str_int_merge": [([1, "x"],)],
        "nest_list_int_float_merge": [([1, 2.5],)],
    }
    for case_id, data in cells.items():
        _put(out, case_id, lambda session, data=data: session.createDataFrame(data))
    _put(
        out,
        "nest_dict_cell_map_conf",
        lambda session: _dict_as_map(session, [({"a": 1, "b": "x"},)]),
    )
    _put(
        out,
        "nest_dict_cell_map_conf_uniform",
        lambda session: _dict_as_map(session, [({"a": 1, "b": 2},)]),
    )
    _put(
        out,
        "nest_struct_schema",
        lambda session: session.createDataFrame(
            [((1, "s"), [1, 2], {"k": 3})], _nested_struct_schema()
        ),
    )
    _put(
        out,
        "nest_map_ddl",
        lambda session: session.createDataFrame([({"k": 1},)], "m MAP<STRING, INT>"),
    )
    _put(
        out,
        "nest_array_ddl",
        lambda session: session.createDataFrame([([1, 2],)], "a ARRAY<INT>"),
    )
    _put(
        out,
        "nest_struct_ddl",
        lambda session: session.createDataFrame([((1, "s"),)], "st STRUCT<x:INT, y:STRING>"),
    )


def _decimal_cases(out: dict[str, Case]) -> None:
    """Decimal cells at differing scales plus the three envelope refusals."""
    cells: dict[str, Any] = {
        "dec_scale2": [(Decimal("1.23"),)],
        "dec_scale18": [(Decimal("1.234567890123456789"),)],
        "dec_mixed_scales": [(Decimal("1.5"),), (Decimal("2.125"),)],
        "dec_negative": [(Decimal("-7.75"),)],
        "dec_zero": [(Decimal("0"),)],
        "dec_large_in_envelope": [(Decimal("12345678901234567890") - Decimal("1"),)],
        "dec_magnitude_over": [(Decimal("100000000000000000000"),)],
        "dec_scale_over": [(Decimal("0.1234567890123456789"),)],
        "dec_nan": [(Decimal("NaN"),)],
        "dec_infinity": [(Decimal("Infinity"),)],
        "dec_with_none": [(None,), (Decimal("1.5"),)],
    }
    for case_id, data in cells.items():
        _put(out, case_id, lambda session, data=data: session.createDataFrame(data))
    _put(
        out,
        "dec_explicit_ddl",
        lambda session: session.createDataFrame([(Decimal("1.25"),)], "dc DECIMAL(10, 2)"),
    )
    _put(
        out,
        "dec_explicit_struct",
        lambda session: session.createDataFrame(
            [(Decimal("1.25"),)],
            StructType([StructField("dc", DecimalType(10, 2), True)]),
        ),
    )


def _temporal_binary_cases(out: dict[str, Case]) -> None:
    """Naive and tz-aware datetimes, dates, times, and binary cells."""
    cells: dict[str, Any] = {
        "ts_naive": [(BASE_TS_NAIVE,), (BASE_TS_NAIVE + datetime.timedelta(hours=1),)],
        "ts_tz_utc": [(BASE_TS_UTC,)],
        "ts_tz_ny": [(BASE_TS_NY,)],
        "ts_mixed_naive_tz": [(BASE_TS_NAIVE,), (BASE_TS_UTC,)],
        "ts_date": [(BASE_DATE,)],
        "ts_time": [(datetime.time(3, 4, 5),)],
        "ts_none_date": [(None,), (BASE_DATE,)],
        "bin_bytes": [(b"\x00\x01",)],
        "bin_bytearray": [(bytearray(b"\x00\x01"),)],
        "bin_memoryview": [(memoryview(b"\x00\x01"),)],
        "bin_none_mix": [(None,), (b"ab",)],
    }
    for case_id, data in cells.items():
        _put(out, case_id, lambda session, data=data: session.createDataFrame(data))
    _put(
        out,
        "ts_explicit_ddl",
        lambda session: session.createDataFrame([(BASE_TS_NAIVE,)], "t TIMESTAMP"),
    )
    _put(
        out,
        "ts_explicit_ntz",
        lambda session: session.createDataFrame(
            [(BASE_TS_NAIVE,)],
            StructType([StructField("t", TimestampNTZType(), True)]),
        ),
    )


def _schema_cases(out: dict[str, Case]) -> None:
    """Explicit schema forms, empty inputs, and the verifySchema refusals."""
    seven = [(1, 1.5, "x", True, BASE_DATE, BASE_TS_NAIVE, Decimal("1.25"))]
    _put(
        out,
        "sch_ddl",
        lambda session: session.createDataFrame(
            seven,
            "i BIGINT, f DOUBLE, s STRING, b BOOLEAN, d DATE, t TIMESTAMP, dc DECIMAL(38, 18)",
        ),
    )
    _put(out, "sch_struct", lambda session: session.createDataFrame(seven, _struct_schema()))
    _put(out, "sch_names", lambda session: session.createDataFrame([(1, "a")], ["i", "s"]))
    _put(out, "sch_names_short", lambda session: session.createDataFrame([(1, "a", 2.5)], ["i"]))
    _put(out, "sch_names_long", lambda session: session.createDataFrame([(1,)], ["i", "s"]))
    _put(
        out, "sch_bare_datatype", lambda session: session.createDataFrame([1.0, 2.5], DoubleType())
    )
    _put(out, "sch_bare_long", lambda session: session.createDataFrame([1, 2], LongType()))
    _put(
        out,
        "sch_bare_nonscalar_cell",
        lambda session: session.createDataFrame([1.0, (1,)], DoubleType()),
    )
    _put(out, "sch_bad_ddl_string", lambda session: session.createDataFrame([(1,)], "colname"))
    _put(out, "sch_int32_preserved", lambda session: session.createDataFrame([(1,)], "i INT"))
    _put(out, "sch_empty_names", lambda session: session.createDataFrame([], ["a", "b"]))
    _put(out, "sch_empty_ddl", lambda session: session.createDataFrame([], "a INT, b STRING"))
    _put(out, "sch_empty_struct", lambda session: session.createDataFrame([], _struct_schema()))
    _put(out, "sch_empty_bare_datatype", lambda session: session.createDataFrame([], DoubleType()))
    _put(
        out,
        "sch_verify_schema_true",
        lambda session: session.createDataFrame([(1,)], ["a"], verifySchema=True),
    )
    _put(
        out,
        "sch_verify_schema_false",
        lambda session: session.createDataFrame([(1,)], ["a"], verifySchema=False),
    )
    _put(out, "sch_sampling_ratio", lambda session: session.createDataFrame([(1,)], ["a"], 0.5))
    _put(out, "sch_names_nonstr", lambda session: session.createDataFrame([(1,)], [1]))
    _put(out, "sch_schema_int", lambda session: session.createDataFrame([(1,)], 7))


def _misc_cases(out: dict[str, Case]) -> None:
    """Top-level dispatch refusals and empty inputs."""
    _put(out, "misc_empty_no_schema", lambda session: session.createDataFrame([]))
    _put(out, "misc_str_input", lambda session: session.createDataFrame("abc"))
    _put(out, "misc_dict_input", lambda session: session.createDataFrame({"a": 1}))
    _put(out, "misc_scalar_list", lambda session: session.createDataFrame([1, 2, 3]))
    _put(out, "misc_set_input", lambda session: session.createDataFrame({1, 2}))


def _pandas_polars_cases(out: dict[str, Case]) -> None:
    """pandas and polars dispatch controls."""
    _put(out, "pd_basic", lambda session: session.createDataFrame(_pandas_frame()))
    _put(
        out,
        "pd_schema_names",
        lambda session: session.createDataFrame(
            _pandas_frame(), ["i", "f", "s", "b", "d", "t", "dc"]
        ),
    )
    _put(
        out,
        "pd_schema_reorder",
        lambda session: session.createDataFrame(
            _pandas_frame(), ["s", "i", "f", "b", "d", "t", "dc"]
        ),
    )
    _put(
        out,
        "pd_schema_ddl",
        lambda session: session.createDataFrame(
            _pandas_frame(),
            "i INT, f DOUBLE, s STRING, b BOOLEAN, d DATE, t TIMESTAMP, dc DECIMAL(10, 2)",
        ),
    )
    _put(
        out,
        "pd_empty",
        lambda session: session.createDataFrame(pytest.importorskip("pandas").DataFrame()),
    )
    _put(out, "pl_basic", lambda session: session.createDataFrame(_polars_frame()))
    _put(
        out,
        "pl_schema_names",
        lambda session: session.createDataFrame(
            _polars_frame(), ["i", "f", "s", "b", "d", "t", "dc"]
        ),
    )
    _put(
        out,
        "pl_nested",
        lambda session: session.createDataFrame(
            pytest.importorskip("polars").DataFrame(
                {"arr": [[1, 2], [3]], "st": [{"x": 1, "y": "a"}, {"x": 2, "y": "b"}]}
            )
        ),
    )
    _put(
        out,
        "pl_empty",
        lambda session: session.createDataFrame(pytest.importorskip("polars").DataFrame()),
    )


def _all_cases() -> dict[str, Case]:
    """Every createDataFrame golden case."""
    out: dict[str, Case] = {}
    _tuple_cases(out)
    _row_cases(out)
    _dict_cases(out)
    _nested_cases(out)
    _decimal_cases(out)
    _temporal_binary_cases(out)
    _schema_cases(out)
    _misc_cases(out)
    _pandas_polars_cases(out)
    return out


def _snapshot(session: ReparkSession, case_id: str, thunk: Case) -> dict[str, object]:
    """Schema string, field nullability, and repr(collect()) for one case."""
    try:
        frame = thunk(session)
    except Exception as error:
        return {"refused": {"class": type(error).__name__, "message": str(error)}}
    try:
        return {
            "nullable": [field.nullable for field in frame.schema.fields],
            "rows": [repr(row) for row in frame.collect()],
            "schema": frame.schema.simpleString(),
        }
    except Exception as error:
        raise AssertionError(f"{case_id}: snapshot failed: {error}") from error


def _build_payload(spark: ReparkSession) -> dict[str, dict[str, object]]:
    """Snapshot every case against one session."""
    cases = _all_cases()
    return {case_id: _snapshot(spark, case_id, thunk) for case_id, thunk in cases.items()}


def test_create_dataframe_goldens_match_committed_bytes(spark: ReparkSession) -> None:
    """Byte-identical createDataFrame inference goldens. pins: facade-3/C-001, C-002, C-003"""
    _assert_record_mode_allowed()
    payload = _build_payload(spark)
    assert len(payload) >= 120, f"need 120+ cases, got {len(payload)}"
    encoded = _canonical_json(payload)
    if _record_requested():
        GOLDEN_PATH.write_text(encoded, encoding="utf-8")
    if not GOLDEN_PATH.is_file():
        raise AssertionError(f"missing golden {GOLDEN_PATH.name}; set {RECORD_ENV}=1 to record")
    expected = GOLDEN_PATH.read_text(encoding="utf-8")
    if encoded != expected:
        actual_ids = set(payload)
        want = json.loads(expected)
        want_ids = set(want)
        missing = sorted(want_ids - actual_ids)
        extra = sorted(actual_ids - want_ids)
        changed = sorted(
            case_id
            for case_id in sorted(actual_ids & want_ids)
            if payload[case_id] != want[case_id]
        )
        raise AssertionError(
            f"golden byte mismatch missing={missing[:12]} extra={extra[:12]} changed={changed[:12]}"
        )


def test_pickle_round_trip_on_inferred_frames(spark: ReparkSession) -> None:
    """Pickled Row objects and schema round-trip equal. pins: facade-3/C-004"""
    frames = [
        spark.createDataFrame([(1, "a", 1.5), (2, "b", 2.5)]),
        spark.createDataFrame([Row(x=1, y=None), Row(x=3, y=Decimal("2.5"))]),
        spark.createDataFrame([{"a": 1, "b": "s"}, {"a": 2, "c": True}]),
        spark.createDataFrame([(BASE_DATE, BASE_TS_NAIVE, b"ab", Decimal("1.25"))]),
        spark.createDataFrame([(1, [1, 2], {"k": 3}, (4, "s"))]),
        spark.createDataFrame([Row(inner=Row(v=1, w="x"))]),
        spark.createDataFrame([(1, "a")], "i INT, s STRING"),
    ]
    for frame in frames:
        schema_rt = pickle.loads(pickle.dumps(frame.schema))
        assert schema_rt == frame.schema
        rows = frame.collect()
        restored = [pickle.loads(pickle.dumps(row)) for row in rows]
        assert restored == rows
        assert [row.__fields__ for row in restored] == [row.__fields__ for row in rows]
        positional = pickle.loads(pickle.dumps(Row(1, "x")))
        assert positional == Row(1, "x")
        assert positional.__fields__ == Row(1, "x").__fields__


def test_record_mode_fails_when_ci_is_set(monkeypatch: pytest.MonkeyPatch) -> None:
    """Record env is refused in CI. pins: facade-3/C-003"""
    monkeypatch.setenv(RECORD_ENV, "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
    monkeypatch.delenv("CI")
    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    with pytest.raises(AssertionError, match="refusing to rewrite goldens"):
        _assert_record_mode_allowed()
