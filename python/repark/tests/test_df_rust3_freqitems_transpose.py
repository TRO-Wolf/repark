"""DF-RUST-3 — freqItems / transpose pins driven by the live PySpark 4.1.2 oracle.

pins: df-rust-3/C-001, df-rust-3/C-002, df-rust-3/C-003, df-rust-3/C-004,
df-rust-3/C-007, df-rust-3/C-008, df-rust-3/C-009, df-rust-3/C-010
"""

from __future__ import annotations

import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkTypeError,
)
from repark.spark import functions as F  # noqa: N812
from repark.spark.types import Row

_ORACLE = json.loads((Path(__file__).parent / "facade_df_rust3_oracle.json").read_text())


def _cell(name: str) -> dict:
    """Return the recorded PySpark 4.1.2 oracle cell."""
    return _ORACLE[name]


@pytest.fixture
def spark() -> ReparkSession:
    """Fresh session per test."""
    session = ReparkSession.builder.appName("pytest-df-rust-3").getOrCreate()
    yield session
    session.stop()


def _base(spark: ReparkSession):
    """The oracle's five-row i/s/d frame."""
    return spark.createDataFrame(
        [(1, "a", 1.0), (1, "b", 2.0), (2, "a", None), (1, None, 2.0), (3, "a", 2.0)],
        "i int, s string, d double",
    )


def _base3(spark: ReparkSession):
    """The dfsubq probe's three-row i/s/d frame (tuple cells)."""
    return spark.createDataFrame(
        [(1, "a", 1.0), (1, "b", 2.0), (2, "a", None)], "i int, s string, d double"
    )


def _kv(spark: ReparkSession):
    """The oracle's two-row k/a/b transpose frame."""
    return spark.createDataFrame([("x", 1, 2), ("y", 3, 4)], "k string, a int, b int")


def _sorted_dict_rows(df) -> list[dict]:
    rows = [row.asDict(recursive=True) for row in df.collect()]
    return [
        {
            key: sorted(value, key=lambda item: (item is None, str(item)))
            if isinstance(value, list)
            else value
            for key, value in row.items()
        }
        for row in rows
    ]


def _check_result(df, cell_name: str, *, sort_arrays: bool = True) -> None:
    """Assert columns, simpleString schema, nullability and sorted dict rows."""
    expected = _cell(cell_name)["result"]
    assert df.columns == expected["columns"]
    assert df.schema.simpleString() == expected["schema"]
    assert [field.nullable for field in df.schema.fields] == expected["nullable"]
    rows = (
        _sorted_dict_rows(df) if sort_arrays else [r.asDict(recursive=True) for r in df.collect()]
    )
    assert [repr(row) for row in rows] == expected["rows"]


def _check_tuple_result(df, cell_name: str, *, sort_arrays: bool = True) -> None:
    """Assert against a `*_tuple` cell — positional rows for duplicate names."""
    expected = _cell(cell_name)["result"]
    assert df.columns == expected["columns"]
    assert df.schema.simpleString() == expected["schema"]
    assert [field.nullable for field in df.schema.fields] == expected["nullable"]
    rows = []
    for row in df.collect():
        items = list(row)
        if sort_arrays:
            items = [
                sorted(value, key=lambda item: (item is None, str(item)))
                if isinstance(value, list)
                else value
                for value in items
            ]
        rows.append(repr(tuple(items)))
    assert rows == expected["rows"]


def _check_error(cell_name: str, exc: Exception, *, message: str = "exact") -> None:
    """Assert class, condition, params and sqlstate of a recorded error cell."""
    expected = _cell(cell_name)["error"]
    assert type(exc).__name__ == expected["raises"]
    if expected.get("condition"):
        assert exc.getErrorClass() == expected["condition"]
    if expected.get("params") is not None:
        assert exc.getMessageParameters() == expected["params"]
    if expected.get("sqlstate"):
        assert exc.getSqlState() == expected["sqlstate"]
    if message == "exact":
        assert str(exc) == expected["message"]
    elif message == "head":
        assert expected["message"].split(";\n")[0].split(" SQLSTATE")[0] in str(exc)


def test_freq_default(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_default."""
    _check_result(_base(spark).freqItems(["i", "s"]), "freq_default")


def test_freq_support_05(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_support_05."""
    _check_result(_base(spark).freqItems(["i", "s", "d"], 0.5), "freq_support_05")


def test_freq_stat_door(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_stat (df.stat.freqItems tuple arg)."""
    _check_result(_base(spark).stat.freqItems(("i",), 0.4), "freq_stat")


def test_freq_tuple_arg(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_tuple."""
    _check_result(_base(spark).freqItems(("s",)), "freq_tuple")


def test_freq_support_one(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_support_one."""
    _check_result(_base(spark).freqItems(["i"], 1.0), "freq_support_one")


def test_freq_empty_frame(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_empty_df (one row of empty typed arrays)."""
    _check_result(_base(spark).limit(0).freqItems(["i", "s"]), "freq_empty_df")


def test_freq_empty_cols(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_empty_cols (row count of empty rows, no columns)."""
    _check_result(_base(spark).freqItems([]), "freq_empty_cols", sort_arrays=False)


def test_freq_struct_col(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_struct_col."""
    frame = spark.createDataFrame([(Row(x=1),)], "st struct<x:int>")
    _check_result(frame.freqItems(["st"]), "freq_struct_col")


def test_freq_array_col(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_array_col."""
    frame = spark.createDataFrame([([1, 2],), ([1, 2],)], "ar array<int>")
    _check_result(frame.freqItems(["ar"]), "freq_array_col")


def test_freq_bool_date(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_bool_date."""
    frame = spark.createDataFrame(
        [(True, date(2024, 1, 1)), (True, date(2024, 1, 1))], "b boolean, dt date"
    )
    _check_result(frame.freqItems(["b", "dt"]), "freq_bool_date")


def test_freq_decimal_ts_bin(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_decimal_ts."""
    frame = spark.createDataFrame(
        [(Decimal("1.50"), datetime(2024, 1, 1, 0, 0), b"\x01\x02")],
        "dec decimal(10,2), ts timestamp, bin binary",
    )
    _check_result(frame.freqItems(["dec", "ts", "bin"]), "freq_decimal_ts")


def test_freq_many_distinct(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cell freq_many_distinct (capacity 3 keeps nothing)."""
    _check_result(spark.range(1000).freqItems(["id"], 0.3), "freq_many_distinct")


def test_freq_dup_col(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — cells freq_dup_col / freq_dup_col_tuple."""
    _check_result(_base(spark).freqItems(["i", "i"]), "freq_dup_col")
    _check_tuple_result(_base3(spark).freqItems(["i", "i"]), "freq_dup_col_tuple")


def test_freq_string_cols_arg(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — cell freq_string_arg (NOT_LIST_OR_TUPLE)."""
    with pytest.raises(PySparkTypeError) as caught:
        _base(spark).freqItems("i")
    _check_error("freq_string_arg", caught.value)


def test_freq_column_element(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — cell freq_col_obj (NOT_ITERABLE)."""
    with pytest.raises(PySparkTypeError) as caught:
        _base(spark).freqItems([F.col("i")])
    _check_error("freq_col_obj", caught.value)


def test_freq_support_tiny(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — cell freq_support_tiny (Scala Double `1.0E-5`)."""
    with pytest.raises(IllegalArgumentException) as caught:
        _base(spark).freqItems(["i"], 1e-5)
    _check_error("freq_support_tiny", caught.value)


def test_freq_support_gt_one(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — cell freq_support_gt_one."""
    with pytest.raises(IllegalArgumentException) as caught:
        _base(spark).freqItems(["i"], 1.5)
    _check_error("freq_support_gt_one", caught.value)


def test_freq_support_int_accepted(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — R-4 divergence: int support is a float (Connect behaviour)."""
    base = _base(spark)
    assert base.freqItems(["i"], 1).collect() == base.freqItems(["i"], 1.0).collect()


def test_freq_support_str_refused(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — R-4 divergence: str support is NOT_FLOAT."""
    with pytest.raises(PySparkTypeError) as caught:
        _base(spark).freqItems(["i"], "0.5")
    assert caught.value.getErrorClass() == "NOT_FLOAT"
    assert "[NOT_FLOAT] Argument `support` should be a float, got str." in str(caught.value)


def test_freq_support_bool_refused(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — R-4 divergence: bool support is NOT_FLOAT."""
    with pytest.raises(PySparkTypeError) as caught:
        _base(spark).freqItems(["i"], True)
    assert caught.value.getErrorClass() == "NOT_FLOAT"


def test_freq_missing_col(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-002 — cell freq_missing_col (UNRESOLVED_COLUMN.WITH_SUGGESTION)."""
    with pytest.raises(AnalysisException) as caught:
        _base(spark).freqItems(["zz"])
    _check_error("freq_missing_col", caught.value, message="head")


def test_transpose_default(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cells transpose_default / transpose_default_tuple."""
    _check_result(_kv(spark).transpose(), "transpose_default", sort_arrays=False)
    _check_tuple_result(_kv(spark).transpose(), "transpose_default_tuple", sort_arrays=False)


def test_transpose_named_index(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_index."""
    _check_result(_kv(spark).transpose("k"), "transpose_index", sort_arrays=False)


def test_transpose_column_index(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_index_col_obj (Column argument)."""
    _check_result(_kv(spark).transpose(F.col("k")), "transpose_index_col_obj", sort_arrays=False)


def test_transpose_int_index_sorted(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_int_index (int keys sorted)."""
    frame = spark.createDataFrame([(2, 1, 5), (1, 3, 6)], "k int, a int, b int")
    _check_result(frame.transpose(), "transpose_int_index", sort_arrays=False)


def test_transpose_double_index(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_bool_float_index (Java double names)."""
    frame = spark.createDataFrame([(1.5, 1), (0.5, 2)], "k double, a int")
    _check_result(frame.transpose(), "transpose_bool_float_index", sort_arrays=False)


def test_transpose_index_order_sorted(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_index_names_order."""
    frame = spark.createDataFrame([("z", 1), ("a", 2), ("m", 3)], "k string, a int")
    _check_result(frame.transpose(), "transpose_index_names_order", sort_arrays=False)


def test_transpose_dup_index(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cells transpose_dup_index / transpose_dup_index_tuple.

    Duplicate index values yield duplicate output columns whose order Spark does
    not pin down; the oracle's dict cell recorded the alternate ordering, so the
    ``asDict`` view is asserted against both legal outcomes.
    """
    result = spark.createDataFrame([("x", 1), ("x", 2)], "k string, a int").transpose()
    expected = _cell("transpose_dup_index")["result"]
    assert result.columns == expected["columns"]
    assert result.schema.simpleString() == expected["schema"]
    assert [field.nullable for field in result.schema.fields] == expected["nullable"]
    assert [r.asDict(recursive=True) for r in result.collect()] in (
        [{"key": "a", "x": 1}],
        [{"key": "a", "x": 2}],
    )
    _check_tuple_result(
        spark.createDataFrame([("x", 1), ("x", 2)], "k string, a int").transpose(),
        "transpose_dup_index_tuple",
        sort_arrays=False,
    )


def test_transpose_null_index_dropped(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_null_index."""
    frame = spark.createDataFrame([("x", 1), (None, 2)], "k string, a int")
    _check_result(frame.transpose(), "transpose_null_index", sort_arrays=False)


def test_transpose_null_values(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_nulls_values."""
    frame = spark.createDataFrame([("x", None, 2), ("y", 3, None)], "k string, a int, b int")
    _check_result(frame.transpose(), "transpose_nulls_values", sort_arrays=False)


def test_transpose_key_clash(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cells transpose_key_col_clash / _tuple."""
    frame = spark.createDataFrame([("key", 1)], "k string, key int")
    _check_result(frame.transpose(), "transpose_key_col_clash", sort_arrays=False)
    _check_tuple_result(
        spark.createDataFrame([("key", 1)], "k string, key int").transpose(),
        "transpose_key_col_clash_tuple",
        sort_arrays=False,
    )


def test_transpose_mixed_types(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_mixed_types (int+double -> double)."""
    frame = spark.createDataFrame([("x", 1, 2.5)], "k string, a int, b double")
    _check_result(frame.transpose(), "transpose_mixed_types", sort_arrays=False)


def test_transpose_map_value(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_map_value (single complex column)."""
    frame = spark.createDataFrame([("x", {1: 2})], "k string, m map<int,int>")
    _check_result(frame.transpose(), "transpose_map_value", sort_arrays=False)


def test_transpose_empty(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_empty (key column, one row per value column)."""
    _check_result(_kv(spark).limit(0).transpose(), "transpose_empty", sort_arrays=False)


def test_transpose_single_col(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — cell transpose_single_col (no value columns, zero rows)."""
    frame = spark.createDataFrame([("x",), ("y",)], "k string")
    _check_result(frame.transpose(), "transpose_single_col", sort_arrays=False)


def test_transpose_array_index(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-004 — cell transpose_array_index (TRANSPOSE_INVALID_INDEX_COLUMN)."""
    frame = spark.createDataFrame([([1], 1)], "k array<int>, a int")
    with pytest.raises(AnalysisException) as caught:
        frame.transpose()
    _check_error("transpose_array_index", caught.value)


def test_transpose_incompatible(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-004 — cell transpose_incompatible (INT/STRING pair)."""
    frame = spark.createDataFrame([("x", 1, "s")], "k string, a int, b string")
    with pytest.raises(AnalysisException) as caught:
        frame.transpose()
    _check_error("transpose_incompatible", caught.value)


def test_transpose_index_other(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-004 — cell transpose_index_other (STRING/INT pair)."""
    with pytest.raises(AnalysisException) as caught:
        _kv(spark).transpose("a")
    _check_error("transpose_index_other", caught.value)


def test_transpose_long_decimal(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-004 — cell transpose_long_decimal (BIGINT/DECIMAL pair)."""
    frame = spark.createDataFrame([("x", 1, Decimal("2.50"))], "k string, a bigint, b decimal(5,2)")
    with pytest.raises(AnalysisException) as caught:
        frame.transpose()
    _check_error("transpose_long_decimal", caught.value)


def test_transpose_missing_index(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-004 — cell transpose_missing_index."""
    with pytest.raises(AnalysisException) as caught:
        _kv(spark).transpose("zz")
    _check_error("transpose_missing_index", caught.value, message="head")


def test_transpose_too_many(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-004 — cell transpose_too_many (500-row limit)."""
    frame = spark.range(2000).select(F.col("id").cast("string").alias("k"), F.col("id").alias("a"))
    with pytest.raises(AnalysisException) as caught:
        frame.transpose()
    _check_error("transpose_too_many", caught.value)


def test_transpose_row_limit_conf(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-004 — spark.sql.transposeMaxValues raises the bound."""
    spark.conf.set("spark.sql.transposeMaxValues", "3000")
    try:
        frame = spark.range(2000).select(
            F.col("id").cast("string").alias("k"), F.col("id").alias("a")
        )
        result = frame.transpose()
        assert len(result.columns) == 2001
    finally:
        spark.conf.unset("spark.sql.transposeMaxValues")


def test_freq_signed_zero_collapses(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-007 — IEEE == float keys: +0.0 and -0.0 are one key."""
    frame = spark.createDataFrame([(0.0,), (-0.0,)], "d double")
    _check_result(frame.freqItems(["d"], 1.0), "freq_signed_zero_cap1")
    _check_result(frame.freqItems(["d"]), "freq_signed_zero_default")
    frame2 = spark.createDataFrame([(0.0,), (-0.0,), (1.0,)], "d double")
    _check_result(frame2.freqItems(["d"], 0.5), "freq_signed_zero_cap2_with_one")
    frame3 = spark.createDataFrame([(0.0,), (0.0,)], "d double")
    _check_result(frame3.freqItems(["d"], 1.0), "freq_two_pos_zero_cap1")


def test_freq_signed_zero_first_key_kept(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-007 — the first-inserted -0.0 survives as the key."""
    frame = spark.createDataFrame([(-0.0,), (0.0,)], "d double")
    _check_result(frame.freqItems(["d"]), "freq_signed_zero_neg_first_default")


def test_freq_signed_zero_float(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-007 — IEEE == covers Float32 keys as well."""
    frame = spark.createDataFrame([(0.0,), (-0.0,)], "d float")
    _check_result(frame.freqItems(["d"], 1.0), "freq_signed_zero_float_cap1")


def test_freq_nan_keys(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-007 — NaN equals nothing: evicted at cap1, duplicated otherwise."""
    nan = float("nan")
    frame = spark.createDataFrame([(nan,), (nan,)], "d double")
    _check_result(frame.freqItems(["d"], 1.0), "freq_nan_keys_cap1")
    _check_result(frame.freqItems(["d"]), "freq_nan_keys_default")


def test_freq_nested_float_keys(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-007 — nested floats keep bit-exact equality inside containers."""
    nan = float("nan")
    arrays = spark.createDataFrame([([0.0],), ([-0.0],)], "ar array<double>")
    _check_result(arrays.freqItems(["ar"], 1.0), "freq_array_pm_zero_cap1")
    arrays_nan = spark.createDataFrame([([nan],), ([nan],)], "ar array<double>")
    _check_result(arrays_nan.freqItems(["ar"], 1.0), "freq_array_nan_cap1")
    structs = spark.createDataFrame([(Row(f=nan),), (Row(f=nan),)], "st struct<f:double>")
    _check_result(structs.freqItems(["st"], 1.0), "freq_struct_nan_cap1")


def test_freq_map_keys_never_equal(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-010 — map keys are never equal: no dedupe, cap1 evicts."""
    frame = spark.createDataFrame([({"a": 1},), ({"a": 1},)], "m map<string,int>")
    _check_result(frame.freqItems(["m"]), "freq_map_dup_default")
    _check_result(frame.freqItems(["m"], 1.0), "freq_map_dup_cap1")
    _check_result(
        spark.createDataFrame(
            [({"a": {"b": 1}},), ({"a": {"b": 1}},)], "m map<string,map<string,int>>"
        ).freqItems(["m"]),
        "freq_map_of_map_default",
    )
    _check_result(
        spark.createDataFrame([({"a": 1},), ({"b": 2},)], "m map<string,int>").freqItems(["m"]),
        "freq_map_distinct_default",
    )


def test_freq_nested_map_keys_dedupe(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-010 — a map inside a struct/array key still dedupes by content."""
    structs = spark.createDataFrame(
        [(("x", {"a": 1}),), (("x", {"a": 1}),)],
        "s struct<x:string,m:map<string,int>>",
    )
    _check_result(structs.freqItems(["s"]), "freq_struct_with_map_default")
    arrays = spark.createDataFrame([([{"a": 1}],), ([{"a": 1}],)], "a array<map<string,int>>")
    _check_result(arrays.freqItems(["a"]), "freq_array_of_map_default")


def test_freq_container_keys_dedupe(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-010 — array/struct keys still compare by content."""
    arrays = spark.createDataFrame([([1, 2],), ([1, 2],)], "a array<int>")
    _check_result(arrays.freqItems(["a"]), "freq_array_dup_default")
    _check_result(arrays.freqItems(["a"], 1.0), "freq_array_dup_cap1")
    structs = spark.createDataFrame([((1, "x"),), ((1, "x"),)], "s struct<x:int,y:string>")
    _check_result(structs.freqItems(["s"], 1.0), "freq_struct_dup_cap1")


def test_freq_case_insensitive_requested_spelling(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-009 — case-insensitive resolve keeps the requested spelling."""
    frame = spark.createDataFrame([(1, "a"), (2, "b"), (1, "a")], "i int, s string")
    result = frame.freqItems(["I"], 1.0)
    assert result.columns == ["I_freqItems"]
    assert [row.asDict() for row in result.collect()] == [{"I_freqItems": [1]}]


def test_freq_renamed_overlay(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-009 — display->engine overlay resolves renamed columns once."""
    frame = spark.createDataFrame([(1, "a"), (2, "b"), (1, "a")], "i int, s string")
    result = frame.withColumnRenamed("i", "j").freqItems(["j"], 1.0)
    assert result.columns == ["j_freqItems"]
    assert [row.asDict() for row in result.collect()] == [{"j_freqItems": [1]}]


def test_transpose_binary_invalid_utf8(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-008 — invalid UTF-8 index bytes render as U+FFFD."""
    frame = spark.createDataFrame([(b"\xff\xfe", 1), (b"ok", 2)], "k binary, a int")
    result = frame.transpose()
    _check_result(result, "transpose_binary_invalid_utf8", sort_arrays=False)
    codepoints = _cell("transpose_binary_invalid_utf8_repr")["result"]
    assert [[name, [ord(char) for char in name]] for name in result.columns] == codepoints


def test_transpose_binary_null_index(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-008 — null binary index rows are dropped before naming."""
    frame = spark.createDataFrame([(None, 1), (b"ok", 2)], "k binary, a int")
    _check_result(frame.transpose(), "transpose_binary_null_index", sort_arrays=False)


def test_freq_items_is_on_both_doors(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-001 — df.freqItems and df.stat.freqItems agree (set-equal)."""
    base = _base(spark)
    left = base.freqItems(["i"]).collect()[0]["i_freqItems"]
    right = base.stat.freqItems(["i"]).collect()[0]["i_freqItems"]
    assert sorted(left) == sorted(right)


def test_transpose_index_is_keyword_arg(spark: ReparkSession) -> None:
    """pins: df-rust-3/C-003 — indexColumn keyword spelling."""
    _check_result(_kv(spark).transpose(indexColumn="k"), "transpose_index", sort_arrays=False)
