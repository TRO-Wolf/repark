"""COLUMN-PARITY-1 oracle pins — ``facade_column_oracle.json`` ``col`` cells (PySpark 4.1.2).

pins: column-parity-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any

import pytest

from repark import functions as F  # noqa: N812 — PySpark idiom
from repark import types as T  # noqa: N812 — PySpark idiom
from repark.errors import (
    AnalysisException,
    PySparkException,
    PySparkRuntimeError,
    PySparkTypeError,
    UnsupportedOperationException,
)
from repark.spark import ReparkSession

ORACLE_PATH = Path(__file__).with_name("facade_column_oracle.json")

BASE_SCHEMA = "i int, d double, s string, st struct<a:int,b:string>"
BASE_ROWS = [(1, 1.0, "a", (1, "x")), (2, math.nan, None, None), (None, None, "c", (3, None))]
NESTED_SCHEMA = "st struct<a:int, inner:struct<x:int,y:int>>"
NESTED_ROWS = [((1, (2, 3)),), (None,)]


@pytest.fixture
def spark() -> ReparkSession:
    """A default session for the column-parity-1 oracle pins."""
    return ReparkSession.builder.appName("pytest-column-parity-1").getOrCreate()


def _oracle_cells() -> dict[str, Any]:
    """Return the recorded PySpark 4.1.2 ``col`` cells."""
    import json

    return json.loads(ORACLE_PATH.read_text(encoding="utf-8"))["cells"]


def _base_df(spark: ReparkSession) -> Any:
    """The ``base_df`` oracle fixture."""
    return spark.createDataFrame(BASE_ROWS, BASE_SCHEMA)


def _nested_df(spark: ReparkSession) -> Any:
    """The ``nested_df`` oracle fixture."""
    return spark.createDataFrame(NESTED_ROWS, NESTED_SCHEMA)


def _assert_frame(frame: Any, columns: list[str], schema: str, rows: list[dict[str, Any]]) -> None:
    """Assert a DataFrame answer against recorded columns, simpleString, and row dicts."""
    assert frame.columns == columns
    assert frame.schema.simpleString() == schema
    assert frame.to_arrow().to_pylist() == rows


def test_oracle_fixture_is_unchanged() -> None:
    """The committed fixture still carries every pinned cell id.

    pins: column-parity-1/C-007
    """
    expected = {
        "isin_list",
        "isin_varargs_name",
        "isin_with_none",
        "isin_empty",
        "isin_empty_varargs",
        "isin_tuple",
        "isin_set",
        "isin_string_vs_int",
        "isin_int_vs_string",
        "isin_columns",
        "isin_list_of_columns",
        "isin_mixed_col_scalar",
        "isin_double_nan",
        "isin_filter",
        "sql_in",
        "sql_in_null",
        "sql_in_mixed",
        "isnan_names",
        "isnan_string",
        "isnan_float",
        "isnan_decimal",
        "sql_isnan",
        "isnan_sql_string",
        "astype_str",
        "astype_bad",
        "name_alias",
        "name_multi",
        "name_metadata",
        "withfield_add",
        "withfield_replace",
        "withfield_replace_type",
        "withfield_name",
        "withfield_nested",
        "withfield_nested_replace",
        "withfield_missing_parent",
        "withfield_non_struct",
        "withfield_name_not_str",
        "withfield_col_not_column",
        "withfield_dup_fields",
        "withfield_empty_name",
        "withfield_case",
        "withfield_on_null_parent_nested",
        "dropfields_one",
        "dropfields_name",
        "dropfields_all",
        "dropfields_missing",
        "dropfields_nested",
        "dropfields_non_struct",
        "dropfields_not_str",
        "dropfields_none",
        "dropfields_case",
        "dropfields_nonexistent_nested",
        "getfield_after_withfield",
        "outer_repr",
        "outer_plain_select",
        "outer_in_scalar",
        "outer_in_exists",
    }
    assert expected <= set(_oracle_cells())


def test_isin_list(spark: ReparkSession) -> None:
    """``i.isin([1, 3])`` flattens a single list argument.

    pins: column-parity-1/C-001 (isin_list)
    """
    out = _base_df(spark).select(_base_df(spark).i.isin([1, 3]).alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": True}, {"r": False}, {"r": None}])


def test_isin_varargs_name(spark: ReparkSession) -> None:
    """``i.isin(1, 2)`` unaliased names ``(i IN (1, 2))``.

    pins: column-parity-1/C-001 (isin_varargs_name)
    """
    out = _base_df(spark).select(F.col("i").isin(1, 2))
    _assert_frame(
        out,
        ["(i IN (1, 2))"],
        "struct<(i IN (1, 2)):boolean>",
        [{"(i IN (1, 2))": True}, {"(i IN (1, 2))": True}, {"(i IN (1, 2))": None}],
    )


def test_isin_with_none(spark: ReparkSession) -> None:
    """``i.isin(1, None)`` keeps SQL three-valued ``IN`` semantics.

    pins: column-parity-1/C-001 (isin_with_none)
    """
    out = _base_df(spark).select(F.col("i").isin(1, None).alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": True}, {"r": None}, {"r": None}])


def test_isin_empty(spark: ReparkSession) -> None:
    """``i.isin([])`` answers false for every row, NULL included.

    pins: column-parity-1/C-001 (isin_empty)
    """
    out = _base_df(spark).select(F.col("i").isin([]))
    _assert_frame(
        out,
        ["(i IN ())"],
        "struct<(i IN ()):boolean>",
        [{"(i IN ())": False}, {"(i IN ())": False}, {"(i IN ())": False}],
    )


def test_isin_empty_varargs(spark: ReparkSession) -> None:
    """``i.isin()`` answers false for every row, NULL included.

    pins: column-parity-1/C-001 (isin_empty_varargs)
    """
    out = _base_df(spark).select(F.col("i").isin())
    _assert_frame(
        out,
        ["(i IN ())"],
        "struct<(i IN ()):boolean>",
        [{"(i IN ())": False}, {"(i IN ())": False}, {"(i IN ())": False}],
    )


def test_isin_tuple(spark: ReparkSession) -> None:
    """``i.isin((1, 2))`` refuses a tuple literal with Spark's error class.

    pins: column-parity-1/C-001 (isin_tuple)
    """
    with pytest.raises(PySparkRuntimeError) as raised:
        _base_df(spark).select(F.col("i").isin((1, 2)).alias("r"))
    assert raised.value.getCondition() == "UNSUPPORTED_FEATURE.LITERAL_TYPE"
    assert "Literal for '[1, 2]'" in str(raised.value)


def test_isin_set(spark: ReparkSession) -> None:
    """``i.isin({1, 2})`` flattens a single set argument.

    pins: column-parity-1/C-001 (isin_set)
    """
    out = _base_df(spark).select(F.col("i").isin({1, 2}).alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": True}, {"r": True}, {"r": None}])


def test_isin_string_vs_int(spark: ReparkSession) -> None:
    """``s.isin(1)`` fails on malformed ANSI coercion; the class diverges (COL-ISIN-1).

    pins: column-parity-1/C-001 (isin_string_vs_int); divergence COL-ISIN-1 / BL-1
    """
    out = _base_df(spark).select(F.col("s").isin(1).alias("r"))
    with pytest.raises(PySparkException) as raised:
        out.collect()
    assert "Cannot cast" in str(raised.value)


def test_isin_int_vs_string(spark: ReparkSession) -> None:
    """``i.isin("1", "x")`` fails on malformed ANSI coercion; the class diverges (COL-ISIN-1).

    pins: column-parity-1/C-001 (isin_int_vs_string); divergence COL-ISIN-1 / BL-1
    """
    with pytest.raises(PySparkException) as raised:
        _base_df(spark).select(F.col("i").isin("1", "x").alias("r")).collect()
    assert "Cannot cast" in str(raised.value)


def test_isin_columns(spark: ReparkSession) -> None:
    """``i.isin(col("i"), lit(2))`` accepts Column values.

    pins: column-parity-1/C-001 (isin_columns)
    """
    out = _base_df(spark).select(F.col("i").isin(F.col("i"), F.lit(2)).alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": True}, {"r": True}, {"r": None}])


def test_isin_list_of_columns(spark: ReparkSession) -> None:
    """``i.isin([lit(1), col("i")])`` flattens a list containing Columns.

    pins: column-parity-1/C-001 (isin_list_of_columns)
    """
    out = _base_df(spark).select(F.col("i").isin([F.lit(1), F.col("i")]).alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": True}, {"r": True}, {"r": None}])


def test_isin_mixed_col_scalar(spark: ReparkSession) -> None:
    """``i.isin(lit(2), 1)`` unaliased names ``(i IN (2, 1))``.

    pins: column-parity-1/C-001 (isin_mixed_col_scalar)
    """
    out = _base_df(spark).select(F.col("i").isin(F.lit(2), 1))
    _assert_frame(
        out,
        ["(i IN (2, 1))"],
        "struct<(i IN (2, 1)):boolean>",
        [{"(i IN (2, 1))": True}, {"(i IN (2, 1))": True}, {"(i IN (2, 1))": None}],
    )


def test_isin_double_nan(spark: ReparkSession) -> None:
    """``d.isin(nan)`` treats NaN as equal to NaN.

    pins: column-parity-1/C-001 (isin_double_nan)
    """
    out = _base_df(spark).select(F.col("d").isin(math.nan).alias("r"))
    _assert_frame(out, ["r"], "struct<r:boolean>", [{"r": False}, {"r": True}, {"r": None}])


def test_isin_filter(spark: ReparkSession) -> None:
    """``df.filter(s.isin("a", "c"))`` keeps only matching rows.

    pins: column-parity-1/C-001 (isin_filter)
    """
    out = _base_df(spark).filter(F.col("s").isin("a", "c"))
    _assert_frame(
        out,
        ["i", "d", "s", "st"],
        "struct<i:int,d:double,s:string,st:struct<a:int,b:string>>",
        [
            {"i": 1, "d": 1.0, "s": "a", "st": {"a": 1, "b": "x"}},
            {"i": None, "d": None, "s": "c", "st": {"a": 3, "b": None}},
        ],
    )


def test_isnan_names(spark: ReparkSession) -> None:
    """``d.isNaN()`` / ``i.isNaN()`` name ``isnan(d)`` / ``isnan(i)``.

    pins: column-parity-1/C-002 (isnan_names)
    """
    out = _base_df(spark).select(F.col("d").isNaN(), F.col("i").isNaN())
    _assert_frame(
        out,
        ["isnan(d)", "isnan(i)"],
        "struct<isnan(d):boolean,isnan(i):boolean>",
        [
            {"isnan(d)": False, "isnan(i)": False},
            {"isnan(d)": True, "isnan(i)": False},
            {"isnan(d)": False, "isnan(i)": False},
        ],
    )


def test_isnan_string(spark: ReparkSession) -> None:
    """``s.isNaN()`` fails on the DOUBLE coercion; the class diverges (COL-ISNAN-1).

    pins: column-parity-1/C-002 (isnan_string); divergence COL-ISNAN-1 / BL-1
    """
    out = _base_df(spark).select(F.col("s").isNaN().alias("r"))
    with pytest.raises(PySparkException) as raised:
        out.collect()
    assert "Cannot cast" in str(raised.value)


def test_isnan_float(spark: ReparkSession) -> None:
    """``f.isNaN()`` on a float column answers the NaN mask.

    pins: column-parity-1/C-002 (isnan_float)
    """
    frame = spark.createDataFrame([(math.nan,), (1.5,), (None,)], "f float")
    out = frame.select(F.col("f").isNaN())
    _assert_frame(
        out,
        ["isnan(f)"],
        "struct<isnan(f):boolean>",
        [{"isnan(f)": True}, {"isnan(f)": False}, {"isnan(f)": False}],
    )


def test_isnan_decimal(spark: ReparkSession) -> None:
    """``x.isNaN()`` on a decimal answers false.

    pins: column-parity-1/C-002 (isnan_decimal)
    """
    out = spark.sql("select cast(1 as decimal(5,2)) x").select(F.col("x").isNaN())
    _assert_frame(out, ["isnan(x)"], "struct<isnan(x):boolean>", [{"isnan(x)": False}])


def test_astype_str(spark: ReparkSession) -> None:
    """``i.astype("string")`` / ``i.astype(LongType())`` cast and keep the child name.

    pins: column-parity-1/C-002 (astype_str)
    """
    out = _base_df(spark).select(F.col("i").astype("string"), F.col("i").astype(T.LongType()))
    assert out.columns == ["i", "i"]
    assert out.schema.simpleString() == "struct<i:string,i:bigint>"
    table = out.to_arrow()
    assert table.column(0).to_pylist() == ["1", "2", None]
    assert table.column(1).to_pylist() == [1, 2, None]


def test_astype_bad(spark: ReparkSession) -> None:
    """``i.astype(5)`` raises ``NOT_DATATYPE_OR_STR`` for ``dataType``.

    pins: column-parity-1/C-002 (astype_bad)
    """
    with pytest.raises(PySparkTypeError) as raised:
        _base_df(spark).select(F.col("i").astype(5))
    assert raised.value.getCondition() == "NOT_DATATYPE_OR_STR"
    assert raised.value.getMessageParameters() == {"arg_name": "dataType", "arg_type": "int"}


def test_name_alias(spark: ReparkSession) -> None:
    """``i.name("x")`` renames like ``alias``.

    pins: column-parity-1/C-002 (name_alias)
    """
    out = _base_df(spark).select(F.col("i").name("x"))
    _assert_frame(out, ["x"], "struct<x:int>", [{"x": 1}, {"x": 2}, {"x": None}])


def test_name_metadata(spark: ReparkSession) -> None:
    """``i.name("x", metadata={"m": 1})`` lands the metadata on the output StructField.

    pins: column-parity-1/C-002 (name_metadata)
    """
    frame = _base_df(spark).select(F.col("i").name("x", metadata={"m": 1}))
    assert frame.schema["x"].metadata == {"m": 1}


def test_name_multi(spark: ReparkSession) -> None:
    """``explode(arr).name("k", "v")`` keeps the first name (COL-NAME-MULTI-1).

    pins: column-parity-1/C-002 (name_multi); divergence COL-NAME-MULTI-1
    """
    frame = spark.createDataFrame([([1, 2],)], "arr array<int>")
    out = frame.select(F.explode("arr").name("k", "v"))
    _assert_frame(out, ["k"], "struct<k:int>", [{"k": 1}, {"k": 2}])


def test_outer_repr(spark: ReparkSession) -> None:
    """``repr(F.col("i").outer())`` renders ``Column<'lazy(i)'>``.

    pins: column-parity-1/C-003 (outer_repr)
    """
    assert repr(F.col("i").outer()) == "Column<'lazy(i)'>"


def test_outer_marker(spark: ReparkSession) -> None:
    """``F.col("i").outer()`` carries the ``_outer`` marker.

    pins: column-parity-1/C-003
    """
    column = F.col("i").outer()
    assert getattr(column, "_outer", False) is True
    assert getattr(F.col("i"), "_outer", False) is False


def test_outer_plain_select(spark: ReparkSession) -> None:
    """``F.col("i").outer()`` in a plain select answers the column unchanged.

    pins: column-parity-1/C-003 (outer_plain_select)
    """
    out = _base_df(spark).select(F.col("i").outer())
    _assert_frame(out, ["i"], "struct<i:int>", [{"i": 1}, {"i": 2}, {"i": None}])


def test_withfield_add(spark: ReparkSession) -> None:
    """``st.withField("c", lit(9))`` appends a field.

    pins: column-parity-1/C-004 (withfield_add)
    """
    out = _base_df(spark).select(F.col("st").withField("c", F.lit(9)).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,b:string,c:int>>",
        [
            {"r": {"a": 1, "b": "x", "c": 9}},
            {"r": None},
            {"r": {"a": 3, "b": None, "c": 9}},
        ],
    )


def test_withfield_replace(spark: ReparkSession) -> None:
    """``st.withField("a", lit(None))`` replaces ``a`` with a ``void`` field.

    pins: column-parity-1/C-004 (withfield_replace)
    """
    out = _base_df(spark).select(F.col("st").withField("a", F.lit(None)).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:void,b:string>>",
        [
            {"r": {"a": None, "b": "x"}},
            {"r": None},
            {"r": {"a": None, "b": None}},
        ],
    )


def test_withfield_replace_type(spark: ReparkSession) -> None:
    """``st.withField("a", lit("z"))`` replaces ``a`` changing its type.

    pins: column-parity-1/C-004 (withfield_replace_type)
    """
    out = _base_df(spark).select(F.col("st").withField("a", F.lit("z")).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:string,b:string>>",
        [
            {"r": {"a": "z", "b": "x"}},
            {"r": None},
            {"r": {"a": "z", "b": None}},
        ],
    )


def test_withfield_name(spark: ReparkSession) -> None:
    """Unaliased ``st.withField("c", lit(9))`` names ``update_fields(st, WithField(9))``.

    pins: column-parity-1/C-004 (withfield_name)
    """
    out = _base_df(spark).select(F.col("st").withField("c", F.lit(9)))
    assert out.columns == ["update_fields(st, WithField(9))"]
    assert out.schema.simpleString() == (
        "struct<update_fields(st, WithField(9)):struct<a:int,b:string,c:int>>"
    )


def test_withfield_nested(spark: ReparkSession) -> None:
    """``st.withField("inner.z", lit(7))`` walks the dotted path into ``inner``.

    pins: column-parity-1/C-004 (withfield_nested)
    """
    out = _nested_df(spark).select(F.col("st").withField("inner.z", F.lit(7)).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,inner:struct<x:int,y:int,z:int>>>",
        [{"r": {"a": 1, "inner": {"x": 2, "y": 3, "z": 7}}}, {"r": None}],
    )


def test_withfield_nested_replace(spark: ReparkSession) -> None:
    """``withField("inner.x", col("st.a"))`` — no dotted traversal on ``col`` (COL-DOTTED-1).

    pins: column-parity-1/C-004 (withfield_nested_replace); divergence COL-DOTTED-1
    """
    with pytest.raises(AnalysisException) as raised:
        _nested_df(spark).select(F.col("st").withField("inner.x", F.col("st.a")).alias("r"))
    assert "st.a" in str(raised.value)


def test_withfield_missing_parent(spark: ReparkSession) -> None:
    """``withField("nope.z", lit(7))`` raises ``FIELD_NOT_FOUND``.

    pins: column-parity-1/C-004 (withfield_missing_parent)
    """
    with pytest.raises(AnalysisException) as raised:
        _nested_df(spark).select(F.col("st").withField("nope.z", F.lit(7)).alias("r"))
    assert "FIELD_NOT_FOUND" in str(raised.value)
    assert "No such struct field `nope` in `a`, `inner`" in str(raised.value)


def test_withfield_non_struct(spark: ReparkSession) -> None:
    """``i.withField("a", lit(1))`` raises ``DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE``.

    pins: column-parity-1/C-004 (withfield_non_struct)
    """
    with pytest.raises(AnalysisException) as raised:
        _base_df(spark).select(F.col("i").withField("a", F.lit(1)).alias("r"))
    assert "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" in str(raised.value)
    assert "update_fields(i, WithField(1))" in str(raised.value)
    assert '"i" has the type "INT"' in str(raised.value)


def test_withfield_name_not_str(spark: ReparkSession) -> None:
    """``st.withField(1, lit(1))`` raises ``NOT_STR`` for ``fieldName``.

    pins: column-parity-1/C-004 (withfield_name_not_str)
    """
    with pytest.raises(PySparkTypeError) as raised:
        F.col("st").withField(1, F.lit(1))
    assert raised.value.getCondition() == "NOT_STR"
    assert raised.value.getMessageParameters() == {"arg_name": "fieldName", "arg_type": "int"}


def test_withfield_col_not_column(spark: ReparkSession) -> None:
    """``st.withField("a", 1)`` raises ``NOT_COLUMN`` for ``col``.

    pins: column-parity-1/C-004 (withfield_col_not_column)
    """
    with pytest.raises(PySparkTypeError) as raised:
        F.col("st").withField("a", 1)
    assert raised.value.getCondition() == "NOT_COLUMN"
    assert raised.value.getMessageParameters() == {"arg_name": "col", "arg_type": "int"}


def test_withfield_dup_fields(spark: ReparkSession) -> None:
    """Duplicate-named struct: schema matches; Row materialization diverges (COL-DUP-1).

    pins: column-parity-1/C-004 (withfield_dup_fields); divergence COL-DUP-1
    """
    frame = spark.sql("select named_struct('a', 1, 'a', 2) st")
    out = frame.select(F.col("st").withField("a", F.lit(3)).alias("r"))
    assert out.schema.simpleString() == "struct<r:struct<a:int,a:int>>"
    with pytest.raises(ValueError):
        out.collect()


def test_withfield_empty_name(spark: ReparkSession) -> None:
    """``withField("", lit(3))`` — the engine names empty struct fields ``colN`` (COL-WITHFIELD-1).

    pins: column-parity-1/C-004 (withfield_empty_name); divergence COL-WITHFIELD-1
    """
    out = _base_df(spark).select(F.col("st").withField("", F.lit(3)).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,b:string,col2:int>>",
        [
            {"r": {"a": 1, "b": "x", "col2": 3}},
            {"r": None},
            {"r": {"a": 3, "b": None, "col2": 3}},
        ],
    )


def test_withfield_case(spark: ReparkSession) -> None:
    """``st.withField("A", lit(5))`` replaces case-insensitively; the new spelling wins.

    pins: column-parity-1/C-004 (withfield_case)
    """
    out = _base_df(spark).select(F.col("st").withField("A", F.lit(5)).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<A:int,b:string>>",
        [
            {"r": {"A": 5, "b": "x"}},
            {"r": None},
            {"r": {"A": 5, "b": None}},
        ],
    )


def test_withfield_on_null_parent_nested(spark: ReparkSession) -> None:
    """``withField("inner.y", lit(2))`` on a NULL intermediate struct stays NULL.

    pins: column-parity-1/C-004 (withfield_on_null_parent_nested)
    """
    frame = spark.createDataFrame([((1, None),)], "st struct<a:int, inner:struct<x:int>>")
    out = frame.select(F.col("st").withField("inner.y", F.lit(2)).alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,inner:struct<x:int,y:int>>>",
        [{"r": {"a": 1, "inner": None}}],
    )


def test_getfield_after_withfield(spark: ReparkSession) -> None:
    """``st.withField("c", lit(9)).getField("c")`` resolves the added field.

    pins: column-parity-1/C-004 (getfield_after_withfield)
    """
    out = _base_df(spark).select(F.col("st").withField("c", F.lit(9)).getField("c").alias("r"))
    _assert_frame(out, ["r"], "struct<r:int>", [{"r": 9}, {"r": None}, {"r": 9}])


def test_dropfields_one(spark: ReparkSession) -> None:
    """``st.dropFields("b")`` removes one field.

    pins: column-parity-1/C-005 (dropfields_one)
    """
    out = _base_df(spark).select(F.col("st").dropFields("b").alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int>>",
        [{"r": {"a": 1}}, {"r": None}, {"r": {"a": 3}}],
    )


def test_dropfields_name(spark: ReparkSession) -> None:
    """Unaliased ``st.dropFields("b")`` names ``update_fields(st, dropfield())``.

    pins: column-parity-1/C-005 (dropfields_name)
    """
    out = _base_df(spark).select(F.col("st").dropFields("b"))
    assert out.columns == ["update_fields(st, dropfield())"]
    assert out.schema.simpleString() == "struct<update_fields(st, dropfield()):struct<a:int>>"


def test_dropfields_all(spark: ReparkSession) -> None:
    """``st.dropFields("a", "b")`` raises ``DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS``.

    pins: column-parity-1/C-005 (dropfields_all)
    """
    with pytest.raises(AnalysisException) as raised:
        _base_df(spark).select(F.col("st").dropFields("a", "b").alias("r"))
    assert "DATATYPE_MISMATCH.CANNOT_DROP_ALL_FIELDS" in str(raised.value)
    assert "Cannot drop all fields in struct" in str(raised.value)


def test_dropfields_missing(spark: ReparkSession) -> None:
    """``st.dropFields("zz")`` on a missing name is a no-op.

    pins: column-parity-1/C-005 (dropfields_missing)
    """
    out = _base_df(spark).select(F.col("st").dropFields("zz").alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,b:string>>",
        [
            {"r": {"a": 1, "b": "x"}},
            {"r": None},
            {"r": {"a": 3, "b": None}},
        ],
    )


def test_dropfields_nested(spark: ReparkSession) -> None:
    """``st.dropFields("inner.x", "a")`` walks the dotted path and drops ``a``.

    pins: column-parity-1/C-005 (dropfields_nested)
    """
    out = _nested_df(spark).select(F.col("st").dropFields("inner.x", "a").alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<inner:struct<y:int>>>",
        [{"r": {"inner": {"y": 3}}}, {"r": None}],
    )


def test_dropfields_non_struct(spark: ReparkSession) -> None:
    """``i.dropFields("a")`` raises ``DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE``.

    pins: column-parity-1/C-005 (dropfields_non_struct)
    """
    with pytest.raises(AnalysisException) as raised:
        _base_df(spark).select(F.col("i").dropFields("a").alias("r"))
    assert "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" in str(raised.value)
    assert "update_fields(i, dropfield())" in str(raised.value)
    assert '"i" has the type "INT"' in str(raised.value)


def test_dropfields_not_str(spark: ReparkSession) -> None:
    """``st.dropFields(1)`` raises ``NOT_STR`` for ``fieldNames`` (COL-DROPFIELDS-TYPE-1).

    pins: column-parity-1/C-005 (dropfields_not_str); declared COL-DROPFIELDS-TYPE-1
    """
    with pytest.raises(PySparkTypeError) as raised:
        F.col("st").dropFields(1)
    assert raised.value.getCondition() == "NOT_STR"
    assert raised.value.getMessageParameters() == {"arg_name": "fieldNames", "arg_type": "int"}


def test_dropfields_none(spark: ReparkSession) -> None:
    """``st.dropFields()`` raises ``UnsupportedOperationException`` ``tail of empty list``.

    pins: column-parity-1/C-005 (dropfields_none)
    """
    with pytest.raises(UnsupportedOperationException, match="tail of empty list"):
        _base_df(spark).select(F.col("st").dropFields().alias("r"))


def test_dropfields_case(spark: ReparkSession) -> None:
    """``st.dropFields("A")`` drops case-insensitively.

    pins: column-parity-1/C-005 (dropfields_case)
    """
    out = _base_df(spark).select(F.col("st").dropFields("A").alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<b:string>>",
        [{"r": {"b": "x"}}, {"r": None}, {"r": {"b": None}}],
    )


def test_dropfields_nonexistent_nested(spark: ReparkSession) -> None:
    """``st.dropFields("inner.zz")`` on a missing nested name is a no-op.

    pins: column-parity-1/C-005 (dropfields_nonexistent_nested)
    """
    out = _nested_df(spark).select(F.col("st").dropFields("inner.zz").alias("r"))
    _assert_frame(
        out,
        ["r"],
        "struct<r:struct<a:int,inner:struct<x:int,y:int>>>",
        [{"r": {"a": 1, "inner": {"x": 2, "y": 3}}}, {"r": None}],
    )


def test_sql_in_null(spark: ReparkSession) -> None:
    """SQL ``IN`` with NULLs answers Spark's three-valued mask.

    pins: column-parity-1/C-006 (sql_in_null)
    """
    out = spark.sql(
        "select 1 in (1, null) a, cast(null as int) in (1) b, 2 in (1, null) c, 2 in (2) d"
    )
    _assert_frame(
        out,
        ["a", "b", "c", "d"],
        "struct<a:boolean,b:boolean,c:boolean,d:boolean>",
        [{"a": True, "b": None, "c": None, "d": True}],
    )


def test_sql_in_mixed(spark: ReparkSession) -> None:
    """SQL ``'a' in ('a', 1)`` raises; the error class diverges (SQL-IN-1).

    pins: column-parity-1/C-006 (sql_in_mixed); divergence SQL-IN-1 / BL-1
    """
    with pytest.raises(PySparkException) as raised:
        spark.sql("select 'a' in ('a', 1) e").collect()
    assert "Cannot cast" in str(raised.value)


def test_sql_isnan(spark: ReparkSession) -> None:
    """SQL ``isnan`` answers Spark's boolean mask including NULL.

    pins: column-parity-1/C-006 (sql_isnan)
    """
    out = spark.sql("select isnan(cast('NaN' as double)) a, isnan(null) b, isnan(1) c")
    _assert_frame(
        out,
        ["a", "b", "c"],
        "struct<a:boolean,b:boolean,c:boolean>",
        [{"a": True, "b": False, "c": False}],
    )


def test_isnan_sql_string(spark: ReparkSession) -> None:
    """SQL ``isnan('NaN')`` refuses the string argument (SQL-ISNAN-1).

    pins: column-parity-1/C-006 (isnan_sql_string); divergence SQL-ISNAN-1
    """
    with pytest.raises(AnalysisException) as raised:
        spark.sql("select isnan('NaN') a").collect()
    assert "isnan" in str(raised.value)


def test_no_sql_spelling_for_struct_edit_names(spark: ReparkSession) -> None:
    """``withField``/``dropFields``/``astype``/``name``/``outer`` have no SQL spelling.

    pins: column-parity-1/C-006
    """
    spark.createDataFrame(
        [(1, (1, "x"))], "i int, st struct<a:int,b:string>"
    ).createOrReplaceTempView("column_parity_1_struct_names")
    for sql in [
        "SELECT withField(st, 'c', 1) FROM column_parity_1_struct_names",
        "SELECT dropFields(st, 'b') FROM column_parity_1_struct_names",
        "SELECT astype(i, 'string') FROM column_parity_1_struct_names",
        "SELECT name(i, 'x') FROM column_parity_1_struct_names",
        "SELECT outer(i) FROM column_parity_1_struct_names",
    ]:
        with pytest.raises(AnalysisException, match="Invalid function"):
            spark.sql(sql)


def test_isnan_struct_refuses(spark: ReparkSession) -> None:
    """``st.isNaN()`` on a struct raises ``DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE``.

    pins: column-parity-1/C-009
    """
    with pytest.raises(AnalysisException) as raised:
        _base_df(spark).select(F.col("st").isNaN())
    assert "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" in str(raised.value)
    assert "isnan" in str(raised.value)
    assert "STRUCT" in str(raised.value)


def test_isnan_array_refuses(spark: ReparkSession) -> None:
    """``arr.isNaN()`` on an array raises ``DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE``.

    pins: column-parity-1/C-009
    """
    frame = spark.createDataFrame([([1, 2],), ([],), (None,)], "arr array<int>")
    with pytest.raises(AnalysisException) as raised:
        frame.select(F.col("arr").isNaN())
    assert "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" in str(raised.value)
    assert "isnan" in str(raised.value)
    assert "ARRAY" in str(raised.value)


def test_groupby_unaliased_arith_key_uses_engine_name(spark: ReparkSession) -> None:
    """Unaliased ``groupBy(i + 1)`` names the key with the engine expression string.

    pins: column-parity-1/C-009; registry COL-GROUPKEY-NAME-1
    """
    out = _base_df(spark).groupBy(F.col("i") + 1).count()
    assert out.columns[1] == "count"
    assert (
        re.fullmatch(r"datafusion\.public\.__repark_cdf_[0-9a-f]+\.i \+ Int32\(1\)", out.columns[0])
        is not None
    )
    assert {row[0]: row[1] for row in out.collect()} == {2: 1, 3: 1, None: 1}


def test_groupby_withfield_key_uses_engine_name(spark: ReparkSession) -> None:
    """Unaliased ``groupBy(withField-getField)`` names the key with the engine string.

    pins: column-parity-1/C-009; registry COL-GROUPKEY-NAME-1
    """
    frame = spark.createDataFrame([((1,),), ((2,),), (None,)], "st struct<a:int>")
    out = frame.groupBy(F.col("st").withField("c", F.lit(9)).getField("c")).count()
    assert out.columns[1] == "count"
    assert (
        re.fullmatch(
            r"update_fields\((?:datafusion\.public\.__repark_cdf_[0-9a-f]+\.)?st,"
            r"Utf8\(\"with\"\),Utf8\(\"c\"\),Int32\(9\)\)\[c\]",
            out.columns[0],
        )
        is not None
    )
    assert {row[0]: row[1] for row in out.collect()} == {9: 2, None: 1}


def test_withfield_empty_name_chain_appends_placeholders(spark: ReparkSession) -> None:
    """Chained ``withField("", ...)`` appends ``col2`` then ``col3`` (COL-WITHFIELD-EMPTY-1).

    pins: column-parity-1/C-009; registry COL-WITHFIELD-EMPTY-1
    """
    out = _base_df(spark).select(
        F.col("st").withField("", F.lit(1)).withField("", F.lit(2)).alias("w")
    )
    assert out.schema.simpleString() == "struct<w:struct<a:int,b:string,col2:int,col3:int>>"
    assert out.to_arrow().to_pylist() == [
        {"w": {"a": 1, "b": "x", "col2": 1, "col3": 2}},
        {"w": None},
        {"w": {"a": 3, "b": None, "col2": 1, "col3": 2}},
    ]
