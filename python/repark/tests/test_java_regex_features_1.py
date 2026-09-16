from __future__ import annotations

import json
import pathlib
from typing import Any

import pyarrow as pa
import pytest

from repark.errors import PySparkException
from repark.spark import functions as F  # noqa: N812

_FIXTURE = json.loads(
    pathlib.Path(__file__).with_name("java_regex_features_1_spark_oracle.json").read_text()
)
_CELLS: dict[str, dict[str, Any]] = {cell["id"]: cell for cell in _FIXTURE["cells"]}

_BOOL = pa.bool_()
_STR = pa.string()
_INT = pa.int32()
_SPLIT_LIST = pa.list_(pa.field("element", pa.string(), nullable=False))
_EXTRACT_ALL_LIST = pa.list_(pa.field("item", pa.string(), nullable=True))

VALUES: dict[str, tuple[Any, pa.DataType, bool]] = {
    "RX-SQL-00": (True, _BOOL, False),
    "RX-SQL-01": (True, _BOOL, False),
    "RX-SQL-02": (True, _BOOL, False),
    "RX-SQL-03": (True, _BOOL, False),
    "RX-SQL-04": (False, _BOOL, False),
    "RX-SQL-05": (False, _BOOL, False),
    "RX-SQL-06": ("aa", _STR, False),
    "RX-SQL-07": ("aX aX", _STR, False),
    "RX-SQL-08": (2, _INT, False),
    "RX-SQL-09": ("123", _STR, False),
    "RX-SQL-10": (3, _INT, False),
    "RX-SQL-11": (["a1", "b2", "c"], _SPLIT_LIST, False),
    "RX-SQL-12": (["aa", "bb", "cc"], _EXTRACT_ALL_LIST, False),
    "RX-SQL-15": ("a[]c", _STR, False),
    "RX-SQL-16": (True, _BOOL, False),
    "RX-SQL-17": (True, _BOOL, False),
    "RX-SQL-18": (False, _BOOL, True),
    "RX-SQL-19": (False, _BOOL, False),
    "RX-SQL-20": (True, _BOOL, False),
    "RX-SQL-21": (True, _BOOL, False),
    "RX-SQL-22": (None, _BOOL, True),
    "RX-SQL-23": ("ab", _STR, False),
    "RX-SQL-24": (False, _BOOL, False),
    "RX-PY-00": (True, _BOOL, False),
    "RX-PY-01": ("aa", _STR, False),
    "RX-PY-02": ("aX aX", _STR, False),
    "RX2-SQL-01": (False, _BOOL, True),
    "RX2-SQL-02": (["a", ",b", ",c"], _SPLIT_LIST, False),
    "RX2-SQL-03": (["", "-", ""], _SPLIT_LIST, False),
    "RX2-SQL-04": (["aaa"], _SPLIT_LIST, False),
    "RX2-SQL-05": ("XX", _STR, False),
    "RX2-SQL-06": ("b", _STR, False),
    "RX2-SQL-07": ("b", _STR, False),
    "RX2-SQL-08": (True, _BOOL, False),
    "RX2-SQL-09": (True, _BOOL, False),
    "RX2-SQL-10": (True, _BOOL, False),
    "RX2-SQL-11": (False, _BOOL, False),
    "RX2-SQL-12": (True, _BOOL, False),
    "RX2-SQL-13": (3, _INT, False),
    "RX2-SQL-14": ("aabcaabc", _STR, False),
    "RX2-SQL-15": (False, _BOOL, False),
    "RX2-SQL-18": (True, _BOOL, False),
    "RX2-SQL-19": (["1", "22", "333"], _EXTRACT_ALL_LIST, False),
    "RX2-SQL-20": (True, _BOOL, False),
    "RX2-SQL-21": (False, _BOOL, False),
    "RX2-SQL-22": (True, _BOOL, False),
    "RX2-SQL-23": (3, _INT, False),
    "RX2-SQL-24": ("b", _STR, False),
    "RX2-SQL-26": (True, _BOOL, False),
    "RX2-SQL-27": ("", _STR, False),
    "RX2-SQL-28": (None, _BOOL, True),
    "RX2-SQL-29": (None, _BOOL, True),
}

ERRORS: dict[str, str] = {
    "RX-SQL-13": "invalid regular expression",
    "RX-SQL-14": "invalid regular expression",
    "RX2-SQL-00": "overrun",
    "RX2-SQL-16": "INVALID_PARAMETER_VALUE.PATTERN",
    "RX2-SQL-17": "invalid regular expression",
    "RX2-SQL-25": "INVALID_PARAMETER_VALUE.PATTERN",
}


def _session():  # type: ignore[no-untyped-def]
    from repark.spark import SparkSession

    return SparkSession.builder.appName("java-regex-features-1").getOrCreate()


def _check_value(cell_id: str) -> None:
    """Run one oracle cell on its own door and pin value, Arrow type and nullability."""
    cell = _CELLS[cell_id]
    value, arrow_type, nullable = VALUES[cell_id]
    if cell["door"] == "sql":
        table = _session().sql(cell["expr"]).toArrow()
    else:
        frame = eval(cell["expr"], {"spark": _session(), "F": F})  # noqa: S307
        table = frame.toArrow()
    assert table.column("v").to_pylist() == [value]
    assert table.schema.field("v").type == arrow_type
    assert table.schema.field("v").nullable == nullable


@pytest.mark.parametrize("cell_id", sorted(VALUES), ids=sorted(VALUES))
def test_oracle_value_cell(cell_id: str) -> None:
    """One oracle value cell answers Spark on its own door. pins: java-regex-features-1/C-001, C-002, C-003, C-006"""
    _check_value(cell_id)


@pytest.mark.parametrize("cell_id", sorted(ERRORS), ids=sorted(ERRORS))
def test_oracle_error_cell(cell_id: str) -> None:
    """One oracle error cell fails loudly with the pinned message. pins: java-regex-features-1/C-004, C-005"""
    cell = _CELLS[cell_id]
    with pytest.raises(PySparkException, match=ERRORS[cell_id]):
        _session().sql(cell["expr"]).toArrow()


def test_python_door_fancy_match_names() -> None:
    """Python-door match names share the Rust kernel. pins: java-regex-features-1/C-006"""
    spark = _session()
    like = spark.range(1).select(F.regexp_like(F.lit("aa"), F.lit(r"(a)\1")).alias("v"))
    assert like.toArrow().column("v").to_pylist() == [True]
    count = spark.range(1).select(F.regexp_count(F.lit("aXbXc"), F.lit("(?=X)")).alias("v"))
    assert count.toArrow().column("v").to_pylist() == [2]
    instr = spark.range(1).select(F.regexp_instr(F.lit("abab"), F.lit("(?<=b)a")).alias("v"))
    assert instr.toArrow().column("v").to_pylist() == [3]


def test_python_door_fancy_extract_names() -> None:
    """Python-door extract names share the Rust kernel. pins: java-regex-features-1/C-006"""
    spark = _session()
    substr = spark.range(1).select(F.regexp_substr(F.lit("foo123bar"), F.lit(r"\d++")).alias("v"))
    assert substr.toArrow().column("v").to_pylist() == ["123"]
    assert substr.toArrow().schema.field("v").type == pa.string()
    extract_all = spark.range(1).select(
        F.regexp_extract_all(F.lit("aa bb"), F.lit(r"(\w)\1"), 0).alias("v")
    )
    assert extract_all.toArrow().column("v").to_pylist() == [["aa", "bb"]]
    split = spark.range(1).select(F.split(F.lit("a,b,c"), "(?=,)").alias("v"))
    assert split.toArrow().column("v").to_pylist() == [["a", ",b", ",c"]]


def test_python_door_null_pattern_stays_null() -> None:
    """NULL pattern on the Python door is NULL, never an error. pins: java-regex-features-1/C-001"""
    spark = _session()
    table = (
        spark.range(1)
        .select(F.lit("ab").rlike(F.lit(None).cast("string")).alias("v"))
        .toArrow()
    )
    assert table.column("v").to_pylist() == [None]
