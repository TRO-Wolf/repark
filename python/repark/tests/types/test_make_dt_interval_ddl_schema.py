"""MAKE-DT-INTERVAL-OVERFLOW-1 delta 2: interval DDL spellings and DDL-string schemas."""

from __future__ import annotations

import datetime as dt

import pytest

from repark import ReparkSession
from repark.errors import PySparkTypeError
from repark.spark._type_table import _parse_datatype_string
from repark.spark.types import DataType

DDL_CELLS = {
    "interval year": "interval year",
    "interval year to year": None,
    "interval year to month": "interval year to month",
    "interval year to day": None,
    "interval year to hour": None,
    "interval year to minute": None,
    "interval year to second": None,
    "interval month": "interval month",
    "interval month to year": None,
    "interval month to month": None,
    "interval month to day": None,
    "interval month to hour": None,
    "interval month to minute": None,
    "interval month to second": None,
    "interval day": "interval day",
    "interval day to year": None,
    "interval day to month": None,
    "interval day to day": None,
    "interval day to hour": "interval day to hour",
    "interval day to minute": "interval day to minute",
    "interval day to second": "interval day to second",
    "interval hour": "interval hour",
    "interval hour to year": None,
    "interval hour to month": None,
    "interval hour to day": None,
    "interval hour to hour": None,
    "interval hour to minute": "interval hour to minute",
    "interval hour to second": "interval hour to second",
    "interval minute": "interval minute",
    "interval minute to year": None,
    "interval minute to month": None,
    "interval minute to day": None,
    "interval minute to hour": None,
    "interval minute to minute": None,
    "interval minute to second": "interval minute to second",
    "interval second": "interval second",
    "interval second to year": None,
    "interval second to month": None,
    "interval second to day": None,
    "interval second to hour": None,
    "interval second to minute": None,
    "interval second to second": None,
    "INTERVAL DAY TO DAY": None,
    "Interval Year To Year": None,
    "INTERVAL HOUR TO SECOND": "interval hour to second",
    "interval  minute  to  second": "interval minute to second",
}
CDF_SCHEMAS = [
    "id int, x INTERVAL DAY TO SECOND",
    "id int, x array<interval day to second>",
    "id int, x interval day",
    "id int, x interval day to second",
    "id int, x interval hour to minute",
    "id int, x interval minute to second",
    "id int, x interval month",
    "id int, x interval second",
    "id int, x interval year",
    "id int, x interval year to month",
    "id int, x map<string,interval second>",
    "id int, x struct<a:array<interval hour>>",
    "id int, x struct<a:interval day,b:int>",
]

ROWS = {
    "td": [(1, dt.timedelta(days=1, seconds=3, microseconds=5)), (2, None)],
    "none": [(1, None)],
    "empty": [],
    "int": [(1, 5)],
    "str": [(1, "a")],
}


@pytest.mark.parametrize("spelling", sorted(DDL_CELLS))
def test_interval_ddl_spelling_answers_spark(spelling: str) -> None:
    expected = DDL_CELLS[spelling]
    doors = {
        "fromDDL": (lambda: DataType.fromDDL(spelling), expected),
        "fromDDL field": (lambda: DataType.fromDDL(f"x {spelling}"), f"struct<x:{expected}>"),
        "fromDDL nested": (
            lambda: DataType.fromDDL(f"x array<{spelling}>"),
            f"struct<x:array<{expected}>>",
        ),
        "_parse_datatype_string": (lambda: _parse_datatype_string(spelling), expected),
    }
    for door, (parse, answer) in doors.items():
        if expected is None:
            with pytest.raises((ValueError, TypeError)):
                parse()
        else:
            assert parse().simpleString() == answer, door


@pytest.mark.parametrize("schema", CDF_SCHEMAS)
@pytest.mark.parametrize("rows", sorted(ROWS))
def test_ddl_string_schema_with_an_ansi_interval_refuses_as_on_main(
    spark: ReparkSession, schema: str, rows: str
) -> None:
    with pytest.raises(PySparkTypeError, match="must be a DDL field list"):
        spark.createDataFrame(ROWS[rows], schema)


def test_ddl_string_schema_without_an_interval_still_answers(spark: ReparkSession) -> None:
    df = spark.createDataFrame([(1, "a")], "id int, x string")
    assert df.dtypes == [("id", "int"), ("x", "string")]
    assert [tuple(row) for row in df.collect()] == [(1, "a")]
