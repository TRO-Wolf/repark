"""Both-door pins for the decimal_overflow torture family."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from _support import read_frame_door, read_sql_door

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark_parity.torture import FAMILIES, Family, FamilyOutput
from repark_parity.torture.decimal_overflow import DECIMAL_OVERFLOW_FAMILY

PARQUET_VIEW = "torture_decimal_overflow_parquet"
CSV_VIEW = "torture_decimal_overflow_csv"


def test_family_satisfies_protocol() -> None:
    """The decimal_overflow module exposes the registry's Family implementor."""
    assert isinstance(FAMILIES["decimal_overflow"], Family)
    assert FAMILIES["decimal_overflow"] is DECIMAL_OVERFLOW_FAMILY


def _sql_outcome(spark: ReparkSession, view: str, select: str) -> Any:
    """Run one SQL-door aggregate and return the exception it raised, else None."""
    try:
        return spark.sql(f"SELECT {select} FROM {view}").to_arrow()
    except Exception as exc:
        return exc


def _agg_outcome(frame: Any, func: Callable[[str], Any], first: str, second: str) -> Any:
    """Run one DataFrame-door aggregate and return the exception it raised, else None."""
    try:
        return frame.agg(func(first), func(second)).to_arrow()
    except Exception as exc:
        return exc


def _open_parquet_view(spark: ReparkSession, data: FamilyOutput, view: str) -> Any:
    """Open the family's Parquet file and register it as a temp view."""
    frame = spark.read.parquet(str(data.parquet_path))
    frame.createOrReplaceTempView(view)  # type: ignore[attr-defined]
    return frame


def test_decimal_overflow_parquet_dataframe_door_row_count(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated decimal_overflow row from Parquet."""
    table = read_frame_door(spark, decimal_overflow_data.parquet_path, "parquet")
    assert table.num_rows == decimal_overflow_data.rows


def test_decimal_overflow_parquet_sql_door_row_count(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """The spark.sql door reads every generated decimal_overflow row from Parquet."""
    table = read_sql_door(spark, decimal_overflow_data.parquet_path, "parquet", PARQUET_VIEW)
    assert table.num_rows == decimal_overflow_data.rows


def test_decimal_overflow_parquet_dataframe_door_schema_matches_declared(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """The DataFrame door answers the declared decimal(38,0) Parquet schema."""
    table = read_frame_door(spark, decimal_overflow_data.parquet_path, "parquet")
    expected = DECIMAL_OVERFLOW_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_decimal_overflow_parquet_sql_door_schema_matches_declared(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """The spark.sql door answers the declared decimal(38,0) Parquet schema."""
    table = read_sql_door(spark, decimal_overflow_data.parquet_path, "parquet", PARQUET_VIEW)
    expected = DECIMAL_OVERFLOW_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_decimal_overflow_csv_dataframe_door_row_count(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated decimal_overflow row from CSV."""
    table = read_frame_door(spark, decimal_overflow_data.csv_path, "csv")
    assert table.num_rows == decimal_overflow_data.rows


def test_decimal_overflow_csv_sql_door_row_count(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """The spark.sql door reads every generated decimal_overflow row from CSV."""
    table = read_sql_door(spark, decimal_overflow_data.csv_path, "csv", CSV_VIEW)
    assert table.num_rows == decimal_overflow_data.rows


@pytest.mark.parametrize(
    "column",
    [
        pytest.param("v", marks=pytest.mark.xfail(strict=True, reason="CSV-INFER-20DIGIT")),
        pytest.param("w", marks=pytest.mark.xfail(strict=True, reason="CSV-INFER-20DIGIT")),
    ],
)
def test_decimal_overflow_inferred_type_matches_declared_csv(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput, column: str
) -> None:
    """Both doors infer the declared decimal type for one 38-digit CSV column."""
    declared = DECIMAL_OVERFLOW_FAMILY.expected_read_schema("csv")
    assert declared is not None
    frame_table = read_frame_door(spark, decimal_overflow_data.csv_path, "csv")
    sql_table = read_sql_door(spark, decimal_overflow_data.csv_path, "csv", CSV_VIEW)
    expected = declared.field(column).type
    assert frame_table.schema.field(column).type == expected
    assert sql_table.schema.field(column).type == expected


@pytest.mark.xfail(strict=True, reason="SUM-DEC-I128WRAP-1")
def test_decimal_overflow_sum_refuses_loud(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """Both doors refuse the decimal(38,0) sum loud instead of answering a wrapped value."""
    frame = _open_parquet_view(spark, decimal_overflow_data, PARQUET_VIEW)
    sql_outcome = _sql_outcome(spark, PARQUET_VIEW, "SUM(v), SUM(w)")
    assert isinstance(sql_outcome, Exception), "sql door answered the overflowing sum"
    frame_outcome = _agg_outcome(frame, F.sum, "v", "w")
    assert isinstance(frame_outcome, Exception), "dataframe door answered the overflowing sum"


def test_decimal_overflow_avg_refuses_loud(
    spark: ReparkSession, decimal_overflow_data: FamilyOutput
) -> None:
    """Both doors refuse the decimal(38,0) average loud at the aggregate boundary."""
    frame = _open_parquet_view(spark, decimal_overflow_data, PARQUET_VIEW)
    sql_outcome = _sql_outcome(spark, PARQUET_VIEW, "AVG(v), AVG(w)")
    assert isinstance(sql_outcome, Exception), "sql door answered the overflowing average"
    frame_outcome = _agg_outcome(frame, F.avg, "v", "w")
    assert isinstance(frame_outcome, Exception), "dataframe door answered the overflowing average"
