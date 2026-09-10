"""Both-door pins for the smartcsv torture family."""

from __future__ import annotations

import pytest
from _support import (
    assert_loud_rows,
    read_frame_door,
    read_frame_door_loud,
    read_sql_door,
    read_sql_door_loud,
)

from repark import ReparkSession
from repark_parity.torture import FAMILIES, Family, FamilyOutput
from repark_parity.torture.smartcsv import SMARTCSV_FAMILY

PARQUET_VIEW = "torture_smartcsv_parquet"
CSV_VIEW = "torture_smartcsv_csv"


def test_family_satisfies_protocol() -> None:
    """The smartcsv module exposes the registry's Family implementor."""
    assert isinstance(FAMILIES["smartcsv"], Family)
    assert FAMILIES["smartcsv"] is SMARTCSV_FAMILY


def test_smartcsv_parquet_dataframe_door_row_count(
    spark: ReparkSession, smartcsv_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated smartcsv row from Parquet."""
    table = read_frame_door(spark, smartcsv_data.parquet_path, "parquet")
    assert table.num_rows == smartcsv_data.rows


def test_smartcsv_parquet_sql_door_row_count(
    spark: ReparkSession, smartcsv_data: FamilyOutput
) -> None:
    """The spark.sql door reads every generated smartcsv row from Parquet."""
    table = read_sql_door(spark, smartcsv_data.parquet_path, "parquet", PARQUET_VIEW)
    assert table.num_rows == smartcsv_data.rows


def test_smartcsv_parquet_dataframe_door_schema_matches_declared(
    spark: ReparkSession, smartcsv_data: FamilyOutput
) -> None:
    """The DataFrame door answers the declared smartcsv Parquet schema verbatim."""
    table = read_frame_door(spark, smartcsv_data.parquet_path, "parquet")
    expected = SMARTCSV_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_smartcsv_parquet_sql_door_schema_matches_declared(
    spark: ReparkSession, smartcsv_data: FamilyOutput
) -> None:
    """The spark.sql door answers the declared smartcsv Parquet schema verbatim."""
    table = read_sql_door(spark, smartcsv_data.parquet_path, "parquet", PARQUET_VIEW)
    expected = SMARTCSV_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_smartcsv_csv_both_doors_complete_or_refuse_loud(
    spark: ReparkSession, smartcsv_data: FamilyOutput
) -> None:
    """The capitalised-header CSV leg reads the row count on both doors or refuses loud."""
    frame_outcome = read_frame_door_loud(spark, smartcsv_data.csv_path, "csv")
    sql_outcome = read_sql_door_loud(spark, smartcsv_data.csv_path, "csv", CSV_VIEW)
    assert_loud_rows(frame_outcome, smartcsv_data.rows, "dataframe")
    assert_loud_rows(sql_outcome, smartcsv_data.rows, "sql")


@pytest.mark.xfail(strict=True, reason="CSV-INFER-HEADER-CASE")
def test_smartcsv_csv_schema_matches_declared(
    spark: ReparkSession, smartcsv_data: FamilyOutput
) -> None:
    """Both doors answer the declared inferred schema for the capitalised-header CSV."""
    declared = SMARTCSV_FAMILY.expected_read_schema("csv")
    assert declared is not None
    frame_table = read_frame_door(spark, smartcsv_data.csv_path, "csv")
    sql_table = read_sql_door(spark, smartcsv_data.csv_path, "csv", CSV_VIEW)
    assert frame_table.schema.equals(declared)
    assert sql_table.schema.equals(declared)
