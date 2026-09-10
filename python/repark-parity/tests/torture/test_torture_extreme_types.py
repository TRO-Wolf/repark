"""Both-door pins for the extreme_types torture family."""

from __future__ import annotations

import pytest
from _support import read_frame_door, read_sql_door

from repark import ReparkSession
from repark_parity.torture import FAMILIES, Family, FamilyOutput
from repark_parity.torture.extreme_types import EXTREME_TYPES_FAMILY

PARQUET_VIEW = "torture_extreme_parquet"
CSV_VIEW = "torture_extreme_csv"


def test_family_satisfies_protocol() -> None:
    """The extreme_types module exposes the registry's Family implementor."""
    assert isinstance(FAMILIES["extreme_types"], Family)
    assert FAMILIES["extreme_types"] is EXTREME_TYPES_FAMILY


def test_extreme_parquet_dataframe_door_row_count(
    spark: ReparkSession, extreme_types_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated extreme_types row from Parquet."""
    table = read_frame_door(spark, extreme_types_data.parquet_path, "parquet")
    assert table.num_rows == extreme_types_data.rows


def test_extreme_parquet_sql_door_row_count(
    spark: ReparkSession, extreme_types_data: FamilyOutput
) -> None:
    """The spark.sql door reads every generated extreme_types row from Parquet."""
    table = read_sql_door(spark, extreme_types_data.parquet_path, "parquet", PARQUET_VIEW)
    assert table.num_rows == extreme_types_data.rows


def test_extreme_parquet_dataframe_door_schema_matches_declared(
    spark: ReparkSession, extreme_types_data: FamilyOutput
) -> None:
    """The DataFrame door answers the declared extreme_types Parquet schema."""
    table = read_frame_door(spark, extreme_types_data.parquet_path, "parquet")
    expected = EXTREME_TYPES_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_extreme_parquet_sql_door_schema_matches_declared(
    spark: ReparkSession, extreme_types_data: FamilyOutput
) -> None:
    """The spark.sql door answers the declared extreme_types Parquet schema."""
    table = read_sql_door(spark, extreme_types_data.parquet_path, "parquet", PARQUET_VIEW)
    expected = EXTREME_TYPES_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_extreme_csv_dataframe_door_row_count(
    spark: ReparkSession, extreme_types_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated extreme_types row from CSV."""
    table = read_frame_door(spark, extreme_types_data.csv_path, "csv")
    assert table.num_rows == extreme_types_data.rows


def test_extreme_csv_sql_door_row_count(
    spark: ReparkSession, extreme_types_data: FamilyOutput
) -> None:
    """The spark.sql door reads every generated extreme_types row from CSV."""
    table = read_sql_door(spark, extreme_types_data.csv_path, "csv", CSV_VIEW)
    assert table.num_rows == extreme_types_data.rows


@pytest.mark.parametrize(
    "column",
    [
        pytest.param("big38", marks=pytest.mark.xfail(strict=True, reason="CSV-INFER-20DIGIT")),
        pytest.param("uuid"),
        pytest.param("paragraph"),
        pytest.param("html"),
    ],
)
def test_extreme_inferred_type_matches_declared_csv(
    spark: ReparkSession, extreme_types_data: FamilyOutput, column: str
) -> None:
    """Both doors infer the declared type for one extreme_types CSV column."""
    declared = EXTREME_TYPES_FAMILY.expected_read_schema("csv")
    assert declared is not None
    frame_table = read_frame_door(spark, extreme_types_data.csv_path, "csv")
    sql_table = read_sql_door(spark, extreme_types_data.csv_path, "csv", CSV_VIEW)
    expected = declared.field(column).type
    assert frame_table.schema.field(column).type == expected
    assert sql_table.schema.field(column).type == expected
