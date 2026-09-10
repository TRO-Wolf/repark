"""Both-door pins for the nested torture family at the declared parity schema."""

from __future__ import annotations

from pathlib import Path

from _support import (
    assert_loud_rows,
    read_frame_door,
    read_frame_door_loud,
    read_sql_door,
    read_sql_door_loud,
)

from repark import ReparkSession
from repark_parity.torture import FAMILIES, Family, FamilyOutput
from repark_parity.torture.nested import NESTED_FAMILY, declared_read_schema

VIEW = "torture_nested_parquet"
CSV_VIEW = "torture_nested_csv"


def test_family_satisfies_protocol() -> None:
    """The nested module exposes the registry's Family implementor."""
    assert isinstance(FAMILIES["nested"], Family)
    assert FAMILIES["nested"] is NESTED_FAMILY


def test_nested_parquet_dataframe_door_row_count(
    spark: ReparkSession, nested_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated nested row from Parquet."""
    table = read_frame_door(spark, nested_data.parquet_path, "parquet")
    assert table.num_rows == nested_data.rows


def test_nested_parquet_sql_door_row_count(spark: ReparkSession, nested_data: FamilyOutput) -> None:
    """The spark.sql door reads every generated nested row from Parquet."""
    table = read_sql_door(spark, nested_data.parquet_path, "parquet", VIEW)
    assert table.num_rows == nested_data.rows


def test_nested_parquet_dataframe_door_schema_matches_declared(
    spark: ReparkSession, nested_data: FamilyOutput
) -> None:
    """The DataFrame door answers the declared parity schema for nested Parquet."""
    table = read_frame_door(spark, nested_data.parquet_path, "parquet")
    expected = NESTED_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_nested_parquet_sql_door_schema_matches_declared(
    spark: ReparkSession, nested_data: FamilyOutput
) -> None:
    """The spark.sql door answers the declared parity schema for nested Parquet."""
    table = read_sql_door(spark, nested_data.parquet_path, "parquet", VIEW)
    expected = NESTED_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_nested_csv_both_doors_complete_or_refuse_loud(
    spark: ReparkSession, nested_data: FamilyOutput
) -> None:
    """The JSON-text CSV leg reads the row count on both doors or refuses loud on both."""
    frame_outcome = read_frame_door_loud(spark, nested_data.csv_path, "csv")
    sql_outcome = read_sql_door_loud(spark, nested_data.csv_path, "csv", CSV_VIEW)
    assert_loud_rows(frame_outcome, nested_data.rows, "dataframe")
    assert_loud_rows(sql_outcome, nested_data.rows, "sql")


def test_nested_depth_width_scaling_matches_declared(spark: ReparkSession, tmp_path: Path) -> None:
    """A depth/width-scaled generate answers that scale's declared schema on both doors."""
    output = NESTED_FAMILY.generate(rows=8, seed=7, out=tmp_path, depth=2, width=1)
    expected = declared_read_schema(depth=2, width=1)
    frame_table = read_frame_door(spark, output.parquet_path, "parquet")
    sql_table = read_sql_door(spark, output.parquet_path, "parquet", "torture_nested_scaled")
    assert frame_table.num_rows == 8
    assert sql_table.num_rows == 8
    assert frame_table.schema.equals(expected)
    assert sql_table.schema.equals(expected)
