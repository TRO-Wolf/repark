"""Both-door pins for the temporal torture family."""

from __future__ import annotations

import pytest
from _support import read_frame_door, read_sql_door

from repark import ReparkSession
from repark_parity.torture import FAMILIES, Family, FamilyOutput
from repark_parity.torture.temporal import TEMPORAL_FAMILY

PARQUET_VIEW = "torture_temporal_parquet"
CSV_VIEW = "torture_temporal_csv"


def test_family_satisfies_protocol() -> None:
    """The temporal module exposes the registry's Family implementor."""
    assert isinstance(FAMILIES["temporal"], Family)
    assert FAMILIES["temporal"] is TEMPORAL_FAMILY


def test_temporal_parquet_dataframe_door_row_count(
    spark: ReparkSession, temporal_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated temporal row from Parquet."""
    table = read_frame_door(spark, temporal_data.parquet_path, "parquet")
    assert table.num_rows == temporal_data.rows


def test_temporal_parquet_sql_door_row_count(
    spark: ReparkSession, temporal_data: FamilyOutput
) -> None:
    """The spark.sql door reads every generated temporal row from Parquet."""
    table = read_sql_door(spark, temporal_data.parquet_path, "parquet", PARQUET_VIEW)
    assert table.num_rows == temporal_data.rows


def test_temporal_parquet_dataframe_door_schema_matches_declared(
    spark: ReparkSession, temporal_data: FamilyOutput
) -> None:
    """The DataFrame door answers the declared temporal Parquet schema."""
    table = read_frame_door(spark, temporal_data.parquet_path, "parquet")
    expected = TEMPORAL_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_temporal_parquet_sql_door_schema_matches_declared(
    spark: ReparkSession, temporal_data: FamilyOutput
) -> None:
    """The spark.sql door answers the declared temporal Parquet schema."""
    table = read_sql_door(spark, temporal_data.parquet_path, "parquet", PARQUET_VIEW)
    expected = TEMPORAL_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.schema.equals(expected)


def test_temporal_csv_dataframe_door_row_count(
    spark: ReparkSession, temporal_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated temporal row from CSV."""
    table = read_frame_door(spark, temporal_data.csv_path, "csv")
    assert table.num_rows == temporal_data.rows


def test_temporal_csv_sql_door_row_count(spark: ReparkSession, temporal_data: FamilyOutput) -> None:
    """The spark.sql door reads every generated temporal row from CSV."""
    table = read_sql_door(spark, temporal_data.csv_path, "csv", CSV_VIEW)
    assert table.num_rows == temporal_data.rows


@pytest.mark.parametrize(
    "column",
    [
        pytest.param("ts_text"),
        pytest.param("day_text"),
        pytest.param("dur_text"),
        pytest.param(
            "months", marks=pytest.mark.xfail(strict=True, reason="CSV-INFER-INT32-WIDTH")
        ),
        pytest.param("days", marks=pytest.mark.xfail(strict=True, reason="CSV-INFER-INT32-WIDTH")),
        pytest.param("nanos", marks=pytest.mark.xfail(strict=True, reason="CSV-INFER-INT32-WIDTH")),
    ],
)
def test_temporal_inferred_type_matches_declared_csv(
    spark: ReparkSession, temporal_data: FamilyOutput, column: str
) -> None:
    """Both doors infer the declared type for one temporal CSV column."""
    declared = TEMPORAL_FAMILY.expected_read_schema("csv")
    assert declared is not None
    frame_table = read_frame_door(spark, temporal_data.csv_path, "csv")
    sql_table = read_sql_door(spark, temporal_data.csv_path, "csv", CSV_VIEW)
    expected = declared.field(column).type
    assert frame_table.schema.field(column).type == expected
    assert sql_table.schema.field(column).type == expected


@pytest.mark.xfail(strict=True, reason="BL-14")
def test_temporal_try_add_zero_hour_promotes(
    spark: ReparkSession, temporal_data: FamilyOutput
) -> None:
    """Both doors promote `try_add(day, INTERVAL 0 HOUR)` to a timestamp column."""
    frame = spark.read.parquet(str(temporal_data.parquet_path))
    frame.createOrReplaceTempView(PARQUET_VIEW)  # type: ignore[attr-defined]
    table = spark.sql(
        f"SELECT try_add(day, INTERVAL 0 HOUR) AS promoted FROM {PARQUET_VIEW}"
    ).to_arrow()
    assert table.schema.field("promoted").type == TEMPORAL_FAMILY.PROMOTED_TYPE


@pytest.mark.xfail(strict=True, reason="DATE-INTERVAL-NSBOUND-1")
def test_temporal_date_plus_one_day_completes(
    spark: ReparkSession, temporal_data: FamilyOutput
) -> None:
    """Both doors complete `day + INTERVAL 1 DAY` at the whole-day edge."""
    frame = spark.read.parquet(str(temporal_data.parquet_path))
    frame.createOrReplaceTempView(PARQUET_VIEW)  # type: ignore[attr-defined]
    table = spark.sql(f"SELECT day + INTERVAL 1 DAY AS moved FROM {PARQUET_VIEW}").to_arrow()
    assert table.num_rows == temporal_data.rows
    assert table.schema.field("moved").type == TEMPORAL_FAMILY.PROMOTED_TYPE
