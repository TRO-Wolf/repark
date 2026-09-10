"""Both-door pins for the inference torture family's declared per-column types."""

from __future__ import annotations

import pytest
from _support import read_frame_door, read_sql_door

from repark import ReparkSession
from repark_parity.torture import FAMILIES, Family, FamilyOutput
from repark_parity.torture.inference import INFERENCE_FAMILY

VIEW = "torture_inference_csv"
PARQUET_VIEW = "torture_inference_parquet"


def test_family_satisfies_protocol() -> None:
    """The inference module exposes the registry's Family implementor."""
    assert isinstance(FAMILIES["inference"], Family)
    assert FAMILIES["inference"] is INFERENCE_FAMILY


def test_inference_csv_dataframe_door_row_count(
    spark: ReparkSession, inference_data: FamilyOutput
) -> None:
    """The DataFrame door reads every generated inference row from CSV."""
    table = read_frame_door(spark, inference_data.csv_path, "csv")
    assert table.num_rows == inference_data.rows


def test_inference_csv_sql_door_row_count(
    spark: ReparkSession, inference_data: FamilyOutput
) -> None:
    """The spark.sql door reads every generated inference row from CSV."""
    table = read_sql_door(spark, inference_data.csv_path, "csv", VIEW)
    assert table.num_rows == inference_data.rows


@pytest.mark.parametrize(
    "column",
    [
        pytest.param("growth"),
        pytest.param("halves"),
        pytest.param(
            "boolish", marks=pytest.mark.xfail(strict=True, reason="CSV-INFER-INT32-WIDTH")
        ),
        pytest.param("datish"),
    ],
)
def test_inferred_type_matches_declared_csv(
    spark: ReparkSession, inference_data: FamilyOutput, column: str
) -> None:
    """Both doors infer the declared type for one inference column."""
    declared = INFERENCE_FAMILY.expected_read_schema("csv")
    assert declared is not None
    frame_table = read_frame_door(spark, inference_data.csv_path, "csv")
    sql_table = read_sql_door(spark, inference_data.csv_path, "csv", VIEW)
    expected = declared.field(column).type
    assert frame_table.schema.field(column).type == expected
    assert sql_table.schema.field(column).type == expected


def test_inference_parquet_dataframe_door_schema_matches_declared(
    spark: ReparkSession, inference_data: FamilyOutput
) -> None:
    """The DataFrame door answers the declared resolved schema and row count from Parquet."""
    table = read_frame_door(spark, inference_data.parquet_path, "parquet")
    expected = INFERENCE_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.num_rows == inference_data.rows
    assert table.schema.equals(expected)


def test_inference_parquet_sql_door_schema_matches_declared(
    spark: ReparkSession, inference_data: FamilyOutput
) -> None:
    """The spark.sql door answers the declared resolved schema and row count from Parquet."""
    table = read_sql_door(spark, inference_data.parquet_path, "parquet", PARQUET_VIEW)
    expected = INFERENCE_FAMILY.expected_read_schema("parquet")
    assert expected is not None
    assert table.num_rows == inference_data.rows
    assert table.schema.equals(expected)
