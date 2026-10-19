"""pins: dynflatten-listnull-1/C-001, C-002, C-003, C-004, C-005, C-006"""

from __future__ import annotations

import importlib
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.types import ArrayType, NullType

_DATASETS_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "datasets"


def _bed() -> Any:
    """Load the measurement-bed generator."""
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__dict__["__path__"] = [str(_DATASETS_DIR)]
        sys.modules[package_name] = package
    return importlib.import_module("repark_datasets.nested.bed")


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A short-lived RePark session for the list-null pins."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-dynflatten-listnull").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


def _write_void_list_parquet(path: Path) -> None:
    """Write a parquet whose user_properties is Arrow list<null> (physical int32 Null)."""
    table = pa.table(
        {
            "id": pa.array([0, 1, 2], type=pa.int64()),
            "user_properties": pa.array([None, [], [None]], type=pa.list_(pa.null())),
        }
    )
    pq.write_table(table, path)


@pytest.mark.parametrize("door", ["read.parquet", "read_parquet"])
def test_parquet_null_list_reads_as_int32_on_both_doors(
    spark: ReparkSession, tmp_path: Path, door: str
) -> None:
    """Parquet list<null> reads as list<int32> on both DataFrame doors."""
    path = tmp_path / "void.parquet"
    _write_void_list_parquet(path)
    if door == "read.parquet":
        frame = spark.read.parquet(str(path))
    else:
        frame = spark.read_parquet(str(path))
    table = frame.orderBy("id").to_arrow()
    field = table.schema.field("user_properties")
    assert pa.types.is_list(field.type)
    assert field.type.value_type == pa.int32()
    assert field.nullable is True
    assert field.type.value_field.nullable is True
    assert table.column("user_properties").to_pylist() == [None, [], [None]]


def test_dynamic_flatten_keeps_parquet_null_list_as_int32_nulls(
    spark: ReparkSession, tmp_path: Path
) -> None:
    """Default dynamicFlatten keeps the parquet-inferred int32 list as nullable NULLs."""
    bed = _bed()
    path, _digest = bed.write_shape("list_struct_1", rows=16, seed=42, out=tmp_path)
    source = spark.read.parquet(str(path))
    source_field = source.to_arrow().schema.field("user_properties")
    assert pa.types.is_list(source_field.type)
    assert source_field.type.value_type == pa.int32()
    flat = source.dynamicFlatten()
    table = flat.orderBy("id").to_arrow()
    field = table.schema.field("user_properties")
    assert field.type == pa.int32()
    assert field.nullable is True
    assert all(value is None for value in table.column("user_properties").to_pylist())
    assert table.num_rows == source.count()
    assert "Legs_leg_id" in table.column_names
    assert "Legs_Name" in table.column_names


def test_create_dataframe_void_list_still_drops(spark: ReparkSession) -> None:
    """drop_null_lists=True still drops an actual ARRAY<VOID> createDataFrame column."""
    frame = spark.sql("SELECT 1 AS id, make_array() AS user_properties")
    props_type = frame.schema["user_properties"].dataType
    assert isinstance(props_type, ArrayType)
    assert isinstance(props_type.elementType, NullType)
    flat = frame.dynamicFlatten()
    assert "user_properties" not in flat.columns
    assert flat.columns == ["id"]
