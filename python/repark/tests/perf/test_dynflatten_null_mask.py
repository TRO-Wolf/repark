"""Gate-scale row set and schema. pins: perf-dynflatten-2-null-mask/C-001, C-003"""

from __future__ import annotations

import hashlib
import importlib
import json
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

_DATASETS_DIR = Path(__file__).resolve().parents[2] / "repark-parity" / "datasets"

ROWS: dict[str, tuple[int, str, str]] = {
    "cartesian_legs_only": (
        202,
        "id: int64\nLegs_leg_id: int64\nLegs_Name: string",
        "2a1bcb0f0003202504e00fb056533c96bd6819ef669093d346791647a692e8a2",
    ),
    "cartesian_tags_only": (
        202,
        "id: int64\nTags: string",
        "d9ee1e309b16da52bfc4f3873e664cc0b6c3311e7540ab6d51320d4155754f6b",
    ),
    "cartesian_two_lists": (
        559,
        "id: int64\nLegs_leg_id: int64\nLegs_Name: string\nTags: string",
        "17a6cdfdb6ab831675c8293da5e3ff78f9554a6ca44f4fef27c3e691fbde621f",
    ),
    "list_struct_1": (
        64,
        "id: int64\nLegs_leg_id: int64\nLegs_Name: string",
        "f283b41ba8162b72c7396dfb718185638c3b018d5def17fc36f2c87edac2d5ac",
    ),
    "list_struct_64": (
        3088,
        "id: int64\nLegs_leg_id: int64\nLegs_Name: string",
        "0521380643c236f54e8345a164c86a025dd399473968f776a996cfa3df221e89",
    ),
    "list_struct_8": (
        365,
        "id: int64\nLegs_leg_id: int64\nLegs_Name: string",
        "3d59bb6c6801553600bd2b937fc49ff706a17668b66085841d689a202046e3d2",
    ),
    "null_typed_list": (
        64,
        "id: int64\nPayload_L1_Name: string\nPayload_L1_Val: int64",
        "024a034a9ddcc53e9d46ed6b91cbf0586529247dc93ff4846de7a2d54662a7d0",
    ),
    "struct_d3": (
        64,
        "Payload_L1_L2_id: int64\nPayload_L1_L2_Name: string\nPayload_L1_L2_Val: int64",
        "df594a4ee32362930ea6e96a94b130d69a781f0cd636f5cca33e9a27c57924f5",
    ),
    "struct_d3_nonull": (
        64,
        "Payload_L1_L2_id: int64\nPayload_L1_L2_Name: string\nPayload_L1_L2_Val: int64",
        "29015d9f328c252ad48d6c5c3282648c72a53e43e4e30717cff40ab6d89d8987",
    ),
    "struct_d6": (
        64,
        "Payload_L1_L2_L3_L4_L5_id: int64\nPayload_L1_L2_L3_L4_L5_Name: string\n"
        "Payload_L1_L2_L3_L4_L5_Val: int64",
        "7f0a172f640d986d8288f78c07ab5f774fdeadae39c13f9acb1016c486860fae",
    ),
    "struct_d6_nonull": (
        64,
        "Payload_L1_L2_L3_L4_L5_id: int64\nPayload_L1_L2_L3_L4_L5_Name: string\n"
        "Payload_L1_L2_L3_L4_L5_Val: int64",
        "1fa43f0ba68e4b71a37125773de3439d66263d821f0892303a08cc0fae8264d6",
    ),
}


def _bed() -> Any:
    """Load the measurement-bed generator."""
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__dict__["__path__"] = [str(_DATASETS_DIR)]
        sys.modules[package_name] = package
    return importlib.import_module("repark_datasets.nested.bed")


def _row_digest(table: Any) -> str:
    """SHA-256 of the table's Python rows in order."""
    payload = json.dumps(table.to_pylist(), sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A short-lived RePark session for the row-set pins."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-dynflatten-null-mask").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_every_bed_shape_is_covered() -> None:
    """The pinned table names every shape the bed generates."""
    assert sorted(ROWS) == sorted(shape.name for shape in _bed().SHAPES)


@pytest.mark.parametrize("shape_name", sorted(ROWS))
def test_gate_scale_flatten_row_set_and_schema_are_unchanged(
    spark: ReparkSession, tmp_path: Path, shape_name: str
) -> None:
    """Row count, Arrow schema and ordered row digest equal the pre-extractor values."""
    import pyarrow.parquet as pq

    bed = _bed()
    path, _digest = bed.write_shape(shape_name, rows=bed.GATE_ROWS, seed=42, out=tmp_path)
    loaded = spark.createDataFrame(pq.read_table(path).to_pylist(), schema=bed.ddl_for(shape_name))
    table = loaded.dynamicFlatten().to_arrow()
    num_rows, schema, digest = ROWS[shape_name]
    assert table.num_rows == num_rows
    assert str(table.schema) == schema
    assert _row_digest(table) == digest
