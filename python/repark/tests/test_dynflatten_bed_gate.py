"""Gate-scale bed flatten on repark. pins: perf-dynflatten-1-measure/C-001, C-002"""

from __future__ import annotations

import importlib
import sys
import types
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark.session import _reset_active_session_for_tests

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
    """A short-lived RePark session for the gate-scale flatten."""
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-dynflatten-bed-gate").getOrCreate()
    try:
        yield session
    finally:
        session.stop()
        _reset_active_session_for_tests()


def test_gate_bed_struct_and_cartesian_flatten(spark: ReparkSession, tmp_path: Path) -> None:
    """64-row bed parquet flattens on repark (value and type on the Arrow path)."""
    import pyarrow.parquet as pq

    bed = _bed()
    for shape_name in ("struct_d3", "cartesian_two_lists", "null_typed_list"):
        path, _digest = bed.write_shape(shape_name, rows=bed.GATE_ROWS, seed=42, out=tmp_path)
        loaded = spark.createDataFrame(
            pq.read_table(path).to_pylist(), schema=bed.ddl_for(shape_name)
        )
        table = loaded.dynamicFlatten().to_arrow()
        assert table.num_rows >= 1
        assert "user_properties" not in table.column_names
        id_name = next(name for name in table.column_names if name == "id" or name.endswith("_id"))
        assert table.schema.field(id_name).type.byte_width == 8
