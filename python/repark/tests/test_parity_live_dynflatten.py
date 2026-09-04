"""Live dynamicFlatten legs against the shared PySpark oracle; contracts in map.md."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path

import _live_parity as lp
import pytest

from repark_parity import assert_frames_equal


def _dynflatten_bed() -> object:
    """Load the measurement-bed generator (datasets tree is not a hatch package)."""
    datasets_dir = Path(__file__).resolve().parents[2] / "repark-parity" / "datasets"
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__dict__["__path__"] = [str(datasets_dir)]
        sys.modules[package_name] = package
    return importlib.import_module("repark_datasets.nested.bed")


def _dynflatten_spark_flatten() -> object:
    """Load the Spark explode program used by the measurement driver."""
    bench_dir = Path(__file__).resolve().parents[2] / "repark-parity" / "bench"
    if str(bench_dir) not in sys.path:
        sys.path.insert(0, str(bench_dir))
    from dynflatten.spark_flatten import spark_dynamic_flatten

    return spark_dynamic_flatten


def _dynflatten_utf8(table: object) -> object:
    """Cast dictionary leaves to utf8 so Spark's decoded parquet matches repark."""
    import pyarrow as pa

    def convert(data_type: pa.DataType) -> pa.DataType:
        if pa.types.is_dictionary(data_type):
            return data_type.value_type
        if pa.types.is_struct(data_type):
            return pa.struct(
                [
                    pa.field(field.name, convert(field.type), nullable=field.nullable)
                    for field in data_type
                ]
            )
        if pa.types.is_list(data_type):
            return pa.list_(convert(data_type.value_type))
        return data_type

    fields = [
        pa.field(field.name, convert(field.type), nullable=field.nullable) for field in table.schema
    ]
    return table.cast(pa.schema(fields))


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize(
    "shape_name",
    ["struct_d3", "list_struct_1", "cartesian_two_lists"],
)
def test_live_dynflatten_matches_spark_explode(
    shape_name: str,
    tmp_path: Path,
    spark_engine: lp.Engine,
) -> None:
    """pins: perf-dynflatten-1-measure/C-002, C-003 DYNFLATTEN-LISTNULL-1 DYNFLATTEN-READNULL-1"""
    from repark import ReparkSession
    from repark.spark.session import _reset_active_session_for_tests

    bed = _dynflatten_bed()
    spark_dynamic_flatten = _dynflatten_spark_flatten()

    path, _digest = bed.write_shape(shape_name, rows=16, seed=42, out=tmp_path)
    _reset_active_session_for_tests()
    session = ReparkSession.builder.appName("pytest-dynflatten-live").getOrCreate()
    try:
        repark_table = session.read.parquet(str(path)).dynamicFlatten().to_arrow()
    finally:
        session.stop()
        _reset_active_session_for_tests()
    spark_frame = spark_engine.session.read.parquet(str(path))
    spark_table = spark_dynamic_flatten(spark_frame).toArrow()
    import pyarrow as pa

    left = _dynflatten_utf8(repark_table)
    right = _dynflatten_utf8(spark_table)
    if shape_name == "struct_d3":
        assert_frames_equal(left, right, order_sensitive=False)
        return
    assert "user_properties" not in left.column_names
    assert "user_properties" in right.column_names
    assert right.schema.field("user_properties").type == pa.int32()
    assert left.schema.field("id").nullable is False
    assert right.schema.field("id").nullable is True
    widened = left.cast(pa.schema([field.with_nullable(True) for field in left.schema]))
    assert_frames_equal(widened, right.drop(["user_properties"]), order_sensitive=False)
