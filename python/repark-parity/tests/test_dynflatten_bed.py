"""Engine-free pins for the dynamicFlatten measurement bed.

pins: perf-dynflatten-1-measure/C-001, C-002
"""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark_parity import assert_frames_equal

_DATASETS_DIR = Path(__file__).resolve().parents[1] / "datasets"
_BED = _DATASETS_DIR / "nested" / "bed.py"
_BENCH_DIR = Path(__file__).resolve().parents[1] / "bench"


def _load_datasets() -> None:
    """Import ``python/repark-parity/datasets`` as ``repark_datasets``."""
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _bed() -> Any:
    _load_datasets()
    return importlib.import_module("repark_datasets.nested.bed")


def test_shapes_cover_the_charter_axes() -> None:
    bed = _bed()
    names = {shape.name for shape in bed.SHAPES}
    assert names == {
        "struct_d3",
        "struct_d6",
        "list_struct_1",
        "list_struct_8",
        "list_struct_64",
        "cartesian_two_lists",
        "null_typed_list",
    }
    assert bed.NULL_PARENT_RATE == 0.30
    assert bed.QUICK_ROWS == 100_000
    assert bed.FULL_ROWS == 1_000_000
    assert bed.GATE_ROWS == 64
    assert "list_struct_64" in bed.FULL_SKIP_SHAPES


def test_struct_d3_schema_has_capitalized_and_dict_and_void_list() -> None:
    bed = _bed()
    schema = bed.schema_for(bed.SHAPE_BY_NAME["struct_d3"])
    assert schema.field("Payload").name == "Payload"
    assert "id" not in schema.names
    assert pa.types.is_struct(schema.field("Payload").type)
    payload = schema.field("Payload").type
    leaf = payload.field("L1").type.field("L2").type
    assert leaf.field("Name").name == "Name"
    assert pa.types.is_dictionary(leaf.field("Name").type)
    void_schema = bed.schema_for(bed.SHAPE_BY_NAME["null_typed_list"])
    assert pa.types.is_list(void_schema.field("user_properties").type)
    assert pa.types.is_null(void_schema.field("user_properties").type.value_type)


def test_small_same_seed_is_table_identical() -> None:
    bed = _bed()
    first = bed.small("struct_d3", rows=64, seed=42)
    second = bed.small("struct_d3", rows=64, seed=42)
    assert_frames_equal(first, second, order_sensitive=True)
    assert bed.table_digest(first) == bed.table_digest(second)


def test_small_seed_change_moves_digest() -> None:
    bed = _bed()
    first = bed.small("list_struct_1", rows=32, seed=42)
    second = bed.small("list_struct_1", rows=32, seed=0)
    assert bed.table_digest(first) != bed.table_digest(second)


def test_null_parent_rate_is_near_thirty_percent() -> None:
    bed = _bed()
    table = bed.small("struct_d3", rows=64, seed=42)
    payload = table.column("Payload")
    nulls = payload.null_count
    rate = nulls / table.num_rows
    assert 0.15 <= rate <= 0.45, rate


def test_cartesian_has_two_sibling_lists() -> None:
    bed = _bed()
    schema = bed.schema_for(bed.SHAPE_BY_NAME["cartesian_two_lists"])
    assert pa.types.is_list(schema.field("Legs").type)
    assert pa.types.is_list(schema.field("Tags").type)
    table = bed.small("cartesian_two_lists", rows=8, seed=42)
    populated = next(
        row for row in table.column("Legs").to_pylist() if isinstance(row, list) and row
    )
    assert len(populated) == bed.CARTESIAN_LIST_WIDTH


def test_list_struct_widths() -> None:
    bed = _bed()
    for width in (1, 8, 64):
        name = f"list_struct_{width}"
        table = bed.small(name, rows=8, seed=42)
        populated = next(
            row for row in table.column("Legs").to_pylist() if isinstance(row, list) and row
        )
        assert len(populated) == width


def test_write_parquet_round_trip(tmp_path: Path) -> None:
    bed = _bed()
    expected = bed.small("null_typed_list", rows=16, seed=42)
    path, digest = bed.write_shape("null_typed_list", rows=16, seed=42, out=tmp_path)
    assert path.is_file()
    assert digest
    restored = __import__("pyarrow.parquet", fromlist=["read_table"]).read_table(path)
    restored = restored.cast(bed.schema_for(bed.SHAPE_BY_NAME["null_typed_list"]))
    assert_frames_equal(restored, expected, order_sensitive=True)


def test_write_bed_gate_manifest(tmp_path: Path) -> None:
    bed = _bed()
    manifest = bed.write_bed(scale="gate", seed=42, out=tmp_path)
    assert manifest["rows"] == 64
    assert len(manifest["files"]) == len(bed.SHAPES)
    assert (tmp_path / "manifest.json").is_file()
    for row in manifest["files"]:
        assert (tmp_path / row["path"]).is_file()


def test_full_scale_skips_list_struct_64() -> None:
    bed = _bed()
    names = {shape.name for shape in bed.shapes_for_scale("full")}
    assert "list_struct_64" not in names
    assert "struct_d6" in names


def test_refuse_real_dataset_flags() -> None:
    bed = _bed()
    for flag in bed.FORBIDDEN_CLI_FLAGS:
        with pytest.raises(ValueError, match="real-dataset flag"):
            bed.refuse_real_dataset_inputs(argv=[flag, "/tmp/real.parquet"], environ={})
    for key in bed.FORBIDDEN_ENV_KEYS:
        with pytest.raises(ValueError, match="real-dataset environment"):
            bed.refuse_real_dataset_inputs(argv=[], environ={key: "/tmp/real.parquet"})


def test_cli_refuses_input_flag() -> None:
    result = subprocess.run(
        [sys.executable, str(_BED), "--input", "/tmp/real.parquet", "--out", "/tmp/x"],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ},
    )
    assert result.returncode == 2
    assert "real-dataset" in result.stderr


def test_write_refuses_repo_out() -> None:
    bed = _bed()
    repo_root = Path(__file__).resolve().parents[3]
    with pytest.raises(ValueError, match="inside the repository"):
        bed.write_shape("struct_d3", rows=8, seed=42, out=repo_root / "target" / "dynflatten-bed")


def test_unknown_shape_refused() -> None:
    bed = _bed()
    with pytest.raises(ValueError, match="unknown shape"):
        bed.generate("not_a_shape", 8, 42)


def _share_fixture(kind: str, execute: float, rewrite: float) -> Any:
    """Build a ranking fixture with a known execute/rewrite split."""
    if str(_BENCH_DIR) not in sys.path:
        sys.path.insert(0, str(_BENCH_DIR))
    from dynflatten.models import EngineTiming, FixtureResult

    return FixtureResult(
        shape=kind,
        kind=kind,
        struct_depth=1,
        list_width=None,
        rows_in=64,
        parquet_bytes=1,
        digest="x",
        repark=EngineTiming(
            engine="repark",
            outcome="ok",
            warmup=0,
            iterations=1,
            median_rewrite_ms=rewrite,
            median_execute_ms=execute,
            median_wall_ms=rewrite + execute,
        ),
        spark=EngineTiming(engine="pyspark", outcome="skip", warmup=0, iterations=0),
        row_set_equal=None,
        wall_ratio_repark_over_spark=None,
    )


def test_rank_candidates_orders_by_share() -> None:
    if str(_BENCH_DIR) not in sys.path:
        sys.path.insert(0, str(_BENCH_DIR))
    from dynflatten.measure import rank_candidates

    ranked = rank_candidates(
        [
            _share_fixture("struct", execute=80.0, rewrite=1.0),
            _share_fixture("cartesian", execute=10.0, rewrite=1.0),
            _share_fixture("list_struct", execute=8.0, rewrite=1.0),
        ]
    )
    assert ranked[0].name == "null_mask_struct_extractor"
    assert ranked[0].verdict == "implement"
    walks = next(item for item in ranked if item.name == "optimizer_wrapper_walks")
    assert walks.verdict == "not worth it"
