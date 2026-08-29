"""Generator-shape and determinism pins for the nested torture family.

Pure pyarrow — no repark, no Spark, no JVM. Data lands only under tmp_path / the
cache helper, never in the repository tree.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import types
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark_parity import assert_frames_equal

_DATASETS_DIR = Path(__file__).resolve().parents[1] / "datasets"
_NESTED_DATAGEN = _DATASETS_DIR / "nested" / "datagen.py"


def _load_datasets() -> None:
    """Import ``python/repark-parity/datasets`` as ``repark_datasets`` (bench loader)."""
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _cache() -> Any:
    _load_datasets()
    return importlib.import_module("repark_datasets._cache")


def _datagen() -> Any:
    _load_datasets()
    return importlib.import_module("repark_datasets.nested.datagen")


def test_small_defaults_are_a9_64_and_42() -> None:
    datagen = _datagen()
    assert datagen.SMALL_ROWS == 64
    assert datagen.DEFAULT_SEED == 42
    table = datagen.small()
    assert table.num_rows == 64
    assert table.schema.equals(datagen.SCHEMA)


def test_small_same_seed_is_table_identical() -> None:
    datagen = _datagen()
    first = datagen.small(rows=64, seed=42)
    second = datagen.small(rows=64, seed=42)
    assert_frames_equal(first, second, order_sensitive=True)


def test_small_seed_change_moves_values() -> None:
    datagen = _datagen()
    first = datagen.small(rows=64, seed=42)
    second = datagen.small(rows=64, seed=0)
    with pytest.raises(Exception, match="mismatch"):
        assert_frames_equal(first, second, order_sensitive=True)


def test_schema_has_required_nested_classes() -> None:
    datagen = _datagen()
    schema = datagen.SCHEMA
    assert schema.field("Legs").name == "Legs"
    assert pa.types.is_list(schema.field("Legs").type)
    assert pa.types.is_struct(schema.field("Legs").type.value_type)
    assert pa.types.is_list(schema.field("Tags").type)
    assert pa.types.is_string(schema.field("Tags").type.value_type)
    assert pa.types.is_list(schema.field("Scores").type)
    assert pa.types.is_int32(schema.field("Scores").type.value_type)
    assert pa.types.is_list(schema.field("user_properties").type)
    assert pa.types.is_null(schema.field("user_properties").type.value_type)
    depth = datagen.schema_nesting_depth(schema.field("Legs").type)
    assert depth >= 6, depth
    assert set(datagen.CLASSES) == {
        "deep_nesting",
        "list_of_struct",
        "capitalized_legs",
        "mixed_element_types",
        "null_typed_list",
        "empty_list_row",
        "null_list_row",
    }


def test_small_emits_every_labeled_row_class() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    legs = table.column("Legs").to_pylist()
    properties = table.column("user_properties").to_pylist()
    assert any(row is None for row in legs)
    assert any(row == [] for row in legs)
    assert any(isinstance(row, list) and len(row) > 0 for row in legs)
    populated = next(row for row in legs if isinstance(row, list) and row)
    assert "leg_id" in populated[0]
    assert "Fills" in populated[0]
    assert any(cell is None for cell in properties)
    assert any(cell == [] for cell in properties)
    # Deep path present on a populated fill when Fills is non-empty.
    deep_seen = False
    for row in legs:
        if not row:
            continue
        for leg in row:
            fills = leg.get("Fills") or []
            for fill in fills:
                extra = (fill.get("Meta") or {}).get("Extra") or {}
                if extra.get("Deep") is not None:
                    deep_seen = True
    assert deep_seen


def test_write_and_reread_parquet_matches_small(tmp_path: Path) -> None:
    datagen = _datagen()
    expected = datagen.small(rows=64, seed=42)
    written = datagen.write_files(rows=64, seed=42, out=tmp_path)
    parquet_path = written / datagen.DATA_PARQUET
    jsonl_path = written / datagen.DATA_JSONL
    assert parquet_path.is_file()
    assert jsonl_path.is_file()
    assert_frames_equal(datagen.read_parquet(parquet_path), expected, order_sensitive=True)
    assert_frames_equal(datagen.read_jsonl(jsonl_path), expected, order_sensitive=True)


def test_cli_writes_under_out(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(_NESTED_DATAGEN),
            "--rows",
            "8",
            "--seed",
            "42",
            "--out",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    datagen = _datagen()
    assert (tmp_path / datagen.DATA_PARQUET).is_file()
    assert (tmp_path / datagen.DATA_JSONL).is_file()
    expected = datagen.small(rows=8, seed=42)
    assert_frames_equal(
        datagen.read_parquet(tmp_path / datagen.DATA_PARQUET),
        expected,
        order_sensitive=True,
    )


def test_refuse_rows_and_seed() -> None:
    datagen = _datagen()
    with pytest.raises(ValueError, match="rows"):
        datagen.small(rows=0, seed=42)
    with pytest.raises(ValueError, match="rows"):
        datagen.small(rows=-1, seed=42)
    with pytest.raises(ValueError, match="seed"):
        datagen.small(rows=8, seed=-1)
    with pytest.raises(ValueError, match="rows"):
        datagen.generate(datagen.MAX_ROWS + 1, 42)


def test_unknown_family_refused() -> None:
    cache = _cache()
    with pytest.raises(ValueError, match="unknown dataset family"):
        cache.family_cache_dir("not-a-family")


def test_default_root_respects_xdg(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    cache = _cache()
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    root = cache.default_datasets_root()
    assert root == tmp_path / "xdg" / "repark-datasets"
    family = cache.family_cache_dir("nested")
    assert family == root / "nested"
    monkeypatch.delenv("XDG_CACHE_HOME")
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    # Path.home() reads the passwd entry, not HOME, on some platforms — pin both.
    monkeypatch.setattr(cache.Path, "home", classmethod(lambda _cls: tmp_path / "home"))
    fallback = cache.default_datasets_root()
    assert fallback == tmp_path / "home" / ".cache" / "repark-datasets"


def test_refuse_symlink_cache(tmp_path: Path) -> None:
    cache = _cache()
    real = tmp_path / "real"
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    with pytest.raises(ValueError, match="symlink"):
        cache.prepare_output_dir(link, root=link)


def test_refuse_repository_output() -> None:
    cache = _cache()
    repo_root = Path(__file__).resolve().parents[3]
    assert (repo_root / "AGENTS.md").is_file()
    with pytest.raises(ValueError, match="inside the repository"):
        cache.refuse_repository_output(repo_root / "target" / "datasets-out")


def test_write_files_refuses_repo_out() -> None:
    datagen = _datagen()
    repo_root = Path(__file__).resolve().parents[3]
    with pytest.raises(ValueError, match="inside the repository"):
        datagen.write_files(rows=8, seed=42, out=repo_root / "target" / "c18-nested")
