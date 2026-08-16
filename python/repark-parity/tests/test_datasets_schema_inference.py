"""Generator-shape and determinism pins for the schema-inference family (DS-2).

Pure pyarrow — no repark, no Spark, no JVM. CSV is the inference battleground;
parquet is typed truth. Facade/smartCsv pins wait for DS-4.
"""

from __future__ import annotations

import csv
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
_DATAGEN_PATH = _DATASETS_DIR / "schema_inference" / "datagen.py"


def _load_datasets() -> None:
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _datagen() -> Any:
    _load_datasets()
    return importlib.import_module("repark_datasets.schema_inference.datagen")


def test_small_defaults_and_conflict_at() -> None:
    datagen = _datagen()
    assert datagen.SMALL_ROWS == 64
    assert datagen.DEFAULT_SEED == 42
    assert datagen.DEFAULT_CONFLICT_AT == 500_000
    table = datagen.small()
    assert table.num_rows == 64
    assert table.schema.equals(datagen.SCHEMA)


def test_small_same_seed_is_table_identical() -> None:
    datagen = _datagen()
    first = datagen.small(rows=64, seed=42, conflict_at=32)
    second = datagen.small(rows=64, seed=42, conflict_at=32)
    assert_frames_equal(first, second, order_sensitive=True)


def test_small_seed_change_moves_values() -> None:
    datagen = _datagen()
    first = datagen.small(rows=64, seed=42, conflict_at=32)
    second = datagen.small(rows=64, seed=0, conflict_at=32)
    with pytest.raises(Exception, match="mismatch"):
        assert_frames_equal(first, second, order_sensitive=True)


def test_manifest_labels_every_class_and_column() -> None:
    datagen = _datagen()
    manifest = datagen.load_manifest()
    assert manifest["family"] == "schema_inference"
    assert manifest["conflict_at_cli_default"] == 500_000
    classes = manifest["classes"]
    assert classes, "manifest must list at least one class"
    table = datagen.small(rows=64, seed=42, conflict_at=32)
    names = set(table.column_names)
    for entry in classes:
        assert entry["column"] in names, entry
        values = table.column(entry["column"]).to_pylist()
        assert any(value is not None for value in values), entry["id"]


def test_int_widens_shifts_at_conflict_at() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42, conflict_at=32)
    values = table.column("int_widens").to_pylist()
    assert all(value <= datagen.INT32_MAX for value in values[:32])
    assert all(value > datagen.INT32_MAX for value in values[32:])
    assert table.schema.field("int_widens").type == pa.int64()


def test_string_vs_float_halves_at_conflict_at() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42, conflict_at=32)
    values = table.column("str_or_float").to_pylist()
    assert all(value.isalpha() for value in values[:32])
    assert all(any(character.isdigit() for character in value) for value in values[32:])


def test_typed_columns_match_declared_parquet_types() -> None:
    datagen = _datagen()
    schema = datagen.SCHEMA
    assert schema.field("boolish_int").type == pa.int32()
    assert pa.types.is_string(schema.field("dateish").type)
    assert pa.types.is_string(schema.field("currency").type)
    assert pa.types.is_string(schema.field("leading_zero_id").type)
    assert pa.types.is_string(schema.field("empty_or_null").type)
    assert pa.types.is_string(schema.field("euro_decimal").type)
    assert schema.field("scientific").type == pa.float64()
    assert pa.types.is_timestamp(schema.field("ts_looking").type)
    assert schema.field("bool_spelling").type == pa.bool_()


def test_write_parquet_matches_small_and_csv_has_conflict_text(tmp_path: Path) -> None:
    datagen = _datagen()
    expected = datagen.small(rows=64, seed=42, conflict_at=32)
    written = datagen.write_files(rows=64, seed=42, out=tmp_path, conflict_at=32)
    parquet_path = written / datagen.DATA_PARQUET
    csv_path = written / datagen.DATA_CSV
    assert parquet_path.is_file()
    assert csv_path.is_file()
    assert_frames_equal(datagen.read_parquet(parquet_path), expected, order_sensitive=True)

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 64
    assert all(len(row["leading_zero_id"]) == 6 for row in rows)
    assert rows[0]["leading_zero_id"].startswith("0")
    post = rows[32]["int_widens"]
    assert int(post) > datagen.INT32_MAX
    currency_marks = {row["currency"][0] for row in rows}
    assert currency_marks & {"$", "€", "£"}
    tokens = {row["empty_or_null"] for row in rows}
    assert "" in tokens
    assert "null" in tokens
    assert any("," in row["euro_decimal"] for row in rows)
    assert any("e" in row["scientific"] for row in rows)


def test_cli_writes_under_out(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(_DATAGEN_PATH),
            "--rows",
            "8",
            "--seed",
            "42",
            "--conflict-at",
            "4",
            "--out",
            str(tmp_path),
        ],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    datagen = _datagen()
    expected = datagen.small(rows=8, seed=42, conflict_at=4)
    assert_frames_equal(
        datagen.read_parquet(tmp_path / datagen.DATA_PARQUET),
        expected,
        order_sensitive=True,
    )


def test_refuse_rows_seed_conflict_at() -> None:
    datagen = _datagen()
    with pytest.raises(ValueError, match="rows"):
        datagen.small(rows=0, seed=42)
    with pytest.raises(ValueError, match="seed"):
        datagen.small(rows=8, seed=-1)
    with pytest.raises(ValueError, match="conflict_at"):
        datagen.small(rows=8, seed=42, conflict_at=-1)
