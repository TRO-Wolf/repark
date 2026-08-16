"""Generator-shape and determinism pins for the extreme-types family (DS-2).

Pure pyarrow — no repark. The smartCsv p>38 demotion is documented here as
POLICY for a later facade pin; this increment does not read through the engine.
"""

from __future__ import annotations

import csv
import importlib
import subprocess
import sys
import types
import uuid
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark_parity import assert_frames_equal

_DATASETS_DIR = Path(__file__).resolve().parents[1] / "datasets"
_DATAGEN_PATH = _DATASETS_DIR / "extreme_types" / "datagen.py"


def _load_datasets() -> None:
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _datagen() -> Any:
    _load_datasets()
    return importlib.import_module("repark_datasets.extreme_types.datagen")


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


def test_manifest_labels_every_class_and_column() -> None:
    datagen = _datagen()
    manifest = datagen.load_manifest()
    assert manifest["family"] == "extreme_types"
    table = datagen.small()
    names = set(table.column_names)
    for entry in manifest["classes"]:
        assert entry["column"] in names, entry
        values = table.column(entry["column"]).to_pylist()
        assert any(value is not None for value in values), entry["id"]


def test_decimal128_scale_and_beyond_38() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    decimal_field = table.schema.field("decimal_hi")
    assert pa.types.is_decimal(decimal_field.type)
    assert decimal_field.type.precision == 24
    assert decimal_field.type.scale == 21
    first = table.column("decimal_hi").to_pylist()[0]
    assert isinstance(first, Decimal)
    assert first >= Decimal("102.102334252345232345233")

    beyond = table.column("beyond_38").to_pylist()
    for token in beyond:
        digits = token.replace(".", "")
        assert len(digits) > 38, token


def test_uuid_paragraph_html_shapes() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    for token in table.column("uuid_col").to_pylist():
        parsed = uuid.UUID(token)
        assert parsed.version == 5
    for text in table.column("paragraph").to_pylist():
        assert len(text) >= 200
        assert "@" not in text
        assert "http" not in text
    for fragment in table.column("html_fragment").to_pylist():
        assert "<div" in fragment
        assert "https://example.com/" in fragment
        assert "http://" not in fragment


def test_write_parquet_matches_small(tmp_path: Path) -> None:
    datagen = _datagen()
    expected = datagen.small(rows=64, seed=42)
    written = datagen.write_files(rows=64, seed=42, out=tmp_path)
    assert (written / datagen.DATA_PARQUET).is_file()
    assert (written / datagen.DATA_CSV).is_file()
    assert_frames_equal(
        datagen.read_parquet(written / datagen.DATA_PARQUET),
        expected,
        order_sensitive=True,
    )
    with (written / datagen.DATA_CSV).open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 64
    assert rows[0]["beyond_38"].startswith("1")
    assert "102.10233425234523234523" in rows[0]["decimal_hi"]


def test_cli_writes_under_out(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            str(_DATAGEN_PATH),
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
    with pytest.raises(ValueError, match="seed"):
        datagen.small(rows=8, seed=-1)
