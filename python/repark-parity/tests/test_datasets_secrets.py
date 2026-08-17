"""Generator-shape, hygiene and determinism pins for the secrets family (DS-3).

ACCEPTANCE PIN — read this before adding an expectation to any of these tests.
Reads of this fixture behave **NORMALLY** today: nothing redacts, masks, warns
about, or refuses a credential-named data column. The opt-in secrets-flagging
mechanism is a roadmap feature that this fixture deliberately PREDATES, so the
fixture's job is to exist and stay honest until that feature arrives, not to
assert behavior that does not exist. Facade-level read pins land in DS-4, not
here.

The needle inventory this family stands in for is the facade's
``prop_key_is_secret`` mirror — but this lane is pure pyarrow and does NOT import
repark, so the needles are carried as labels in ``manifest.json`` and re-derived
here with the same fold (lowercase, hyphen/dot to underscore, then underscores
stripped for the compact form).
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
_DATAGEN_PATH = _DATASETS_DIR / "secrets" / "datagen.py"


def _load_datasets() -> None:
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _datagen() -> Any:
    _load_datasets()
    return importlib.import_module("repark_datasets.secrets.datagen")


def _fold(name: str) -> tuple[str, str]:
    """The ``prop_key_is_secret`` fold: (lower with separators folded, compact form)."""
    lower = name.lower().replace("-", "_").replace(".", "_")
    return lower, lower.replace("_", "")


def test_small_defaults_are_a9_64_and_42() -> None:
    datagen = _datagen()
    assert datagen.SMALL_ROWS == 64
    assert datagen.DEFAULT_SEED == 42
    assert datagen.DEFAULT_CLI_ROWS == 1_000_000
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
    assert manifest["family"] == "secrets"
    assert manifest["fake_value_prefix"] == datagen.FAKE_PREFIX
    classes = manifest["classes"]
    assert classes, "manifest must list at least one class"
    table = datagen.small()
    names = list(table.column_names)
    declared = [entry["column"] for entry in classes]
    assert declared == names, "manifest must cover the schema, in schema order"
    for entry in classes:
        values = table.column(entry["column"]).to_pylist()
        assert any(value is not None for value in values), entry["id"]


def test_manifest_needle_labels_match_the_column_names() -> None:
    """Each labeled needle must actually be present in the column name it labels."""
    datagen = _datagen()
    for entry in datagen.load_manifest()["classes"]:
        lower, compact = _fold(entry["column"])
        if entry["secret"]:
            needle = entry["needle"]
            assert needle is not None, entry["id"]
            haystack = compact if entry["needle_form"] == "compact" else lower
            assert needle in haystack, entry
        else:
            assert entry["needle"] is None, entry
            assert entry["needle_form"] == "none", entry


def test_bucket_key_is_the_documented_carve_out() -> None:
    """`bucket_key` ends with `_key` yet is NOT secret — the `bucket` carve-out."""
    datagen = _datagen()
    lower, _compact = _fold(datagen.CARVE_OUT_COLUMN)
    assert lower.endswith("_key")
    assert "bucket" in lower
    by_column = {entry["column"]: entry for entry in datagen.load_manifest()["classes"]}
    assert by_column[datagen.CARVE_OUT_COLUMN]["secret"] is False
    table = datagen.small()
    for value in table.column(datagen.CARVE_OUT_COLUMN).to_pylist():
        assert value.startswith("warehouse/")
        assert not value.startswith(datagen.FAKE_PREFIX)


def test_every_secret_value_is_obviously_fake() -> None:
    """Hard hygiene fence: synthetic marker present, real credential shapes absent."""
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    for column, _class_id in datagen.SECRET_COLUMNS:
        values = table.column(column).to_pylist()
        assert any(value is not None for value in values), column
        for value in values:
            if value is None:
                continue
            assert value.startswith(datagen.FAKE_PREFIX), (column, value)
            for forbidden in datagen.FORBIDDEN_VALUE_PREFIXES:
                assert not value.startswith(forbidden), (column, value)
            assert "@" not in value
            assert "://" not in value


def test_secret_values_vary_in_length_but_not_in_shape() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    lengths = {len(value) for value in table.column("apiKey").to_pylist()}
    assert len(lengths) > 1, "filler must vary the value length"
    assert datagen.fake_secret("apikey-camel", 42) == f"{datagen.FAKE_PREFIX}apikey-camel-000042"


def test_nullable_secret_column_has_nulls_and_values() -> None:
    datagen = _datagen()
    table = datagen.small(rows=64, seed=42)
    values = table.column(datagen.NULLABLE_SECRET_COLUMN).to_pylist()
    assert values[0] is None
    assert any(value is not None for value in values)
    assert sum(1 for value in values if value is None) == len(
        [index for index in range(64) if index % datagen.NULL_EVERY == 0]
    )
    assert table.schema.field(datagen.NULLABLE_SECRET_COLUMN).nullable is True


def test_declared_types_are_string_except_the_id() -> None:
    datagen = _datagen()
    schema = datagen.SCHEMA
    assert schema.field("id").type == pa.int64()
    for column, _class_id in datagen.SECRET_COLUMNS:
        assert pa.types.is_string(schema.field(column).type), column


def test_write_parquet_matches_small_and_csv_carries_the_columns(tmp_path: Path) -> None:
    datagen = _datagen()
    expected = datagen.small(rows=64, seed=42)
    written = datagen.write_files(rows=64, seed=42, out=tmp_path)
    parquet_path = written / datagen.DATA_PARQUET
    csv_path = written / datagen.DATA_CSV
    assert parquet_path.is_file()
    assert csv_path.is_file()
    assert_frames_equal(datagen.read_parquet(parquet_path), expected, order_sensitive=True)

    with csv_path.open(encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 64
    assert set(rows[0]) == set(expected.column_names)
    assert rows[0]["apiKey"].startswith(datagen.FAKE_PREFIX)
    assert rows[0]["api_key"].startswith(datagen.FAKE_PREFIX)
    # The nullable credential column writes an empty cell where the value is null.
    assert rows[0][datagen.NULLABLE_SECRET_COLUMN] == ""


def test_cli_writes_under_out(tmp_path: Path) -> None:
    result = subprocess.run(
        [sys.executable, str(_DATAGEN_PATH), "--rows", "8", "--seed", "42", "--out", str(tmp_path)],
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
