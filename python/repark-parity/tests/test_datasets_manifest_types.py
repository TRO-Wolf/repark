"""Cross-check: every manifest-declared type matches the family's real Arrow schema (DS-3).

Each torture family carries a checked-in ``manifest.json`` whose class rows name a
column and the type that column is supposed to have. Nothing bound those strings to
the actual ``SCHEMA`` until now, so a one-sided edit — retype the field and forget the
manifest, or relabel the manifest and forget the field — passed every gate silently.

This module is the DS-2-review rider: it walks all four labeled families and asserts
manifest type string == Arrow field type, after normalizing the two cosmetic
differences between how a human writes a type and how pyarrow renders one:

* **spacing** — pyarrow renders ``decimal128(24, 21)``; manifests write
  ``decimal128(24,21)``.
* **spelling** — pyarrow renders ``float64`` as ``double`` and ``date32`` as
  ``date32[day]``; the manifests use the constructor names.

Both directions are pinned: no manifest row may name a column the schema lacks, and
no schema field may go unlabeled except the ones this file names explicitly.
"""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from typing import Any

import pytest

_DATASETS_DIR = Path(__file__).resolve().parents[1] / "datasets"

#: Families that carry a manifest.json. ``nested`` labels row classes, not columns.
LABELED_FAMILIES: tuple[str, ...] = ("schema_inference", "extreme_types", "secrets", "smartcsv")

#: Schema fields deliberately not labeled as a torture class. DS-2's two families use
#: ``id`` purely as a join key; DS-3's two label every column.
EXPECTED_UNLABELED: dict[str, set[str]] = {
    "schema_inference": {"id"},
    "extreme_types": {"id"},
    "secrets": set(),
    "smartcsv": set(),
}

#: pyarrow's rendering → the constructor spelling the manifests use.
_TYPE_ALIASES: dict[str, str] = {
    "double": "float64",
    "float": "float32",
    "halffloat": "float16",
    "date32[day]": "date32",
    "date64[ms]": "date64",
}


def _load_datasets() -> None:
    package_name = "repark_datasets"
    if package_name not in sys.modules:
        package = types.ModuleType(package_name)
        package.__path__ = [str(_DATASETS_DIR)]  # type: ignore[attr-defined]
        sys.modules[package_name] = package


def _datagen(family: str) -> Any:
    _load_datasets()
    return importlib.import_module(f"repark_datasets.{family}.datagen")


def normalize_type(text: str) -> str:
    """Canonical form for comparing a declared type string with an Arrow type."""
    collapsed = "".join(str(text).split()).lower()
    return _TYPE_ALIASES.get(collapsed, collapsed)


def _typed_classes(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    """Manifest rows that declare both a column and a type (file-scoped rows do not)."""
    return [
        entry
        for entry in manifest["classes"]
        if entry.get("column") is not None and entry.get("parquet_type") is not None
    ]


def test_normalize_type_folds_spacing_and_spelling() -> None:
    """The normalizer itself, so a silent no-op normalizer cannot hide a mismatch."""
    assert normalize_type("decimal128(24, 21)") == normalize_type("decimal128(24,21)")
    assert normalize_type("double") == "float64"
    assert normalize_type("date32[day]") == "date32"
    assert normalize_type("timestamp[us]") == "timestamp[us]"
    assert normalize_type("int64") != normalize_type("int32")


@pytest.mark.parametrize("family", LABELED_FAMILIES)
def test_manifest_types_match_the_arrow_schema(family: str) -> None:
    datagen = _datagen(family)
    manifest = datagen.load_manifest()
    assert manifest["family"] == family
    schema = datagen.SCHEMA
    typed = _typed_classes(manifest)
    assert typed, f"{family}: manifest declares no typed classes"
    for entry in typed:
        column = entry["column"]
        assert column in schema.names, (family, entry)
        declared = normalize_type(entry["parquet_type"])
        actual = normalize_type(str(schema.field(column).type))
        assert declared == actual, (family, entry["id"], entry["parquet_type"], actual)


@pytest.mark.parametrize("family", LABELED_FAMILIES)
def test_manifest_covers_every_schema_field(family: str) -> None:
    """Reverse direction: an unlabeled new column must be a deliberate, listed choice."""
    datagen = _datagen(family)
    manifest = datagen.load_manifest()
    labeled = {entry["column"] for entry in _typed_classes(manifest)}
    schema_names = set(datagen.SCHEMA.names)
    assert labeled <= schema_names, (family, labeled - schema_names)
    assert schema_names - labeled == EXPECTED_UNLABELED[family], family


@pytest.mark.parametrize("family", LABELED_FAMILIES)
def test_manifest_class_ids_are_unique(family: str) -> None:
    datagen = _datagen(family)
    ids = [entry["id"] for entry in datagen.load_manifest()["classes"]]
    assert len(ids) == len(set(ids)), family
