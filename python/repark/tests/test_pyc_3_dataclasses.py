"""PYC-3: shipped dataclass containers converted to Pydantic v2 BaseModel.

Pins the accepted-input set the production builders already construct, so a conversion
that narrows those shapes fails here. Behaviour of MERGE SQL and smartCsv inference
stays on ``test_merge_into.py`` / ``test_t4_csv_smart.py``.
"""

from __future__ import annotations

import ast
import re
import tomllib
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

from repark.spark._csv_smart import (
    ColumnIngestReport,
    ColumnResolution,
    IngestReport,
    PreparedCsv,
)
from repark.spark.merge import _Clause

_PYC_3_DATACLASS_KEYS: tuple[str, ...] = (
    "python/repark/src/repark/spark/_csv_smart.py",
    "python/repark/src/repark/spark/merge.py",
)

# Unique (kind, action) shapes the eight MergeIntoWriter terminal methods construct.
_CLAUSE_SHAPES: tuple[dict[str, object], ...] = (
    {"kind": "matched", "action": "update_all"},
    {"kind": "matched", "action": "update", "assignments": {"name": 'source."name"'}},
    {"kind": "matched", "action": "delete", "predicate_sql": 'source."id" > 0'},
    {"kind": "not_matched", "action": "insert_all"},
    {"kind": "not_matched", "action": "insert", "assignments": {"id": 'source."id"'}},
    {"kind": "not_matched_by_source", "action": "update_all"},
    {
        "kind": "not_matched_by_source",
        "action": "update",
        "assignments": {"name": "'x'"},
    },
    {"kind": "not_matched_by_source", "action": "delete"},
)


def _dataclass_exception_keys() -> list[str]:
    conventions = Path(__file__).resolve().parents[3] / "scripts" / "check_python_conventions.py"
    tree = ast.parse(conventions.read_text(encoding="utf-8"))
    keys: list[str] = []
    for node in tree.body:
        target_name = None
        value = None
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_name, value = node.target.id, node.value
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                target_name, value = target.id, node.value
        if target_name != "DATACLASS_EXCEPTIONS" or not isinstance(value, ast.Dict):
            continue
        for key in value.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.append(key.value)
    assert keys, "did not bind DATACLASS_EXCEPTIONS as a module-level dict literal"
    return keys


def _module_source(relative: str) -> str:
    repo = Path(__file__).resolve().parents[3]
    return (repo / relative).read_text(encoding="utf-8")


_DATACLASS_IMPORT = re.compile(r"^\s*(from dataclasses import|import dataclasses)\b", re.M)


def test_pyc_3_merge_clause_is_basemodel() -> None:
    assert issubclass(_Clause, BaseModel)
    assert _DATACLASS_IMPORT.search(_module_source(_PYC_3_DATACLASS_KEYS[1])) is None


def test_pyc_3_csv_containers_are_basemodels() -> None:
    for model in (ColumnIngestReport, IngestReport, ColumnResolution, PreparedCsv):
        assert issubclass(model, BaseModel), model
    assert _DATACLASS_IMPORT.search(_module_source(_PYC_3_DATACLASS_KEYS[0])) is None


@pytest.mark.parametrize("fields", list(_CLAUSE_SHAPES))
def test_pyc_3_clause_accepted_shapes(fields: dict[str, object]) -> None:
    clause = _Clause(**fields)  # type: ignore[arg-type]
    assert clause.kind == fields["kind"]
    assert clause.action == fields["action"]
    assert clause.predicate_sql == fields.get("predicate_sql")
    expected_assignments = fields.get("assignments", {})
    assert clause.assignments == expected_assignments


def test_pyc_3_clause_assignment_defaults_are_not_shared() -> None:
    first = _Clause(kind="matched", action="update_all")
    second = _Clause(kind="matched", action="update_all")
    first.assignments["name"] = "1"
    assert second.assignments == {}


def test_pyc_3_extra_fields_refused() -> None:
    """extra='forbid' on every converted model — drop it and this goes red."""
    with pytest.raises(ValidationError):
        _Clause(kind="matched", action="update_all", unknown=True)  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ColumnIngestReport(
            name="id",
            resolved_type="int32",
            fallback_count=0,
            null_count=0,
            sample_count=1,
            unknown=True,  # type: ignore[call-arg]
        )
    with pytest.raises(ValidationError):
        IngestReport(path="/tmp/sample.csv", unknown=True)  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        ColumnResolution(
            rung="string",
            fallback_count=0,
            null_count=0,
            sample_count=0,
            unknown=True,  # type: ignore[call-arg]
        )
    report = IngestReport(path="/tmp/sample.csv")
    with pytest.raises(ValidationError):
        PreparedCsv(headers=[], rows=[], report=report, unknown=True)  # type: ignore[call-arg]


def test_pyc_3_strict_types_refused() -> None:
    """strict=True — lax pydantic would coerce these; dataclass stored them as-is.

    ``bool`` is rejected for ``int`` in both lax and strict, so it does not pin
    ``strict=True``. Use bytes→str, str→int, and tuple→list coercions that only
    fail when strict.
    """
    with pytest.raises(ValidationError):
        _Clause(kind=b"matched", action="update_all")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        IngestReport(path="/tmp/sample.csv", skipped_lines="2")  # type: ignore[arg-type]
    with pytest.raises(ValidationError):
        ColumnIngestReport(
            name="id",
            resolved_type="int32",
            fallback_count="0",  # type: ignore[arg-type]
            null_count=0,
            sample_count=1,
        )
    with pytest.raises(ValidationError):
        ColumnResolution(
            rung="string",
            fallback_count="0",  # type: ignore[arg-type]
            null_count=0,
            sample_count=0,
        )
    report = IngestReport(path="/tmp/sample.csv")
    with pytest.raises(ValidationError):
        PreparedCsv(headers=("id",), rows=[], report=report)  # type: ignore[arg-type]


def test_pyc_3_frozen_models_refuse_field_reassignment() -> None:
    """frozen=True on every model except IngestReport (which is assigned after init)."""
    clause = _Clause(kind="matched", action="update_all")
    with pytest.raises(ValidationError):
        clause.kind = "not_matched"  # type: ignore[misc]
    column = ColumnIngestReport(
        name="id",
        resolved_type="int32",
        fallback_count=0,
        null_count=0,
        sample_count=1,
    )
    with pytest.raises(ValidationError):
        column.name = "x"  # type: ignore[misc]
    resolution = ColumnResolution(rung="string", fallback_count=0, null_count=0, sample_count=0)
    with pytest.raises(ValidationError):
        resolution.rung = "int32"  # type: ignore[misc]
    prepared = PreparedCsv(headers=[], rows=[], report=IngestReport(path="/tmp/sample.csv"))
    with pytest.raises(ValidationError):
        prepared.headers = ["id"]  # type: ignore[misc]


def test_pyc_3_csv_accepted_construction() -> None:
    column = ColumnIngestReport(
        name="amount",
        resolved_type="decimal128(10,2)",
        fallback_count=1,
        null_count=0,
        sample_count=3,
        decimal_precision=10,
        decimal_scale=2,
    )
    resolution = ColumnResolution(
        rung="decimal128",
        fallback_count=1,
        null_count=0,
        sample_count=3,
        decimal_precision=10,
        decimal_scale=2,
    )
    all_null = ColumnResolution(rung="string", fallback_count=0, null_count=4, sample_count=0)
    report = IngestReport(path="/tmp/sample.csv", skipped_lines=2, delimiter=";")
    prepared = PreparedCsv(headers=["amount"], rows=[["1.0"]], report=report)
    empty = PreparedCsv(headers=[], rows=[], report=IngestReport(path="/tmp/empty.csv"))
    assert column.name == "amount"
    assert resolution.rung == "decimal128"
    assert all_null.sample_count == 0
    assert prepared.headers == ["amount"]
    assert prepared.report.delimiter == ";"
    assert empty.headers == []
    assert empty.rows == []


def test_pyc_3_ingest_report_post_init_mutation() -> None:
    """prepare_messy_csv / load_smart_csv assign fields after construction."""
    report = IngestReport(path="/tmp/sample.csv")
    report.header_row_index = None
    report.synthesized_headers = True
    report.ragged_rows_padded = 1
    report.data_row_count = 2
    report.inference_rows_scanned = 2
    report.inference_capped = False
    report.sampling_rows_limit = 10_000
    report.columns = [
        ColumnIngestReport(
            name="_c0",
            resolved_type="string",
            fallback_count=0,
            null_count=0,
            sample_count=2,
        )
    ]
    assert report.synthesized_headers is True
    assert report.columns[0].name == "_c0"


def test_pyc_3_ingest_report_to_dict_identity() -> None:
    """describe_ingest payload: decimal keys only when set; nested columns are dicts."""
    with_decimal = ColumnIngestReport(
        name="amount",
        resolved_type="decimal128(10,2)",
        fallback_count=1,
        null_count=0,
        sample_count=3,
        decimal_precision=10,
        decimal_scale=2,
    )
    without_decimal = ColumnIngestReport(
        name="id",
        resolved_type="int32",
        fallback_count=0,
        null_count=0,
        sample_count=3,
    )
    report = IngestReport(
        path="/tmp/sample.csv",
        skipped_lines=1,
        delimiter=",",
        bom_stripped=True,
        ragged_rows_padded=0,
        header_normalized=False,
        null_tokens=["", "null"],
        columns=[with_decimal, without_decimal],
        synthesized_headers=False,
        data_row_count=3,
        inference_rows_scanned=3,
        inference_capped=False,
        sampling_rows_limit=10_000,
        header_row_index=1,
    )
    payload = report.to_dict()
    assert set(payload) == {
        "source",
        "path",
        "skipped_lines",
        "header_row_index",
        "delimiter",
        "bom_stripped",
        "ragged_rows_padded",
        "header_normalized",
        "null_tokens",
        "synthesized_headers",
        "data_row_count",
        "inference_rows_scanned",
        "inference_capped",
        "sampling_rows_limit",
        "columns",
    }
    assert payload["source"] == "smartCsv"
    assert payload["path"] == "/tmp/sample.csv"
    assert payload["skipped_lines"] == 1
    assert payload["header_row_index"] == 1
    assert payload["delimiter"] == ","
    assert payload["bom_stripped"] is True
    assert payload["ragged_rows_padded"] == 0
    assert payload["header_normalized"] is False
    assert payload["null_tokens"] == ["", "null"]
    assert payload["synthesized_headers"] is False
    assert payload["data_row_count"] == 3
    assert payload["inference_rows_scanned"] == 3
    assert payload["inference_capped"] is False
    assert payload["sampling_rows_limit"] == 10_000
    assert payload["columns"][0]["decimal_precision"] == 10
    assert payload["columns"][0]["decimal_scale"] == 2
    assert set(payload["columns"][0]) == {
        "name",
        "resolved_type",
        "fallback_count",
        "null_count",
        "sample_count",
        "decimal_precision",
        "decimal_scale",
    }
    assert "decimal_precision" not in payload["columns"][1]
    assert "decimal_scale" not in payload["columns"][1]
    assert set(payload["columns"][1]) == {
        "name",
        "resolved_type",
        "fallback_count",
        "null_count",
        "sample_count",
    }


def test_pyc_3_pydantic_is_wheel_hard_dep() -> None:
    """C-010: pydantic is a [project] hard dep (not an extra) and is in uv.lock."""
    repo = Path(__file__).resolve().parents[3]
    with (repo / "python" / "repark" / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    hard_deps: list[str] = pyproject["project"]["dependencies"]
    assert any(item.startswith("pydantic>=2.10") and item.endswith(",<3") for item in hard_deps)
    extras = pyproject["project"].get("optional-dependencies", {})
    extra_tokens = [dep for group in extras.values() for dep in group]
    assert not any("pydantic" in dep for dep in extra_tokens)

    with (repo / "uv.lock").open("rb") as handle:
        lock = tomllib.load(handle)
    repark_pkg = next(pkg for pkg in lock["package"] if pkg["name"] == "repark")
    assert any(dep.get("name") == "pydantic" for dep in repark_pkg["dependencies"])
    pydantic_req = next(
        row for row in repark_pkg["metadata"]["requires-dist"] if row["name"] == "pydantic"
    )
    assert pydantic_req["specifier"] == ">=2.10,<3"
    assert "marker" not in pydantic_req


def test_pyc_3_exception_rows_deleted_not_zeroed() -> None:
    keys = _dataclass_exception_keys()
    still_present = [key for key in _PYC_3_DATACLASS_KEYS if key in keys]
    assert still_present == [], f"PYC-3 rows were zeroed instead of deleted: {still_present}"
    assert "scripts/check_parity_live_dual_wire.py" in keys
    assert any(key.startswith("python/repark-parity/") for key in keys)
