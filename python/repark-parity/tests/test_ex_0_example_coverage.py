"""Pins for the v0.7 example-drift gate.

pins: ex-0-example-drift-gate/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import ast
import importlib.util
from pathlib import Path
from types import ModuleType

_REPO = Path(__file__).resolve().parents[3]
_GATE = _REPO / "scripts" / "check_example_coverage.py"
_WRAPPER = _REPO / "scripts" / "check_example_coverage.sh"
_MAKEFILE = _REPO / "Makefile"


def _load_gate() -> ModuleType:
    """Load the example-coverage gate as a module."""
    spec = importlib.util.spec_from_file_location("check_example_coverage", _GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_ex_0_enumerator_emits_five_families_and_repark_sql() -> None:
    """C-001: the AST walk names every family and the native SQL door."""
    gate = _load_gate()
    rows = gate.enumerate_public_surface(_REPO)
    families = {family for family, _name in rows}
    assert families == set(gate.FAMILIES)
    names = [name for _family, name in rows]
    assert rows == sorted(rows, key=lambda row: (row[0], row[1]))
    assert "repark.sql" in names
    assert any(name.startswith("F.") for name in names)
    assert any(name.startswith("DataFrame.") for name in names)
    assert any(name.startswith("ta.") for name in names)
    assert any(name.startswith("DataFrameReader.") for name in names)
    assert any(name.startswith("SparkSession.") for name in names)
    assert len(rows) == 679


def test_ex_0_uncovered_name_is_red() -> None:
    """C-003: an enumerated name with no COVERS, backlog, or exception is red."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, set(), set(), set(), 0)
    assert findings == [
        "public name F.abs has no example COVERS row and is not in the backlog or exceptions"
    ]


def test_ex_0_stale_backlog_name_is_red() -> None:
    """C-004: a backlog name that is not in the inventory is red."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, {"F.abs"}, {"gone"}, set(), 1)
    assert any("backlog names gone, which is not in the inventory" in item for item in findings)


def test_ex_0_covered_name_still_in_backlog_is_red() -> None:
    """C-004: a name an example now covers cannot stay in the backlog."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, {"F.abs"}, {"F.abs"}, set(), 1)
    needle = "backlog still lists F.abs, which an example now covers"
    assert any(needle in item for item in findings)


def test_ex_0_backlog_count_mismatch_is_red() -> None:
    """C-005: backlog length must equal BACKLOG_BASELINE (ratchet down only)."""
    gate = _load_gate()
    findings = gate.coverage_findings({"F.abs"}, set(), {"F.abs"}, set(), 0)
    assert any("backlog count is 1, baseline is 0" in item for item in findings)


def test_ex_0_seed_examples_declare_covers_and_leave_the_backlog() -> None:
    """C-002 / C-008: seed scripts exist, declare COVERS, and those names are not backlogged."""
    gate = _load_gate()
    scripts = gate.example_scripts(_REPO)
    relatives = {path.relative_to(_REPO).as_posix() for path in scripts}
    assert "docs/examples/functions/abs.py" in relatives
    assert "docs/examples/dataframe/select_filter.py" in relatives
    assert "docs/examples/io/parquet_roundtrip.py" in relatives
    assert "docs/examples/ta/sma.py" in relatives
    covered: set[str] = set()
    for script in scripts:
        names = gate.covers_from_script(script)
        assert names
        covered.update(names)
    assert "F.abs" in covered
    assert "DataFrame.select" in covered
    assert "DataFrameWriter.parquet" in covered
    assert "ta.sma" in covered
    backlog = set(gate.parse_named_lines(_REPO / gate.BACKLOG_RELATIVE, kind="backlog"))
    assert covered.isdisjoint(backlog)
    assert len(backlog) == gate.BACKLOG_BASELINE
    assert gate.BACKLOG_BASELINE == 658


def test_ex_0_exceptions_file_names_only_inventory_rows() -> None:
    """C-006: cloud exceptions are inventoried names with a reason."""
    gate = _load_gate()
    mapping = gate.parse_exceptions_file(_REPO / gate.EXCEPTIONS_RELATIVE)
    enumerated = {name for _family, name in gate.enumerate_public_surface(_REPO)}
    assert mapping
    for name, reason in mapping.items():
        assert name in enumerated
        assert reason
    assert "SparkSession.read_postgres" in mapping
    assert "DataFrameReader.jdbc" in mapping


def test_ex_0_execute_nonzero_is_red(tmp_path: Path) -> None:
    """C-007: a script that exits nonzero is a finding."""
    gate = _load_gate()
    failing = tmp_path / "failing_example.py"
    failing.write_text("raise SystemExit(7)\n", encoding="utf-8")
    findings = gate.execute_examples([failing])
    assert len(findings) == 1
    assert "exited 7" in findings[0]


def test_ex_0_makefile_wires_the_target_into_ci() -> None:
    """C-009: make ci depends on check-example-coverage; the wrapper uses isolated Python."""
    makefile = _MAKEFILE.read_text(encoding="utf-8")
    assert "check-example-coverage" in makefile
    ci_line = next(line for line in makefile.splitlines() if line.startswith("ci:"))
    assert "check-example-coverage" in ci_line
    wrapper = _WRAPPER.read_text(encoding="utf-8")
    assert 'exec python3 -I "$repo_root/scripts/check_example_coverage.py"' in wrapper
    gate_source = _GATE.read_text(encoding="utf-8")
    assert "skipping example execution" in gate_source


def test_ex_0_no_product_code_in_the_gate_script() -> None:
    """C-010: the gate does not import engine crates or edit product modules."""
    source = _GATE.read_text(encoding="utf-8")
    tree = ast.parse(source)
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported.add(node.module.split(".", 1)[0])
    assert "repark" not in imported
    assert "crates" not in source
