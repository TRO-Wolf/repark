"""Pin CAP-1's exact source-file ceilings and ratchets.

pins: cap-1-source-file-line-cap/C-001, C-002, C-003, C-004, C-005, C-006
pins: cap-1-source-file-line-cap/C-007, C-008, C-009, C-010, C-011, C-012
"""

from __future__ import annotations

import importlib.util
import re
from pathlib import Path
from types import ModuleType

import pytest

_REPO = Path(__file__).resolve().parents[3]
_RUST_GATE_PATH = _REPO / "scripts" / "check_rust_file_size.py"
_PYTHON_GATE_PATH = _REPO / "scripts" / "check_lib_py.py"
_APPROVED_EXEMPT_PATHS: tuple[tuple[str, ...], ...] = (
    ("tests", "goldens"),
    ("tests", "fixtures"),
)
_RUST_BASELINES: tuple[tuple[str, int], ...] = (
    ("crates/repark-core/src/catalog_config.rs", 1044),
    ("crates/repark-core/src/dynamic_flatten/tests.rs", 1442),
    ("crates/repark-core/src/session/tests/session.rs", 1412),
    ("crates/repark-core/tests/declared_sorted.rs", 1348),
    ("crates/repark-functions/src/analyzer.rs", 1142),
    ("crates/repark-functions/src/datetime.rs", 1700),
    ("crates/repark-iceberg/src/catalog/tests/catalog.rs", 1843),
    ("crates/repark-iceberg/src/write/alter.rs", 1630),
    ("crates/repark-iceberg/src/write/append.rs", 1884),
    ("crates/repark-iceberg/src/write/merge/mod.rs", 1795),
    ("crates/repark-iceberg/src/write/merge/tests/merge.rs", 1068),
    ("crates/repark-iceberg/src/write/merge/tests/occ_conflict.rs", 1023),
    ("crates/repark-iceberg/src/write/merge/tests/streaming_scan.rs", 3028),
    ("crates/repark-iceberg/src/write/overwrite.rs", 1053),
    ("crates/repark-iceberg/src/write/predicate_dml.rs", 1142),
    ("crates/repark-iceberg/src/write/predicate_dml/tests/predicate_dml.rs", 1442),
    ("crates/repark-python/src/column/mod.rs", 1052),
    ("crates/repark-python/src/dataframe.rs", 1126),
    ("crates/repark-python/src/session.rs", 1177),
    ("crates/repark-spark/src/alter.rs", 1830),
    ("crates/repark-spark/src/metadata_tables.rs", 1062),
    ("crates/repark-spark/src/tests/alter.rs", 1436),
    ("crates/repark-spark/src/tests/call.rs", 1303),
    ("crates/repark-spark/src/tests/ctas.rs", 1361),
    ("crates/repark-spark/src/tests/dml.rs", 1154),
    ("crates/repark-spark/src/tests/insert_overwrite.rs", 1233),
    ("crates/repark-spark/src/tests/merge.rs", 1303),
    ("crates/repark-spark/src/tests/partitioned_merge.rs", 1068),
    ("crates/repark-spark/src/tests/transform_overwrite.rs", 1181),
    ("crates/repark-spark/src/window_range.rs", 1225),
    ("crates/repark-sql/src/guards/tests.rs", 1207),
    ("crates/repark-sql/src/tests.rs", 1520),
    ("crates/repark-sql/tests/cross_door.rs", 1259),
    ("crates/repark-ta/src/momentum.rs", 2098),
    ("crates/repark-ta/src/overlap.rs", 1578),
    ("crates/repark-ta/src/udf/mod.rs", 1821),
)
_PYTHON_BASELINES: tuple[tuple[str, int], ...] = (
    ("python/repark-parity/bench/tpcds/runner.py", 1252),
    ("python/repark-parity/bench/tpch/runner.py", 1773),
    ("python/repark-parity/compat/runner.py", 1279),
    ("python/repark-parity/tests/test_compat_harness.py", 1021),
    ("python/repark/src/repark/spark/column.py", 1589),
    ("python/repark/src/repark/spark/dataframe/core.py", 6302),
    ("python/repark/src/repark/spark/dataframe/joins_columns.py", 1239),
    ("python/repark/src/repark/spark/dataframe/plan_collapse.py", 1168),
    ("python/repark/src/repark/spark/dataframe/writer_readwriter.py", 1113),
    ("python/repark/src/repark/spark/functions.py", 1985),
    ("python/repark/src/repark/spark/functions_expr.py", 2255),
    ("python/repark/src/repark/spark/functions_udf.py", 1300),
    ("python/repark/src/repark/spark/ml/feature/_transformers.py", 2717),
    ("python/repark/src/repark/spark/session/reader.py", 1022),
    ("python/repark/src/repark/spark/session/session_core.py", 2411),
    ("python/repark/src/repark/spark/ta.py", 1818),
    ("python/repark/src/repark/spark/types.py", 1834),
    ("python/repark/tests/_live_parity.py", 1778),
    ("python/repark/tests/test_display_styles.py", 1175),
    ("python/repark/tests/test_dynamic_flatten.py", 1618),
    ("python/repark/tests/test_explode_rewrite.py", 1135),
    ("python/repark/tests/test_interchange_parity.py", 1533),
    ("python/repark/tests/test_join_parity.py", 1232),
    ("python/repark/tests/test_mapinarrow.py", 1578),
    ("python/repark/tests/test_ml_boost_oracle.py", 2244),
    ("python/repark/tests/test_pandas_udf.py", 1478),
    ("python/repark/tests/test_partition_value_audit.py", 1665),
    ("python/repark/tests/test_session_timezone_parity.py", 1328),
    ("python/repark/tests/test_ta.py", 1020),
    ("python/repark/tests/test_tpch_compare_unit.py", 1551),
    ("python/repark/tests/test_udf.py", 1170),
    ("python/repark/tests/test_window_parity.py", 1422),
)


def _load_gate(path: Path, name: str) -> ModuleType:
    """Load one gate module without adding the scripts directory to sys.path."""
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_lines(path: Path, count: int, *, blank: bool = False) -> None:
    """Write an exact splitlines-compatible number of source lines."""
    path.parent.mkdir(parents=True, exist_ok=True)
    line = "\n" if blank else "value = 1\n"
    path.write_text(line * count, encoding="utf-8")


def _contains_approved_exempt_path(path: Path, repo: Path) -> bool:
    """Apply the charter's two exact path exemptions independently of the gates."""
    parts = path.relative_to(repo).parts
    return any(
        parts[index : index + len(exempt)] == exempt
        for exempt in _APPROVED_EXEMPT_PATHS
        for index in range(len(parts) - len(exempt) + 1)
    )


def _measure_sources(roots: tuple[str, ...], suffix: str) -> dict[str, int]:
    """Measure the chartered live-tree domain with the same line definition."""
    measured: dict[str, int] = {}
    for root_name in roots:
        root = _REPO / root_name
        for path in sorted(root.rglob(f"*{suffix}")):
            if not path.is_file() or _contains_approved_exempt_path(path, _REPO):
                continue
            relative = path.relative_to(_REPO).as_posix()
            measured[relative] = len(path.read_text(encoding="utf-8").splitlines())
    return measured


def _baselines(module: ModuleType) -> dict[str, int]:
    """Return only exact baselines from a gate's actionable exception rows."""
    return {path: row[0] for path, row in module.EXCEPTIONS.items()}


def test_cap_1_rust_boundary_counts_blank_lines(tmp_path: Path) -> None:
    """C-001: 1,000 blank Rust lines pass and line 1,001 fails."""
    gate = _load_gate(_RUST_GATE_PATH, "cap_1_rust_boundary")
    path = tmp_path / "crates" / "sample.rs"
    _write_lines(path, 1000, blank=True)
    assert gate.check_file(path, tmp_path) == []
    _write_lines(path, 1001, blank=True)
    errors = gate.check_file(path, tmp_path)
    assert len(errors) == 1
    assert "is 1001 lines (default 1000)" in errors[0]


def test_cap_1_python_boundary_counts_blank_lines(tmp_path: Path) -> None:
    """C-002: 1,000 blank Python lines pass and line 1,001 fails."""
    gate = _load_gate(_PYTHON_GATE_PATH, "cap_1_python_boundary")
    path = tmp_path / "python" / "package" / "sample.py"
    _write_lines(path, 1000, blank=True)
    assert gate.check_file(path, tmp_path) == []
    _write_lines(path, 1001, blank=True)
    errors = gate.check_file(path, tmp_path)
    assert len(errors) == 1
    assert "is 1001 lines (default 1000)" in errors[0]


def test_cap_1_exception_tables_equal_the_measured_debt() -> None:
    """C-003/C-004: exception paths and baselines equal the live offender set."""
    rust_gate = _load_gate(_RUST_GATE_PATH, "cap_1_rust_census")
    python_gate = _load_gate(_PYTHON_GATE_PATH, "cap_1_python_census")
    rust_measured = _measure_sources(("crates",), ".rs")
    python_measured = _measure_sources(("python", "scripts"), ".py")
    rust_debt = {path: count for path, count in rust_measured.items() if count > 1000}
    python_debt = {path: count for path, count in python_measured.items() if count > 1000}
    rust_approved = dict(_RUST_BASELINES)
    python_approved = dict(_PYTHON_BASELINES)
    assert rust_gate.DEFAULT_CEILING == 1000
    assert python_gate.DEFAULT_CEILING == 1000
    assert _baselines(rust_gate) == rust_approved
    assert _baselines(python_gate) == python_approved
    assert rust_debt == rust_approved
    assert python_debt == python_approved
    assert len(rust_approved) == 36
    assert len(python_approved) == 32


def test_cap_1_growth_above_exact_baseline_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C-004: one new line above an exception baseline is red."""
    gate = _load_gate(_RUST_GATE_PATH, "cap_1_growth")
    path = tmp_path / "crates" / "large.rs"
    relative = "crates/large.rs"
    monkeypatch.setattr(
        gate,
        "EXCEPTIONS",
        {relative: (1001, "Existing cohesive debt.", "Split the behavior family.")},
    )
    _write_lines(path, 1002)
    errors = gate.check_file(path, tmp_path)
    assert len(errors) == 1
    assert "grew to 1002 lines (exact baseline 1001)" in errors[0]


def test_cap_1_shrink_requires_baseline_update(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C-005: a partial shrink is red until the recorded baseline follows it down."""
    gate = _load_gate(_RUST_GATE_PATH, "cap_1_shrink")
    path = tmp_path / "crates" / "large.rs"
    monkeypatch.setattr(
        gate,
        "EXCEPTIONS",
        {"crates/large.rs": (1002, "Existing cohesive debt.", "Split the behavior family.")},
    )
    _write_lines(path, 1001)
    errors = gate.check_file(path, tmp_path)
    assert len(errors) == 1
    assert "ratchet the baseline down to 1001" in errors[0]


@pytest.mark.parametrize(
    ("gate_path", "relative"),
    (
        (_RUST_GATE_PATH, "crates/large.rs"),
        (_PYTHON_GATE_PATH, "python/large.py"),
    ),
)
def test_cap_1_default_compliant_exception_requires_removal(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gate_path: Path,
    relative: str,
) -> None:
    """C-005: a file at the default cannot retain an exception row."""
    gate = _load_gate(gate_path, f"cap_1_retirement_{gate_path.stem}")
    path = tmp_path / relative
    monkeypatch.setattr(
        gate,
        "EXCEPTIONS",
        {relative: (1000, "Existing cohesive debt.", "Split the behavior family.")},
    )
    _write_lines(path, 1000)
    errors = gate.check_file(path, tmp_path)
    assert len(errors) == 1
    assert "remove the exception row" in errors[0]


@pytest.mark.parametrize("gate_path", (_RUST_GATE_PATH, _PYTHON_GATE_PATH))
@pytest.mark.parametrize(
    ("reason", "split_seam", "expected"),
    (
        ("", "Split the behavior family.", "debt reason must not be empty"),
        ("Existing cohesive debt.", "", "split seam must not be empty"),
    ),
)
def test_cap_1_malformed_exception_records_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    gate_path: Path,
    reason: str,
    split_seam: str,
    expected: str,
) -> None:
    """C-007: production guards reject empty exception metadata."""
    gate = _load_gate(gate_path, f"cap_1_malformed_{gate_path.stem}_{expected[:4]}")
    relative = "crates/large.rs" if gate_path == _RUST_GATE_PATH else "python/large.py"
    path = tmp_path / relative
    _write_lines(path, 1001)
    monkeypatch.setattr(gate, "EXCEPTIONS", {relative: (1001, reason, split_seam)})
    errors = gate.check_file(path, tmp_path)
    assert len(errors) == 1
    assert expected in errors[0]


@pytest.mark.parametrize(
    ("gate_path", "root", "suffix"),
    (
        (_RUST_GATE_PATH, "crates", ".rs"),
        (_PYTHON_GATE_PATH, "python", ".py"),
    ),
)
def test_cap_1_fail_closed_conditions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    gate_path: Path,
    root: str,
    suffix: str,
) -> None:
    """C-006: missing rows, unreadable paths, and empty scans remain loud."""
    gate = _load_gate(gate_path, f"cap_1_fail_closed_{root}")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    monkeypatch.setattr(gate, "__file__", str(scripts / gate_path.name))
    if root == "python":
        (tmp_path / "python").mkdir()
        monkeypatch.setattr(gate, "SCAN_ROOTS", ("python",))
    else:
        (tmp_path / "crates").mkdir()
    source = tmp_path / root / f"small{suffix}"
    _write_lines(source, 1)
    monkeypatch.setattr(
        gate,
        "EXCEPTIONS",
        {f"{root}/missing{suffix}": (1001, "Existing debt.", "Split one family.")},
    )
    assert gate.main() != 0
    assert "outside the scan set" in capsys.readouterr().err
    outside = tmp_path / f"outside{suffix}"
    _write_lines(outside, 1001)
    monkeypatch.setattr(
        gate,
        "EXCEPTIONS",
        {outside.relative_to(tmp_path).as_posix(): (1001, "Existing debt.", "Split one family.")},
    )
    assert gate.main() != 0
    assert "outside the scan set" in capsys.readouterr().err
    monkeypatch.setattr(gate, "EXCEPTIONS", {})
    source.unlink()
    assert gate.main() != 0
    assert "scan set is empty" in capsys.readouterr().err
    unreadable = tmp_path / root / f"unreadable{suffix}"
    unreadable.mkdir()
    errors = gate.check_file(unreadable, tmp_path)
    assert len(errors) == 1
    assert "unreadable" in errors[0]


@pytest.mark.parametrize(
    ("gate_path", "expected"),
    (
        (_RUST_GATE_PATH, "crates/ not found"),
        (_PYTHON_GATE_PATH, "scan root not found: missing"),
    ),
)
def test_cap_1_missing_scan_roots_fail(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    gate_path: Path,
    expected: str,
) -> None:
    """C-001/C-002/C-006: a missing declared source root is red."""
    gate = _load_gate(gate_path, f"cap_1_missing_root_{gate_path.stem}")
    tools = tmp_path / "tools"
    tools.mkdir()
    monkeypatch.setattr(gate, "__file__", str(tools / gate_path.name))
    monkeypatch.setattr(gate, "EXCEPTIONS", {})
    if gate_path == _PYTHON_GATE_PATH:
        _write_lines(tmp_path / "python" / "small.py", 1)
        monkeypatch.setattr(gate, "SCAN_ROOTS", ("python", "missing"))
    assert gate.main() != 0
    assert expected in capsys.readouterr().err


def test_cap_1_exception_records_are_actionable() -> None:
    """C-007: every exact exception carries a reason and a cohesive split seam."""
    for path, name in (
        (_RUST_GATE_PATH, "cap_1_rust_records"),
        (_PYTHON_GATE_PATH, "cap_1_python_records"),
    ):
        gate = _load_gate(path, name)
        assert list(gate.EXCEPTIONS) == sorted(gate.EXCEPTIONS)
        for baseline, reason, split_seam in gate.EXCEPTIONS.values():
            assert baseline > gate.DEFAULT_CEILING
            assert reason.strip()
            assert split_seam.strip()
            assert "TASK-" not in reason
            assert "TASK-" not in split_seam


def test_cap_1_exact_baseline_is_green_and_growth_is_red(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """C-008: line-neutral work passes; growth names the owner-approval boundary."""
    gate = _load_gate(_PYTHON_GATE_PATH, "cap_1_narrow_fix")
    path = tmp_path / "python" / "large.py"
    monkeypatch.setattr(
        gate,
        "EXCEPTIONS",
        {"python/large.py": (1001, "Existing cohesive debt.", "Split the behavior family.")},
    )
    _write_lines(path, 1001)
    assert gate.check_file(path, tmp_path) == []
    _write_lines(path, 1002)
    errors = gate.check_file(path, tmp_path)
    assert len(errors) == 1
    assert "explicit owner approval" in errors[0]


def test_cap_1_existing_gate_surfaces_remain_dual_wired() -> None:
    """C-009: CAP-1 changes existing guards instead of adding a parallel gate."""
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    ci_target = next(line for line in makefile.splitlines() if line.startswith("ci:"))
    assert "check-rust-file-size" in ci_target
    assert "check-lib-py" in ci_target
    assert "\t@./scripts/check_rust_file_size.sh" in makefile
    assert "\t@./scripts/check_lib_py.sh" in makefile
    workflow = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert workflow.count("run: ./scripts/check_rust_file_size.sh") == 1
    assert workflow.count("run: ./scripts/check_lib_py.sh") == 1
    assert "check-source-file-size" not in makefile
    assert not (_REPO / "scripts" / "check_source_file_size.py").exists()


def test_cap_1_facade_no_stub_scope_is_unchanged(tmp_path: Path) -> None:
    """C-010: only facade re-export modules require the binding marker."""
    gate = _load_gate(_PYTHON_GATE_PATH, "cap_1_no_stub")
    facade = tmp_path / "python" / "repark" / "src" / "repark" / "binding.py"
    outside = tmp_path / "python" / "repark-parity" / "support.py"
    facade.parent.mkdir(parents=True)
    outside.parent.mkdir(parents=True)
    facade.write_text("from thing import value\n", encoding="utf-8")
    outside.write_text("from thing import value\n", encoding="utf-8")
    errors = gate.check_file(facade, tmp_path)
    assert len(errors) == 1
    assert "re-export-only module" in errors[0]
    assert gate.check_file(outside, tmp_path) == []
    facade.write_text(
        '"""re-export binding for the public value."""\nfrom thing import value\n',
        encoding="utf-8",
    )
    assert gate.check_file(facade, tmp_path) == []


def test_cap_1_only_named_fixture_paths_are_exempt(tmp_path: Path) -> None:
    """C-011: only tests/goldens and tests/fixtures bypass source scanning."""
    for gate_path, name, suffix in (
        (_RUST_GATE_PATH, "cap_1_rust_exemptions", ".rs"),
        (_PYTHON_GATE_PATH, "cap_1_python_exemptions", ".py"),
    ):
        gate = _load_gate(gate_path, name)
        assert gate.EXEMPT_PATHS == _APPROVED_EXEMPT_PATHS
        for approved in _APPROVED_EXEMPT_PATHS:
            path = tmp_path.joinpath("root", *approved, f"large{suffix}")
            assert gate._is_exempt(path, tmp_path)
        near_miss = tmp_path / "root" / "test" / "goldens" / f"large{suffix}"
        assert not gate._is_exempt(near_miss, tmp_path)


@pytest.mark.parametrize(
    ("gate_path", "root", "suffix"),
    (
        (_RUST_GATE_PATH, "crates", ".rs"),
        (_PYTHON_GATE_PATH, "python", ".py"),
    ),
)
def test_cap_1_fixture_exemptions_are_wired_into_the_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    gate_path: Path,
    root: str,
    suffix: str,
) -> None:
    """C-011: main skips approved fixture paths and scans a near miss."""
    gate = _load_gate(gate_path, f"cap_1_scan_exemptions_{root}")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    monkeypatch.setattr(gate, "__file__", str(scripts / gate_path.name))
    if root == "python":
        monkeypatch.setattr(gate, "SCAN_ROOTS", ("python",))
    source_root = tmp_path / root
    _write_lines(source_root / f"small{suffix}", 1)
    for approved in _APPROVED_EXEMPT_PATHS:
        _write_lines(source_root.joinpath(*approved, f"large{suffix}"), 1001)
    monkeypatch.setattr(gate, "EXCEPTIONS", {})
    assert gate.main() == 0
    assert "1 files clean" in capsys.readouterr().out
    near_miss = source_root / "test" / "goldens" / f"large{suffix}"
    _write_lines(near_miss, 1001)
    assert gate.main() != 0
    assert near_miss.relative_to(tmp_path).as_posix() in capsys.readouterr().err


def test_cap_1_prose_and_navigation_name_the_generalized_gate() -> None:
    """C-012: contracts and maps describe the generalized existing guards."""
    agents = (_REPO / "AGENTS.md").read_text(encoding="utf-8")
    development = (_REPO / "DEVELOPMENT.md").read_text(encoding="utf-8")
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    script_map = (_REPO / "scripts" / "map.md").read_text(encoding="utf-8")
    workflow_map = (_REPO / ".github" / "workflows" / "map.md").read_text(encoding="utf-8")
    session_map = (
        _REPO / "python" / "repark" / "src" / "repark" / "spark" / "session" / "map.md"
    ).read_text(encoding="utf-8")
    dataframe_map = (
        _REPO / "python" / "repark" / "src" / "repark" / "spark" / "dataframe" / "map.md"
    ).read_text(encoding="utf-8")
    core_session_map = (_REPO / "crates" / "repark-core" / "src" / "session" / "map.md").read_text(
        encoding="utf-8"
    )
    call_map = (_REPO / "crates" / "repark-spark" / "src" / "call" / "map.md").read_text(
        encoding="utf-8"
    )
    function_design = (_REPO / "docs" / "design" / "spark-function-parity.md").read_text(
        encoding="utf-8"
    )
    carrier_paths = (
        _REPO / "AGENTS.md",
        _REPO / "DEVELOPMENT.md",
        _REPO / "Makefile",
        _REPO / "map.md",
        _REPO / "scripts" / "map.md",
        _REPO / ".github" / "workflows" / "ci.yml",
        _REPO / ".github" / "workflows" / "map.md",
        _REPO / "python" / "repark-parity" / "tests" / "map.md",
        _REPO / "task" / "ledgers" / "staging" / "map.md",
    )
    assert "Python source file-size + facade thinness" in agents
    assert "Python source file-size" in development
    assert "Python source ceiling + facade no-stub" in makefile
    assert "Python source-size and facade thinness guard" in script_map
    assert "Python source-size + facade thinness" in workflow_map
    assert "`check_lib_py` exception baseline" in session_map
    assert (
        "Under CAP-1, `core.py` and `plan_collapse.py` carry exact exception rows" in dataframe_map
    )
    assert "CAP-1 records the file again at its exact source-size baseline" in core_session_map
    assert "beyond its exact\n`check_rust_file_size` baseline" in call_map
    assert "CAP-1 (2026-08-26) lowers the\ncurrent default" in function_design
    for path in carrier_paths:
        text = path.read_text(encoding="utf-8")
        assert re.search(r"(?<![\d,])1,?000(?!\d)", text) is None, path
