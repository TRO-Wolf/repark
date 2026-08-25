"""PYC-6: arm D-presence ratchet; tests ignored; style D declined."""

from __future__ import annotations

import ast
import importlib.util
import tomllib
from pathlib import Path
from types import ModuleType

import pytest

_REPO = Path(__file__).resolve().parents[3]
_GATE = _REPO / "scripts" / "check_docstring_presence.py"
_PRESENCE_RULES: tuple[str, ...] = ("D101", "D102", "D103", "D105", "D107")
_STYLE_DECLINED: tuple[str, ...] = ("D401", "D202", "D205", "D413")
# Arming seed (2026-08-22). Independent of EXCEPTIONS AST so a redistribution
# at constant sum goes red (C1-Q-003).
_SEEDED_CEILINGS: tuple[tuple[str, int], ...] = (
    ("python/repark-parity/bench/bench_coalesce_chain.py", 1),
    ("python/repark-parity/bench/bench_mor_merge.py", 1),
    ("python/repark-parity/bench/fuzz/bank.py", 1),
    ("python/repark-parity/bench/fuzz/datagen.py", 3),
    ("python/repark-parity/bench/fuzz/generator.py", 4),
    ("python/repark-parity/bench/fuzz/run_fuzz.py", 1),
    ("python/repark-parity/bench/fuzz/runner.py", 5),
    ("python/repark-parity/bench/tpcds/query_worker.py", 1),
    ("python/repark-parity/bench/tpcds/run_tpcds.py", 1),
    ("python/repark-parity/bench/tpcds/runner.py", 1),
    ("python/repark-parity/bench/tpch/check_baseline_ratios.py", 1),
    ("python/repark-parity/bench/tpch/query_worker.py", 1),
    ("python/repark-parity/bench/tpch/run_tpch.py", 1),
    ("python/repark-parity/bench/tpch/runner.py", 1),
    ("python/repark-parity/bench/tpch/sail_engine.py", 4),
    ("python/repark-parity/bench/write/merge_runner.py", 5),
    ("python/repark-parity/bench/write/overwrite_runner.py", 4),
    ("python/repark-parity/bench/write/run_write_bench.py", 1),
    ("python/repark-parity/bench/write/runner.py", 4),
    ("python/repark-parity/bench/write/schemas.py", 1),
    ("python/repark-parity/compat/classify.py", 1),
    ("python/repark-parity/compat/compare_reports.py", 2),
    ("python/repark-parity/compat/redact.py", 1),
    ("python/repark-parity/compat/runner.py", 3),
    ("python/repark-parity/record_ta_goldens.py", 3),
    ("python/repark/src/repark/errors.py", 6),
    ("python/repark/src/repark/spark/dataframe/writer_readwriter.py", 1),
    ("python/repark/src/repark/spark/functions_udf.py", 50),
    ("python/repark/src/repark/spark/merge.py", 4),
    ("python/repark/src/repark/spark/polars.py", 3),
    ("python/repark/src/repark/spark/session/builder_conf.py", 1),
    ("python/repark/src/repark/spark/storage.py", 4),
    ("python/repark/src/repark/spark/types.py", 2),
    ("scripts/check_lib_py.py", 2),
    ("scripts/check_lib_rs.py", 2),
    ("scripts/check_parity_live_dual_wire.py", 3),
    ("scripts/check_python_conventions.py", 2),
    ("scripts/check_rust_file_size.py", 2),
    ("scripts/sync_map_md.py", 2),
)


def _load_gate() -> ModuleType:
    spec = importlib.util.spec_from_file_location("check_docstring_presence", _GATE)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _module() -> ast.Module:
    return ast.parse(_GATE.read_text(encoding="utf-8"))


def _assign_value(tree: ast.Module, name: str) -> ast.AST:
    for node in tree.body:
        target_name = None
        value = None
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            target_name, value = node.target.id, node.value
        elif isinstance(node, ast.Assign) and len(node.targets) == 1:
            target = node.targets[0]
            if isinstance(target, ast.Name):
                target_name, value = target.id, node.value
        if target_name == name and value is not None:
            return value
    raise AssertionError(f"did not bind {name} as a module-level assignment")


def _string_tuple(tree: ast.Module, name: str) -> list[str]:
    value = _assign_value(tree, name)
    assert isinstance(value, ast.Tuple), name
    out: list[str] = []
    for element in value.elts:
        assert isinstance(element, ast.Constant) and isinstance(element.value, str), name
        out.append(element.value)
    return out


def _exceptions_table(tree: ast.Module) -> dict[str, int]:
    value = _assign_value(tree, "EXCEPTIONS")
    assert isinstance(value, ast.Dict), "EXCEPTIONS"
    table: dict[str, int] = {}
    for key, item in zip(value.keys, value.values, strict=True):
        assert isinstance(key, ast.Constant) and isinstance(key.value, str)
        assert isinstance(item, ast.Tuple) and item.elts
        ceiling_node = item.elts[0]
        assert isinstance(ceiling_node, ast.Constant) and isinstance(ceiling_node.value, int)
        table[key.value] = ceiling_node.value
    return table


def test_pyc_6_presence_rules_are_the_five_owner_ruled() -> None:
    tree = _module()
    assert _string_tuple(tree, "PRESENCE_RULES") == list(_PRESENCE_RULES)


def test_pyc_6_style_d_not_selected() -> None:
    """Style D stays declined; presence-only is the whole select list."""
    tree = _module()
    rules = set(_string_tuple(tree, "PRESENCE_RULES"))
    for code in _STYLE_DECLINED:
        assert code not in rules
    with (_REPO / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    selected: list[str] = pyproject["tool"]["ruff"]["lint"]["select"]
    for code in (*_PRESENCE_RULES, *_STYLE_DECLINED, "D", "PL", "A"):
        assert code not in selected


def test_pyc_6_tests_keep_d_per_file_ignore() -> None:
    with (_REPO / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    ignores: dict[str, list[str]] = pyproject["tool"]["ruff"]["lint"]["per-file-ignores"]
    assert "D" in ignores["python/repark/tests/**"]
    assert "D" in ignores["python/repark-parity/tests/**"]


def test_pyc_6_exceptions_seed_is_the_measured_table() -> None:
    """Ceilings equal the 2026-08-22 measurement; no slack; no tests paths."""
    table = _exceptions_table(_module())
    assert table == dict(_SEEDED_CEILINGS)
    assert list(table) == sorted(table)
    for relative in table:
        assert "/tests/" not in relative
        assert (_REPO / relative).is_file()


def test_pyc_6_ruff_pin_matches_makefile() -> None:
    tree = _module()
    pin_node = _assign_value(tree, "RUFF_PIN")
    assert isinstance(pin_node, ast.Constant) and pin_node.value == "0.15.22"
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    ruff_line = next(line for line in makefile.splitlines() if line.startswith("RUFF"))
    assert "ruff@0.15.22" in ruff_line


def test_pyc_6_dual_wired_make_ci_and_workflow() -> None:
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    ci_target = next(line for line in makefile.splitlines() if line.startswith("ci:"))
    assert "check-docstring-presence" in ci_target
    assert "\t@./scripts/check_docstring_presence.sh" in makefile
    workflow = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    run_lines = [
        line.strip()
        for line in workflow.splitlines()
        if "check_docstring_presence.sh" in line and line.strip().startswith("run:")
    ]
    assert run_lines == ["run: ./scripts/check_docstring_presence.sh"]


def test_pyc_6_prose_homes_name_the_gate() -> None:
    """C-010: STATUS / sequence / maps / AGENTS / DEVELOPMENT name the arming.

    Retargeted by DL-4 (2026-08-25): the STATUS bullet and the slate's PYC appendix moved,
    verbatim, to PYC's history record when the campaign was filed as closed; the live homes
    of the fact are AGENTS.md, DEVELOPMENT.md and scripts/map.md, which this test still reads.
    """
    for relative in (
        "AGENTS.md",
        "docs/history/pyc/status-record.md",
        "DEVELOPMENT.md",
        "scripts/map.md",
    ):
        text = (_REPO / relative).read_text(encoding="utf-8")
        assert "check_docstring_presence" in text or "check-docstring-presence" in text, relative


def test_pyc_6_on_pre_commit_hook() -> None:
    """Sub-second at arming, so it joins the hook the way map-sync did."""
    pre_commit = (_REPO / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    assert "id: docstring-presence-guard" in pre_commit
    assert "check_docstring_presence.sh" in pre_commit
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    printf_line = next(
        line for line in makefile.splitlines() if "printf" in line and "check_map_md.sh" in line
    )
    assert "check_docstring_presence.sh" in printf_line
    assert "check_python_conventions.sh" not in printf_line


def test_pyc_6_check_counts_unlisted_file_is_red() -> None:
    """C-009 / C1-Q-001: an unlisted path with findings cannot return []."""
    gate = _load_gate()
    fixture: dict[str, tuple[int, str]] = {}
    errors: list[str] = gate.check_counts(
        {"scripts/new_file.py"}, {"scripts/new_file.py": 1}, fixture
    )
    assert errors == [
        error for error in errors if "scripts/new_file.py" in error and "no EXCEPTIONS" in error
    ]
    assert len(errors) == 1


def test_pyc_6_check_counts_over_ceiling_is_red() -> None:
    """C-009: growing a listed file past a ceiling-1 row is red (C2-Q-002)."""
    gate = _load_gate()
    fixture: dict[str, tuple[int, str]] = {"scripts/listed.py": (1, "fixture")}
    errors: list[str] = gate.check_counts({"scripts/listed.py"}, {"scripts/listed.py": 2}, fixture)
    assert len(errors) == 1
    assert "scripts/listed.py" in errors[0]
    assert "ceiling 1" in errors[0]


def test_pyc_6_check_counts_stale_and_zero_rows_are_red() -> None:
    """C-008: stale key and zero-count row cannot return []."""
    gate = _load_gate()
    fixture: dict[str, tuple[int, str]] = {"scripts/listed.py": (1, "fixture")}
    stale: list[str] = gate.check_counts(set(), {}, fixture)
    assert len(stale) == 1
    assert "not in the scan set" in stale[0]
    zero: list[str] = gate.check_counts({"scripts/listed.py"}, {}, fixture)
    assert len(zero) == 1
    assert "measures 0" in zero[0]
    assert "scripts/listed.py" in zero[0]


def test_pyc_6_empty_ruff_stdout_is_fail_closed() -> None:
    """C-008 / C1-Q-002: empty stdout is not zero findings."""
    gate = _load_gate()
    with pytest.raises(RuntimeError, match="stdout is empty"):
        gate.parse_ruff_stdout(0, "", "")
    with pytest.raises(RuntimeError, match="stdout is empty"):
        gate.parse_ruff_stdout(1, "  \n", "")
    assert gate.parse_ruff_stdout(0, "[]", "") == []


def test_pyc_6_ruff_isolated_and_ignore_noqa() -> None:
    """C1-SEC-001: pyproject / noqa cannot drop presence diagnostics."""
    source = _GATE.read_text(encoding="utf-8")
    assert '"--isolated"' in source
    assert '"--ignore-noqa"' in source


def test_pyc_6_ruff_is_invoked_on_collected_files() -> None:
    """C1-L-001 / C2-Q-001: reverting to directory discovery stays red."""
    source = _GATE.read_text(encoding="utf-8")
    run_ruff_body = source.split("def run_ruff", 1)[1].split("\ndef ", 1)[0]
    assert "*relatives" in run_ruff_body
    assert "*SCAN_ROOTS" not in run_ruff_body
    assert "run_ruff(repo, paths)" in source


def test_pyc_6_wrapper_uses_isolated_python() -> None:
    """C1-SEC-002: script dir cannot shadow json/subprocess."""
    wrapper = (_REPO / "scripts" / "check_docstring_presence.sh").read_text(encoding="utf-8")
    assert 'exec python3 -I "$repo_root/scripts/check_docstring_presence.py"' in wrapper


def test_pyc_6_relative_diagnostic_resolves_against_repo() -> None:
    """C1-L-002: relative ruff names are not resolved against process cwd."""
    gate = _load_gate()
    relative = "python/repark/src/repark/errors.py"
    assert gate.resolve_diagnostic_path(_REPO, relative) == relative
