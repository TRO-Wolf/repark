"""PYC-5 close: hook off pre-commit, ANN201 off facade tests, leftover tables."""

from __future__ import annotations

import ast
import tomllib
from pathlib import Path

_REPO = Path(__file__).resolve().parents[3]


def _exception_keys(table_name: str) -> list[str]:
    tree = ast.parse(
        (_REPO / "scripts" / "check_python_conventions.py").read_text(encoding="utf-8")
    )
    found_table = False
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
        if target_name != table_name or not isinstance(value, ast.Dict):
            continue
        found_table = True
        for key in value.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.append(key.value)
    assert found_table, f"did not bind {table_name} as a module-level dict literal"
    return keys


def test_pyc_5_nested_def_exceptions_empty() -> None:
    assert _exception_keys("NESTED_DEF_EXCEPTIONS") == []


def test_pyc_5_dataclass_exceptions_only_dual_wire() -> None:
    assert _exception_keys("DATACLASS_EXCEPTIONS") == ["scripts/check_parity_live_dual_wire.py"]


def test_pyc_5_ann201_not_ignored_on_tests() -> None:
    """Isolated ruff ANN201 on facade tests was 0; the ignore is no longer earned."""
    with (_REPO / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    ignores: dict[str, list[str]] = pyproject["tool"]["ruff"]["lint"]["per-file-ignores"]
    assert "ANN201" not in ignores["python/repark/tests/**"]
    assert "ANN201" not in ignores["python/repark-parity/tests/**"]
    assert "ANN202" in ignores["python/repark/tests/**"]
    assert "ANN001" in ignores["python/repark/tests/**"]
    assert "S101" in ignores["python/repark/tests/**"]


def test_pyc_5_conventions_guard_not_on_pre_commit_hook() -> None:
    """C-005: hook writer is the install-hooks printf, not check-map-md."""
    pre_commit = (_REPO / ".pre-commit-config.yaml").read_text(encoding="utf-8")
    assert "id: python-conventions-guard" not in pre_commit
    assert "check_python_conventions.sh" not in pre_commit
    assert "cannot drift" not in pre_commit
    assert "PYC-5 dropped" in pre_commit
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    printf_line = next(
        line for line in makefile.splitlines() if "printf" in line and "check_map_md.sh" in line
    )
    assert "check_python_conventions.sh" not in printf_line
    agents = (_REPO / "AGENTS.md").read_text(encoding="utf-8")
    assert "**Not** on the pre-commit hook as of PYC-5" in agents
    assert "sub-second budget" in agents


def test_pyc_5_prose_homes_name_the_hook_drop() -> None:
    """C-008: STATUS / sequence / maps / AGENTS / DEVELOPMENT name the drop.

    Retargeted by DL-4 (2026-08-25): PYC's STATUS bullet moved, verbatim, to its history
    record when the campaign was filed as closed; the live homes of the fact are AGENTS.md,
    DEVELOPMENT.md and the skill map, which this test still reads.
    """
    not_on_hook = "**Not** on the pre-commit hook as of PYC-5"
    for relative in (
        "AGENTS.md",
        "docs/history/pyc/status-record.md",
        ".agents/skills/code-quality/map.md",
    ):
        text = (_REPO / relative).read_text(encoding="utf-8")
        assert not_on_hook in text, relative
    development = (_REPO / "DEVELOPMENT.md").read_text(encoding="utf-8")
    assert "not** on pre-commit" in development
    scripts_map = (_REPO / "scripts" / "map.md").read_text(encoding="utf-8")
    assert "not on pre-commit" in scripts_map
    # The slate's copy of the sentence left with DL-4 (a merged unit leaves the slate whole);
    # the fact stays pinned in the four homes above.


def test_pyc_5_conventions_stays_in_make_ci_and_workflow() -> None:
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    ci_target = next(line for line in makefile.splitlines() if line.startswith("ci:"))
    assert "check-python-conventions" in ci_target
    workflow = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    run_lines = [
        line.strip()
        for line in workflow.splitlines()
        if "check_python_conventions.sh" in line and line.strip().startswith("run:")
    ]
    assert run_lines == ["run: ./scripts/check_python_conventions.sh"]


def _function_return_annotation(path: Path, name: str) -> ast.expr | None:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and node.name == name:
            return node.returns
    raise AssertionError(f"did not find def {name} in {path}")


def test_pyc_5_door_and_guarded_have_return_annotations() -> None:
    """C-004: revert the two `-> object` annotations and this pin goes red."""
    door_path = _REPO / "python" / "repark" / "tests" / "test_lrs4_door_domain.py"
    guarded_path = _REPO / "python" / "repark" / "tests" / "test_polars_core.py"
    assert _function_return_annotation(door_path, "door") is not None
    assert _function_return_annotation(guarded_path, "guarded") is not None
