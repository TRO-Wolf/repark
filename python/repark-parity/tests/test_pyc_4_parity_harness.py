"""Parity-harness BaseModel conversion and nested-def pins.

Pins the EXCEPTIONS-table identity, the dual-wire sanctioned leftover, the ANN
split, and the CensusRow type check. Behaviour of the comparator and census
runner stays on ``test_compare_reports.py`` / ``test_compat_harness.py``.
"""

from __future__ import annotations

import ast
import re
import sys
import tomllib
from pathlib import Path

import pytest
from pydantic import BaseModel, ValidationError

_PARITY_ROOT = Path(__file__).resolve().parents[1]
if str(_PARITY_ROOT) not in sys.path:
    sys.path.insert(0, str(_PARITY_ROOT))

from compat.bootstrap import PatchEntry  # noqa: E402
from compat.classify import CensusRow  # noqa: E402
from compat.compare_reports import compute_denominators  # noqa: E402

_REPO = Path(__file__).resolve().parents[3]
_CONVENTIONS = _REPO / "scripts" / "check_python_conventions.py"

_CONVERTED_DATACLASS_FILES: tuple[str, ...] = (
    "python/repark-parity/bench/fuzz/bank.py",
    "python/repark-parity/bench/fuzz/compare.py",
    "python/repark-parity/bench/fuzz/datagen.py",
    "python/repark-parity/bench/fuzz/generator.py",
    "python/repark-parity/bench/fuzz/minimizer.py",
    "python/repark-parity/bench/fuzz/runner.py",
    "python/repark-parity/bench/tpcds/compare.py",
    "python/repark-parity/bench/tpcds/queries.py",
    "python/repark-parity/bench/tpcds/runner.py",
    "python/repark-parity/bench/tpch/compare.py",
    "python/repark-parity/bench/tpch/queries.py",
    "python/repark-parity/bench/tpch/runner.py",
    "python/repark-parity/bench/write/merge_runner.py",
    "python/repark-parity/bench/write/overwrite_runner.py",
    "python/repark-parity/bench/write/runner.py",
    "python/repark-parity/compat/bootstrap.py",
    "python/repark-parity/compat/classify.py",
    "python/repark-parity/compat/compare_reports.py",
    "python/repark-parity/compat/fetch.py",
    "python/repark-parity/compat/runner.py",
)

_LIFTED_TO_ZERO: tuple[str, ...] = (
    "python/repark-parity/bench/fuzz/bank.py",
    "python/repark-parity/bench/fuzz/runner.py",
    "python/repark-parity/compat/bootstrap.py",
)

_PRAGMA_SITES: tuple[tuple[str, str], ...] = (
    ("python/repark-parity/bench/fuzz/minimizer.py", "still_diverges"),
    ("python/repark-parity/bench/tpch/runner.py", "_alarm_handler"),
    ("python/repark-parity/bench/tpcds/runner.py", "_alarm_handler"),
    ("python/repark-parity/compat/runner.py", "_handler"),
    ("python/repark-parity/tests/test_compat_harness.py", "spy"),
    ("scripts/check_parity_live_dual_wire.py", "field"),
)

_DATACLASS_IMPORT = re.compile(r"^\s*(from dataclasses import|import dataclasses)\b", re.M)
_DUAL_WIRE = "scripts/check_parity_live_dual_wire.py"


def _exception_keys(table_name: str) -> list[str]:
    tree = ast.parse(_CONVENTIONS.read_text(encoding="utf-8"))
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


def _collect_nested_function_names(
    node: ast.AST,
    *,
    inside_function: bool,
    found: list[str],
) -> None:
    is_fn = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    if is_fn and inside_function:
        found.append(node.name)
    child_inside = inside_function or is_fn
    for child in ast.iter_child_nodes(node):
        _collect_nested_function_names(child, inside_function=child_inside, found=found)


def _nested_function_names(relative: str) -> list[str]:
    path = _REPO / relative
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[str] = []
    _collect_nested_function_names(tree, inside_function=False, found=found)
    return found


def test_pyc_4_nested_def_exceptions_table_is_empty() -> None:
    """C-001: every nested-def EXCEPTIONS row is deleted, not zeroed."""
    assert _exception_keys("NESTED_DEF_EXCEPTIONS") == []


def test_pyc_4_dataclass_exceptions_are_only_dual_wire() -> None:
    """C-002: converted harness files leave the table; dual-wire stays (no venv pydantic)."""
    keys = _exception_keys("DATACLASS_EXCEPTIONS")
    assert keys == [_DUAL_WIRE]
    still_present = [path for path in _CONVERTED_DATACLASS_FILES if path in keys]
    assert still_present == [], f"PYC-4 rows were zeroed instead of deleted: {still_present}"


def _basemodel_class_nodes(relative: str) -> list[ast.ClassDef]:
    tree = ast.parse((_REPO / relative).read_text(encoding="utf-8"), filename=relative)
    found: list[ast.ClassDef] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        base_names = []
        for base in node.bases:
            if isinstance(base, ast.Name):
                base_names.append(base.id)
            elif isinstance(base, ast.Attribute):
                base_names.append(base.attr)
        if "BaseModel" in base_names:
            found.append(node)
    return found


def _model_config_kwargs(class_node: ast.ClassDef) -> dict[str, object]:
    for stmt in class_node.body:
        names: list[str] = []
        value = None
        if isinstance(stmt, ast.Assign):
            for target in stmt.targets:
                if isinstance(target, ast.Name):
                    names.append(target.id)
            value = stmt.value
        elif isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
            names.append(stmt.target.id)
            value = stmt.value
        if "model_config" not in names or not isinstance(value, ast.Call):
            continue
        kwargs: dict[str, object] = {}
        for keyword in value.keywords:
            if keyword.arg is None:
                continue
            if isinstance(keyword.value, ast.Constant):
                kwargs[keyword.arg] = keyword.value.value
        return kwargs
    return {}


@pytest.mark.parametrize("relative", list(_CONVERTED_DATACLASS_FILES))
def test_pyc_4_converted_files_do_not_import_dataclasses(relative: str) -> None:
    source = (_REPO / relative).read_text(encoding="utf-8")
    assert _DATACLASS_IMPORT.search(source) is None, relative
    models = _basemodel_class_nodes(relative)
    assert models, f"{relative} has no BaseModel subclass"


@pytest.mark.parametrize("relative", list(_CONVERTED_DATACLASS_FILES))
def test_pyc_4_converted_models_forbid_extras_and_are_not_strict(relative: str) -> None:
    """Every converted BaseModel sets extra='forbid' and does not set strict=True."""
    models = _basemodel_class_nodes(relative)
    assert models, f"{relative} has no BaseModel subclass"
    for class_node in models:
        kwargs = _model_config_kwargs(class_node)
        assert kwargs.get("extra") == "forbid", f"{relative}:{class_node.name} extra={kwargs}"
        assert kwargs.get("strict") in (None, False), (
            f"{relative}:{class_node.name} set strict={kwargs.get('strict')}"
        )


def test_pyc_4_dual_wire_stays_dataclass_and_pragma() -> None:
    """The dual-wire gate runs as bare python3; it cannot take pydantic."""
    source = (_REPO / _DUAL_WIRE).read_text(encoding="utf-8")
    assert _DATACLASS_IMPORT.search(source) is not None
    assert "pydantic" not in source
    nested = _nested_function_names(_DUAL_WIRE)
    assert nested == ["field"], f"dual-wire nested defs changed: {nested}"
    pragma_lines = [line for line in source.splitlines() if "# nested-def:" in line]
    assert pragma_lines, "dual-wire field lost its nested-def pragma"
    reasons = [line.split("# nested-def:", 1)[1].strip() for line in pragma_lines]
    assert all(reasons), "empty nested-def pragma reason"


@pytest.mark.parametrize("relative", list(_LIFTED_TO_ZERO))
def test_pyc_4_lifted_modules_have_no_nested_defs(relative: str) -> None:
    nested = _nested_function_names(relative)
    assert nested == [], f"{relative} re-grew nested defs: {nested}"


def _pragma_reason_walk(
    node: ast.AST,
    *,
    inside_function: bool,
    name: str,
    lines: list[str],
) -> str | None:
    is_fn = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    if is_fn and inside_function and node.name == name:
        text = lines[node.lineno - 1]
        marker = text.find("# nested-def:")
        if marker == -1:
            return None
        return text[marker + len("# nested-def:") :].strip()
    child_inside = inside_function or is_fn
    for child in ast.iter_child_nodes(node):
        reason = _pragma_reason_walk(child, inside_function=child_inside, name=name, lines=lines)
        if reason is not None:
            return reason
    return None


def _pragma_reason_on_nested_def(relative: str, name: str) -> str | None:
    path = _REPO / relative
    lines = path.read_text(encoding="utf-8").splitlines()
    tree = ast.parse("\n".join(lines), filename=str(path))
    return _pragma_reason_walk(tree, inside_function=False, name=name, lines=lines)


@pytest.mark.parametrize(("relative", "name"), list(_PRAGMA_SITES))
def test_pyc_4_callback_sites_stay_pragmas(relative: str, name: str) -> None:
    nested = _nested_function_names(relative)
    assert name in nested, f"{relative} lost nested {name}: {nested}"
    reason = _pragma_reason_on_nested_def(relative, name)
    assert reason, f"{relative}:{name} lost its nested-def pragma on the def line"


def test_pyc_4_compat_runner_only_nested_def_is_the_alarm_handler() -> None:
    nested = _nested_function_names("python/repark-parity/compat/runner.py")
    assert nested == ["_handler"], f"compat/runner.py nested defs changed: {nested}"


def test_pyc_4_compat_harness_suite_walker_is_module_level() -> None:
    nested = _nested_function_names("python/repark-parity/tests/test_compat_harness.py")
    assert "_collect_suite_test_ids" not in nested
    assert "spy" in nested


def test_pyc_4_census_row_and_patch_entry_are_basemodels() -> None:
    assert issubclass(CensusRow, BaseModel)
    assert issubclass(PatchEntry, BaseModel)


def test_pyc_4_stage_timing_accepts_positional_args() -> None:
    """Write-bench production constructs StageTiming(name, seconds)."""
    from bench.write.runner import StageTiming

    stage = StageTiming("ctas", 1.5)
    assert stage.name == "ctas"
    assert stage.seconds == 1.5
    with pytest.raises(ValidationError):
        StageTiming("ctas", 1.5, extra=True)  # type: ignore[call-arg]


def test_pyc_4_tpcds_query_result_stores_unknown_status() -> None:
    """Dataclass stored unknown labels; exit_code/status_ledger are the gates."""
    from bench.tpcds.runner import QueryResult

    row = QueryResult(
        query_nr=1,
        status="BANANA",
        repark_wall_s=None,
        duckdb_wall_s=None,
        ratio=None,
        repark_rows=None,
        duckdb_rows=None,
    )
    assert row.status == "BANANA"


def test_pyc_4_extra_fields_refused() -> None:
    """C-007: extra='forbid' — unknown kwargs raise, they are not ignored."""
    with pytest.raises(ValidationError):
        CensusRow(test_id="a", module="m", status="PASS", unknown="x")  # type: ignore[call-arg]
    with pytest.raises(ValidationError):
        PatchEntry(  # type: ignore[call-arg]
            target="t",
            source="s",
            kind="replace",
            notes="n",
            extra_field=True,
        )


def test_pyc_4_census_row_rejects_int_test_id() -> None:
    """Dataclass stored int 0; pydantic does not coerce int→str even in lax mode."""
    with pytest.raises(ValidationError):
        CensusRow(test_id=0, module="", status="PASS")  # type: ignore[arg-type]


def test_pyc_4_denominator_dummy_ids_are_strings() -> None:
    """The recorded-denominator gate keys dummy rows as carried-N, not enumerate ints."""
    actual = compute_denominators({"carried-0": "PASS", "carried-1": "NEEDS-JVM"}, junit=False)
    assert actual["pass"] == 1
    assert actual["all_collected"] == 2
    assert actual["engine_relevant"] == 1
    source = (_REPO / "python/repark-parity/compat/compare_reports.py").read_text(encoding="utf-8")
    assert "dict(enumerate(carried))" not in source
    assert 'f"carried-{index}"' in source


def test_pyc_4_parity_package_declares_pydantic() -> None:
    with (_REPO / "python" / "repark-parity" / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    hard_deps: list[str] = pyproject["project"]["dependencies"]
    assert any(item.startswith("pydantic>=2.10") and item.endswith(",<3") for item in hard_deps)


_ISOLATED_PY_TEST_TOKEN = "--with pyarrow --with pytest --with 'pydantic>=2.10,<3'"


def _isolated_parity_recipe_lines(text: str) -> list[str]:
    """Lines that actually invoke the isolated parity pytest (not comments)."""
    return [
        line
        for line in text.splitlines()
        if "--no-project" in line and "pydantic" in line and not line.lstrip().startswith("#")
    ]


def test_pyc_4_isolated_py_test_installs_pydantic() -> None:
    """`--no-project` ignores pyproject.toml; CI/make must `--with pydantic` (C1-Q-001)."""
    makefile = (_REPO / "Makefile").read_text(encoding="utf-8")
    workflow = (_REPO / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    makefile_lines = _isolated_parity_recipe_lines(makefile)
    workflow_lines = _isolated_parity_recipe_lines(workflow)
    assert makefile_lines, "Makefile lost the isolated --no-project parity recipe"
    assert workflow_lines, "ci.yml lost the isolated --no-project parity recipe"
    assert all(_ISOLATED_PY_TEST_TOKEN in line for line in makefile_lines)
    assert all(_ISOLATED_PY_TEST_TOKEN in line for line in workflow_lines)


def test_pyc_4_ann_ignores_split_parity_from_facade() -> None:
    """Parity tests do not inherit ANN201/ANN202. PYC-5 dropped unearned facade ANN201."""
    with (_REPO / "pyproject.toml").open("rb") as handle:
        pyproject = tomllib.load(handle)
    ignores: dict[str, list[str]] = pyproject["tool"]["ruff"]["lint"]["per-file-ignores"]
    assert "**/tests/**" not in ignores
    facade = ignores["python/repark/tests/**"]
    parity = ignores["python/repark-parity/tests/**"]
    assert "ANN201" not in facade
    assert "ANN202" in facade
    assert "ANN201" not in parity
    assert "ANN202" not in parity
    assert "ANN001" in parity
    assert "S101" in parity
