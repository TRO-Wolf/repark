"""PYC-1: ``core.py``, ``plan_collapse.py``, and ``udf_bridge.py`` have no nested ``def``.

The conventions gate holds the same rule over the whole tree; this pin names the
three modules this unit emptied so a regression that re-nests a helper fails here
even if someone re-seeds an EXCEPTIONS row.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from types import ModuleType

import pytest

from repark.spark.dataframe import DataFrame, core, plan_collapse, udf_bridge


def _collect_nested_function_names(
    node: ast.AST,
    *,
    inside_function: bool,
    found: list[str],
) -> None:
    """Walk ``node``, recording every ``def`` that has a ``def`` ancestor."""
    is_fn = isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)
    if is_fn and inside_function:
        found.append(f"{node.name}:{node.lineno}")
    child_inside = inside_function or is_fn
    for child in ast.iter_child_nodes(node):
        _collect_nested_function_names(child, inside_function=child_inside, found=found)


def _nested_function_names(path: Path) -> list[str]:
    """Every ``def`` that has a ``def`` ancestor (including inside ``try`` / ``if``).

    The conventions gate only counts a nested ``def`` whose *immediate* parent is a
    ``def``, so a helper parked under ``try:`` is invisible to it. PYC-1 emptied the
    ancestor set too (``_emit_side``).
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[str] = []
    _collect_nested_function_names(tree, inside_function=False, found=found)
    return found


@pytest.mark.parametrize("module", [core, plan_collapse, udf_bridge])
def test_pyc_1_dataframe_modules_have_no_nested_defs(module: ModuleType) -> None:
    source_file = inspect.getsourcefile(module)
    assert source_file is not None, f"{module.__name__} has no source file"
    nested = _nested_function_names(Path(source_file))
    assert nested == [], f"{module.__name__} re-grew nested defs: {nested}"


def _pandas_loaded_at_module_scope(node: ast.AST) -> bool:
    """True when ``node`` is a module-level pandas import (including under ``try`` / ``if``)."""
    if isinstance(node, ast.Import):
        return any(alias.name.split(".", 1)[0] == "pandas" for alias in node.names)
    if isinstance(node, ast.ImportFrom):
        return (node.module or "").split(".", 1)[0] == "pandas"
    if isinstance(node, ast.Expr) and isinstance(node.value, ast.Call):
        func = node.value.func
        if isinstance(func, ast.Name) and func.id == "__import__":
            args = node.value.args
            return bool(args) and isinstance(args[0], ast.Constant) and args[0].value == "pandas"
    return False


def _collect_module_scope_nodes(node: ast.AST, found: list[ast.AST]) -> None:
    """Accumulate import-time statements, descending into ``try`` / ``if``."""
    found.append(node)
    if isinstance(node, ast.Try):
        for child in node.body + node.orelse + node.finalbody:
            _collect_module_scope_nodes(child, found)
        for handler in node.handlers:
            for child in handler.body:
                _collect_module_scope_nodes(child, found)
    elif isinstance(node, ast.If):
        for child in node.body + node.orelse:
            _collect_module_scope_nodes(child, found)


def _module_scope_nodes(tree: ast.Module) -> list[ast.AST]:
    """Statements that run at import, including nested module-level ``try`` / ``if``."""
    found: list[ast.AST] = []
    for statement in tree.body:
        _collect_module_scope_nodes(statement, found)
    return found


def test_pyc_1_udf_bridge_does_not_import_pandas_at_module_scope() -> None:
    """C-011: core imports udf_bridge at load; pandas must stay inside callbacks.

    MUTATION: ``import pandas`` / ``try: import pandas`` at ``udf_bridge.py`` module
    body (keep ``__import__("pandas")`` inside ``_run_pandas_udf_arrow_batches``) → red.
    """
    source_file = inspect.getsourcefile(udf_bridge)
    assert source_file is not None
    tree = ast.parse(Path(source_file).read_text(encoding="utf-8"))
    assert isinstance(tree, ast.Module)
    hits = [node for node in _module_scope_nodes(tree) if _pandas_loaded_at_module_scope(node)]
    assert hits == [], "pandas must not load when udf_bridge is imported"


def test_pyc_1_exception_rows_deleted_not_zeroed() -> None:
    """C-002: emptied files leave the EXCEPTIONS table, they are not kept at 0."""
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
        if target_name != "NESTED_DEF_EXCEPTIONS" or not isinstance(value, ast.Dict):
            continue
        for key in value.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.append(key.value)
    assert keys, "did not bind NESTED_DEF_EXCEPTIONS as a module-level dict literal"
    assert "python/repark/src/repark/spark/dataframe/core.py" not in keys
    assert "python/repark/src/repark/spark/dataframe/plan_collapse.py" not in keys


def test_pyc_1_mia_finalize_holds_the_live_names_list() -> None:
    """C-008: finalize extra-args must be the live list, not a snapshot copy.

    MUTATION: ``weakref.finalize(..., list(self._mia_temp_views))`` → red.
    """
    source = inspect.getsource(DataFrame._ensure_mia_view_cleanup)
    assert "list(self._mia_temp_views)" not in source
    assert "self._mia_temp_views)" in source
