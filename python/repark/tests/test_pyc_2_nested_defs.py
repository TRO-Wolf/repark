"""PYC-2: remaining shipped nested ``def``s are lifted or pragma-sanctioned.

The conventions gate holds the same rule over the whole tree; this pin names the
ten modules this unit emptied (or reduced to a sanctioned pragma) so a regression
that re-nests a helper fails here even if someone re-seeds an EXCEPTIONS row.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path
from types import ModuleType

import pytest

from repark.spark import functions, polars, row, types, udtf
from repark.spark.dataframe import joins_columns
from repark.spark.ml.ext import _arrow_util
from repark.spark.ml.feature import _transformers
from repark.spark.session import _funcs as session_funcs
from repark.spark.session import session_core

_LIFTED_MODULES: tuple[ModuleType, ...] = (
    joins_columns,
    session_core,
    session_funcs,
    functions,
    polars,
    row,
    _arrow_util,
    _transformers,
)

_PYC_2_EXCEPTION_KEYS: tuple[str, ...] = (
    "python/repark/src/repark/spark/dataframe/joins_columns.py",
    "python/repark/src/repark/spark/session/session_core.py",
    "python/repark/src/repark/spark/session/_funcs.py",
    "python/repark/src/repark/spark/udtf.py",
    "python/repark/src/repark/spark/types.py",
    "python/repark/src/repark/spark/functions.py",
    "python/repark/src/repark/spark/polars.py",
    "python/repark/src/repark/spark/row.py",
    "python/repark/src/repark/spark/ml/ext/_arrow_util.py",
    "python/repark/src/repark/spark/ml/feature/_transformers.py",
)


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
    ``def``. PYC-2 empties the ancestor set on the lifted modules (same as PYC-1).
    """
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    found: list[str] = []
    _collect_nested_function_names(tree, inside_function=False, found=found)
    return found


def _source_path(module: ModuleType) -> Path:
    source_file = inspect.getsourcefile(module)
    assert source_file is not None, f"{module.__name__} has no source file"
    return Path(source_file)


@pytest.mark.parametrize("module", list(_LIFTED_MODULES))
def test_pyc_2_lifted_modules_have_no_nested_defs(module: ModuleType) -> None:
    nested = _nested_function_names(_source_path(module))
    assert nested == [], f"{module.__name__} re-grew nested defs: {nested}"


def test_pyc_2_types_verifier_stays_a_pragma() -> None:
    """The per-type verifier IS the function's product — a lift would be wrong."""
    path = _source_path(types)
    nested = _nested_function_names(path)
    names = [entry.split(":", 1)[0] for entry in nested]
    assert names == ["verifier"], f"types.py nested defs changed: {nested}"
    source = path.read_text(encoding="utf-8")
    assert "# nested-def:" in source
    pragma_lines = [line for line in source.splitlines() if "# nested-def:" in line]
    assert pragma_lines, "types.py verifier lost its nested-def pragma"
    reasons = [line.split("# nested-def:", 1)[1].strip() for line in pragma_lines]
    assert all(reasons), "empty nested-def pragma reason"
    assert any("verifier" in reason or "product" in reason for reason in reasons)


def test_pyc_2_udtf_builder_stays_a_pragma() -> None:
    """The ``udtf`` decorator factory closes over ``returnType`` — pragma, not a lift."""
    path = _source_path(udtf)
    nested = _nested_function_names(path)
    names = [entry.split(":", 1)[0] for entry in nested]
    assert names == ["_build"], f"udtf.py nested defs changed: {nested}"
    source = path.read_text(encoding="utf-8")
    pragma_lines = [line for line in source.splitlines() if "# nested-def:" in line]
    assert pragma_lines, "udtf.py _build lost its nested-def pragma"
    reasons = [line.split("# nested-def:", 1)[1].strip() for line in pragma_lines]
    assert all(reasons), "empty nested-def pragma reason"
    assert any("returnType" in reason or "decorator" in reason for reason in reasons)


def _nested_def_exception_keys() -> list[str]:
    conventions = Path(__file__).resolve().parents[3] / "scripts" / "check_python_conventions.py"
    tree = ast.parse(conventions.read_text(encoding="utf-8"))
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
        if target_name != "NESTED_DEF_EXCEPTIONS" or not isinstance(value, ast.Dict):
            continue
        found_table = True
        for key in value.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                keys.append(key.value)
    assert found_table, "did not bind NESTED_DEF_EXCEPTIONS as a module-level dict literal"
    return keys


def test_pyc_2_exception_rows_deleted_not_zeroed() -> None:
    """Emptied shipped files leave the EXCEPTIONS table; they are not kept at 0."""
    keys = _nested_def_exception_keys()
    still_present = [key for key in _PYC_2_EXCEPTION_KEYS if key in keys]
    assert still_present == [], f"PYC-2 rows were zeroed instead of deleted: {still_present}"


def test_pyc_2_cdf_finalize_passes_session_and_view_name() -> None:
    """Temp-view cleanup is ``finalize(frame, func, session, name)``, not a closure.

    MUTATION: ``weakref.finalize(frame, _drop_view)`` with a nested def → red.
    """
    source = inspect.getsource(session_funcs._register_cdf_view_cleanup)
    assert "weakref.finalize(frame," in source
    assert "session, view_name)" in source
    assert "def _drop_view" not in source


def test_pyc_2_ext_finalize_passes_session_and_view_name() -> None:
    """Ext ML temp-view cleanup uses the same extra-args finalize form as MIA/CDF."""
    source = inspect.getsource(_arrow_util._own_ext_temp_view)
    assert "weakref.finalize(result_frame," in source
    assert "session, view_name)" in source
    assert "def _drop(" not in source
