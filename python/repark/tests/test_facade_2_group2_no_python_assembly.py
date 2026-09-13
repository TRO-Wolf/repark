"""FACADE-2 Group-2 methods must not assemble display/SQL/join text in Python."""

from __future__ import annotations

import ast
from pathlib import Path

COLUMN_PATH = Path(__file__).resolve().parents[1] / "src" / "repark" / "spark" / "column.py"
GROUP2_METHODS: frozenset[str] = frozenset(
    {
        "_binary",
        "__neg__",
        "__ne__",
        "__invert__",
        "eqNullSafe",
        "substr",
        "_string_predicate",
        "_bitwise",
        "is_null",
        "is_not_null",
        "_from_when_pairs",
        "alias",
        "__getitem__",
        "getField",
        "getItem",
        "cast",
        "try_cast",
        "_with_sort_order",
    }
)
_REJECT_ATTRS: frozenset[str] = frozenset({"_reject_nested_generator", "_reject_higher_order"})
_ERROR_NAMES: frozenset[str] = frozenset(
    {
        "AnalysisException",
        "ParseException",
        "PySparkTypeError",
        "PySparkValueError",
        "UnsupportedOperationException",
        "AttributeError",
        "TypeError",
        "ValueError",
    }
)


def _column_class(tree: ast.Module) -> ast.ClassDef:
    """Return the `Column` class from `column.py`."""
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == "Column":
            return node
    raise AssertionError("Column class not found in column.py")


def _group2_functions(column_class: ast.ClassDef) -> dict[str, ast.FunctionDef]:
    """Map Group-2 method names to their AST function nodes."""
    found: dict[str, ast.FunctionDef] = {}
    for item in column_class.body:
        if isinstance(item, ast.FunctionDef) and item.name in GROUP2_METHODS:
            found[item.name] = item
    return found


def _is_reject_or_error_call(node: ast.Call) -> bool:
    """True when the call is a generator/HOF refusal or an exception constructor."""
    func = node.func
    if isinstance(func, ast.Attribute) and func.attr in _REJECT_ATTRS:
        return True
    return isinstance(func, ast.Name) and func.id in _ERROR_NAMES


def _is_stringy(node: ast.AST) -> bool:
    """True when `node` is a string literal, f-string, or render-part call."""
    if isinstance(node, ast.JoinedStr):
        return True
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return True
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr in {
            "sql_expr_part",
            "spark_display_part",
            "spark_wrap_display_part",
            "join_sql_part",
        }
    return False


def _is_text_assembly(node: ast.AST) -> bool:
    """True when `node` concatenates or interpolates display/SQL/join text."""
    if isinstance(node, ast.JoinedStr):
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Mod):
        return True
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        return _is_stringy(node.left) or _is_stringy(node.right)
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        return node.func.attr in {"format", "join"}
    return False


def _assembly_sites(function: ast.FunctionDef) -> list[str]:
    """Line labels of display/SQL/join text assembly inside `function`."""
    sites: list[str] = []
    stack: list[tuple[ast.AST, bool]] = [(child, False) for child in ast.iter_child_nodes(function)]
    while stack:
        node, allowed = stack.pop()
        next_allowed = allowed or isinstance(node, ast.Raise)
        if isinstance(node, ast.Call) and _is_reject_or_error_call(node):
            next_allowed = True
        if not next_allowed and _is_text_assembly(node):
            sites.append(f"{function.name}:{node.lineno}")
        stack.extend((child, next_allowed) for child in ast.iter_child_nodes(node))
    return sites


def test_group2_methods_exist_on_column() -> None:
    """Every named Group-2 family method is present on Column. pins: facade-2/C-009"""
    tree = ast.parse(COLUMN_PATH.read_text(encoding="utf-8"), filename=str(COLUMN_PATH))
    found = _group2_functions(_column_class(tree))
    missing = sorted(GROUP2_METHODS - found.keys())
    extra_ok = found.keys() <= GROUP2_METHODS
    assert missing == [], f"missing Group-2 methods: {missing}"
    assert extra_ok


def test_group2_methods_do_not_assemble_display_sql_or_join_text() -> None:
    """No f-string/concat/format/join in Group-2 methods builds render text. pins: facade-2/C-009"""
    tree = ast.parse(COLUMN_PATH.read_text(encoding="utf-8"), filename=str(COLUMN_PATH))
    found = _group2_functions(_column_class(tree))
    sites: list[str] = []
    for name in sorted(GROUP2_METHODS):
        sites.extend(_assembly_sites(found[name]))
    assert sites == [], f"Group-2 Python text assembly remains: {sites}"
