#!/usr/bin/env python3
"""Enforce the Python conventions Ruff cannot express.

SSOT for the nested-`def` ban, the "Pydantic, never `dataclasses`/`attrs`" rule, and the
SQP-1 SQL-string-literal-escape rule. Prose points here and never restates the tables.
Mirrors the check_rust_file_size dual-wire shape (py = logic + SSOT, sh = wrapper).

The other Python conventions are already mechanically enforced and are
deliberately NOT re-implemented here: type coverage is Ruff's `ANN` rule set
(selected in pyproject.toml), public-docstring presence is
`check_docstring_presence.py`, and naming is a review duty no linter can judge.

Rules over every *.py under SCAN_ROOTS (recursive):

1. **No function defined inside another function.** A nested `def` is invisible
   to tests and to imports, and it is rebuilt on every call of its parent. Two
   sanctioned outs, in this order:
   - An inline pragma `# nested-def: <reason>` on the `def` line or on one of
     its decorator lines, for the three cases the contract allows (a decorator
     closing over its own arguments, a callback whose closure over local state
     IS the point, and `functools.wraps` wrappers). The reason is required and
     must be non-empty; the pragma alone does not pass.
   - A row in NESTED_DEF_EXCEPTIONS carrying a per-file ceiling and a reason.
     Ceilings ratchet DOWN only.

2. **No `dataclasses` or `attrs`.** Pydantic v2 `BaseModel` is the single
   structured-data container. A row in DATACLASS_EXCEPTIONS with a reason is
   the only out; there is no inline pragma, because the fix is mechanical and a
   per-site escape hatch would make the rule decay.

3. **No direct constant quote-doubling `replace` call for SQL (SQP-1).** The receiver-blind AST
   rule evaluates strings, bounded integer `+`/`-`, `chr`, concatenation, and repetition. It
   rejects the one-quote to two-quote call outside the product and standalone-harness helpers.
   The shipped helper-call inventory is pinned separately; this syntax rule does not claim
   semantic completeness.

Exit 0 on clean; non-zero with path, line, measured count and the sanctioned
outs. Fail-closed: an unreadable file, a file that will not parse, an empty
scan set, or an EXCEPTIONS key whose path no longer exists is an error, never
a skip.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

# Trees the guard owns. The shipped package first; the parity harness and the
# repo scripts are held to the same rule because they are read as examples.
SCAN_ROOTS: tuple[str, ...] = (
    "python/repark/src",
    "python/repark-parity",
    "scripts",
)

# Inline escape for a nested `def`, e.g.
#     def decorator(argument: str):  # nested-def: decorator closes over argument
# The text after the colon is the reason and may not be empty.
NESTED_DEF_PRAGMA = "# nested-def:"

# repo-relative posix path -> (ceiling, reason). Keys sorted alphabetically; ceilings ratchet
# DOWN only. The table is empty: remaining nested defs carry inline `# nested-def:` pragmas
# (signal handlers, shrink predicate, spy, dual-wire comparator). A row whose file drops to
# zero is deleted rather than kept at 0.
NESTED_DEF_EXCEPTIONS: dict[str, tuple[int, str]] = {}

# repo-relative posix path -> reason. Keys sorted alphabetically. Every row is debt: the file
# still imports `dataclasses` and is converted to Pydantic when its environment allows it. A
# row is deleted when its file converts; rows are never added without the owner ruling that
# the file genuinely cannot take a BaseModel.
DATACLASS_EXCEPTIONS: dict[str, str] = {
    "scripts/check_parity_live_dual_wire.py": (
        "CI dual-wire guard runs as python3 without the wheel venv; cannot take pydantic"
    ),
}

_BANNED_CONTAINER_MODULES = frozenset({"dataclasses", "attr", "attrs"})
_MAX_CONSTANT_INTEGER = 0x10FFFF
_MAX_CONSTANT_INTEGER_DEPTH = 4
_MAX_CONSTANT_TEXT_DEPTH = 16
_MAX_CONSTANT_TEXT_NODES = 64
_MAX_CONSTANT_TEXT_LENGTH = 2

# Direct constant quote-doubling belongs only in the shared helper.
SQL_LITERAL_HELPER_FILES: frozenset[str] = frozenset(
    {
        "python/repark-parity/src/repark_parity/sql.py",
        "python/repark/src/repark/spark/_idents.py",
    }
)


def _is_function(node: ast.AST) -> bool:
    return isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef)


def _pragma_lines(node: ast.FunctionDef | ast.AsyncFunctionDef) -> range:
    """Physical lines a nested-def pragma may sit on: decorators through `def`."""
    first = min([decorator.lineno for decorator in node.decorator_list] + [node.lineno])
    return range(first, node.lineno + 1)


def _has_pragma_with_reason(node: ast.FunctionDef | ast.AsyncFunctionDef, lines: list[str]) -> bool:
    for lineno in _pragma_lines(node):
        if not 1 <= lineno <= len(lines):
            continue
        text = lines[lineno - 1]
        marker = text.find(NESTED_DEF_PRAGMA)
        if marker == -1:
            continue
        reason = text[marker + len(NESTED_DEF_PRAGMA) :].strip()
        if reason:
            return True
    return False


def find_nested_definitions(
    tree: ast.Module, lines: list[str]
) -> list[ast.FunctionDef | ast.AsyncFunctionDef]:
    """Every `def` whose immediate parent is a `def`, minus the pragma-sanctioned ones.

    A method of a class defined inside a function is NOT nested: its immediate parent is the
    class, a different question from a local function.
    """
    found: list[ast.FunctionDef | ast.AsyncFunctionDef] = []
    stack: list[ast.AST] = [tree]
    while stack:
        parent = stack.pop()
        for child in ast.iter_child_nodes(parent):
            if _is_function(child) and _is_function(parent):
                assert isinstance(child, ast.FunctionDef | ast.AsyncFunctionDef)
                if not _has_pragma_with_reason(child, lines):
                    found.append(child)
            stack.append(child)
    return found


def find_banned_container_imports(tree: ast.Module) -> list[tuple[int, str]]:
    """Import sites of `dataclasses` / `attrs`, as (line, module)."""
    hits: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                root = alias.name.split(".")[0]
                if root in _BANNED_CONTAINER_MODULES:
                    hits.append((node.lineno, alias.name))
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or "").split(".")[0]
            if root in _BANNED_CONTAINER_MODULES:
                hits.append((node.lineno, node.module or ""))
    return hits


def _constant_integer(node: ast.AST, depth: int = 0) -> int | None:
    """Evaluate bounded integer literals and unary or binary addition and subtraction."""
    if depth > _MAX_CONSTANT_INTEGER_DEPTH:
        return None
    if (
        isinstance(node, ast.Constant)
        and isinstance(node.value, int)
        and not isinstance(node.value, bool)
    ):
        return node.value if abs(node.value) <= _MAX_CONSTANT_INTEGER else None
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.UAdd | ast.USub):
        operand = _constant_integer(node.operand, depth + 1)
        if operand is None:
            return None
        value = operand if isinstance(node.op, ast.UAdd) else -operand
        return value if abs(value) <= _MAX_CONSTANT_INTEGER else None
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add | ast.Sub):
        left = _constant_integer(node.left, depth + 1)
        right = _constant_integer(node.right, depth + 1)
        if left is None or right is None:
            return None
        value = left + right if isinstance(node.op, ast.Add) else left - right
        return value if abs(value) <= _MAX_CONSTANT_INTEGER else None
    return None


def _constant_text(node: ast.AST) -> str | None:
    """Evaluate only bounded constant forms used to spell quote arguments."""
    values: dict[int, str | None] = {}
    stack: list[tuple[ast.AST, int, bool]] = [(node, 0, False)]
    visited = 0
    while stack:
        current, depth, expanded = stack.pop()
        if depth > _MAX_CONSTANT_TEXT_DEPTH:
            return None
        if not expanded:
            visited += 1
            if visited > _MAX_CONSTANT_TEXT_NODES:
                return None
            stack.append((current, depth, True))
            if isinstance(current, ast.BinOp) and isinstance(current.op, ast.Add | ast.Mult):
                stack.append((current.right, depth + 1, False))
                stack.append((current.left, depth + 1, False))
            continue
        value: str | None = None
        if isinstance(current, ast.Constant) and type(current.value) is str:
            if len(current.value) <= _MAX_CONSTANT_TEXT_LENGTH:
                value = current.value
        elif (
            isinstance(current, ast.Call)
            and isinstance(current.func, ast.Name)
            and current.func.id == "chr"
            and len(current.args) == 1
            and not current.keywords
        ):
            integer = _constant_integer(current.args[0])
            if integer is not None:
                try:
                    value = chr(integer)
                except ValueError:
                    value = None
        elif isinstance(current, ast.BinOp) and isinstance(current.op, ast.Add):
            left = values.get(id(current.left))
            right = values.get(id(current.right))
            if left is not None and right is not None:
                length = len(left) + len(right)
                if length <= _MAX_CONSTANT_TEXT_LENGTH:
                    value = left + right
        elif isinstance(current, ast.BinOp) and isinstance(current.op, ast.Mult):
            text = values.get(id(current.left))
            count = _constant_integer(current.right)
            if text is None:
                text = values.get(id(current.right))
                count = _constant_integer(current.left)
            if text is not None and count is not None and count >= 0:
                length = len(text) * count
                if length <= _MAX_CONSTANT_TEXT_LENGTH:
                    value = text * count
        values[id(current)] = value
    return values.get(id(node))


def find_sql_quote_doubling(tree: ast.Module) -> list[int]:
    """Return receiver-blind replace calls with whitelisted constant quote arguments."""
    hits: list[int] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
            continue
        if node.func.attr != "replace" or len(node.args) != 2 or node.keywords:
            continue
        if _constant_text(node.args[0]) == "'" and _constant_text(node.args[1]) == "''":
            hits.append(node.lineno)
    return sorted(hits)


def check_file(path: Path, repo: Path) -> list[str]:
    relative = path.relative_to(repo).as_posix()
    try:
        source = path.read_text(encoding="utf-8")
    except OSError as error:
        return [f"ERROR: {relative} is unreadable ({error}) — refuse to pass closed"]
    try:
        tree = ast.parse(source)
    except SyntaxError as error:
        return [f"ERROR: {relative} does not parse ({error}) — refuse to pass closed"]
    except (MemoryError, OverflowError, RecursionError) as error:
        return [
            f"ERROR: {relative} exceeds Python parser resource limits "
            f"({type(error).__name__}) — refuse to pass closed"
        ]

    lines = source.splitlines()
    errors: list[str] = []

    nested = find_nested_definitions(tree, lines)
    ceiling, nested_reason = NESTED_DEF_EXCEPTIONS.get(relative, (0, "no exception row"))
    if len(nested) > ceiling:
        sites = ", ".join(
            f"{node.name}:{node.lineno}" for node in sorted(nested, key=lambda n: n.lineno)
        )
        errors.append(
            f"ERROR: {relative} defines {len(nested)} nested function(s) (ceiling {ceiling}). "
            f"Sites: {sites}. Reason on file: {nested_reason}. "
            f"Sanctioned outs: (1) lift the definition to module or class level and pass what it "
            f"needs as arguments, (2) add `{NESTED_DEF_PRAGMA} <reason>` on the def or a decorator "
            f"line if it is a decorator factory, a state-capturing callback, or a functools.wraps "
            f"wrapper, or (3) raise the row in NESTED_DEF_EXCEPTIONS in "
            f"scripts/check_python_conventions.py with a reason (ceilings ratchet down only)."
        )

    if relative not in DATACLASS_EXCEPTIONS:
        for lineno, module in find_banned_container_imports(tree):
            errors.append(
                f"ERROR: {relative}:{lineno} imports `{module}` — this codebase uses Pydantic v2 "
                f"`BaseModel` for all structured data. Sanctioned outs: (1) convert the container "
                f"to a BaseModel (`model_config = ConfigDict(frozen=True)` for the frozen case), "
                f"or (2) add a row to DATACLASS_EXCEPTIONS in "
                f"scripts/check_python_conventions.py with a reason."
            )

    if relative not in SQL_LITERAL_HELPER_FILES:
        for lineno in find_sql_quote_doubling(tree):
            errors.append(
                f"ERROR: {relative}:{lineno} directly calls replace with constant one-quote and "
                f"two-quote arguments. Use `repark.spark._idents.sql_string_literal` for the "
                f"Spark door, or `escape_sql_single_quotes` for a DataFusion-native statement. "
                f"The direct operation belongs only in {sorted(SQL_LITERAL_HELPER_FILES)}."
            )

    return errors


def main() -> int:
    repo = Path(__file__).resolve().parent.parent

    errors: list[str] = []
    for table_name, table in (
        ("NESTED_DEF_EXCEPTIONS", NESTED_DEF_EXCEPTIONS),
        ("DATACLASS_EXCEPTIONS", DATACLASS_EXCEPTIONS),
    ):
        for relative in sorted(table):
            if not (repo / relative).is_file():
                errors.append(
                    f"ERROR: {table_name} key has no file on disk: {relative} "
                    f"(remove the row or restore the path)"
                )

    paths: list[Path] = []
    for root in SCAN_ROOTS:
        root_path = repo / root
        if not root_path.is_dir():
            print(f"ERROR: scan root {root} not found", file=sys.stderr)
            return 2
        paths.extend(
            path
            for path in sorted(root_path.rglob("*.py"))
            if path.is_file() and "__pycache__" not in path.parts
        )

    if not paths:
        print(
            "ERROR: python conventions scan set is empty — refuse to pass closed",
            file=sys.stderr,
        )
        return 2

    for path in paths:
        errors.extend(check_file(path, repo))

    if errors:
        for error in errors:
            print(error, file=sys.stderr)
        print(
            f"python-conventions: FAIL — {len(errors)} violation(s) across {len(paths)} files",
            file=sys.stderr,
        )
        return 1

    print(
        f"python-conventions: {len(paths)} files clean "
        f"(nested-def rows {len(NESTED_DEF_EXCEPTIONS)}, "
        f"dataclass rows {len(DATACLASS_EXCEPTIONS)}, "
        f"sql-escape helpers {sorted(SQL_LITERAL_HELPER_FILES)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
