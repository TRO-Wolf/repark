#!/usr/bin/env python3
"""Enforce the two Python conventions Ruff cannot express.

SSOT for the nested-`def` ban and the "Pydantic, never `dataclasses`/`attrs`"
rule. Prose (AGENTS.md "Python", .agent/skills/code-quality/SKILL.md, docs/skills/*)
points here and never restates the tables. Mirrors the check_rust_file_size
dual-wire shape (py = logic + SSOT, sh = wrapper).

The other two Python conventions are already mechanically enforced and are
deliberately NOT re-implemented here: type coverage is Ruff's `ANN` rule set
(selected in pyproject.toml) and naming is a review duty no linter can judge.

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

# repo-relative posix path -> (ceiling, reason). Keys sorted alphabetically.
# Seeded from the measured tree at the commit that armed this guard (2026-08-21):
# 66 nested defs across 21 files. PYC-1 deleted the core.py (23) and
# plan_collapse.py (12) rows. PYC-2 deleted the remaining 10 shipped-package
# rows (12 lifts + 2 pragmas). PYC-3 does not touch this table. Every remaining
# row is debt, not a sanction — the ceiling is the count on that day, it goes
# DOWN as PYC lands, and a row whose file drops to zero is deleted rather than
# kept at 0.
NESTED_DEF_EXCEPTIONS: dict[str, tuple[int, str]] = {
    "python/repark-parity/bench/fuzz/bank.py": (
        1,
        "row-buffer flush local to the minimized-table parser; RATCHET: PYC",
    ),
    "python/repark-parity/bench/fuzz/minimizer.py": (
        1,
        "the still-diverges predicate handed to the shrink loop; RATCHET: PYC "
        "(a callback whose closure is arguably the point — may end as a pragma)",
    ),
    "python/repark-parity/bench/fuzz/runner.py": (
        1,
        "the execute callback passed into the minimizer; RATCHET: PYC",
    ),
    "python/repark-parity/bench/tpcds/runner.py": (
        1,
        "SIGALRM handler closing over the per-query timeout; RATCHET: PYC "
        "(signal handlers are the callback case — may end as a pragma)",
    ),
    "python/repark-parity/bench/tpch/runner.py": (
        1,
        "SIGALRM handler closing over the per-query timeout; RATCHET: PYC "
        "(signal handlers are the callback case — may end as a pragma)",
    ),
    "python/repark-parity/compat/bootstrap.py": (
        5,
        "monkeypatched setUp/tearDown factories for the reused PySpark session; "
        "each closes over the class being patched; RATCHET: PYC",
    ),
    "python/repark-parity/compat/runner.py": (
        4,
        "recursive suite walkers plus the worker alarm handler; RATCHET: PYC "
        "(the walkers lift to module level with an accumulator argument)",
    ),
    "python/repark-parity/tests/test_compat_harness.py": (
        2,
        "a spy and a suite walker local to two tests; RATCHET: PYC",
    ),
    "scripts/check_parity_live_dual_wire.py": (
        1,
        "field comparator local to compare(); RATCHET: PYC",
    ),
}

# repo-relative posix path -> reason. Keys sorted alphabetically. Every row is
# debt: the file still imports `dataclasses` and PYC converts it to Pydantic.
# A row is deleted when its file converts; rows are never added without the
# owner ruling that the file genuinely cannot take a BaseModel. PYC-3 deleted
# the two shipped-package rows (merge.py, _csv_smart.py).
DATACLASS_EXCEPTIONS: dict[str, str] = {
    "python/repark-parity/bench/fuzz/bank.py": "fuzz corpus records; RATCHET: PYC",
    "python/repark-parity/bench/fuzz/compare.py": "fuzz comparison rows; RATCHET: PYC",
    "python/repark-parity/bench/fuzz/datagen.py": "generated-column specs; RATCHET: PYC",
    "python/repark-parity/bench/fuzz/generator.py": "query-shape specs; RATCHET: PYC",
    "python/repark-parity/bench/fuzz/minimizer.py": "shrink-step records; RATCHET: PYC",
    "python/repark-parity/bench/fuzz/runner.py": "fuzz run configuration; RATCHET: PYC",
    "python/repark-parity/bench/tpcds/compare.py": "TPC-DS comparison rows; RATCHET: PYC",
    "python/repark-parity/bench/tpcds/queries.py": "TPC-DS query records; RATCHET: PYC",
    "python/repark-parity/bench/tpcds/runner.py": "TPC-DS run configuration; RATCHET: PYC",
    "python/repark-parity/bench/tpch/compare.py": "TPC-H comparison rows; RATCHET: PYC",
    "python/repark-parity/bench/tpch/queries.py": "TPC-H query records; RATCHET: PYC",
    "python/repark-parity/bench/tpch/runner.py": "TPC-H run configuration; RATCHET: PYC",
    "python/repark-parity/bench/write/merge_runner.py": "MERGE bench config; RATCHET: PYC",
    "python/repark-parity/bench/write/overwrite_runner.py": "overwrite bench config; RATCHET: PYC",
    "python/repark-parity/bench/write/runner.py": "write bench config; RATCHET: PYC",
    "python/repark-parity/compat/bootstrap.py": "harness bootstrap options; RATCHET: PYC",
    "python/repark-parity/compat/classify.py": "census classification rows; RATCHET: PYC",
    "python/repark-parity/compat/compare_reports.py": "report diff rows; RATCHET: PYC",
    "python/repark-parity/compat/fetch.py": "upstream fetch options; RATCHET: PYC",
    "python/repark-parity/compat/runner.py": "compat run configuration; RATCHET: PYC",
    "scripts/check_parity_live_dual_wire.py": "dual-wire comparison rows; RATCHET: PYC",
}

_BANNED_CONTAINER_MODULES = frozenset({"dataclasses", "attr", "attrs"})


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

    A method of a class that is itself defined inside a function is NOT a nested
    definition: its immediate parent is the class, and a local class is a
    different question from a local function.
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
        f"dataclass rows {len(DATACLASS_EXCEPTIONS)})"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
