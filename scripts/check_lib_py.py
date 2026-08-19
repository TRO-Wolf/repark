#!/usr/bin/env python3
"""Enforce Python package thinness under python/repark/src/repark/**/*.py.

SSOT for Python facade file size (r27 T1). Sibling of check_lib_rs. Prose
(AGENTS.md / CLAUDE.md) points here and never restates the ceilings.

Rules over every *.py under python/repark/src/repark/ (recursive):
1. Per-file line ceiling: default 2500 (docs count). EXCEPTIONS table overrides
   with reason + ratchet note. Ceilings ratchet DOWN only.
2. No-stub rule: a module whose body is only a module docstring + import /
   re-export / __all__ / pass statements must start its docstring with the
   exact substring ``re-export binding`` (case-sensitive, first line). Package
   ``__init__.py`` files are EXEMPT from the no-stub rule (still under ceiling).

Exit 0 on clean; non-zero with path, measured count, ceiling, and outs.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

DEFAULT_CEILING = 2500

# repo-relative posix path -> (ceiling, reason). Keys sorted. Ceilings DOWN only.
EXCEPTIONS: dict[str, tuple[int, str]] = {
    "python/repark/src/repark/spark/dataframe/core.py": (
        7225,  # measured 7191 after DF1 native dynamic_flatten (planner loop deleted)
        "DataFrame class + plan glue after the T0 nested-class and T0b plan-collapse "
        "extracts; RATCHET: after method-region mixins (technique B) if shipped",
    ),
    "python/repark/src/repark/spark/ml/feature/_transformers.py": (
        2800,  # measured ~2733
        "ML feature transformers battery; RATCHET: after per-transformer modules",
    ),
    "python/repark/src/repark/spark/session/_funcs.py": (
        8400,  # measured ~8254
        "session free-function residual post-r26 package split; "
        "RATCHET: after further session extract",
    ),
}

REEXPORT_MARK = "re-export binding"


def _is_reexport_only(tree: ast.Module) -> bool:
    """True if every top-level stmt is import, re-export binding, __all__, or pass."""
    for node in tree.body:
        if isinstance(node, (ast.Import, ast.ImportFrom, ast.Pass)):
            continue
        if isinstance(node, ast.Assign):
            if (
                len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
                and node.targets[0].id == "__all__"
            ):
                continue
            return False
        if isinstance(node, ast.AnnAssign):
            if isinstance(node.target, ast.Name) and node.target.id == "__all__":
                continue
            return False
        if isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant):
            if isinstance(node.value.value, str):
                continue
            return False
        if isinstance(node, ast.For):
            continue
        if isinstance(node, ast.Delete):
            continue
        if isinstance(node, ast.If):
            return False
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            return False
        return False
    return True


def check_file(path: Path, repo: Path) -> list[str]:
    errors: list[str] = []
    rel = path.relative_to(repo).as_posix()
    text = path.read_text(encoding="utf-8")
    line_count = len(text.splitlines())
    ceiling, reason = EXCEPTIONS.get(rel, (DEFAULT_CEILING, "default ceiling"))
    if line_count > ceiling:
        errors.append(
            f"ERROR: {rel} is {line_count} lines (ceiling {ceiling}). "
            f"Reason on file: {reason}. "
            f"Sanctioned outs: (1) split the module, or (2) edit EXCEPTIONS in "
            f"scripts/check_lib_py.py with a reason (ceilings ratchet down only)."
        )

    if path.name == "__init__.py":
        return errors

    try:
        tree = ast.parse(text, filename=str(path))
    except SyntaxError as exc:
        errors.append(f"ERROR: {rel}: syntax error ({exc})")
        return errors

    if _is_reexport_only(tree):
        doc = ast.get_docstring(tree) or ""
        first = doc.splitlines()[0] if doc else ""
        if REEXPORT_MARK not in first:
            errors.append(
                f"ERROR: {rel}: re-export-only module must start its docstring with "
                f"the exact substring `{REEXPORT_MARK}` (case-sensitive, first line). "
                f"First line was: {first!r}."
            )
    return errors


def main() -> int:
    repo = Path(__file__).resolve().parent.parent
    root = repo / "python" / "repark" / "src" / "repark"
    if not root.is_dir():
        print("ERROR: python/repark/src/repark not found", file=sys.stderr)
        return 2

    all_errors: list[str] = []
    checked = 0
    for path in sorted(root.rglob("*.py")):
        checked += 1
        all_errors.extend(check_file(path, repo))

    if all_errors:
        for err in all_errors:
            print(err, file=sys.stderr)
        print(
            f"lib-py: FAIL — {len(all_errors)} violation(s) across {checked} files",
            file=sys.stderr,
        )
        return 1

    print(f"lib-py: {checked} files clean (ceilings held; no-stub rule held)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
