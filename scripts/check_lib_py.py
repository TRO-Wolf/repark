#!/usr/bin/env python3
"""Enforce Python source size and facade module thinness.

SSOT for Python facade file size. Sibling of check_lib_rs. Prose points here and never
restates the ceilings.

Rules:
1. Every *.py under python/ and scripts/ has the DEFAULT_CEILING. Blank lines count.
   EXCEPTIONS records exact baselines, debt reasons, and split seams.
2. An excepted file must equal its baseline. Growth fails. Shrinkage also fails until the
   baseline ratchets down or the row retires at the default.
3. Sources under tests/goldens/ and tests/fixtures/ are generated-test inputs and are outside
   the scan.
4. The facade-only no-stub rule: a module whose body is only a module docstring + import /
   re-export / __all__ / pass statements must start its docstring with the exact substring
   ``re-export binding`` (case-sensitive, first line). Package ``__init__.py`` files are
   EXEMPT from the no-stub rule (still under ceiling).

Exit 0 on clean; non-zero with path, measured count, ceiling, and outs.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

DEFAULT_CEILING = 1000
SCAN_ROOTS: tuple[str, ...] = ("python", "scripts")
FACADE_ROOT = "python/repark/src/repark"
EXEMPT_PATHS: tuple[tuple[str, ...], ...] = (("tests", "goldens"), ("tests", "fixtures"))

# repo-relative posix path -> (exact baseline, debt reason, cohesive split seam). Every row
# retires when its file reaches DEFAULT_CEILING. A baseline increase requires explicit owner
# approval; ordinary edits only ratchet rows down.
EXCEPTIONS: dict[str, tuple[int, str, str]] = {
    "python/repark-parity/bench/tpcds/runner.py": (
        1252,
        "TPC-DS orchestration, execution, and reporting share one runner.",
        "Split query execution from result collection and reporting.",
    ),
    "python/repark-parity/bench/tpch/runner.py": (
        1773,
        "TPC-H orchestration, execution, and reporting share one runner.",
        "Split query execution from result collection and reporting.",
    ),
    "python/repark-parity/compat/runner.py": (
        1279,
        "Compatibility discovery, subprocess execution, and report assembly share one module.",
        "Extract worker execution from census result classification.",
    ),
    "python/repark-parity/tests/test_compat_harness.py": (
        1021,
        "Compatibility harness scenarios narrowly exceed the default.",
        "Split worker isolation from classification and report cases.",
    ),
    "python/repark/src/repark/spark/column.py": (
        1589,
        "Column expression methods remain on one facade class.",
        "Extract a cohesive method family behind re-export bindings.",
    ),
    "python/repark/src/repark/spark/dataframe/core.py": (
        6371,
        "The DataFrame facade still combines many plan-building method families.",
        "Extract one existing method region when a charter changes that responsibility.",
    ),
    "python/repark/src/repark/spark/dataframe/joins_columns.py": (
        1239,
        "Join and column-selection helpers share one facade region.",
        "Split join planning from column projection helpers.",
    ),
    "python/repark/src/repark/spark/dataframe/plan_collapse.py": (
        1168,
        "Plan-collapse transforms share one planner support module.",
        "Split transform families along their existing plan-node boundaries.",
    ),
    "python/repark/src/repark/spark/dataframe/writer_readwriter.py": (
        1113,
        "DataFrameWriter and DataFrameReader facade methods share one region.",
        "Split writer and reader bindings into separate cohesive modules.",
    ),
    "python/repark/src/repark/spark/functions.py": (
        1985,
        "Facade function exports and wrappers remain consolidated.",
        "Split by function family while preserving the public re-export surface.",
    ),
    "python/repark/src/repark/spark/functions_expr.py": (
        2265,
        "Expression-building function families share one module.",
        "Split string, collection, or predicate expression families.",
    ),
    "python/repark/src/repark/spark/functions_udf.py": (
        1300,
        "Python UDF and pandas UDF facade paths share one module.",
        "Split scalar UDF declarations from pandas UDF batch contracts.",
    ),
    "python/repark/src/repark/spark/ml/feature/_transformers.py": (
        2717,
        "ML feature transformer facades share one module.",
        "Split transformers by feature family with stable public re-exports.",
    ),
    "python/repark/src/repark/spark/session/reader.py": (
        1026,
        "DataFrameReader formats and option handling narrowly exceed the default.",
        "Split format-specific readers from shared option validation.",
    ),
    "python/repark/src/repark/spark/session/session_core.py": (
        2411,
        "SparkSession lifecycle and query entry points share one facade module.",
        "Split construction and configuration from query and catalog methods.",
    ),
    "python/repark/src/repark/spark/ta.py": (
        1818,
        "Technical-analysis facade wrappers share one generated-like public surface.",
        "Split wrappers by indicator family while preserving exports.",
    ),
    "python/repark/src/repark/spark/types.py": (
        1834,
        "Spark SQL type definitions and conversion helpers share one module.",
        "Split type declarations from parsing and conversion helpers.",
    ),
    "python/repark/tests/_live_parity.py": (
        1877,
        "Live-mirror declarations and oracle helpers share one test support module.",
        "Split registry declarations from execution and comparison helpers.",
    ),
    "python/repark/tests/test_display_styles.py": (
        1175,
        "Display-format scenarios share one test module.",
        "Split text, HTML, and truncation style families.",
    ),
    "python/repark/tests/test_dynamic_flatten.py": (
        1618,
        "Dynamic-flatten parity and refusal cases share one module.",
        "Split structural flattening from list and refusal scenarios.",
    ),
    "python/repark/tests/test_explode_rewrite.py": (
        1135,
        "Explode rewrite shapes share one test battery.",
        "Split scalar, nested, and multiple-generator scenarios.",
    ),
    "python/repark/tests/test_interchange_parity.py": (
        1533,
        "Dataframe-interchange parity scenarios share one module.",
        "Split protocol export from import and type-conversion cases.",
    ),
    "python/repark/tests/test_join_parity.py": (
        1232,
        "Join parity modes share one test module.",
        "Split equi-join, non-equi, and null-semantics scenarios.",
    ),
    "python/repark/tests/test_mapinarrow.py": (
        1578,
        "Arrow map-type behavior cases share one battery.",
        "Split construction, conversion, and nested-operation families.",
    ),
    "python/repark/tests/test_ml_boost_oracle.py": (
        2244,
        "Boosted-model oracle cases and fixtures share one module.",
        "Split estimator families while retaining the independent oracle boundary.",
    ),
    "python/repark/tests/test_pandas_udf.py": (
        1478,
        "Pandas UDF modes and failure cases share one module.",
        "Split scalar, grouped, and iterator UDF scenario families.",
    ),
    "python/repark/tests/test_partition_value_audit.py": (
        1665,
        "Partition-value audit cases share one broad parity matrix.",
        "Split partition transforms from temporal and type-conversion cases.",
    ),
    "python/repark/tests/test_session_timezone_parity.py": (
        1328,
        "Session-timezone parity cases share one module.",
        "Split casts from date functions and window behavior.",
    ),
    "python/repark/tests/test_ta.py": (
        1020,
        "Technical-analysis facade cases narrowly exceed the default.",
        "Split indicator families while keeping oracle comparisons.",
    ),
    "python/repark/tests/test_tpch_compare_unit.py": (
        1551,
        "TPC-H comparison-unit scenarios share one module.",
        "Split schema, row, and reporting comparison families.",
    ),
    "python/repark/tests/test_udf.py": (
        1170,
        "General UDF behavior and error cases share one module.",
        "Split scalar UDF execution from registration and refusal cases.",
    ),
    "python/repark/tests/test_window_parity.py": (
        1481,
        "Window parity frames and functions share one module.",
        "Split frame semantics from ranking and analytic function families.",
    ),
}

REEXPORT_MARK = "re-export binding"


def _is_exempt(path: Path, repo: Path) -> bool:
    """Return whether a source path is under an approved generated-test directory."""
    parts = path.relative_to(repo).parts
    return any(
        parts[index : index + len(exempt)] == exempt
        for exempt in EXEMPT_PATHS
        for index in range(len(parts) - len(exempt) + 1)
    )


def _validate_exception(relative: str, exception: tuple[int, str, str]) -> list[str]:
    """Validate one exception row as actionable debt above the default."""
    baseline, reason, split_seam = exception
    errors: list[str] = []
    if baseline <= DEFAULT_CEILING:
        errors.append(
            f"ERROR: {relative}: exception baseline {baseline} is not above default "
            f"{DEFAULT_CEILING}; remove the exception row."
        )
    if not reason.strip():
        errors.append(f"ERROR: {relative}: exception debt reason must not be empty.")
    if not split_seam.strip():
        errors.append(f"ERROR: {relative}: exception split seam must not be empty.")
    return errors


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
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        return [f"ERROR: {rel}: unreadable ({exc})"]
    line_count = len(text.splitlines())
    exception = EXCEPTIONS.get(rel)
    if exception is None and line_count > DEFAULT_CEILING:
        errors.append(
            f"ERROR: {rel} is {line_count} lines (default {DEFAULT_CEILING}). "
            "Sanctioned outs: (1) split at a cohesive boundary, or (2) add an "
            "owner-approved EXCEPTIONS row with the exact baseline, debt reason, and split seam."
        )
    elif exception is not None:
        baseline, reason, split_seam = exception
        errors.extend(_validate_exception(rel, exception))
        if line_count > baseline:
            errors.append(
                f"ERROR: {rel} grew to {line_count} lines (exact baseline {baseline}). "
                f"Debt: {reason} Split seam: {split_seam} "
                "Split the file, make the change line-neutral, or obtain explicit owner approval "
                "for a reviewed baseline amendment."
            )
        elif line_count < baseline:
            action = (
                "remove the exception row"
                if line_count <= DEFAULT_CEILING
                else f"ratchet the baseline down to {line_count}"
            )
            errors.append(
                f"ERROR: {rel} shrank to {line_count} lines below exact baseline {baseline}; "
                f"{action}."
            )

    facade_root = repo / FACADE_ROOT
    if not path.is_relative_to(facade_root) or path.name == "__init__.py":
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
    all_errors: list[str] = []
    roots = [repo / root for root in SCAN_ROOTS]
    for root in roots:
        if not root.is_dir():
            all_errors.append(f"ERROR: scan root not found: {root.relative_to(repo).as_posix()}")

    paths = sorted(
        path
        for root in roots
        if root.is_dir()
        for path in root.rglob("*.py")
        if path.is_file() and not _is_exempt(path, repo)
    )
    scanned = {path.relative_to(repo).as_posix() for path in paths}
    for rel in sorted(EXCEPTIONS):
        if rel not in scanned:
            all_errors.append(
                f"ERROR: EXCEPTIONS key is outside the scan set: {rel} "
                "(remove the row or restore the source path)"
            )

    if not paths:
        all_errors.append("ERROR: Python source scan set is empty — refuse to pass closed")

    checked = 0
    for path in paths:
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

    print(
        f"lib-py: {checked} files clean "
        f"(default ceiling {DEFAULT_CEILING}; {len(EXCEPTIONS)} exceptions; facade no-stub held)"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
