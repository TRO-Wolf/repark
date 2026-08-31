#!/usr/bin/env python3
"""Enumerate the public surface and fail when an example is missing.

SSOT for the v0.7 example-drift gate. Prose points here and never restates the
backlog baseline. Walks facade sources by AST so ``make ci`` stays
native-build-free. When ``repark._native`` imports, every example script is
executed and ``F.__all__`` / ``ta.__all__`` are cross-checked against the walk.

pins: ex-0-example-drift-gate/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009
"""

from __future__ import annotations

import argparse
import ast
import importlib
import os
import subprocess
import sys
import tempfile
from pathlib import Path

INVENTORY_RELATIVE = "docs/examples/inventory.txt"
BACKLOG_RELATIVE = "docs/examples/backlog.txt"
EXCEPTIONS_RELATIVE = "docs/examples/exceptions.txt"
EXAMPLES_ROOT_RELATIVE = "docs/examples"
FUNCTIONS_SOURCE = "python/repark/src/repark/spark/functions.py"
TA_SOURCE = "python/repark/src/repark/spark/ta.py"
FAMILIES: tuple[str, ...] = ("dataframe", "functions", "io", "session", "ta")
BACKLOG_BASELINE = 658
EXAMPLE_TIMEOUT_SECONDS = 120
NATIVE_MODULE = "repark._native"
CLOUD_ENV_PREFIXES: tuple[str, ...] = ("AWS_",)

CLASS_SURFACES: tuple[tuple[str, str, str, str, str | None], ...] = (
    (
        "dataframe",
        "DataFrame",
        "python/repark/src/repark/spark/dataframe/core.py",
        "DataFrame",
        None,
    ),
    (
        "dataframe",
        "GroupedData",
        "python/repark/src/repark/spark/dataframe/joins_columns.py",
        "GroupedData",
        None,
    ),
    (
        "dataframe",
        "DataFrameNaFunctions",
        "python/repark/src/repark/spark/dataframe/actions_export.py",
        "DataFrameNaFunctions",
        None,
    ),
    (
        "dataframe",
        "DataFrameStatFunctions",
        "python/repark/src/repark/spark/dataframe/writer_readwriter.py",
        "DataFrameStatFunctions",
        None,
    ),
    (
        "io",
        "DataFrameReader",
        "python/repark/src/repark/spark/session/reader.py",
        "DataFrameReader",
        None,
    ),
    (
        "io",
        "DataFrameWriter",
        "python/repark/src/repark/spark/dataframe/writer_readwriter.py",
        "DataFrameWriter",
        None,
    ),
    (
        "io",
        "DataFrameWriterV2",
        "python/repark/src/repark/spark/dataframe/writer_readwriter.py",
        "DataFrameWriterV2",
        None,
    ),
    (
        "session",
        "SparkSession",
        "python/repark/src/repark/spark/session/session_core.py",
        "ReparkSession",
        None,
    ),
    (
        "session",
        "SparkSession.Builder",
        "python/repark/src/repark/spark/session/session_core.py",
        "ReparkSession",
        "Builder",
    ),
)


def repo_root() -> Path:
    """Return the repository root that contains this script."""
    return Path(__file__).resolve().parent.parent


def is_public_name(name: str) -> bool:
    """Return True when ``name`` is a public identifier (no leading underscore)."""
    return bool(name) and not name.startswith("_")


def string_list(value: ast.AST, *, where: str) -> list[str]:
    """Read a list or tuple of string constants from an AST value."""
    if not isinstance(value, (ast.List, ast.Tuple)):
        raise RuntimeError(f"{where}: expected a list or tuple of strings")
    names: list[str] = []
    for element in value.elts:
        if not isinstance(element, ast.Constant) or not isinstance(element.value, str):
            raise RuntimeError(f"{where}: every element must be a string constant")
        names.append(element.value)
    return names


def parse_source(path: Path) -> ast.Module:
    """Parse one UTF-8 Python file as an AST module."""
    return ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def dunder_all(tree: ast.Module, *, where: str) -> list[str]:
    """Return the module-level ``__all__`` list."""
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    return string_list(node.value, where=where)
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "__all__"
            and node.value is not None
        ):
            return string_list(node.value, where=where)
    raise RuntimeError(f"{where}: no module-level __all__")


def assignment_names(node: ast.AST) -> list[str]:
    """Return public names bound by an assignment statement."""
    names: list[str] = []
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and is_public_name(target.id):
                names.append(target.id)
    elif (
        isinstance(node, ast.AnnAssign)
        and isinstance(node.target, ast.Name)
        and is_public_name(node.target.id)
    ):
        names.append(node.target.id)
    return names


def class_def(tree: ast.Module, class_name: str, *, where: str) -> ast.ClassDef:
    """Return the named top-level class."""
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            return node
    raise RuntimeError(f"{where}: class {class_name} is missing")


def nested_class_def(owner: ast.ClassDef, nested_name: str, *, where: str) -> ast.ClassDef:
    """Return a class defined directly on ``owner``."""
    for node in owner.body:
        if isinstance(node, ast.ClassDef) and node.name == nested_name:
            return node
    raise RuntimeError(f"{where}: nested class {nested_name} is missing")


def public_class_members(class_node: ast.ClassDef) -> list[str]:
    """Return public method, property, and alias names on one class body."""
    names: list[str] = []
    seen: set[str] = set()
    for node in class_node.body:
        candidate: str | None = None
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and is_public_name(node.name):
            candidate = node.name
        else:
            bound = assignment_names(node)
            if bound:
                for name in bound:
                    if name not in seen:
                        seen.add(name)
                        names.append(name)
                continue
        if candidate is not None and candidate not in seen:
            seen.add(candidate)
            names.append(candidate)
    return names


def enumerate_public_surface(root: Path) -> list[tuple[str, str]]:
    """Walk the facade sources and return sorted ``(family, name)`` rows.

    Args:
        root: Repository root.

    Returns:
        Deterministic rows: family tag then the public name spelling.
    """
    rows: list[tuple[str, str]] = []
    functions_path = root / FUNCTIONS_SOURCE
    for function_name in dunder_all(
        parse_source(functions_path),
        where=FUNCTIONS_SOURCE,
    ):
        if not is_public_name(function_name):
            raise RuntimeError(f"{FUNCTIONS_SOURCE}: private name in __all__: {function_name}")
        rows.append(("functions", f"F.{function_name}"))
    ta_path = root / TA_SOURCE
    for kernel_name in dunder_all(parse_source(ta_path), where=TA_SOURCE):
        if not is_public_name(kernel_name):
            raise RuntimeError(f"{TA_SOURCE}: private name in __all__: {kernel_name}")
        rows.append(("ta", f"ta.{kernel_name}"))
    for family, prefix, relative, class_name, nested in CLASS_SURFACES:
        source_path = root / relative
        tree = parse_source(source_path)
        owner = class_def(tree, class_name, where=relative)
        target = nested_class_def(owner, nested, where=relative) if nested is not None else owner
        for member in public_class_members(target):
            rows.append((family, f"{prefix}.{member}"))
    rows.append(("session", "repark.sql"))
    rows.sort(key=lambda row: (row[0], row[1]))
    names = [name for _, name in rows]
    if len(names) != len(set(names)):
        duplicate = next(name for name in names if names.count(name) > 1)
        raise RuntimeError(f"duplicate public name in the inventory: {duplicate}")
    if not rows:
        raise RuntimeError("enumerator produced an empty inventory")
    return rows


def parse_named_lines(path: Path, *, kind: str) -> list[str]:
    """Read non-comment, non-blank lines from a checked-in list file."""
    if not path.is_file():
        raise RuntimeError(f"{kind} file is missing: {path}")
    names: list[str] = []
    for raw in path.read_text(encoding="utf-8").splitlines():
        stripped = raw.strip()
        if not stripped or stripped.startswith("#"):
            continue
        names.append(stripped)
    return names


def parse_inventory_file(path: Path) -> list[tuple[str, str]]:
    """Read the checked-in inventory snapshot (``family<TAB>name``)."""
    rows: list[tuple[str, str]] = []
    for line in parse_named_lines(path, kind="inventory"):
        if "\t" not in line:
            raise RuntimeError(f"inventory row is not family<TAB>name: {line}")
        family, name = line.split("\t", 1)
        if family not in FAMILIES:
            raise RuntimeError(f"inventory family {family!r} is not in {FAMILIES}")
        if not name:
            raise RuntimeError(f"inventory row has an empty name: {line}")
        rows.append((family, name))
    return rows


def parse_exceptions_file(path: Path) -> dict[str, str]:
    """Read ``name<TAB>reason`` exception rows."""
    mapping: dict[str, str] = {}
    for line in parse_named_lines(path, kind="exceptions"):
        if "\t" not in line:
            raise RuntimeError(f"exceptions row is not name<TAB>reason: {line}")
        name, reason = line.split("\t", 1)
        reason = reason.strip()
        if not name or not reason:
            raise RuntimeError(f"exceptions row needs a name and a reason: {line}")
        if name in mapping:
            raise RuntimeError(f"exceptions file repeats {name}")
        mapping[name] = reason
    return mapping


def example_scripts(root: Path) -> list[Path]:
    """Return every ``docs/examples/<family>/*.py`` script, sorted."""
    examples_root = root / EXAMPLES_ROOT_RELATIVE
    if not examples_root.is_dir():
        raise RuntimeError(f"examples root is missing: {examples_root}")
    scripts: list[Path] = []
    for family in FAMILIES:
        family_dir = examples_root / family
        if not family_dir.is_dir():
            continue
        for path in sorted(family_dir.glob("*.py")):
            if path.name.startswith("_"):
                continue
            scripts.append(path)
    return scripts


def covers_from_script(path: Path) -> list[str]:
    """Read the module-level ``COVERS: list[str]`` from one example script."""
    tree = parse_source(path)
    for node in tree.body:
        value: ast.AST | None = None
        if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            if node.target.id == "COVERS" and node.value is not None:
                value = node.value
        elif isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "COVERS":
                    value = node.value
        if value is None:
            continue
        names = string_list(value, where=str(path))
        if not names:
            raise RuntimeError(f"{path}: COVERS is empty")
        if len(names) != len(set(names)):
            raise RuntimeError(f"{path}: COVERS repeats a name")
        docstring = ast.get_docstring(tree)
        if not docstring or not docstring.strip():
            raise RuntimeError(f"{path}: module docstring is missing")
        return names
    raise RuntimeError(f"{path}: module-level COVERS list is missing")


def coverage_findings(
    enumerated: set[str],
    covered: set[str],
    backlog: set[str],
    exceptions: set[str],
    backlog_baseline: int,
) -> list[str]:
    """Return one finding string per coverage or ratchet violation.

    pins: ex-0-example-drift-gate/C-003, C-004, C-005
    """
    findings: list[str] = []
    unknown_covered = sorted(covered - enumerated)
    for name in unknown_covered:
        findings.append(f"COVERS names {name}, which is not in the inventory")
    unknown_exceptions = sorted(exceptions - enumerated)
    for name in unknown_exceptions:
        findings.append(f"exceptions names {name}, which is not in the inventory")
    unknown_backlog = sorted(backlog - enumerated)
    for name in unknown_backlog:
        findings.append(f"backlog names {name}, which is not in the inventory")
    covered_in_backlog = sorted(backlog & covered)
    for name in covered_in_backlog:
        findings.append(f"backlog still lists {name}, which an example now covers")
    excepted_in_backlog = sorted(backlog & exceptions)
    for name in excepted_in_backlog:
        findings.append(f"backlog still lists {name}, which is an exception")
    uncovered = enumerated - covered - backlog - exceptions
    for name in sorted(uncovered):
        findings.append(
            f"public name {name} has no example COVERS row and is not in the backlog or exceptions"
        )
    if len(backlog) != backlog_baseline:
        findings.append(
            f"backlog count is {len(backlog)}, baseline is {backlog_baseline} "
            "(ratchet down only; set BACKLOG_BASELINE to the new lower count)"
        )
    return findings


def inventory_findings(
    enumerated: list[tuple[str, str]],
    snapshot: list[tuple[str, str]],
) -> list[str]:
    """Return findings when the checked-in inventory snapshot disagrees."""
    if enumerated == snapshot:
        return []
    enumerated_set = set(enumerated)
    snapshot_set = set(snapshot)
    findings: list[str] = []
    for row in sorted(enumerated_set - snapshot_set):
        findings.append(f"inventory snapshot is missing {row[0]}\t{row[1]}")
    for row in sorted(snapshot_set - enumerated_set):
        findings.append(f"inventory snapshot has stale {row[0]}\t{row[1]}")
    if not findings:
        findings.append("inventory snapshot order disagrees with the enumerator")
    return findings


def native_module_importable() -> bool:
    """Return True when the compiled native module imports in this interpreter."""
    try:
        importlib.import_module(NATIVE_MODULE)
    except Exception:
        return False
    return True


def live_all_names(module_path: str) -> set[str]:
    """Import ``module_path`` and return its ``__all__`` as a set of strings."""
    module = importlib.import_module(module_path)
    exported = getattr(module, "__all__", None)
    if not isinstance(exported, (list, tuple)):
        raise RuntimeError(f"{module_path}.__all__ is missing")
    names: set[str] = set()
    for item in exported:
        if not isinstance(item, str):
            raise RuntimeError(f"{module_path}.__all__ contains a non-string")
        names.add(item)
    return names


def live_all_findings(enumerated: list[tuple[str, str]]) -> list[str]:
    """Cross-check AST ``F.*`` / ``ta.*`` names against a live import."""
    functions_live = {f"F.{name}" for name in live_all_names("repark.spark.functions")}
    ta_live = {f"ta.{name}" for name in live_all_names("repark.spark.ta")}
    functions_ast = {name for family, name in enumerated if family == "functions"}
    ta_ast = {name for family, name in enumerated if family == "ta"}
    findings: list[str] = []
    for name in sorted(functions_live - functions_ast):
        findings.append(f"live F.__all__ has {name} missing from the AST walk")
    for name in sorted(functions_ast - functions_live):
        findings.append(f"AST walk has {name} missing from live F.__all__")
    for name in sorted(ta_live - ta_ast):
        findings.append(f"live ta.__all__ has {name} missing from the AST walk")
    for name in sorted(ta_ast - ta_live):
        findings.append(f"AST walk has {name} missing from live ta.__all__")
    return findings


def execution_environment() -> dict[str, str]:
    """Copy the process environment without cloud credential keys."""
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(CLOUD_ENV_PREFIXES):
            del env[key]
    return env


def execute_examples(scripts: list[Path]) -> list[str]:
    """Run every example script; return findings for nonzero or crashed runs."""
    findings: list[str] = []
    env = execution_environment()
    for script in scripts:
        with tempfile.TemporaryDirectory(prefix="repark-example-") as temporary:
            completed = subprocess.run(
                [sys.executable, str(script)],
                cwd=temporary,
                env=env,
                check=False,
                capture_output=True,
                text=True,
                timeout=EXAMPLE_TIMEOUT_SECONDS,
            )
            if completed.returncode != 0:
                detail = (completed.stderr or completed.stdout or "").strip()
                if len(detail) > 500:
                    detail = detail[-500:]
                findings.append(
                    f"example {script.as_posix()} exited {completed.returncode}: {detail}"
                )
    return findings


def write_inventory(path: Path, rows: list[tuple[str, str]]) -> None:
    """Write the inventory snapshot in sorted ``family<TAB>name`` form."""
    path.parent.mkdir(parents=True, exist_ok=True)
    body = "\n".join(f"{family}\t{name}" for family, name in rows)
    path.write_text(body + "\n", encoding="utf-8")


def report(label: str, findings: list[str]) -> None:
    """Print a labelled finding block to stderr."""
    if not findings:
        return
    sys.stderr.write(f"{label}: {len(findings)} finding(s)\n")
    for item in findings:
        sys.stderr.write(f"  {item}\n")


def run_gate(
    root: Path,
    *,
    write_snapshot: bool,
    skip_execute: bool,
    require_execute: bool,
    backlog_baseline: int,
) -> int:
    """Run the drift gate over ``root``. Returns the process exit code."""
    try:
        enumerated = enumerate_public_surface(root)
        inventory_path = root / INVENTORY_RELATIVE
        if write_snapshot:
            write_inventory(inventory_path, enumerated)
        snapshot = parse_inventory_file(inventory_path)
        snapshot_findings = inventory_findings(enumerated, snapshot)
        scripts = example_scripts(root)
        covered: set[str] = set()
        for script in scripts:
            for name in covers_from_script(script):
                covered.add(name)
        backlog = set(parse_named_lines(root / BACKLOG_RELATIVE, kind="backlog"))
        exceptions = set(parse_exceptions_file(root / EXCEPTIONS_RELATIVE).keys())
        findings = coverage_findings(
            {name for _, name in enumerated},
            covered,
            backlog,
            exceptions,
            backlog_baseline,
        )
        live_findings: list[str] = []
        execute_findings: list[str] = []
        native = native_module_importable()
        if native:
            live_findings = live_all_findings(enumerated)
            if not skip_execute:
                execute_findings = execute_examples(scripts)
        elif require_execute:
            findings.append(
                "native module is not importable; --require-execute cannot run examples"
            )
        elif not skip_execute:
            sys.stderr.write(
                "example-coverage: skipping example execution (native module is not importable)\n"
            )
    except RuntimeError as error:
        sys.stderr.write(f"example-coverage: {error}\n")
        return 2
    report("example-coverage inventory", snapshot_findings)
    report("example-coverage", findings)
    report("example-coverage live __all__", live_findings)
    report("example-coverage execute", execute_findings)
    if snapshot_findings or findings or live_findings or execute_findings:
        return 1
    family_counts: dict[str, int] = dict.fromkeys(FAMILIES, 0)
    for family, _name in enumerated:
        family_counts[family] += 1
    summary = ", ".join(f"{family}={family_counts[family]}" for family in FAMILIES)
    sys.stderr.write(
        f"example-coverage: {len(enumerated)} public names ({summary}); "
        f"{len(covered)} covered; {len(backlog)} backlog; "
        f"{len(exceptions)} exceptions; {len(scripts)} examples\n"
    )
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line flags for the gate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--write-inventory",
        action="store_true",
        help="rewrite docs/examples/inventory.txt from the AST walk",
    )
    parser.add_argument(
        "--skip-execute",
        action="store_true",
        help="never run example scripts",
    )
    parser.add_argument(
        "--require-execute",
        action="store_true",
        help="fail when the native module is not importable",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point. Exit 0 clean, 1 findings, 2 usage or environment error."""
    args = parse_args(argv)
    if args.skip_execute and args.require_execute:
        sys.stderr.write("example-coverage: --skip-execute conflicts with --require-execute\n")
        return 2
    return run_gate(
        repo_root(),
        write_snapshot=args.write_inventory,
        skip_execute=args.skip_execute,
        require_execute=args.require_execute,
        backlog_baseline=BACKLOG_BASELINE,
    )


if __name__ == "__main__":
    sys.exit(main())
