#!/usr/bin/env python3
"""Enumerate the public surface and fail when an example is missing.

SSOT for the v0.7 example-drift gate. Prose points here and never restates the
baselines. Walks facade sources by AST so ``make ci`` stays native-build-free.
``F.*`` is the union of ``functions.py`` ``__all__``, the installer export
tables that ``install_into`` appends at import, and public defs on that
canonical module.
When ``repark._native`` imports, every example script is executed and
``F.__all__`` / ``ta.__all__`` are cross-checked against the walk.

Closed EX-0 set (roadmap colon list plus session): ``F.*``, DataFrame /
GroupedData / DataFrameNaFunctions / DataFrameStatFunctions public members,
TA ``__all__``, DataFrameReader / DataFrameWriter / DataFrameWriterV2 public
members, ``repark.sql``, and SparkSession / SparkSession.Builder public
members. Names starting with ``_`` and every dunder are skipped
(``DataFrame.__getitem__`` is excluded by that dunder rule). Not in this
inventory: Column (40), Window (14), WindowSpec (8), Catalog (28), Row (4
public), types.__all__ (28), ml.__all__ (28), RuntimeConfig (5), SparkContext
(3), UDFRegistration (3), StorageLevel (0 public). Widening is an owner
decision (on the order of 120+ names). Measured 2026-08-31.

A ``COVERS`` entry must be used in that script's body. Class-surface names
bind only on a repark-rooted local (assignment dataflow from a door or session
builder). Module covers such as ``repark.sql`` bind only on the module alias.
A repark-rooted receiver can still list a method it calls only trivially —
review holds that honesty. ``exceptions.txt`` has the same exact-count ratchet
as the backlog.

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
FUNCTIONS_INSTALLER_SOURCES: tuple[str, ...] = (
    "python/repark/src/repark/spark/functions_try.py",
    "python/repark/src/repark/spark/functions_lambda.py",
    "python/repark/src/repark/spark/functions_declared.py",
)
FUNCTION_EXPORT_BINDINGS: frozenset[str] = frozenset(
    {
        "TRY_EXPORTS",
        "HIGHER_ORDER_EXPORTS",
        "SKETCH_NAMES",
        "CSV_XML_XPATH_NAMES",
        "VARIANT_NAMES",
        "GEOSPATIAL_NAMES",
    }
)
FUNCTION_EXPORT_DICT_KEYS: frozenset[str] = frozenset({"FNP15_MESSAGES"})
FAMILIES: tuple[str, ...] = ("dataframe", "functions", "io", "session", "ta")
BACKLOG_BASELINE = 742
EXCEPTIONS_BASELINE = 2
EXAMPLE_TIMEOUT_SECONDS = 120
NATIVE_MODULE = "repark._native"
CLOUD_ENV_PREFIXES: tuple[str, ...] = ("AWS_",)
FUNCTIONS_MODULES: frozenset[str] = frozenset({"repark.functions", "repark.spark.functions"})
TA_MODULES: frozenset[str] = frozenset({"repark.spark.ta", "repark.ta"})
FUNCTIONS_ALIAS_HINTS: frozenset[str] = frozenset({"F", "functions"})
TA_ALIAS_HINTS: frozenset[str] = frozenset({"ta"})
SESSION_BUILDER_HINTS: frozenset[str] = frozenset(
    {"SparkSession", "ReparkSession", "ReParkSession"}
)
KIND_FUNCTIONS = "functions"
KIND_TA = "ta"
KIND_REPARK = "repark"
KIND_SESSION = "session"
KIND_LOCAL = "local"
KIND_OTHER = "other"
REPARK_ROOTED_KINDS: frozenset[str] = frozenset(
    {KIND_FUNCTIONS, KIND_TA, KIND_REPARK, KIND_SESSION, KIND_LOCAL}
)
CHILD_ENV_DROP: frozenset[str] = frozenset({"PYTHONPATH", "PYTHONSTARTUP", "PYTHONHOME"})

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


def assigned_value(node: ast.AST) -> tuple[str | None, ast.AST | None]:
    """Return ``(name, value)`` for a module-level assignment, if any."""
    if isinstance(node, ast.Assign) and len(node.targets) == 1:
        target = node.targets[0]
        if isinstance(target, ast.Name):
            return target.id, node.value
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return node.target.id, node.value
    return None, None


def dict_string_keys(value: ast.AST, *, where: str) -> list[str]:
    """Read string keys from a dict literal."""
    if not isinstance(value, ast.Dict):
        raise RuntimeError(f"{where}: expected a dict literal")
    keys: list[str] = []
    for key in value.keys:
        if key is None or not isinstance(key, ast.Constant) or not isinstance(key.value, str):
            raise RuntimeError(f"{where}: every dict key must be a string constant")
        keys.append(key.value)
    return keys


def public_module_defs(tree: ast.Module) -> list[str]:
    """Return public FunctionDef and ClassDef names at module level."""
    names: list[str] = []
    for node in tree.body:
        if isinstance(
            node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)
        ) and is_public_name(node.name):
            names.append(node.name)
    return names


def collect_function_export_names(root: Path) -> list[str]:
    """Return the F.* public names: ``__all__`` plus installer exports plus defs.

    ``functions.py`` ``__all__`` is mutated at import by ``install_into`` from
    the try / lambda / declared-absent modules. The AST reads those export
    tables as well as the static ``__all__`` literal and public defs on the
    canonical module.

    pins: ex-0-example-drift-gate/C-001
    """
    names: set[str] = set()
    functions_tree = parse_source(root / FUNCTIONS_SOURCE)
    names.update(dunder_all(functions_tree, where=FUNCTIONS_SOURCE))
    names.update(public_module_defs(functions_tree))
    for relative in FUNCTIONS_INSTALLER_SOURCES:
        tree = parse_source(root / relative)
        for node in tree.body:
            bound, value = assigned_value(node)
            if bound is None or value is None:
                continue
            if bound in FUNCTION_EXPORT_BINDINGS:
                names.update(string_list(value, where=f"{relative}:{bound}"))
            elif bound in FUNCTION_EXPORT_DICT_KEYS:
                names.update(dict_string_keys(value, where=f"{relative}:{bound}"))
    return sorted(names)


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
    for function_name in collect_function_export_names(root):
        if not is_public_name(function_name):
            raise RuntimeError(f"private F.* export name: {function_name}")
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


def is_covers_assignment(node: ast.AST) -> bool:
    """Return True when ``node`` is the module-level ``COVERS`` binding."""
    if isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
        return node.target.id == "COVERS"
    if isinstance(node, ast.Assign):
        return any(
            isinstance(target, ast.Name) and target.id == "COVERS" for target in node.targets
        )
    return False


def expression_root_id(node: ast.AST) -> str | None:
    """Return the leftmost Name id of an attribute or call chain, if any."""
    current: ast.AST = node
    while True:
        if isinstance(current, ast.Attribute):
            current = current.value
        elif isinstance(current, ast.Call):
            current = current.func
        elif isinstance(current, ast.Name):
            return current.id
        else:
            return None


def door_aliases(tree: ast.Module) -> tuple[dict[str, set[str]], dict[str, set[str] | None]]:
    """Return module aliases and imported call names per door.

    The second map uses ``None`` for a star-import (every name on that door).
    """
    aliases: dict[str, set[str]] = {
        "functions": set(FUNCTIONS_ALIAS_HINTS),
        "ta": set(TA_ALIAS_HINTS),
        "repark": {"repark"},
    }
    imported: dict[str, set[str] | None] = {
        "functions": set(),
        "ta": set(),
        "repark": set(),
    }
    for node in tree.body:
        if isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.rsplit(".", 1)[-1]
                if alias.name in FUNCTIONS_MODULES:
                    aliases["functions"].add(bound)
                if alias.name in TA_MODULES:
                    aliases["ta"].add(bound)
                if alias.name == "repark":
                    aliases["repark"].add(bound)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            for alias in node.names:
                bound = alias.asname or alias.name
                if module in FUNCTIONS_MODULES:
                    if alias.name == "*":
                        imported["functions"] = None
                    elif imported["functions"] is not None:
                        imported["functions"].add(bound)
                elif module in TA_MODULES:
                    if alias.name == "*":
                        imported["ta"] = None
                    elif imported["ta"] is not None:
                        imported["ta"].add(bound)
                elif module in {"repark.spark", "repark"} and alias.name == "functions":
                    aliases["functions"].add(bound)
                elif module in {"repark.spark", "repark"} and alias.name == "ta":
                    aliases["ta"].add(bound)
                elif module == "repark" and alias.name == "sql" and imported["repark"] is not None:
                    imported["repark"].add(bound)
    return aliases, imported


def classify_expression(node: ast.AST, kinds: dict[str, str]) -> str:
    """Return the kind of an expression from its leftmost Name."""
    root = expression_root_id(node)
    if root is None:
        return KIND_OTHER
    return kinds.get(root, KIND_OTHER)


def record_assignment(target: ast.AST, value: ast.AST, kinds: dict[str, str]) -> None:
    """Classify a local from a simple assignment's right-hand side."""
    if not isinstance(target, ast.Name):
        return
    kind = classify_expression(value, kinds)
    kinds[target.id] = KIND_LOCAL if kind in REPARK_ROOTED_KINDS else KIND_OTHER


def walk_statements(statements: list[ast.stmt], kinds: dict[str, str]) -> None:
    """Walk statements in source order and classify simple assignments."""
    for node in statements:
        if is_covers_assignment(node):
            continue
        if isinstance(node, ast.Assign):
            for target in node.targets:
                record_assignment(target, node.value, kinds)
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            record_assignment(node.target, node.value, kinds)
        elif isinstance(node, ast.FunctionDef):
            walk_statements(list(node.body), kinds)
        elif isinstance(node, ast.If):
            walk_statements(list(node.body) + list(node.orelse), kinds)
        elif isinstance(node, ast.Try):
            nested: list[ast.stmt] = list(node.body)
            for handler in node.handlers:
                nested.extend(handler.body)
            nested.extend(node.orelse)
            nested.extend(node.finalbody)
            walk_statements(nested, kinds)
        elif isinstance(node, ast.With):
            walk_statements(list(node.body), kinds)
        elif isinstance(node, (ast.For, ast.While)):
            walk_statements(list(node.body) + list(node.orelse), kinds)


def name_kinds(
    tree: ast.Module,
) -> tuple[dict[str, str], dict[str, set[str]], dict[str, set[str] | None]]:
    """Return name kinds, door aliases, and imported call names."""
    aliases, imported = door_aliases(tree)
    kinds: dict[str, str] = {}
    for name in aliases["functions"]:
        kinds[name] = KIND_FUNCTIONS
    for name in aliases["ta"]:
        kinds[name] = KIND_TA
    for name in aliases["repark"]:
        kinds[name] = KIND_REPARK
    for node in tree.body:
        if isinstance(node, ast.ImportFrom):
            for alias in node.names:
                bound = alias.asname or alias.name
                if alias.name in SESSION_BUILDER_HINTS or bound in SESSION_BUILDER_HINTS:
                    kinds[bound] = KIND_SESSION
        elif isinstance(node, ast.Import):
            for alias in node.names:
                bound = alias.asname or alias.name.rsplit(".", 1)[-1]
                if bound in SESSION_BUILDER_HINTS:
                    kinds[bound] = KIND_SESSION
    walk_statements(list(tree.body), kinds)
    return kinds, aliases, imported


def class_cover_is_used(cover: str, receiver_kind: str) -> bool:
    """Return True when a class-surface cover matches this receiver kind."""
    if cover.startswith("SparkSession.Builder."):
        return receiver_kind in {KIND_SESSION, KIND_LOCAL}
    if cover == "SparkSession.builder":
        return receiver_kind == KIND_SESSION
    if cover.startswith("SparkSession."):
        return receiver_kind == KIND_LOCAL
    return receiver_kind == KIND_LOCAL


def cover_is_used(cover: str, tree: ast.Module) -> bool:
    """Return True when ``cover`` is referenced in the script body.

    Family-aware. ``F.*`` / ``ta.*`` bind on their door alias or an imported
    call. ``repark.sql`` binds on the ``repark`` module alias only.
    Class-surface names bind when the Attribute receiver's root is a
    repark-rooted local (assignment dataflow). Session builder names
    (``SparkSession.builder``, ``SparkSession.Builder.*``) also bind on the
    session-builder class root. ``object().agg`` and ``repark.sql`` do not
    bind ``DataFrame.agg`` or ``SparkSession.sql``.

    pins: ex-0-example-drift-gate/C-002
    """
    local = cover.rsplit(".", 1)[-1]
    kinds, _aliases, imported = name_kinds(tree)
    for node in tree.body:
        if is_covers_assignment(node):
            continue
        for child in ast.walk(node):
            if isinstance(child, ast.Attribute) and child.attr == local:
                receiver_kind = classify_expression(child.value, kinds)
                if cover.startswith("F."):
                    if receiver_kind == KIND_FUNCTIONS:
                        return True
                    continue
                if cover.startswith("ta."):
                    if receiver_kind == KIND_TA:
                        return True
                    continue
                if cover == "repark.sql":
                    if receiver_kind == KIND_REPARK:
                        return True
                    continue
                if class_cover_is_used(cover, receiver_kind):
                    return True
                continue
            if (
                isinstance(child, ast.Call)
                and isinstance(child.func, ast.Name)
                and child.func.id == local
            ):
                if cover.startswith("F."):
                    imported_functions = imported["functions"]
                    if imported_functions is None or local in imported_functions:
                        return True
                elif cover.startswith("ta."):
                    imported_ta = imported["ta"]
                    if imported_ta is None or local in imported_ta:
                        return True
                elif cover == "repark.sql":
                    imported_sql = imported["repark"]
                    if imported_sql is not None and local in imported_sql:
                        return True
    return False


def unused_cover_names(path: Path) -> list[str]:
    """Return COVERS entries that the script body never uses."""
    tree = parse_source(path)
    return [name for name in covers_from_script(path) if not cover_is_used(name, tree)]


def coverage_findings(
    enumerated: set[str],
    covered: set[str],
    backlog: set[str],
    exceptions: set[str],
    backlog_baseline: int,
    exceptions_baseline: int,
) -> list[str]:
    """Return one finding string per coverage or ratchet violation.

    pins: ex-0-example-drift-gate/C-003, C-004, C-005, C-006
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
    covered_in_exceptions = sorted(exceptions & covered)
    for name in covered_in_exceptions:
        findings.append(f"exceptions still lists {name}, which an example now covers")
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
    if len(exceptions) != exceptions_baseline:
        findings.append(
            f"exceptions count is {len(exceptions)}, baseline is {exceptions_baseline} "
            "(additions must bump EXCEPTIONS_BASELINE in the same commit)"
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
    """Copy the process environment without cloud keys or Python path overrides."""
    env = dict(os.environ)
    for key in list(env):
        if key.startswith(CLOUD_ENV_PREFIXES) or key in CHILD_ENV_DROP:
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
        unused_findings: list[str] = []
        for script in scripts:
            try:
                relative = script.relative_to(root).as_posix()
            except ValueError:
                relative = script.as_posix()
            for name in covers_from_script(script):
                covered.add(name)
            for name in unused_cover_names(script):
                unused_findings.append(
                    f"{relative}: COVERS names {name} which the script body never uses"
                )
        backlog = set(parse_named_lines(root / BACKLOG_RELATIVE, kind="backlog"))
        exceptions = set(parse_exceptions_file(root / EXCEPTIONS_RELATIVE).keys())
        findings = coverage_findings(
            {name for _, name in enumerated},
            covered,
            backlog,
            exceptions,
            backlog_baseline,
            EXCEPTIONS_BASELINE,
        )
        findings.extend(unused_findings)
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
