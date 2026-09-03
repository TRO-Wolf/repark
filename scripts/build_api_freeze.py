#!/usr/bin/env python3
"""Build and check the v1.0 frozen-API inventory from the review packet and the tree."""

from __future__ import annotations

import argparse
import ast
import importlib.util
import json
import re
import sys
from pathlib import Path
from types import ModuleType

PACKET_RELATIVE = "docs/design/v1-0-api-review-2026-09-02.json"
FREEZE_RELATIVE = "docs/design/v1-0-api-freeze.json"
GATE_RELATIVE = "scripts/check_example_coverage.py"
SURFACES_SOURCE = "crates/repark-common/src/surfaces.rs"
SPARK_MATRIX_SOURCE = "crates/repark-spark/src/matrix.rs"
ANSI_MATRIX_SOURCE = "crates/repark-sql/src/matrix.rs"
ERRORS_SOURCE = "python/repark/src/repark/errors.py"
CARDINALITY_SOURCE = "crates/repark-functions/src/cardinality.rs"
CATALOG_CONFIG_SOURCE = "crates/repark-core/src/catalog_config.rs"
SCAN_PRUNE_SOURCE = "crates/repark-iceberg/src/write/scan_prune.rs"
FILE_SCOPED_REWRITE_SOURCE = "crates/repark-iceberg/src/write/file_scoped_rewrite.rs"
ROOT_CARGO_SOURCE = "Cargo.toml"
FACADE_PYPROJECT_SOURCE = "python/repark/pyproject.toml"

FUNCTION_DEF_SOURCES: tuple[str, ...] = (
    "python/repark/src/repark/spark/functions.py",
    "python/repark/src/repark/spark/functions_agg.py",
    "python/repark/src/repark/spark/functions_bitwise.py",
    "python/repark/src/repark/spark/functions_collections.py",
    "python/repark/src/repark/spark/functions_datetime.py",
    "python/repark/src/repark/spark/functions_declared.py",
    "python/repark/src/repark/spark/functions_expr.py",
    "python/repark/src/repark/spark/functions_lambda.py",
    "python/repark/src/repark/spark/functions_math.py",
    "python/repark/src/repark/spark/functions_session.py",
    "python/repark/src/repark/spark/functions_try.py",
    "python/repark/src/repark/spark/functions_udf.py",
    "python/repark/src/repark/spark/functions_url.py",
    "python/repark/src/repark/spark/functions_window.py",
)

KIND_PYTHON = "python"
KIND_SURFACE = "surface"
KIND_CONF = "conf"
KIND_ERROR = "error"
KIND_PACKAGING = "packaging"
KIND_MIXED = "mixed"

DOOR_SPARK = "repark-spark"
DOOR_ANSI = "repark-sql"

MATRIX_SOURCES: dict[str, str] = {
    DOOR_SPARK: SPARK_MATRIX_SOURCE,
    DOOR_ANSI: ANSI_MATRIX_SOURCE,
}

DISPOSITION_TESTED = "tested"
DISPOSITION_ABSENT = "declared_absent"

K1_SURFACES: tuple[str, ...] = (
    "CTAS",
    "CTAS_TARGET_ROUTING",
    "CREATE_OR_REPLACE_TABLE",
    "CREATE_TABLE_COLUMN_DEF",
    "DROP_TABLE",
    "CREATE_SCHEMA",
    "DROP_SCHEMA",
    "ALTER_TABLE_RENAME",
    "ALTER_TABLE_SCHEMA_EVOLUTION",
    "ALTER_TABLE_PROPERTIES",
    "ALTER_TABLE_PARTITION_FIELDS",
    "TABLE_OPTION_FORMAT",
    "TABLE_OPTION_FORMAT_VERSION",
    "TABLE_OPTION_PARTITIONING",
    "TABLE_OPTION_LOCATION",
    "TABLE_OPTION_RAW_PROPERTIES",
    "TABLE_OPTION_SORT_ORDER",
    "TABLE_OPTION_UNKNOWN_KEY_REFUSE",
    "PARTITION_TRANSFORM_VALIDATION",
    "MOR_TABLE_CREATION",
    "SCHEMA_OPTION_LOCATION",
)

ROW_SURFACES: dict[str, tuple[str, ...]] = {
    "K1": K1_SURFACES,
    "K2": ("INSERT_INTO", "INSERT_OVERWRITE", "DELETE", "UPDATE", "MERGE", "TRUNCATE"),
    "K3": ("TIME_TRAVEL", "BRANCH_TAG_DDL", "METADATA_TABLES"),
    "K4": (
        "INTROSPECTION",
        "GUARD_MULTI_STATEMENT",
        "GUARD_READ_ONLY_CATALOG",
        "GUARD_LOCAL_FILESYSTEM",
        "GUARD_WRITE_TO_BRANCH",
        "GUARD_MOR_MULTI_SPEC_DML",
    ),
    "K5": ("MAINTENANCE_CALL",),
    "K7": (
        "SEMANTICS_NULL_ORDERING",
        "SEMANTICS_DECIMAL_ARITHMETIC",
        "SEMANTICS_CAST_MATRIX",
        "SEMANTICS_SESSION_TIMEZONE",
        "SEMANTICS_WINDOW_FRAMES",
        "SEMANTICS_JOIN_NULL_KEYS",
        "SEMANTICS_FLOAT_DETERMINISM",
    ),
}

UNPARTITIONED_SURFACES: tuple[str, ...] = (
    "SELECT_PASSTHROUGH",
    "WRONG_DOOR_SNIFF",
    "IDENTIFIER_CASE_FOLDING",
    "TA_FUNCTIONS",
    "SQL_DIALECT_SEAM",
    "CROSS_DOOR_EQUIVALENCE",
)

SPARK_DOOR_ROWS: frozenset[str] = frozenset({"K1", "K2", "K3", "K4", "K5"})

CONF_MEMBERS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    (
        "repark.sql.maxArrayElements",
        ('"repark.sql.maxArrayElements"', '"repark.sql.max_array_elements"'),
        CARDINALITY_SOURCE,
    ),
    (
        "repark.sql.allowLocalFilesystemDDL",
        ('"repark.sql.allowLocalFilesystemDDL"', '"repark.sql.allow_local_filesystem_ddl"'),
        CARDINALITY_SOURCE,
    ),
    (
        "repark.sql.allowCreateFormatVersion3",
        ('"repark.sql.allowCreateFormatVersion3"', '"repark.sql.allow_create_format_version_3"'),
        CARDINALITY_SOURCE,
    ),
    ("spark.sql.catalog.", ('"spark.sql.catalog."',), CATALOG_CONFIG_SOURCE),
    ("repark.sql.catalog.", ('"repark.sql.catalog."',), CATALOG_CONFIG_SOURCE),
    ("repark.merge.scan-pruning", ('"repark.merge.scan-pruning"',), SCAN_PRUNE_SOURCE),
    (
        "repark.merge.file-scoped-rewrite",
        ('"repark.merge.file-scoped-rewrite"', '"repark.merge.file_scoped_rewrite"'),
        FILE_SCOPED_REWRITE_SOURCE,
    ),
)

CONF_EXCEPTED_PREFIX = "repark.merge."

PACKAGING_MEMBERS: tuple[tuple[str, tuple[str, ...], str], ...] = (
    ("distribution name", ('name = "repark"',), FACADE_PYPROJECT_SOURCE),
    ("abi3 wheel tag", ('features = ["abi3-py312"]',), ROOT_CARGO_SOURCE),
    ("python floor", ('requires-python = ">=3.12"',), FACADE_PYPROJECT_SOURCE),
    ("extension module", ('module-name = "repark._native"',), FACADE_PYPROJECT_SOURCE),
    (
        "declared extras",
        ("[project.optional-dependencies]", "numpy = [", "pandas = [", "polars = [", "ml-ext = ["),
        FACADE_PYPROJECT_SOURCE,
    ),
    ("version SSOT", ('dynamic = ["version"]',), FACADE_PYPROJECT_SOURCE),
)

MIXED_ROW = "M1"
ERROR_ROW = "O1"
CONF_ROW = "L1"
PACKAGING_ROW = "N1"
ANSI_ROW = "K6"

POLICY = (
    "additive-only within a major; a breaking change to a frozen row needs a major version "
    "and a one-minor deprecation shim; unfrozen rows may change at any minor with a "
    "changelog line"
)
DECISION_DATE = "2026-09-02"

SURFACE_ID_PATTERN = re.compile(r"^    ([A-Z][A-Z0-9_]*);$", re.MULTILINE)
MATRIX_ROW_PATTERN = re.compile(r"surfaces::([A-Z][A-Z0-9_]*),\s*\n\s*(absent|t)\(")
EXCEPT_PATTERN = re.compile(r"`([^`]+)`")


def repo_root() -> Path:
    """Return the repository root that contains this script."""
    return Path(__file__).resolve().parent.parent


def load_gate(root: Path) -> ModuleType:
    """Load the example-coverage enumerator as a module."""
    spec = importlib.util.spec_from_file_location("check_example_coverage", root / GATE_RELATIVE)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"{GATE_RELATIVE}: not loadable under {root}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def source_paths(root: Path) -> tuple[str, ...]:
    """Return every repo-relative path the enumerators read."""
    gate = load_gate(root)
    paths: list[str] = [
        GATE_RELATIVE,
        PACKET_RELATIVE,
        SURFACES_SOURCE,
        SPARK_MATRIX_SOURCE,
        ANSI_MATRIX_SOURCE,
        ERRORS_SOURCE,
        CARDINALITY_SOURCE,
        CATALOG_CONFIG_SOURCE,
        SCAN_PRUNE_SOURCE,
        FILE_SCOPED_REWRITE_SOURCE,
        ROOT_CARGO_SOURCE,
        FACADE_PYPROJECT_SOURCE,
    ]
    paths.extend(FUNCTION_DEF_SOURCES)
    paths.extend(gate.FUNCTIONS_INSTALLER_SOURCES)
    paths.extend(relative for _family, _prefix, relative in gate.MODULE_SURFACES)
    paths.extend(relative for _family, _prefix, relative, _cls, _nested in gate.CLASS_SURFACES)
    return tuple(sorted(set(paths)))


def required_parameters(node: ast.FunctionDef | ast.AsyncFunctionDef) -> list[str]:
    """Return the parameter names a caller must supply positionally or by keyword."""
    args = node.args
    positional = list(args.posonlyargs) + list(args.args)
    if positional and positional[0].arg in {"self", "cls"}:
        positional = positional[1:]
    supplied = len(args.defaults)
    required = [item.arg for item in positional[: len(positional) - supplied]]
    for index, item in enumerate(args.kwonlyargs):
        if args.kw_defaults[index] is None:
            required.append(item.arg)
    return required


def module_function_defs(tree: ast.Module) -> dict[str, list[str]]:
    """Return required parameters for every top-level function in one module."""
    defs: dict[str, list[str]] = {}
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defs.setdefault(node.name, required_parameters(node))
    return defs


def class_body_signatures(class_node: ast.ClassDef) -> dict[str, list[str]]:
    """Return required parameters for the methods and simple aliases on one class."""
    defs: dict[str, list[str]] = {}
    for node in class_node.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            defs.setdefault(node.name, required_parameters(node))
    aliases: dict[str, list[str]] = {}
    for node in class_node.body:
        if not isinstance(node, ast.Assign) or not isinstance(node.value, ast.Name):
            continue
        target = node.value.id
        if target not in defs:
            continue
        for bound in node.targets:
            if isinstance(bound, ast.Name):
                aliases.setdefault(bound.id, defs[target])
    defs.update(aliases)
    return defs


def functions_signatures(root: Path, gate: ModuleType) -> dict[str, list[str]]:
    """Return required parameters for every ``F.*`` name that has a definition."""
    found: dict[str, list[str]] = {}
    for relative in FUNCTION_DEF_SOURCES:
        tree = gate.parse_source(root / relative)
        for name, params in module_function_defs(tree).items():
            found.setdefault(name, params)
    return found


def python_signatures(root: Path, gate: ModuleType) -> dict[str, list[str] | None]:
    """Return every enumerated public name mapped to its required parameters or None."""
    signatures: dict[str, list[str] | None] = {}
    for _family, name in gate.enumerate_public_surface(root):
        signatures[name] = None
    free_functions = functions_signatures(root, gate)
    for name, params in free_functions.items():
        key = f"F.{name}"
        if key in signatures:
            signatures[key] = params
    for _family, prefix, relative in gate.MODULE_SURFACES:
        tree = gate.parse_source(root / relative)
        for name, params in module_function_defs(tree).items():
            key = f"{prefix}.{name}"
            if key in signatures:
                signatures[key] = params
    for _family, prefix, relative, class_name, nested in gate.CLASS_SURFACES:
        tree = gate.parse_source(root / relative)
        owner = gate.class_def(tree, class_name, where=relative)
        target = (
            gate.nested_class_def(owner, nested, where=relative) if nested is not None else owner
        )
        for member, params in class_body_signatures(target).items():
            key = f"{prefix}.{member}"
            if key in signatures:
                signatures[key] = params
    return signatures


def surface_ids(root: Path) -> list[str]:
    """Return every surface id declared in the dialect-neutral registry."""
    text = (root / SURFACES_SOURCE).read_text(encoding="utf-8")
    names = SURFACE_ID_PATTERN.findall(text)
    if not names:
        raise RuntimeError(f"{SURFACES_SOURCE}: no surface ids parsed")
    return names


def door_dispositions(root: Path, door: str) -> dict[str, str]:
    """Return each surface id mapped to one door's Tested or DeliberatelyAbsent disposition."""
    text = (root / MATRIX_SOURCES[door]).read_text(encoding="utf-8")
    rows: dict[str, str] = {}
    for name, shorthand in MATRIX_ROW_PATTERN.findall(text):
        rows[name] = DISPOSITION_ABSENT if shorthand == "absent" else DISPOSITION_TESTED
    if not rows:
        raise RuntimeError(f"{MATRIX_SOURCES[door]}: no matrix rows parsed")
    return rows


def error_class_names(root: Path) -> list[str]:
    """Return the error taxonomy exported by the facade errors module."""
    tree = ast.parse((root / ERRORS_SOURCE).read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.Assign):
            continue
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == "__all__":
                return [
                    element.value
                    for element in getattr(node.value, "elts", [])
                    if isinstance(element, ast.Constant) and isinstance(element.value, str)
                ]
    raise RuntimeError(f"{ERRORS_SOURCE}: no __all__ found")


def literal_present(root: Path, relative: str, literal: str) -> bool:
    """Return True when a source file contains the literal text."""
    path = root / relative
    if not path.is_file():
        return False
    return literal in path.read_text(encoding="utf-8")


def excepted_members(recommend: str) -> list[str]:
    """Return the member spellings a `YES except` decision leaves pre-stable."""
    if not recommend.startswith("YES except"):
        return []
    return EXCEPT_PATTERN.findall(recommend)


def is_frozen(recommend: str) -> bool:
    """Return True when the decision freezes the surface."""
    return recommend.startswith("YES")


def python_row_members(
    row: dict, signatures: dict[str, list[str] | None]
) -> tuple[list[dict], list[str]]:
    """Return the frozen member records and the resolved exception names for one Python row."""
    names = row.get("member_names") or []
    excepted = excepted_members(row["recommend"])
    resolved: list[str] = []
    for spelling in excepted:
        resolved.extend(name for name in names if name == spelling or name.endswith(f".{spelling}"))
    members = [
        {"name": name, "required_params": signatures.get(name)}
        for name in names
        if name not in set(resolved)
    ]
    return members, sorted(set(resolved))


def surface_row_members(row_id: str, root: Path, ids: list[str]) -> list[dict]:
    """Return the frozen surface records for one SQL-door row."""
    door = DOOR_SPARK if row_id in SPARK_DOOR_ROWS else DOOR_ANSI
    names = ids if row_id == ANSI_ROW else list(ROW_SURFACES[row_id])
    dispositions = door_dispositions(root, door)
    return [
        {"name": name, "door": door, "disposition": dispositions[name]}
        for name in names
        if name in dispositions
    ]


def conf_row_members() -> list[dict]:
    """Return the frozen session and catalog configuration keys."""
    return [
        {"name": name, "literals": list(literals), "source": source}
        for name, literals, source in CONF_MEMBERS
        if not name.startswith(CONF_EXCEPTED_PREFIX)
    ]


def packaging_row_members() -> list[dict]:
    """Return the frozen packaging and module-layout facts."""
    return [
        {"name": name, "literals": list(literals), "source": source}
        for name, literals, source in PACKAGING_MEMBERS
    ]


def error_row_members(root: Path) -> list[dict]:
    """Return the frozen error-class names."""
    return [{"name": name, "source": ERRORS_SOURCE} for name in error_class_names(root)]


def row_kind(row_id: str) -> str:
    """Return the member kind one packet row carries."""
    if row_id in ROW_SURFACES or row_id == ANSI_ROW:
        return KIND_SURFACE
    if row_id == CONF_ROW:
        return KIND_CONF
    if row_id == PACKAGING_ROW:
        return KIND_PACKAGING
    if row_id == ERROR_ROW:
        return KIND_ERROR
    if row_id == MIXED_ROW:
        return KIND_MIXED
    return KIND_PYTHON


def build_row(
    row: dict, root: Path, signatures: dict[str, list[str] | None], ids: list[str]
) -> dict:
    """Return one freeze-inventory row from one packet row."""
    row_id = row["id"]
    kind = row_kind(row_id)
    frozen = is_frozen(row["recommend"])
    record = {
        "id": row_id,
        "surface": row["surface"],
        "kind": kind,
        "decision": row["recommend"],
        "frozen": frozen,
        "excepted": excepted_members(row["recommend"]),
        "members": [],
    }
    if not frozen:
        return record
    if kind == KIND_PYTHON:
        members, resolved = python_row_members(row, signatures)
        record["members"] = members
        record["excepted"] = resolved or record["excepted"]
    elif kind == KIND_SURFACE:
        excepted = set(excepted_members(row["recommend"]))
        record["members"] = [
            member
            for member in surface_row_members(row_id, root, ids)
            if member["name"] not in excepted
        ]
    elif kind == KIND_CONF:
        record["members"] = conf_row_members()
    elif kind == KIND_PACKAGING:
        record["members"] = packaging_row_members()
    elif kind == KIND_ERROR:
        record["members"] = error_row_members(root)
    else:
        raise RuntimeError(f"{row_id}: a frozen row of kind {kind} has no enumerator")
    return record


def build(root: Path) -> dict:
    """Return the freeze inventory built from the packet and the tree."""
    gate = load_gate(root)
    packet = json.loads((root / PACKET_RELATIVE).read_text(encoding="utf-8"))
    signatures = python_signatures(root, gate)
    ids = surface_ids(root)
    rows = [build_row(row, root, signatures, ids) for row in packet["rows"]]
    frozen_rows = [row for row in rows if row["frozen"]]
    return {
        "date": DECISION_DATE,
        "packet": PACKET_RELATIVE,
        "policy": POLICY,
        "counts": {
            "rows": len(rows),
            "frozen_rows": len(frozen_rows),
            "unfrozen_rows": len(rows) - len(frozen_rows),
            "frozen_names": sum(len(row["members"]) for row in frozen_rows),
        },
        "rows": rows,
    }


def partition_findings(ids: list[str]) -> list[str]:
    """Return findings when the SQL-door row partition no longer covers the registry."""
    assigned: list[str] = list(UNPARTITIONED_SURFACES)
    for names in ROW_SURFACES.values():
        assigned.extend(names)
    findings: list[str] = []
    duplicates = sorted({name for name in assigned if assigned.count(name) > 1})
    for name in duplicates:
        findings.append(f"surface `{name}` is claimed by more than one packet row")
    for name in sorted(set(assigned) - set(ids)):
        findings.append(f"surface `{name}` is claimed by a packet row but is not in the registry")
    for name in sorted(set(ids) - set(assigned)):
        findings.append(f"surface `{name}` is in the registry but no packet row claims it")
    return findings


def python_member_findings(row: dict, signatures: dict[str, list[str] | None]) -> list[str]:
    """Return findings for one frozen Python row."""
    findings: list[str] = []
    for member in row["members"]:
        name = member["name"]
        if name not in signatures:
            findings.append(f"{row['id']}: frozen name `{name}` is gone from the public surface")
            continue
        recorded = member["required_params"]
        if recorded is None:
            continue
        live = signatures[name]
        if live != recorded:
            findings.append(f"{row['id']}: `{name}` required parameters moved {recorded} -> {live}")
    return findings


def surface_member_findings(row: dict, root: Path, ids: list[str]) -> list[str]:
    """Return findings for one frozen SQL-door row."""
    findings: list[str] = []
    cache: dict[str, dict[str, str]] = {}
    for member in row["members"]:
        name = member["name"]
        door = member["door"]
        if name not in ids:
            findings.append(f"{row['id']}: frozen surface `{name}` is gone from the registry")
            continue
        if door not in cache:
            cache[door] = door_dispositions(root, door)
        live = cache[door].get(name)
        if live != member["disposition"]:
            findings.append(
                f"{row['id']}: `{name}` on {door} moved {member['disposition']} -> {live}"
            )
    return findings


def literal_member_findings(row: dict, root: Path) -> list[str]:
    """Return findings for one frozen configuration or packaging row."""
    findings: list[str] = []
    for member in row["members"]:
        source = member["source"]
        for literal in member["literals"]:
            if not literal_present(root, source, literal):
                findings.append(f"{row['id']}: frozen literal `{literal}` is gone from {source}")
    return findings


def error_member_findings(row: dict, root: Path) -> list[str]:
    """Return findings for the frozen error taxonomy."""
    live = set(error_class_names(root))
    return [
        f"{row['id']}: frozen error class `{member['name']}` is gone from {ERRORS_SOURCE}"
        for member in row["members"]
        if member["name"] not in live
    ]


def row_findings(
    row: dict, root: Path, signatures: dict[str, list[str] | None], ids: list[str]
) -> list[str]:
    """Return findings for one inventory row."""
    if not row["frozen"]:
        if row["members"]:
            return [f"{row['id']}: an unfrozen row must list no frozen members"]
        return []
    if row["kind"] == KIND_PYTHON:
        return python_member_findings(row, signatures)
    if row["kind"] == KIND_SURFACE:
        return surface_member_findings(row, root, ids)
    if row["kind"] == KIND_ERROR:
        return error_member_findings(row, root)
    if row["kind"] in {KIND_CONF, KIND_PACKAGING}:
        return literal_member_findings(row, root)
    return [f"{row['id']}: a frozen row of kind {row['kind']} has no enumerator"]


def decision_findings(root: Path, inventory: dict) -> list[str]:
    """Return findings when the inventory and the packet disagree on a decision."""
    packet = json.loads((root / PACKET_RELATIVE).read_text(encoding="utf-8"))
    decided = {row["id"]: row.get("decision") for row in packet["rows"]}
    recommended = {row["id"]: row["recommend"] for row in packet["rows"]}
    findings: list[str] = []
    for row in inventory["rows"]:
        row_id = row["id"]
        if recommended.get(row_id) != row["decision"]:
            findings.append(f"{row_id}: the inventory decision is not the packet recommendation")
        if decided.get(row_id) != row["decision"]:
            findings.append(f"{row_id}: the packet decision column is not the recorded decision")
    return findings


def findings(root: Path, inventory: dict) -> list[str]:
    """Return every way the tree has drifted from the frozen inventory."""
    gate = load_gate(root)
    signatures = python_signatures(root, gate)
    ids = surface_ids(root)
    found = partition_findings(ids)
    found.extend(decision_findings(root, inventory))
    for row in inventory["rows"]:
        found.extend(row_findings(row, root, signatures, ids))
    if inventory["policy"] != POLICY:
        found.append("the inventory policy sentence is not the owner's wording")
    return found


def load_inventory(root: Path) -> dict:
    """Return the checked-in freeze inventory."""
    return json.loads((root / FREEZE_RELATIVE).read_text(encoding="utf-8"))


def write_inventory(root: Path, inventory: dict) -> None:
    """Write the freeze inventory to its checked-in path."""
    path = root / FREEZE_RELATIVE
    path.write_text(json.dumps(inventory, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--write", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Write or check the frozen-API inventory."""
    args = parse_args(argv)
    root = repo_root()
    if args.write:
        write_inventory(root, build(root))
        print(f"api-freeze: wrote {FREEZE_RELATIVE}")
        return 0
    found = findings(root, load_inventory(root))
    for finding in found:
        print(f"api-freeze: {finding}")
    if found:
        print(f"api-freeze: {len(found)} finding(s)")
        return 1
    print("api-freeze: the frozen surface matches the tree")
    return 0


if __name__ == "__main__":
    sys.exit(main())
