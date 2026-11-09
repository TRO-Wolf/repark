from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import re
import sys
import tempfile
from collections import Counter
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

ANSWERED = "ANSWERED"
REFUSED = "REFUSED"
PANIC = "PANIC"
SETUP_REFUSED = "SETUP-REFUSED"
NOT_EXPRESSIBLE = "NOT-EXPRESSIBLE"
SPARK_CATALOG = "probe_ice"
REPARK_CATALOG = "ice"
NAMESPACE = "ns"
KINDS = ("v2-cow", "v2-mor", "v3")
TABLE_MODES = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
WRITE_KEYS = ("write.delete.mode", "write.update.mode", "write.merge.mode")
SESSION_ZONE = "America/New_York"
NATIVE_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
STATIC_ERRORS = ("ParseException", "AnalysisException", "UnsupportedOperationException")
LINEAGE_COLUMN = "_last_updated_sequence_number"
EMPTY_PLAN_MARK = "LocalRelation <empty>"
ALL_SEATS = ("select", "delete", "update", "merge")
DML_SEATS = ("delete", "update", "merge")
FINDING_LABELS = {
    "WRONG-WRITE",
    "EXTRA-COMMIT",
    "FEWER-COMMIT",
    "LINEAGE-DIFFER",
    "SEAT-INCONSISTENT",
    "CONJUNCT-DROPPED",
}
COLUMNS = (
    "id BIGINT NOT NULL, i INT, l BIGINT, dec DECIMAL(10,2), d DOUBLE, s STRING, b BOOLEAN, "
    "dt DATE, ts TIMESTAMP, ntz TIMESTAMP_NTZ, st STRUCT<x: INT, y: STRING>, arr ARRAY<INT>, "
    "m MAP<STRING, INT>, flag STRING"
)
NATIVE_COLUMNS = COLUMNS.replace(" m MAP<STRING, INT>,", "")
SPARK_SEED = (
    "(1, 1, 1, CAST('1.10' AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE), 'a', true, DATE '2026-01-01', "
    "TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP_NTZ '2026-01-01 00:00:00', "
    "named_struct('x', 1, 'y', 'a'), array(1, 2), map('k', 1), 'f1')",
    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'f2')",
    "(3, NULL, NULL, NULL, CAST('NaN' AS DOUBLE), '', NULL, NULL, NULL, NULL, "
    "named_struct('x', 3, 'y', CAST(NULL AS STRING)), CAST(array() AS ARRAY<INT>), "
    "CAST(map() AS MAP<STRING, INT>), 'f3')",
    "(4, NULL, NULL, NULL, CAST('-0.0' AS DOUBLE), 'a ', false, NULL, NULL, NULL, NULL, "
    "array(CAST(NULL AS INT)), NULL, 'f4')",
    "(5, 2147483647, 9223372036854775807, CAST('99999999.99' AS DECIMAL(10,2)), "
    "CAST('Infinity' AS DOUBLE), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'f5')",
    "(6, -1, NULL, NULL, CAST('0.0' AS DOUBLE), 'A', NULL, NULL, NULL, NULL, NULL, array(2, 1), "
    "NULL, 'f6')",
    "(7, NULL, NULL, NULL, NULL, 'é', NULL, DATE '1970-01-01', TIMESTAMP '2026-03-08 02:30:00', "
    "TIMESTAMP_NTZ '2026-03-08 02:30:00', NULL, NULL, NULL, 'f7')",
    "(8, 1, 1, CAST('1.10' AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE), 'a', true, DATE '2026-01-01', "
    "TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP_NTZ '2026-01-01 00:00:00', "
    "named_struct('x', 1, 'y', 'a'), array(1, 2), map('k', 1), 'f8')",
)
NATIVE_SEED = (
    "(1, 1, 1, CAST('1.10' AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE), 'a', true, DATE '2026-01-01', "
    "TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP_NTZ '2026-01-01 00:00:00', "
    "named_struct('x', 1, 'y', 'a'), ARRAY[1, 2], 'f1')",
    "(2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'f2')",
    "(3, NULL, NULL, NULL, CAST('NaN' AS DOUBLE), '', NULL, NULL, NULL, NULL, "
    "named_struct('x', 3, 'y', CAST(NULL AS STRING)), CAST(ARRAY[] AS ARRAY<INT>), 'f3')",
    "(4, NULL, NULL, NULL, CAST('-0.0' AS DOUBLE), 'a ', false, NULL, NULL, NULL, NULL, "
    "ARRAY[CAST(NULL AS INT)], 'f4')",
    "(5, 2147483647, 9223372036854775807, CAST('99999999.99' AS DECIMAL(10,2)), "
    "CAST('Infinity' AS DOUBLE), NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'f5')",
    "(6, -1, NULL, NULL, CAST('0.0' AS DOUBLE), 'A', NULL, NULL, NULL, NULL, NULL, ARRAY[2, 1], "
    "'f6')",
    "(7, NULL, NULL, NULL, NULL, 'é', NULL, DATE '1970-01-01', TIMESTAMP '2026-03-08 02:30:00', "
    "TIMESTAMP_NTZ '2026-03-08 02:30:00', NULL, NULL, 'f7')",
    "(8, 1, 1, CAST('1.10' AS DECIMAL(10,2)), CAST('1.5' AS DOUBLE), 'a', true, DATE '2026-01-01', "
    "TIMESTAMP '2026-01-01 00:00:00', TIMESTAMP_NTZ '2026-01-01 00:00:00', "
    "named_struct('x', 1, 'y', 'a'), ARRAY[1, 2], 'f8')",
)
CONJUNCT_ATOMS = (
    ("x1", "st.y IS NULL", ()),
    ("x2", "st.x = 1", ()),
    ("x3", "m['k'] = 1", ()),
    ("x4", "CASE WHEN i = 1 THEN true END", ()),
    ("x5", "element_at(arr, 1) = 1", ("arr[1] = 1",)),
    ("x6", "coalesce(i, 0) = 0", ()),
)
CONJUNCT_FORMS = (
    ("f1", "id = 1 AND {x}"),
    ("f2", "{x} AND id = 1"),
    ("f3", "id = 1 OR {x}"),
    ("f4", "NOT ({x}) AND id = 1"),
    ("f5", "id IN (1, 2) AND {x}"),
    ("f6", "({x}) = true"),
)
PREDICATES = tuple(
    (
        f"{atom_id}{form_id}",
        template.format(x=atom),
        tuple(template.format(x=alternative) for alternative in alternatives),
    )
    for atom_id, atom, alternatives in CONJUNCT_ATOMS
    for form_id, template in CONJUNCT_FORMS
)
PREDICATE_TEXT = {pid: text for pid, text, _ in PREDICATES}
SZ_FORM = {
    "c1": "d = 0.0",
    "c2": "d = -0.0",
    "c3": "d <=> 0.0",
    "c4": "d IN (0.0)",
    "c5": "d <> 0.0",
    "c6": "d < 0.0",
    "c7": "d <= 0.0",
}
SZ_NATIVE_ALT = {"c3": "d IS NOT DISTINCT FROM 0.0"}
SZ_VARIANTS = ("t", "tf", "u", "uf")
SZ_PAIRS = ("t+u", "tf+uf")
SZ_SINGLE_OPS = ("grp", "dst", "ord", "cst", "mxmn", "win")
SZ_PAIR_OPS = ("jn", "isc", "exc")
SZ_DML = (("m1", ("t+u", "tf+uf")), ("m2", ("t", "tf")), ("m3", ("t", "tf")))
SZ_SQL_DOORS = ("spark-sql", "native")
SZ_DF_DOORS = ("filter-str", "f-expr")
SZ_FINDING_EXCLUDE = {"EQUAL", "BOTH-REFUSED", "ORACLE-MISSING", "SETUP-REFUSED"}


def sanitize(text: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", text.lower())


def scrub(text: str, warehouse_root: str) -> str:
    text = text.replace(warehouse_root, "<warehouse>")
    temporary = re.escape(tempfile.gettempdir())
    text = re.sub(temporary + r"/[^\s'\"),\]\\]*", "<tmp>", text)
    home = re.escape(str(Path.home()))
    return re.sub(home + r"(?:/[^\s'\"),\]\\]*)?", "<home>", text)


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def excerpt(text: str, limit: int = 300) -> str:
    if len(text) <= limit:
        return text
    head = text[:limit]
    boundary = head.rfind(" ")
    return head[:boundary] if boundary > 0 else head


def norm(value: Any) -> Any:
    if isinstance(value, float):
        if value != value:
            return "NaN"
        if value in (float("inf"), float("-inf")):
            return "Infinity" if value > 0 else "-Infinity"
        return "-0.0" if str(value) == "-0.0" else repr(value)
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): norm(item) for key, item in sorted(value.items())}
    if isinstance(value, (list, tuple)):
        items = [norm(item) for item in value]
        if value and all(isinstance(item, tuple) and len(item) == 2 for item in value):
            return sorted(items, key=lambda pair: json.dumps(pair, sort_keys=True, default=str))
        return items
    return value


def normalize_rows(rows: list[dict[str, Any]]) -> list[list[Any]]:
    return [[norm(value) for value in row.values()] for row in rows]


def row_digest(row: dict[str, Any], skip: tuple[str, ...]) -> str:
    body = {key: norm(item) for key, item in sorted(row.items()) if key not in skip}
    text = json.dumps(body, sort_keys=True, default=str)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def error_record(ctx: dict[str, Any], error: BaseException, stage: str) -> dict[str, Any]:
    text = scrub(str(error), ctx["scrub"])
    panic = type(error).__name__ == "PanicException" or "panicked at" in text
    return {
        "class": type(error).__name__,
        "stage": stage,
        "message": excerpt(text),
        "panic": panic,
    }


def attempt(ctx: dict[str, Any], call: Any, stage: str) -> tuple[Any, dict[str, Any] | None]:
    try:
        return call(), None
    except KeyboardInterrupt:
        raise
    except BaseException as error:
        return None, error_record(ctx, error, stage)


def outcome_of(error: dict[str, Any] | None) -> str:
    if error is None:
        return ANSWERED
    return PANIC if error["panic"] else REFUSED


def to_arrow(frame: Any) -> Any:
    return frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()


def execute_sql(door: dict[str, Any], text: str) -> list[dict[str, Any]]:
    return to_arrow(door["sql"](text)).to_pylist()


def run_statement(
    ctx: dict[str, Any], door: dict[str, Any], text: str, stage: str
) -> tuple[Any, dict[str, Any] | None]:
    return attempt(ctx, lambda: execute_sql(door, text), stage)


def metadata_order(path: Path) -> tuple[int, str]:
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def read_metadata(table_dir: Path) -> dict[str, Any]:
    candidates = sorted((table_dir / "metadata").glob("*.metadata.json"), key=metadata_order)
    if not candidates:
        return {"format_version": None, "snapshot_count": 0, "current_snapshot_id": None}
    document = json.loads(candidates[-1].read_text(encoding="utf-8"))
    properties = document.get("properties") or {}
    return {
        "format_version": document.get("format-version"),
        "snapshot_count": len(document.get("snapshots") or []),
        "current_snapshot_id": document.get("current-snapshot-id"),
        "write_properties": {key: properties[key] for key in WRITE_KEYS if key in properties},
    }


def props_text(kind: str) -> str:
    version, mode = TABLE_MODES[kind]
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )


def table_columns(door_name: str, part: str) -> str:
    if part in ("u", "uf"):
        columns = "id BIGINT, d DOUBLE"
    else:
        columns = NATIVE_COLUMNS if door_name == "native" else COLUMNS
    if part in ("tf", "uf"):
        columns = columns.replace("d DOUBLE", "d FLOAT")
    return columns


def seed_values(door_name: str, part: str) -> str:
    if part in ("u", "uf"):
        values = "(10, CAST('0.0' AS DOUBLE)), (11, CAST('-0.0' AS DOUBLE))"
    else:
        values = ", ".join(NATIVE_SEED if door_name == "native" else SPARK_SEED)
    if part in ("tf", "uf"):
        values = values.replace("AS DOUBLE)", "AS FLOAT)")
    return values


def create_sql(door_name: str, qualified: str, columns: str, kind: str) -> str:
    if door_name == "native":
        return f"CREATE TABLE {qualified} ({columns})"
    return f"CREATE TABLE {qualified} ({columns}) USING iceberg TBLPROPERTIES ({props_text(kind)})"


def seed_sql(door_name: str, qualified: str, part: str) -> str:
    return f"INSERT INTO {qualified} VALUES {seed_values(door_name, part)}"


def observe(
    ctx: dict[str, Any], door: dict[str, Any], qualified: str, table_dir: Path, lineage: bool
) -> dict[str, Any]:
    state = read_metadata(table_dir)
    rows, error = attempt(
        ctx, lambda: execute_sql(door, f"SELECT * FROM {qualified} ORDER BY id"), "observe"
    )
    state["error"] = error
    state["ids"] = None
    state["rows"] = None
    state["lineage"] = None
    if rows is not None:
        state["ids"] = sorted(row["id"] for row in rows)
        state["rows"] = [[row["id"], row_digest(row, ("flag", "id")), row["flag"]] for row in rows]
    if lineage and rows is not None:
        found, lineage_error = attempt(
            ctx,
            lambda: execute_sql(
                door, f"SELECT id, {LINEAGE_COLUMN} AS seq FROM {qualified} ORDER BY id"
            ),
            "lineage",
        )
        if lineage_error is None:
            state["lineage"] = [[row["id"], row["seq"]] for row in found]
    return state


def snapshot_of(state: dict[str, Any]) -> dict[str, Any]:
    return {"count": state["snapshot_count"], "current": state["current_snapshot_id"]}


def derive_ids(seat: str, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"ids": None, "lost": [], "damaged": []}
    if before["rows"] is None or after["rows"] is None:
        return out
    before_map = {row[0]: row for row in before["rows"]}
    after_rows: dict[Any, list[Any]] = {}
    for row in after["rows"]:
        after_rows.setdefault(row[0], []).append(row)
    lost = sorted(set(before_map) - set(after_rows))
    out["lost"] = lost
    changed: list[Any] = []
    damaged: list[Any] = []
    for ident, rows in sorted(after_rows.items()):
        if ident not in before_map:
            damaged.append(ident)
            continue
        old = before_map[ident]
        for new in rows:
            if new[1] != old[1]:
                damaged.append(ident)
            if new[2] != old[2]:
                changed.append(ident)
                if new[2] != "z":
                    damaged.append(ident)
    out["damaged"] = sorted(set(damaged))
    if seat in ("update", "merge"):
        out["ids"] = sorted(changed)
    elif seat == "delete":
        out["ids"] = lost
    return out


def lineage_advanced(before: dict[str, Any], after: dict[str, Any]) -> list[Any] | None:
    if before["lineage"] is None or after["lineage"] is None:
        return None
    old = dict(before["lineage"])
    return sorted(ident for ident, seq in after["lineage"] if ident in old and old[ident] != seq)


def plan_is_empty(door: dict[str, Any], text: str) -> bool | None:
    try:
        plan = door["session"].sql(text)._jdf.queryExecution().optimizedPlan().toString()
    except Exception:
        return None
    return EMPTY_PLAN_MARK in plan


def finish_setup(record: dict[str, Any], error: dict[str, Any]) -> dict[str, Any]:
    record["outcome"] = SETUP_REFUSED
    record["error"] = error
    return record


def write_line(handle: Any, record: dict[str, Any], warehouse_root: str) -> None:
    text = json.dumps(record, sort_keys=True, default=str)
    handle.write(scrub(text, warehouse_root) + "\n")


def conjunct_statement(seat: str, source: str, predicate: str) -> str:
    if seat == "select":
        return f"SELECT id FROM {source} WHERE {predicate}"
    if seat == "delete":
        return f"DELETE FROM {source} WHERE {predicate}"
    if seat == "update":
        return f"UPDATE {source} SET flag = 'z' WHERE {predicate}"
    return (
        f"MERGE INTO {source} AS t USING (SELECT id AS sid FROM {source}) s ON t.id = s.sid "
        f"WHEN MATCHED AND {predicate} THEN UPDATE SET flag = 'z'"
    )


def native_candidates(pid: str) -> list[str]:
    for key, text, alternatives in PREDICATES:
        if key == pid:
            return [*alternatives, text]
    return []


def resolve_native_spellings(ctx: dict[str, Any], door: dict[str, Any]) -> dict[str, Any]:
    qualified = f"{door['catalog']}.{NAMESPACE}.spelling_probe"
    _, error = run_statement(
        ctx, door, create_sql("native", qualified, NATIVE_COLUMNS, "v2-cow"), "create"
    )
    if error is None:
        _, error = run_statement(ctx, door, seed_sql("native", qualified, "t"), "seed")
    resolved: dict[str, Any] = {}
    for pid, _, _ in PREDICATES:
        attempts: list[dict[str, Any]] = []
        chosen = None
        for candidate in native_candidates(pid):
            _, probe_error = run_statement(
                ctx, door, f"SELECT id FROM {qualified} WHERE {candidate}", "spelling"
            )
            attempts.append({"spelling": candidate, "error": probe_error})
            if probe_error is None or probe_error["class"] not in STATIC_ERRORS:
                chosen = candidate
                break
        resolved[pid] = (chosen, attempts)
    return resolved


def conjunct_spec(door: str, seat: str, kind: str, pid: str) -> dict[str, Any]:
    return {
        "door": door,
        "seat": seat,
        "kind": kind,
        "pid": pid,
        "table": sanitize(f"cj_{door}_{seat}_{kind}_{pid}"),
        "key": f"{door}|{seat}|{kind}|{pid}",
    }


def conjunct_plan(engine: str, only: list[str] | None) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for seat in ALL_SEATS:
        kinds = ("v2-cow",) if seat == "select" else KINDS
        for kind in kinds:
            specs.extend(conjunct_spec("spark-sql", seat, kind, pid) for pid, _, _ in PREDICATES)
    if engine == "repark":
        for seat in ALL_SEATS:
            specs.extend(conjunct_spec("native", seat, "v2-cow", pid) for pid, _, _ in PREDICATES)
    return filter_only(specs, only)


def sz_family(oid: str) -> str:
    if oid in ("m1", "m2", "m3"):
        return "dml"
    if oid == "ord":
        return "order"
    return "read"


def sz_spec(door: str, oid: str, variant: str, kind: str) -> dict[str, Any]:
    return {
        "door": door,
        "oid": oid,
        "variant": variant,
        "kind": kind,
        "family": sz_family(oid),
        "key": f"{door}|{oid}|{variant}|{kind}",
    }


def signed_zero_plan(engine: str, only: list[str] | None) -> list[dict[str, Any]]:
    doors = ["spark-sql", "filter-str", "f-expr"]
    if engine == "repark":
        doors.append("native")
    specs: list[dict[str, Any]] = []
    for door in doors:
        for variant in SZ_VARIANTS:
            specs.extend(sz_spec(door, oid, variant, "v2-cow") for oid in SZ_FORM)
    for door in SZ_SQL_DOORS:
        if door not in doors:
            continue
        for variant in SZ_VARIANTS:
            specs.extend(sz_spec(door, oid, variant, "v2-cow") for oid in SZ_SINGLE_OPS)
        for variant in SZ_PAIRS:
            specs.extend(sz_spec(door, oid, variant, "v2-cow") for oid in SZ_PAIR_OPS)
    for kind in KINDS:
        for oid, variants in SZ_DML:
            specs.extend(sz_spec("spark-sql", oid, variant, kind) for variant in variants)
    return filter_only(specs, only)


def filter_only(specs: list[dict[str, Any]], only: list[str] | None) -> list[dict[str, Any]]:
    if not only:
        return specs
    return [spec for spec in specs if spec_matches(spec, only)]


def spec_matches(spec: dict[str, Any], only: list[str]) -> bool:
    values = {
        str(spec[name])
        for name in ("door", "seat", "oid", "kind", "pid", "variant")
        if name in spec
    }
    for token in only:
        if ":" in token:
            door, step = token.split(":", 1)
            if spec["door"] == door and step in (spec.get("seat"), spec.get("oid")):
                return True
        elif token in values:
            return True
    return False


def base_record(ctx: dict[str, Any], spec: dict[str, Any], qualified: str) -> dict[str, Any]:
    return {
        "record": "cell",
        "engine": ctx["engine"],
        "leg": ctx["label"],
        "table": "conjunct",
        "key": spec["key"],
        "door": spec["door"],
        "seat": spec["seat"],
        "kind": spec["kind"],
        "pid": spec["pid"],
        "predicate": PREDICATE_TEXT[spec["pid"]],
        "spelling": None,
        "table_name": qualified,
        "outcome": None,
        "error": None,
        "statement": None,
        "setup": None,
        "pre_select": None,
        "plan_empty": None,
        "ids": None,
        "rows_before": None,
        "rows_after": None,
        "lost": None,
        "damaged": None,
        "snapshot_before": None,
        "snapshot_after": None,
        "lineage_before": None,
        "lineage_after": None,
        "lineage_advanced": None,
        "observe_error": None,
    }


def spelling_for(ctx: dict[str, Any], spec: dict[str, Any]) -> tuple[str | None, Any]:
    if spec["door"] != "native":
        return PREDICATE_TEXT[spec["pid"]], None
    return ctx["spellings"][spec["pid"]]


def run_conjunct_cell(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    door = ctx["doors"][spec["door"]]
    qualified = f"{door['catalog']}.{NAMESPACE}.{spec['table']}"
    record = base_record(ctx, spec, qualified)
    text, attempts = spelling_for(ctx, spec)
    record["spelling"] = text
    if text is None:
        record["outcome"] = NOT_EXPRESSIBLE
        record["setup"] = {"spelling_attempts": attempts}
        return record
    table_dir = door["root"] / NAMESPACE / spec["table"]
    lineage = spec["kind"] == "v3"
    ddl = create_sql(door["name"], qualified, table_columns(door["name"], "t"), spec["kind"])
    seed = seed_sql(door["name"], qualified, "t")
    _, error = run_statement(ctx, door, ddl, "create")
    if error is None:
        _, error = run_statement(ctx, door, seed, "seed")
    if error is not None:
        record["setup"] = {"ddl": ddl, "seed": seed}
        return finish_setup(record, error)
    before = observe(ctx, door, qualified, table_dir, lineage)
    pre_text = conjunct_statement("select", qualified, text)
    pre_rows, pre_error = run_statement(ctx, door, pre_text, "pre-select")
    record["pre_select"] = {
        "outcome": outcome_of(pre_error),
        "error": pre_error,
        "ids": None if pre_error else sorted(row["id"] for row in pre_rows),
    }
    if ctx["engine"] == "spark" and spec["door"] == "spark-sql":
        record["plan_empty"] = plan_is_empty(door, pre_text)
    statement = conjunct_statement(spec["seat"], qualified, text)
    record["statement"] = statement
    rows, error = run_statement(ctx, door, statement, "sql")
    result_ids = (
        sorted(row["id"] for row in rows) if error is None and spec["seat"] == "select" else None
    )
    after = observe(ctx, door, qualified, table_dir, lineage)
    record.update(
        outcome=outcome_of(error),
        error=error,
        snapshot_before=snapshot_of(before),
        snapshot_after=snapshot_of(after),
        rows_before=before["rows"],
        rows_after=after["rows"],
        lineage_before=before["lineage"],
        lineage_after=after["lineage"],
        lineage_advanced=lineage_advanced(before, after) if lineage else None,
        observe_error=before["error"] or after["error"],
    )
    if spec["seat"] == "select":
        record["ids"] = result_ids
        record["damaged"] = []
        record["lost"] = [] if after["rows"] == before["rows"] else ["source-changed"]
    else:
        derived = derive_ids(spec["seat"], before, after)
        record.update(ids=derived["ids"], lost=derived["lost"], damaged=derived["damaged"])
    return record


def sz_record(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "record": "cell",
        "engine": ctx["engine"],
        "leg": ctx["label"],
        "table": "signed-zero",
        "key": spec["key"],
        "door": spec["door"],
        "op": spec["oid"],
        "family": spec["family"],
        "variant": spec["variant"],
        "kind": spec["kind"],
        "predicate": SZ_FORM.get(spec["oid"]),
        "statement": None,
        "attempts": None,
        "outcome": None,
        "error": None,
        "rows": None,
        "setup": None,
    }


def sz_table(catalog: str, part: str) -> str:
    return f"{catalog}.{NAMESPACE}.sz_{part}"


def sz_sql_candidates(oid: str, variant: str, catalog: str) -> list[str]:
    parts = variant.split("+")
    first = sz_table(catalog, parts[0])
    second = sz_table(catalog, parts[-1])
    ids = "4, 6" if parts[0] in ("t", "tf") else "10, 11"
    if oid in SZ_FORM:
        primary = f"SELECT id FROM {first} WHERE {SZ_FORM[oid]}"
        if oid in SZ_NATIVE_ALT:
            return [f"SELECT id FROM {first} WHERE {SZ_NATIVE_ALT[oid]}", primary]
        return [primary]
    templates = {
        "grp": f"SELECT d, count(*) AS c FROM {first} GROUP BY d",
        "dst": f"SELECT DISTINCT d FROM {first}",
        "ord": f"SELECT id, d FROM {first} ORDER BY d",
        "cst": f"SELECT id, CAST(d AS STRING) AS s FROM {first} ORDER BY id",
        "mxmn": f"SELECT max(d) AS mx, min(d) AS mn FROM {first} WHERE id IN ({ids})",
        "win": f"SELECT id, count(*) OVER (PARTITION BY d) AS c FROM {first} ORDER BY id",
        "jn": f"SELECT t.id AS tid, u.id AS uid FROM {first} AS t JOIN {second} AS u ON t.d = u.d",
        "isc": f"SELECT d FROM {first} INTERSECT SELECT d FROM {second}",
        "exc": f"SELECT d FROM {first} EXCEPT SELECT d FROM {second}",
    }
    if oid == "cst":
        return [f"SELECT id, CAST(d AS VARCHAR) AS s FROM {first} ORDER BY id", templates[oid]]
    return [templates[oid]]


def sz_dml_statement(oid: str, names: list[str]) -> str:
    target, source = names[0], names[-1]
    if oid == "m1":
        return (
            f"MERGE INTO {target} AS t USING {source} AS u ON t.d = u.d "
            "WHEN MATCHED THEN UPDATE SET flag = 'z'"
        )
    if oid == "m2":
        return f"DELETE FROM {target} WHERE d = 0.0"
    return f"UPDATE {target} SET flag = 'z' WHERE d = 0.0"


def first_setup_error(ctx: dict[str, Any], door: dict[str, Any], variant: str) -> Any:
    for part in variant.split("+"):
        error = ctx["setup"].get((door["name"], part))
        if error is not None:
            return error
    return None


def first_answer(
    ctx: dict[str, Any], door: dict[str, Any], candidates: list[str]
) -> tuple[str, Any, dict[str, Any] | None, list[dict[str, Any]]]:
    attempts: list[dict[str, Any]] = []
    for statement in candidates:
        rows, error = run_statement(ctx, door, statement, "read")
        attempts.append({"statement": statement, "error": error})
        if error is None or door["name"] != "native" or error["class"] not in STATIC_ERRORS:
            return statement, rows, error, attempts
    return candidates[-1], None, attempts[-1]["error"], attempts


def df_ids(door: dict[str, Any], door_name: str, table: str, text: str) -> list[dict[str, Any]]:
    frame = door["session"].table(table)
    argument = text if door_name == "filter-str" else door["functions"].expr(text)
    return to_arrow(frame.filter(argument).select("id")).to_pylist()


def run_sz_frame(
    ctx: dict[str, Any], spec: dict[str, Any], door: dict[str, Any], record: dict[str, Any]
) -> dict[str, Any]:
    table = sz_table(door["catalog"], spec["variant"])
    text = SZ_FORM[spec["oid"]]
    rows, error = attempt(ctx, lambda: df_ids(door, spec["door"], table, text), "read")
    record.update(
        statement=f"table({table}).filter({text!r}).select('id')",
        outcome=outcome_of(error),
        error=error,
        rows=normalize_rows(rows) if error is None else None,
    )
    return record


def run_sz_dml(ctx: dict[str, Any], spec: dict[str, Any], record: dict[str, Any]) -> dict[str, Any]:
    door = ctx["doors"]["spark-sql"]
    parts = spec["variant"].split("+")
    stem = f"szd_{spec['kind']}_{spec['oid']}"
    names = [f"{door['catalog']}.{NAMESPACE}.{sanitize(stem + '_' + part)}" for part in parts]
    for name, part in zip(names, parts, strict=True):
        ddl = create_sql(door["name"], name, table_columns(door["name"], part), spec["kind"])
        _, error = run_statement(ctx, door, ddl, "create")
        if error is None:
            _, error = run_statement(ctx, door, seed_sql(door["name"], name, part), "seed")
        if error is not None:
            return finish_setup(record, error)
    statement = sz_dml_statement(spec["oid"], names)
    _, error = run_statement(ctx, door, statement, "dml")
    post, post_error = run_statement(
        ctx, door, f"SELECT id, d, flag FROM {names[0]} ORDER BY id", "post"
    )
    record.update(
        statement=statement,
        outcome=outcome_of(error),
        error=error,
        rows=normalize_rows(post) if post_error is None else None,
        post_error=post_error,
    )
    return record


def run_sz_cell(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    record = sz_record(ctx, spec)
    if spec["family"] == "dml":
        return run_sz_dml(ctx, spec, record)
    door = ctx["doors"][spec["door"]]
    setup_error = first_setup_error(ctx, door, spec["variant"])
    if setup_error is not None:
        return finish_setup(record, setup_error)
    if spec["door"] in SZ_DF_DOORS:
        return run_sz_frame(ctx, spec, door, record)
    candidates = sz_sql_candidates(spec["oid"], spec["variant"], door["catalog"])
    if door["name"] != "native":
        candidates = candidates[-1:]
    statement, rows, error, attempts = first_answer(ctx, door, candidates)
    record.update(
        statement=statement,
        attempts=attempts if len(candidates) > 1 else None,
        outcome=outcome_of(error),
        error=error,
        rows=normalize_rows(rows) if error is None else None,
    )
    return record


def setup_read_tables(ctx: dict[str, Any], door: dict[str, Any]) -> None:
    setup_log: dict[str, Any] = ctx["header"].setdefault("sz_setup", {})
    for part in SZ_VARIANTS:
        name = sz_table(door["catalog"], part)
        ddl = create_sql(door["name"], name, table_columns(door["name"], part), "v2-cow")
        _, error = run_statement(ctx, door, ddl, "create")
        if error is None:
            _, error = run_statement(ctx, door, seed_sql(door["name"], name, part), "seed")
        ctx["setup"][(door["name"], part)] = error
        setup_log[f"{door['name']}|{part}"] = error


def load_cells(path: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    header: dict[str, Any] = {}
    cells: dict[str, dict[str, Any]] = {}
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record["record"] == "leg":
            header = record
        else:
            cells[record["key"]] = record
    return header, cells


def load_many(paths: list[str]) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
    headers: list[dict[str, Any]] = []
    merged: dict[str, dict[str, Any]] = {}
    for path in paths:
        header, cells = load_cells(path)
        headers.append(header)
        merged.update(cells)
    return headers, merged


def touched_ids(record: dict[str, Any]) -> set[Any]:
    return set(record["ids"] or []) | set(record["lost"] or []) | set(record["damaged"] or [])


def outside_own_select(record: dict[str, Any]) -> list[Any] | None:
    if record["seat"] not in DML_SEATS or record["outcome"] != ANSWERED:
        return None
    pre = record["pre_select"]
    if pre is None or pre["outcome"] != ANSWERED:
        return None
    return sorted(touched_ids(record) - set(pre["ids"]))


def conjunct_dropped(record: dict[str, Any]) -> bool:
    extra = outside_own_select(record)
    return bool(extra)


def spark_labels(record: dict[str, Any], oracle: dict[str, Any] | None) -> list[str]:
    if record["outcome"] == PANIC:
        return [PANIC]
    if record["outcome"] == SETUP_REFUSED:
        return [SETUP_REFUSED]
    if oracle is None or oracle["outcome"] in (SETUP_REFUSED, NOT_EXPRESSIBLE):
        return ["ORACLE-MISSING"]
    mine, theirs = record["outcome"], oracle["outcome"]
    if mine == REFUSED and theirs == REFUSED:
        return ["BOTH-REFUSED"]
    if mine == ANSWERED and theirs == REFUSED:
        return ["ACCEPTED-WHERE-SPARK-REFUSED"]
    if mine == REFUSED and theirs == ANSWERED:
        return ["REFUSED-WHERE-SPARK-ACCEPTED"]
    labels: list[str] = []
    if record["seat"] == "select":
        if record["ids"] != oracle["ids"]:
            labels.append("WRONG-READ")
    else:
        own = record["pre_select"]["ids"] if record["pre_select"]["outcome"] == ANSWERED else None
        differs = (
            record["ids"] != oracle["ids"]
            or record["damaged"] != oracle["damaged"]
            or record["lost"] != oracle["lost"]
        )
        if differs or (own is not None and own != record["ids"]):
            labels.append("WRONG-WRITE")
    if not labels and record["lineage_advanced"] != oracle["lineage_advanced"]:
        labels.append("LINEAGE-DIFFER")
    if snapshot_delta(record) > snapshot_delta(oracle) or (
        record["snapshot_after"] is not None
        and record["seat"] == "select"
        and snapshot_delta(record) != 0
    ):
        labels.append("EXTRA-COMMIT")
    if snapshot_delta(record) < snapshot_delta(oracle):
        labels.append("FEWER-COMMIT")
    return labels or ["EQUAL"]


def snapshot_delta(record: dict[str, Any]) -> int:
    before, after = record["snapshot_before"], record["snapshot_after"]
    if before is None or after is None:
        return 0
    return after["count"] - before["count"]


def native_labels(record: dict[str, Any]) -> list[str]:
    if record["outcome"] == NOT_EXPRESSIBLE:
        return [NOT_EXPRESSIBLE]
    if record["outcome"] == SETUP_REFUSED:
        return [SETUP_REFUSED]
    if record["outcome"] == PANIC:
        return [PANIC]
    if record["outcome"] == REFUSED:
        return ["NATIVE-REFUSED"]
    if record["seat"] == "select":
        return ["EQUAL"]
    pre = record["pre_select"]
    labels: list[str] = []
    if pre["outcome"] != ANSWERED:
        return ["ORACLE-MISSING"]
    if record["ids"] != pre["ids"] or (record["lost"] and record["seat"] != "delete"):
        labels.append("SEAT-INCONSISTENT")
    if record["damaged"]:
        labels.append("WRONG-WRITE")
    return labels or ["EQUAL"]


def folded_predicates(spark_cells: dict[str, dict[str, Any]]) -> set[str]:
    return {cell["pid"] for cell in spark_cells.values() if cell.get("plan_empty") is True}


def label_all(
    repark_cells: dict[str, dict[str, Any]], spark_cells: dict[str, dict[str, Any]]
) -> dict[str, list[str]]:
    folded = folded_predicates(spark_cells)
    result: dict[str, list[str]] = {}
    for key, record in repark_cells.items():
        if record["door"] == "native":
            labels = native_labels(record)
        else:
            labels = spark_labels(record, spark_cells.get(key))
        labels = [label for label in labels if label != "EQUAL"]
        if conjunct_dropped(record):
            labels.append("CONJUNCT-DROPPED")
        if record["pid"] in folded and record["seat"] in DML_SEATS and FINDING_LABELS & set(labels):
            labels.insert(0, "KNOWN-EMPTY-DML")
        result[key] = labels or ["EQUAL"]
    return result


def markdown_cell(text: Any) -> str:
    return str(text).replace("|", "\\|").replace("\n", " ")


def ids_text(ids: Any) -> str:
    if ids is None:
        return "-"
    return "{" + ",".join(str(item) for item in ids) + "}"


def outcome_text(record: dict[str, Any] | None) -> str:
    if record is None:
        return "missing"
    if record["outcome"] == ANSWERED:
        extra = ""
        if record["damaged"] and record["seat"] != "select":
            extra = f" damaged {ids_text(record['damaged'])}"
        if record["lost"] and record["seat"] not in ("delete",):
            extra += f" lost {ids_text(record['lost'])}"
        return f"ok {ids_text(record['ids'])}{extra}"
    message = excerpt(" ".join(record["error"]["message"].split()), 80) if record["error"] else ""
    return f"{record['outcome']} {record['error']['class'] if record['error'] else ''} {message}"


def order_key(record: dict[str, Any]) -> tuple[Any, ...]:
    doors = ("spark-sql", "native")
    seats = ALL_SEATS
    kinds = KINDS
    return (
        doors.index(record["door"]),
        seats.index(record["seat"]),
        kinds.index(record["kind"]),
        record["pid"],
    )


def conjunct_table_text(
    repark_cells: dict[str, dict[str, Any]],
    spark_cells: dict[str, dict[str, Any]],
    labels: dict[str, list[str]],
) -> str:
    lines = ["# DML conjunct drop, one row per RePark cell", ""]
    totals: Counter[str] = Counter()
    for entry in labels.values():
        totals.update(entry)
    lines.append("## Label totals (RePark cells)")
    lines.append("")
    lines.extend(f"- `{label}`: {count}" for label, count in sorted(totals.items()))
    spark_extra = sum(1 for cell in spark_cells.values() if conjunct_dropped(cell))
    lines.append("")
    lines.append(f"Spark cells with a DML touching rows outside its own SELECT: {spark_extra}")
    lines.append("")
    lines.append("## Counts per door and seat (RePark cells)")
    lines.append("")
    all_labels = sorted(totals)
    lines.append("| door | seat | cells | " + " | ".join(all_labels) + " |")
    lines.append("|---|---|---|" + "---|" * len(all_labels))
    for door in ("spark-sql", "native"):
        for seat in ALL_SEATS:
            members = [
                key
                for key, record in repark_cells.items()
                if record["door"] == door and record["seat"] == seat
            ]
            if not members:
                continue
            counter: Counter[str] = Counter()
            for key in members:
                counter.update(labels[key])
            counts = " | ".join(str(counter.get(label, 0)) for label in all_labels)
            lines.append(f"| {door} | {seat} | {len(members)} | {counts} |")
    lines.append("")
    lines.append("## Predicates")
    lines.append("")
    lines.extend(f"- `{pid}`: `{markdown_cell(text)}`" for pid, text, _ in PREDICATES)
    lines.append("")
    lines.append("## Finding cells (one line each)")
    lines.append("")
    for key, record in sorted(repark_cells.items(), key=lambda item: order_key(item[1])):
        if not FINDING_LABELS & set(labels[key]):
            continue
        spark_text = "-" if record["door"] == "native" else outcome_text(spark_cells.get(key))
        extra = outside_own_select(record)
        lines.append(
            f"- {record['pid']} `{record['predicate']}` | {record['seat']} | {record['door']} | "
            f"{record['kind']} | {', '.join(labels[key])} | Spark: {spark_text} | "
            f"RePark: {outcome_text(record)} | "
            f"own select {ids_text((record['pre_select'] or {}).get('ids'))}"
            f" | outside own select {ids_text(extra) if extra is not None else '-'}"
        )
    lines.append("")
    lines.append("## Full table (one row per RePark cell)")
    lines.append("")
    lines.append(
        "| pid | door | seat | kind | RePark | Spark | RePark own select "
        "| outside own select | labels |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|")
    for key, record in sorted(repark_cells.items(), key=lambda item: order_key(item[1])):
        oracle_text = "-" if record["door"] == "native" else outcome_text(spark_cells.get(key))
        pre = record["pre_select"]
        own = "-" if pre is None else ids_text(pre["ids"])
        extra = outside_own_select(record)
        cells = [
            record["pid"],
            record["door"],
            record["seat"],
            record["kind"],
            outcome_text(record),
            oracle_text,
            own,
            "-" if extra is None else ids_text(extra),
            ", ".join(labels[key]),
        ]
        lines.append("| " + " | ".join(markdown_cell(cell) for cell in cells) + " |")
    return "\n".join(lines) + "\n"


def oracle_key(key: str) -> str:
    if key.startswith("native|"):
        return "spark-sql|" + key[len("native|") :]
    return key


def sz_labels(record: dict[str, Any], oracle: dict[str, Any] | None) -> list[str]:
    if record["outcome"] == PANIC:
        return [PANIC]
    if record["outcome"] == SETUP_REFUSED:
        return [SETUP_REFUSED]
    if oracle is None or oracle["outcome"] == SETUP_REFUSED:
        return ["ORACLE-MISSING"]
    mine, theirs = record["outcome"], oracle["outcome"]
    if mine == REFUSED and theirs == REFUSED:
        return ["BOTH-REFUSED"]
    if mine == ANSWERED and theirs == REFUSED:
        return ["ACCEPTED-WHERE-SPARK-REFUSED"]
    if mine == REFUSED and theirs == ANSWERED:
        return ["NATIVE-REFUSED" if record["door"] == "native" else "REFUSED-WHERE-SPARK-ACCEPTED"]
    family = record["family"]
    bad = "WRONG-WRITE" if family == "dml" else "WRONG-READ"
    labels: list[str] = []
    if canon_rows(record["rows"]) != canon_rows(oracle["rows"]):
        if sign_free(record["rows"]) == sign_free(oracle["rows"]):
            labels.append("SIGN-ONLY")
        labels.append(bad)
    if family == "order" and sequence_rows(record["rows"]) != sequence_rows(oracle["rows"]):
        labels.append("ORDER-DIFFER")
    return labels or ["EQUAL"]


def canon_rows(rows: list[list[Any]]) -> list[str]:
    return sorted(json.dumps(row, sort_keys=True, default=str) for row in rows)


def sequence_rows(rows: list[list[Any]]) -> list[str]:
    return [json.dumps(row, sort_keys=True, default=str) for row in rows]


def sign_free(rows: list[list[Any]]) -> list[str]:
    return sorted(text.replace('"-0.0"', '"0.0"') for text in canon_rows(rows))


def cell_text(value: Any) -> str:
    return "NULL" if value is None else str(value)


def sz_show(record: dict[str, Any] | None) -> str:
    if record is None:
        return "-"
    if record["outcome"] != ANSWERED:
        if record["error"]:
            message = excerpt(" ".join(record["error"]["message"].split()), 60)
            return f"{record['outcome']} {record['error']['class']} {message}"
        return record["outcome"]
    if record["rows"] is None:
        return "-"
    items = ["(" + ",".join(cell_text(value) for value in row) + ")" for row in record["rows"]]
    if record["family"] == "read":
        items.sort()
    if not items:
        return "none"
    text = "; ".join(items)
    return text if len(text) <= 240 else text[:237] + "..."


def read_cell(
    cells: dict[str, dict[str, Any]], door: str, oid: str, variant: str
) -> dict[str, Any] | None:
    return cells.get(f"{door}|{oid}|{variant}|v2-cow")


def sz_order(repark_cells: dict[str, dict[str, Any]]) -> list[tuple[str, str]]:
    seen: list[tuple[str, str]] = []
    for record in repark_cells.values():
        if record["family"] == "dml":
            continue
        pair = (record["op"], record["variant"])
        if pair not in seen:
            seen.append(pair)
    return seen


def signed_zero_table_text(
    repark_cells: dict[str, dict[str, Any]],
    spark_cells: dict[str, dict[str, Any]],
    labels: dict[str, list[str]],
) -> str:
    lines = ["# Signed zero, reads and writes against the Spark oracle", ""]
    totals: Counter[str] = Counter()
    for entry in labels.values():
        totals.update(entry)
    lines.append("## Label totals (RePark cells)")
    lines.append("")
    lines.extend(f"- `{label}`: {count}" for label, count in sorted(totals.items()))
    lines.append("")
    lines.append("## Reads: Spark result against RePark result (v2-cow)")
    lines.append("")
    lines.append(
        "| op | variant | Spark spark-sql | RePark spark-sql | RePark native | Spark filter-str "
        "| RePark filter-str | Spark f-expr | RePark f-expr | labels |"
    )
    lines.append("|---|---|---|---|---|---|---|---|---|---|")
    for oid, variant in sz_order(repark_cells):
        columns = [
            sz_show(read_cell(spark_cells, "spark-sql", oid, variant)),
            sz_show(read_cell(repark_cells, "spark-sql", oid, variant)),
            sz_show(read_cell(repark_cells, "native", oid, variant)),
            sz_show(read_cell(spark_cells, "filter-str", oid, variant)),
            sz_show(read_cell(repark_cells, "filter-str", oid, variant)),
            sz_show(read_cell(spark_cells, "f-expr", oid, variant)),
            sz_show(read_cell(repark_cells, "f-expr", oid, variant)),
        ]
        notes = []
        for door in ("spark-sql", "native", "filter-str", "f-expr"):
            key = f"{door}|{oid}|{variant}|v2-cow"
            if key in labels:
                notes.append(f"{door}: {', '.join(labels[key])}")
        cells = [oid, variant, *columns, "; ".join(notes)]
        lines.append("| " + " | ".join(markdown_cell(cell) for cell in cells) + " |")
    lines.append("")
    lines.append("## Writes: spark-sql door, three kinds (post-state rows of t, ordered by id)")
    lines.append("")
    lines.append("| op | variant | kind | Spark | RePark | labels |")
    lines.append("|---|---|---|---|---|---|")
    for key, record in repark_cells.items():
        if record["family"] != "dml":
            continue
        oracle = spark_cells.get(oracle_key(key))
        cells = [
            record["op"],
            record["variant"],
            record["kind"],
            sz_show(oracle),
            sz_show(record),
            ", ".join(labels[key]),
        ]
        lines.append("| " + " | ".join(markdown_cell(cell) for cell in cells) + " |")
    lines.append("")
    lines.append("## Finding lines (one line each)")
    lines.append("")
    for key, record in repark_cells.items():
        flagged = [
            label for label in labels[key] if label not in SZ_FINDING_EXCLUDE and label != "EQUAL"
        ]
        if not flagged:
            continue
        oracle = spark_cells.get(oracle_key(key))
        lines.append(
            f"- `{key}` | {', '.join(labels[key])} | Spark: {sz_show(oracle)} | "
            f"RePark: {sz_show(record)}"
        )
    return "\n".join(lines) + "\n"


def render(arguments: argparse.Namespace) -> int:
    _, spark_cells = load_many(arguments.spark)
    _, repark_cells = load_many(arguments.main)
    if arguments.table == "conjunct":
        labels = label_all(repark_cells, spark_cells)
    else:
        labels = {
            key: sz_labels(record, spark_cells.get(oracle_key(key)))
            for key, record in repark_cells.items()
        }
    out = Path(arguments.tables_dir)
    if arguments.table == "conjunct":
        text = conjunct_table_text(repark_cells, spark_cells, labels)
        target = out / "conjunct-table.md"
    else:
        text = signed_zero_table_text(repark_cells, spark_cells, labels)
        target = out / "signed-zero-table.md"
    target.write_text(text, encoding="utf-8")
    totals: Counter[str] = Counter()
    for entry in labels.values():
        totals.update(entry)
    print(dict(sorted(totals.items())))
    return 0


def build_spark_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as spark_functions

    root = warehouse / "spark"
    session = (
        SparkSession.builder.master("local[1]")
        .appName("write-path-followup")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", SESSION_ZONE)
        .config("spark.local.dir", str(warehouse / "spark-local"))
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars", arguments.ivy_jar)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{SPARK_CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{SPARK_CATALOG}.warehouse", str(root))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    door = {
        "name": "spark-sql",
        "sql": session.sql,
        "catalog": SPARK_CATALOG,
        "root": root,
        "session": session,
        "functions": spark_functions,
    }
    execute_sql(door, f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{NAMESPACE}")
    jar = Path(arguments.ivy_jar)
    header = {
        "record": "leg",
        "engine": "spark",
        "label": arguments.label,
        "spark_version": session.version,
        "jdk_version": session.sparkContext._jvm.System.getProperty("java.version"),
        "session_time_zone": session.conf.get("spark.sql.session.timeZone"),
        "iceberg_jar": jar.name,
        "iceberg_jar_sha256": sha256_of(jar),
        "python": sys.version.split()[0],
        "venv": sys.prefix,
    }
    doors = dict.fromkeys(("spark-sql", "filter-str", "f-expr"), door)
    return {
        "engine": "spark",
        "label": arguments.label,
        "scrub": str(warehouse),
        "doors": doors,
        "header": header,
        "close": session.stop,
    }


def build_repark_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    import pyarrow

    import repark
    from repark import ReparkSession
    from repark import functions as repark_functions
    from repark._native import PyReparkSession

    native_root = warehouse / "native"
    facade_root = warehouse / "facade"
    native_root.mkdir(parents=True)
    facade_root.mkdir(parents=True)
    native = PyReparkSession.native(config=NATIVE_CONFIG)
    native.register_memory_catalog(REPARK_CATALOG, str(native_root))
    native.create_namespace(REPARK_CATALOG, NAMESPACE, str(native_root / NAMESPACE))
    repark._ANSI_NATIVE = native
    facade = (
        ReparkSession.builder.appName("write-path-followup")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", SESSION_ZONE)
        .getOrCreate()
    )
    facade.register_memory_catalog(REPARK_CATALOG, str(facade_root))
    facade_door = {
        "name": "spark-sql",
        "sql": facade.sql,
        "catalog": REPARK_CATALOG,
        "root": facade_root,
        "session": facade,
        "functions": repark_functions,
    }
    native_door = {
        "name": "native",
        "sql": repark.sql,
        "catalog": REPARK_CATALOG,
        "root": native_root,
        "session": None,
        "functions": None,
    }
    wheel = Path(arguments.wheel)
    context = {
        "engine": "repark",
        "label": arguments.label,
        "scrub": str(warehouse),
        "doors": {
            "spark-sql": facade_door,
            "filter-str": facade_door,
            "f-expr": facade_door,
            "native": native_door,
        },
        "header": {
            "record": "leg",
            "engine": "repark",
            "label": arguments.label,
            "wheel_file": wheel.name,
            "wheel_sha256": sha256_of(wheel),
            "repark_file": repark.__file__,
            "repark_version": importlib.metadata.version("repark"),
            "pyarrow_version": pyarrow.__version__,
            "native_config": NATIVE_CONFIG,
            "python": sys.version.split()[0],
            "venv": sys.prefix,
        },
        "close": lambda: None,
    }
    zone_rows = execute_sql(facade_door, "SELECT current_timezone() AS z")
    if zone_rows[0]["z"] != SESSION_ZONE:
        execute_sql(facade_door, f"SET spark.sql.session.timeZone={SESSION_ZONE}")
        zone_rows = execute_sql(facade_door, "SELECT current_timezone() AS z")
    context["header"]["session_time_zone"] = zone_rows[0]["z"]
    native_zone, _ = attempt(
        context, lambda: execute_sql(native_door, "SELECT current_timezone() AS z"), "zone"
    )
    context["header"]["native_time_zone"] = native_zone[0]["z"] if native_zone else None
    facade_location = facade_root / NAMESPACE
    execute_sql(
        facade_door, f"CREATE NAMESPACE {REPARK_CATALOG}.{NAMESPACE} LOCATION '{facade_location}'"
    )
    return context


def build_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    if arguments.engine == "spark":
        context = build_spark_context(arguments, warehouse)
    else:
        context = build_repark_context(arguments, warehouse)
    context["setup"] = {}
    return context


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--table", choices=("conjunct", "signed-zero"), required=True)
    parser.add_argument("--spark", nargs="+")
    parser.add_argument("--main", nargs="+")
    parser.add_argument("--tables-dir")
    parser.add_argument("--engine", choices=("spark", "repark"))
    parser.add_argument("--label")
    parser.add_argument("--warehouse")
    parser.add_argument("--out")
    parser.add_argument("--only", nargs="+")
    parser.add_argument("--ivy-jar")
    parser.add_argument("--wheel")
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    arguments = parse_arguments(argv)
    if arguments.render:
        return render(arguments)
    engine = arguments.engine or "repark"
    if arguments.table == "conjunct":
        specs = conjunct_plan(engine, arguments.only)
    else:
        specs = signed_zero_plan(engine, arguments.only)
    if arguments.plan_only:
        print(f"cells planned: {len(specs)}")
        return 0
    missing = [
        name for name in ("engine", "label", "warehouse", "out") if getattr(arguments, name) is None
    ]
    if arguments.engine == "spark" and arguments.ivy_jar is None:
        missing.append("ivy-jar")
    if arguments.engine == "repark" and arguments.wheel is None:
        missing.append("wheel")
    if missing:
        print(f"missing arguments: {', '.join(missing)}", file=sys.stderr)
        return 2
    warehouse = Path(arguments.warehouse)
    if warehouse.exists() and any(warehouse.iterdir()):
        print("warehouse must be empty or absent", file=sys.stderr)
        return 2
    warehouse.mkdir(parents=True, exist_ok=True)
    context = build_context(arguments, warehouse)
    if arguments.table == "conjunct":
        if any(spec["door"] == "native" for spec in specs):
            context["spellings"] = resolve_native_spellings(context, context["doors"]["native"])
    else:
        for name in SZ_SQL_DOORS:
            if name in context["doors"]:
                setup_read_tables(context, context["doors"][name])
    out = Path(arguments.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    outcomes: Counter[str] = Counter()
    with out.open("w", encoding="utf-8") as handle:
        context["header"]["table"] = arguments.table
        context["header"]["spelling_resolution"] = context.get("spellings")
        write_line(handle, context["header"], context["scrub"])
        for spec in specs:
            if arguments.table == "conjunct":
                record = run_conjunct_cell(context, spec)
            else:
                record = run_sz_cell(context, spec)
            outcomes[record["outcome"]] += 1
            write_line(handle, record, context["scrub"])
            handle.flush()
    context["close"]()
    print(f"cells: {len(specs)} outcomes: {dict(sorted(outcomes.items()))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
