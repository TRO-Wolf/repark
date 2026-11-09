"""Write-path envelope matrix: one predicate set across every seat and door, one engine leg per run.

One invocation runs one leg over a cell plan and appends a leg header line, then one JSON line per
cell. The --render mode reads the leg files and writes the cell table, the summary and the detail
lines. Measurement only.
"""

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
SEATS = ("select", "delete", "update", "insert", "ctas", "merge")
DML_SEATS = ("delete", "update", "insert", "ctas", "merge")
WRITE_KEYS = ("write.delete.mode", "write.update.mode", "write.merge.mode")
TABLE_MODES = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
SESSION_ZONE = "America/New_York"
NATIVE_CONFIG = {"repark.sql.allowCreateFormatVersion3": "true"}
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
PREDICATES = (
    ("p01", "i = 1", ()),
    ("p02", "i <> 1", ()),
    ("p03", "i IN (1, NULL)", ()),
    ("p04", "i NOT IN (1, NULL)", ()),
    ("p05", "i IS NULL", ()),
    ("p06", "i BETWEEN 1 AND 2", ()),
    ("p07", "i <=> NULL", ("i IS NOT DISTINCT FROM NULL",)),
    ("p08", "i + 1 > 0", ()),
    ("p09", "CAST(i AS STRING) = '1'", ("CAST(i AS VARCHAR) = '1'",)),
    ("p10", "l = 9223372036854775807", ()),
    ("p11", "dec = 1.10", ()),
    ("p12", "dec > 1.1", ()),
    ("p13", "d = CAST('NaN' AS DOUBLE)", ()),
    ("p14", "isnan(d)", ("is_nan(d)",)),
    ("p15", "d = 0.0", ()),
    ("p16", "d > 0", ()),
    ("p17", "s = ''", ()),
    ("p18", "s = 'a '", ()),
    ("p19", "s LIKE 'a%'", ()),
    ("p20", "upper(s) = 'A'", ()),
    ("p21", "length(s) = 0", ()),
    ("p22", "b", ()),
    ("p23", "b IS NOT TRUE", ()),
    ("p24", "dt = DATE '1970-01-01'", ()),
    ("p25", "ts > TIMESTAMP '2026-01-01 00:00:00'", ()),
    (
        "p26",
        "ntz = TIMESTAMP_NTZ '2026-03-08 02:30:00'",
        ("ntz = TIMESTAMP '2026-03-08 02:30:00'",),
    ),
    ("p27", "st.x = 1", ()),
    ("p28", "st IS NULL", ()),
    ("p29", "st.y IS NULL", ()),
    ("p30", "st = named_struct('x', 1, 'y', 'a')", ()),
    ("p31", "array_contains(arr, 1)", ("contains(arr, 1)",)),
    ("p32", "size(arr) = 0", ("cardinality(arr) = 0",)),
    ("p33", "element_at(arr, 1) = 1", ("arr[1] = 1",)),
    ("p34", "m['k'] = 1", ()),
    ("p35", "coalesce(i, 0) = 0", ()),
    ("p36", "CASE WHEN i = 1 THEN true END", ()),
)
PREDICATE_TEXT = {pid: text for pid, text, _ in PREDICATES}
STATIC_ERRORS = ("ParseException", "AnalysisException", "UnsupportedOperationException")
ROLES = {"t": "_t", "t2": "_t2", "t3": "_t3"}
LINEAGE_COLUMN = "_last_updated_sequence_number"
EMPTY_PLAN_MARK = "LocalRelation <empty>"


def sanitize(text: str) -> str:
    """Return a lower-case table identifier."""
    return re.sub(r"[^a-z0-9_]", "_", text.lower())


def scrub(text: str, warehouse_root: str) -> str:
    """Replace the warehouse root, the temporary directory and the home directory."""
    text = text.replace(warehouse_root, "<warehouse>")
    temporary = re.escape(tempfile.gettempdir())
    text = re.sub(temporary + r"/[^\s'\"),\]\\]*", "<tmp>", text)
    home = re.escape(str(Path.home()))
    return re.sub(home + r"(?:/[^\s'\"),\]\\]*)?", "<home>", text)


def sha256_of(path: Path) -> str:
    """Return the sha256 hex digest of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def excerpt(text: str, limit: int = 300) -> str:
    """Return the first ``limit`` characters, cut back to a word boundary."""
    if len(text) <= limit:
        return text
    head = text[:limit]
    boundary = head.rfind(" ")
    return head[:boundary] if boundary > 0 else head


def norm(value: Any) -> Any:
    """Normalise one Python value so equal data compares equal across engines."""
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


def row_digest(row: dict[str, Any], skip: tuple[str, ...]) -> str:
    """Return a short digest of a normalised row without the skipped columns."""
    body = {key: norm(item) for key, item in sorted(row.items()) if key not in skip}
    text = json.dumps(body, sort_keys=True, default=str)
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:10]


def error_record(ctx: dict[str, Any], error: BaseException, stage: str) -> dict[str, Any]:
    """Return the scrubbed, truncated record of one raised error."""
    text = scrub(str(error), ctx["scrub"])
    panic = type(error).__name__ == "PanicException" or "panicked at" in text
    return {
        "class": type(error).__name__,
        "stage": stage,
        "message": excerpt(text),
        "panic": panic,
    }


def attempt(ctx: dict[str, Any], call: Any, stage: str) -> tuple[Any, dict[str, Any] | None]:
    """Run one call and return its value, or the error record if it raised."""
    try:
        return call(), None
    except KeyboardInterrupt:
        raise
    except BaseException as error:
        return None, error_record(ctx, error, stage)


def outcome_of(error: dict[str, Any] | None) -> str:
    """Return the outcome kind of a statement from its error record."""
    if error is None:
        return ANSWERED
    return PANIC if error["panic"] else REFUSED


def to_arrow(frame: Any) -> Any:
    """Return the Arrow table of a result frame."""
    return frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()


def execute_sql(door: dict[str, Any], text: str) -> list[dict[str, Any]]:
    """Run one statement on a door and return its rows."""
    return to_arrow(door["sql"](text)).to_pylist()


def metadata_order(path: Path) -> tuple[int, str]:
    """Order metadata files by their leading version number."""
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def read_metadata(table_dir: Path) -> dict[str, Any]:
    """Read the newest metadata JSON of a table: snapshots, format and write modes."""
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
    """Return the TBLPROPERTIES body for a table kind."""
    version, mode = TABLE_MODES[kind]
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}', 'write.merge.mode' = '{mode}'"
    )


def create_sql(door: dict[str, Any], qualified: str, kind: str) -> str:
    """Return the CREATE TABLE text a door is given for a table kind."""
    if door["name"] == "native":
        return f"CREATE TABLE {qualified} ({NATIVE_COLUMNS})"
    return f"CREATE TABLE {qualified} ({COLUMNS}) USING iceberg TBLPROPERTIES ({props_text(kind)})"


def seed_sql(door: dict[str, Any], qualified: str) -> str:
    """Return the single INSERT ... VALUES that seeds a table."""
    rows = NATIVE_SEED if door["name"] == "native" else SPARK_SEED
    return f"INSERT INTO {qualified} VALUES " + ", ".join(rows)


def observe(
    ctx: dict[str, Any], door: dict[str, Any], qualified: str, table_dir: Path, lineage: bool
) -> dict[str, Any]:
    """Read a table's snapshot identity, per-row digests and, for v3, row lineage."""
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
    """Return the snapshot identity of a table state."""
    return {"count": state["snapshot_count"], "current": state["current_snapshot_id"]}


def spec_key(spec: dict[str, Any]) -> str:
    """Return the cell key shared by every leg."""
    return f"{spec['door']}|{spec['seat']}|{spec['kind']}|{spec['pid']}"


def build_plan(engine: str, only: list[str] | None, pids: list[str] | None) -> list[dict[str, Any]]:
    """Return the ordered cell specs of one leg."""
    specs: list[dict[str, Any]] = []

    def add(door: str, seat: str, kind: str) -> None:
        for pid, _, _ in PREDICATES:
            spec = {"door": door, "seat": seat, "kind": kind, "pid": pid}
            spec["table"] = sanitize(f"{door}_{seat}_{kind}_{pid}")
            specs.append(spec)

    for seat in SEATS:
        for kind in ("v2-cow",) if seat == "select" else KINDS:
            add("spark-sql", seat, kind)
    for door in ("filter-str", "f-expr"):
        for seat in ("select", "insert"):
            add(door, seat, "v2-cow")
    if engine == "repark":
        for seat in SEATS:
            add("native", seat, "v2-cow")
    if only:
        specs = [spec for spec in specs if matches_only(spec, only)]
    if pids:
        specs = [spec for spec in specs if spec["pid"] in pids]
    return specs


def matches_only(spec: dict[str, Any], only: list[str]) -> bool:
    """Whether a spec is selected by --only tokens (door, seat, kind or door:seat)."""
    for token in only:
        if ":" in token:
            door, seat = token.split(":", 1)
            if spec["door"] == door and spec["seat"] == seat:
                return True
        elif token in (spec["door"], spec["seat"], spec["kind"]):
            return True
    return False


def base_record(ctx: dict[str, Any], spec: dict[str, Any], qualified: str) -> dict[str, Any]:
    """Return a cell record with every field present."""
    return {
        "record": "cell",
        "engine": ctx["engine"],
        "leg": ctx["label"],
        "key": spec_key(spec),
        "door": spec["door"],
        "seat": spec["seat"],
        "kind": spec["kind"],
        "pid": spec["pid"],
        "predicate": PREDICATE_TEXT[spec["pid"]],
        "spelling": None,
        "table": qualified,
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
        "target": None,
        "snapshot_before": None,
        "snapshot_after": None,
        "lineage_before": None,
        "lineage_after": None,
        "lineage_advanced": None,
        "observe_error": None,
    }


def finish_setup(record: dict[str, Any], error: dict[str, Any]) -> dict[str, Any]:
    """Close a cell whose table could not be created or seeded."""
    record["outcome"] = SETUP_REFUSED
    record["error"] = error
    return record


def run_statement(
    ctx: dict[str, Any], door: dict[str, Any], text: str, stage: str
) -> tuple[Any, dict[str, Any] | None]:
    """Run one SQL statement and return its rows and error record."""
    return attempt(ctx, lambda: execute_sql(door, text), stage)


def df_frame(door: dict[str, Any], spec: dict[str, Any], qualified: str, predicate: str) -> Any:
    """Return the filtered DataFrame of a DataFrame-door cell."""
    argument = predicate if spec["door"] == "filter-str" else door["functions"].expr(predicate)
    return door["session"].table(qualified).filter(argument)


def seat_text(spec: dict[str, Any], names: dict[str, str], predicate: str) -> str | None:
    """Return the statement text of a SQL seat."""
    source, second, third = names["t"], names["t2"], names["t3"]
    seat = spec["seat"]
    if seat == "select":
        return f"SELECT id FROM {source} WHERE {predicate}"
    if seat == "delete":
        return f"DELETE FROM {source} WHERE {predicate}"
    if seat == "update":
        return f"UPDATE {source} SET flag = 'z' WHERE {predicate}"
    if seat == "insert":
        return f"INSERT INTO {second} SELECT * FROM {source} WHERE {predicate}"
    if seat == "ctas":
        return f"CREATE TABLE {third} AS SELECT * FROM {source} WHERE {predicate}"
    return (
        f"MERGE INTO {source} AS t USING (SELECT id FROM {source}) s ON t.id = s.id "
        f"WHEN MATCHED AND {predicate} THEN UPDATE SET flag = 'z'"
    )


def df_text(spec: dict[str, Any], names: dict[str, str]) -> str:
    """Return a readable statement text for a DataFrame door."""
    argument = '"P"' if spec["door"] == "filter-str" else 'F.expr("P")'
    tail = ".select('id')" if spec["seat"] == "select" else f".write.insertInto({names['t2']})"
    return f"table({names['t']}).filter({argument}){tail}"


def derive_ids(seat: str, before: dict[str, Any], after: dict[str, Any]) -> dict[str, Any]:
    """Derive the affected id set and the damage sets of a write seat on table t."""
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


def derive_target(source: dict[str, Any], target: dict[str, Any]) -> dict[str, Any]:
    """Derive the copied id set and its damage for an insert or CTAS target table."""
    if target["rows"] is None or source["rows"] is None:
        return {"ids": None, "damaged": []}
    source_map = {row[0]: row for row in source["rows"]}
    damaged = [
        row[0] for row in target["rows"] if row[0] not in source_map or source_map[row[0]] != row
    ]
    return {"ids": sorted(row[0] for row in target["rows"]), "damaged": sorted(set(damaged))}


def lineage_advanced(before: dict[str, Any], after: dict[str, Any]) -> list[Any] | None:
    """Return the ids whose row-lineage sequence number differs after the statement."""
    if before["lineage"] is None or after["lineage"] is None:
        return None
    old = dict(before["lineage"])
    return sorted(ident for ident, seq in after["lineage"] if ident in old and old[ident] != seq)


def native_spelling(ctx: dict[str, Any], pid: str) -> tuple[str | None, list[dict[str, Any]]]:
    """Return the resolved native spelling of a predicate and the attempts made."""
    return ctx["spellings"][pid]


def spelling_for(ctx: dict[str, Any], spec: dict[str, Any]) -> tuple[str | None, Any]:
    """Return the predicate text a cell's door is given."""
    if spec["door"] != "native":
        return PREDICATE_TEXT[spec["pid"]], None
    text, attempts = native_spelling(ctx, spec["pid"])
    return text, attempts


def target_state(
    ctx: dict[str, Any], door: dict[str, Any], names: dict[str, str], seat: str
) -> tuple[dict[str, Any] | None, Any]:
    """Observe the insert or CTAS target table after the statement."""
    key = "t2" if seat == "insert" else "t3"
    name = names[key]
    return observe(ctx, door, name, door["root"] / NAMESPACE / name.split(".")[-1], False), key


def run_cell(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """Run one cell on fresh tables and return its record."""
    door = ctx["doors"][spec["door"]]
    prefix = f"{door['catalog']}.{NAMESPACE}."
    names = {key: prefix + spec["table"] + suffix for key, suffix in ROLES.items()}
    record = base_record(ctx, spec, names["t"])
    text, attempts = spelling_for(ctx, spec)
    record["spelling"] = text
    if text is None:
        record["outcome"] = NOT_EXPRESSIBLE
        record["setup"] = {"spelling_attempts": attempts}
        return record
    table_dir = door["root"] / NAMESPACE / (spec["table"] + ROLES["t"])
    lineage = spec["kind"] == "v3"
    ddl = create_sql(door, names["t"], spec["kind"])
    _, error = run_statement(ctx, door, ddl, "create")
    seed = seed_sql(door, names["t"])
    if error is None:
        _, error = run_statement(ctx, door, seed, "seed")
    if error is None and spec["seat"] == "insert":
        second_ddl = create_sql(door, names["t2"], spec["kind"])
        _, error = run_statement(ctx, door, second_ddl, "create-target")
    if error is not None:
        record["setup"] = {"ddl": ddl, "seed": seed}
        return finish_setup(record, error)
    before = observe(ctx, door, names["t"], table_dir, lineage)
    pre_select, pre_error = run_statement(
        ctx, door, seat_text({"seat": "select"}, names, text), "pre-select"
    )
    record["pre_select"] = {
        "outcome": outcome_of(pre_error),
        "error": pre_error,
        "ids": None if pre_error else sorted(row["id"] for row in pre_select),
    }
    if ctx["engine"] == "spark" and spec["door"] == "spark-sql":
        record["plan_empty"] = plan_is_empty(door, seat_text({"seat": "select"}, names, text))
    result_ids, error = execute_seat(ctx, door, spec, names, text, record)
    after = observe(ctx, door, names["t"], table_dir, lineage)
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
    seat = spec["seat"]
    if seat == "select":
        record["ids"] = result_ids
        record["damaged"] = []
        record["lost"] = [] if after["rows"] == before["rows"] else ["source-changed"]
    elif seat in ("delete", "update", "merge"):
        derived = derive_ids(seat, before, after)
        record.update(ids=derived["ids"], lost=derived["lost"], damaged=derived["damaged"])
    else:
        target, key = target_state(ctx, door, names, seat)
        derived = derive_target(before, target)
        record["ids"] = derived["ids"]
        record["damaged"] = derived["damaged"]
        record["lost"] = [] if after["rows"] == before["rows"] else ["source-changed"]
        record["target"] = {
            "table": key,
            "snapshot": snapshot_of(target),
            "error": target["error"],
        }
    return record


def plan_is_empty(door: dict[str, Any], text: str) -> bool | None:
    """Whether Spark's optimised plan for a SELECT is an empty relation."""
    try:
        plan = door["session"].sql(text)._jdf.queryExecution().optimizedPlan().toString()
    except Exception:
        return None
    return EMPTY_PLAN_MARK in plan


def execute_seat(
    ctx: dict[str, Any],
    door: dict[str, Any],
    spec: dict[str, Any],
    names: dict[str, str],
    predicate: str,
    record: dict[str, Any],
) -> tuple[list[Any] | None, dict[str, Any] | None]:
    """Run the cell's seat statement and return the selected ids and the error record."""
    if spec["door"] in ("filter-str", "f-expr"):
        record["statement"] = df_text(spec, names)
        frame, error = attempt(ctx, lambda: df_frame(door, spec, names["t"], predicate), "filter")
        if error is not None:
            return None, error
        if spec["seat"] == "select":
            rows, error = attempt(ctx, lambda: to_arrow(frame.select("id")).to_pylist(), "collect")
            return (None if error else sorted(row["id"] for row in rows)), error
        _, error = attempt(ctx, lambda: frame.write.insertInto(names["t2"]), "write")
        return None, error
    statement = seat_text(spec, names, predicate)
    record["statement"] = statement
    rows, error = run_statement(ctx, door, statement, "sql")
    if error is None and spec["seat"] == "select":
        return sorted(row["id"] for row in rows), None
    return None, error


def native_candidates(pid: str) -> list[str]:
    """Return the ordered native spellings tried for a predicate."""
    for key, text, alternatives in PREDICATES:
        if key == pid:
            return [*alternatives, text]
    return []


def resolve_native_spellings(ctx: dict[str, Any], door: dict[str, Any]) -> dict[str, Any]:
    """Resolve each predicate's native spelling against a scratch native table."""
    qualified = f"{door['catalog']}.{NAMESPACE}.spelling_probe"
    _, error = run_statement(ctx, door, create_sql(door, qualified, "v2-cow"), "create")
    if error is None:
        _, error = run_statement(ctx, door, seed_sql(door, qualified), "seed")
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


def setup_templates(ctx: dict[str, Any]) -> dict[str, Any]:
    """Return the CREATE and seed statements each door and kind is given, with <t> as the table."""
    templates: dict[str, Any] = {"create": {}, "seed": {}}
    for name in ("spark-sql", "native"):
        door = ctx["doors"].get(name)
        if door is None:
            continue
        templates["seed"][name] = seed_sql(door, "<t>")
        for kind in KINDS if name == "spark-sql" else ("v2-cow",):
            templates["create"][f"{name}|{kind}"] = create_sql(door, "<t>", kind)
    return templates


def write_line(handle: Any, record: dict[str, Any], warehouse_root: str) -> None:
    """Write one scrubbed JSON line."""
    text = json.dumps(record, sort_keys=True, default=str)
    handle.write(scrub(text, warehouse_root) + "\n")


def build_spark_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    """Start the Spark oracle session and describe its door."""
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as spark_functions

    root = warehouse / "spark"
    session = (
        SparkSession.builder.master("local[1]")
        .appName("write-path-envelope")
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
    """Open the native and facade doors and describe the installed wheel."""
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
        ReparkSession.builder.appName("write-path-envelope")
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
    """Build the engine context for one leg."""
    if arguments.engine == "spark":
        return build_spark_context(arguments, warehouse)
    return build_repark_context(arguments, warehouse)


def load_cells(path: str) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    """Read one leg file: its header and its cells keyed by cell key."""
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
    """Read several leg files of one engine into one cell map."""
    headers: list[dict[str, Any]] = []
    merged: dict[str, dict[str, Any]] = {}
    for path in paths:
        header, cells = load_cells(path)
        headers.append(header)
        merged.update(cells)
    return headers, merged


def ids_of(record: dict[str, Any] | None) -> Any:
    """Return the id list a cell touched, or None."""
    return None if record is None else record["ids"]


def snapshot_delta(record: dict[str, Any]) -> int:
    """Return how many snapshots the statement added to the source table."""
    before, after = record["snapshot_before"], record["snapshot_after"]
    if before is None or after is None:
        return 0
    return after["count"] - before["count"]


def target_delta(record: dict[str, Any]) -> int:
    """Return how many snapshots the statement added to the insert or CTAS target."""
    target = record["target"]
    if target is None:
        return 0
    return target["snapshot"]["count"]


def native_labels(record: dict[str, Any]) -> list[str]:
    """Return the labels of a native-door cell, judged against the door's own select."""
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


def spark_labels(record: dict[str, Any], oracle: dict[str, Any] | None) -> list[str]:
    """Return the labels of a facade-door cell against the Spark oracle cell."""
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
    if (
        snapshot_delta(record) > snapshot_delta(oracle)
        or target_delta(record) > target_delta(oracle)
        or (
            record["snapshot_after"] is not None
            and record["seat"] == "select"
            and snapshot_delta(record) != 0
        )
    ):
        labels.append("EXTRA-COMMIT")
    if snapshot_delta(record) < snapshot_delta(oracle) or target_delta(record) < target_delta(
        oracle
    ):
        labels.append("FEWER-COMMIT")
    return labels or ["EQUAL"]


def folded_predicates(spark_cells: dict[str, dict[str, Any]]) -> set[str]:
    """Return the predicate ids Spark's optimiser folds to an empty relation."""
    return {cell["pid"] for cell in spark_cells.values() if cell.get("plan_empty") is True}


def label_all(
    repark_cells: dict[str, dict[str, Any]], spark_cells: dict[str, dict[str, Any]]
) -> dict[str, list[str]]:
    """Label every RePark cell; folded-empty write cells carry KNOWN-EMPTY-DML."""
    folded = folded_predicates(spark_cells)
    result: dict[str, list[str]] = {}
    finding = {
        "WRONG-WRITE",
        "EXTRA-COMMIT",
        "FEWER-COMMIT",
        "LINEAGE-DIFFER",
        "SEAT-INCONSISTENT",
    }
    for key, record in repark_cells.items():
        if record["door"] == "native":
            labels = native_labels(record)
        else:
            labels = spark_labels(record, spark_cells.get(key))
        if record["pid"] in folded and record["seat"] in DML_SEATS and finding & set(labels):
            labels = ["KNOWN-EMPTY-DML", *labels]
        result[key] = labels
    return result


def markdown_cell(text: Any) -> str:
    """Escape a value for a markdown table cell."""
    return str(text).replace("|", "\\|").replace("\n", " ")


def ids_text(ids: Any) -> str:
    """Render an id list compactly."""
    if ids is None:
        return "-"
    return "{" + ",".join(str(item) for item in ids) + "}"


def outcome_text(record: dict[str, Any] | None) -> str:
    """Render a cell's outcome and affected ids."""
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


def table_row(record: dict[str, Any], oracle: dict[str, Any] | None, labels: list[str]) -> str:
    """Return one envelope-table row."""
    oracle_text = "-" if record["door"] == "native" else outcome_text(oracle)
    pre = record["pre_select"]
    own = "-" if pre is None else ids_text(pre["ids"])
    delta = snapshot_delta(record) if record["snapshot_before"] else "-"
    cells = [
        record["pid"],
        record["door"],
        record["seat"],
        record["kind"],
        outcome_text(record),
        oracle_text,
        own,
        delta,
        ", ".join(labels),
    ]
    return "| " + " | ".join(markdown_cell(cell) for cell in cells) + " |"


def render_table(
    repark_cells: dict[str, dict[str, Any]],
    spark_cells: dict[str, dict[str, Any]],
    labels: dict[str, list[str]],
) -> str:
    """Return the envelope-table markdown, one row per RePark cell."""
    header = (
        "| pid | door | seat | kind | RePark | Spark | RePark own select | snapshots added "
        "| labels |\n"
        "|---|---|---|---|---|---|---|---|---|\n"
    )
    rows = [
        table_row(record, spark_cells.get(key), labels[key])
        for key, record in sorted(repark_cells.items(), key=lambda item: order_key(item[1]))
    ]
    predicates = "\n".join(f"- `{pid}`: `{text}`" for pid, text, _ in PREDICATES)
    return (
        "# Envelope table, one row per cell\n\n"
        "Predicate texts (Spark spelling):\n\n"
        + predicates
        + "\n\n"
        + header
        + "\n".join(rows)
        + "\n"
    )


def order_key(record: dict[str, Any]) -> tuple[Any, ...]:
    """Order cells by door, seat, kind and predicate."""
    doors = ("spark-sql", "filter-str", "f-expr", "native")
    return (
        doors.index(record["door"]),
        SEATS.index(record["seat"]),
        KINDS.index(record["kind"]),
        record["pid"],
    )


def render_summary(
    repark_cells: dict[str, dict[str, Any]],
    spark_cells: dict[str, dict[str, Any]],
    labels: dict[str, list[str]],
    headers: dict[str, list[dict[str, Any]]],
) -> str:
    """Return the summary markdown: counts per label by seat and door, and per predicate."""
    by_group: dict[tuple[str, str], Counter[str]] = {}
    by_predicate: dict[str, Counter[str]] = {}
    totals: Counter[str] = Counter()
    for key, record in repark_cells.items():
        group = by_group.setdefault((record["door"], record["seat"]), Counter())
        for label in labels[key]:
            group[label] += 1
            totals[label] += 1
            if label != "EQUAL":
                by_predicate.setdefault(record["pid"], Counter())[label] += 1
    lines = ["# Summary\n", f"RePark cells: {len(repark_cells)}; Spark cells: {len(spark_cells)}\n"]
    lines.append("## Label totals\n")
    lines.extend(f"- `{label}`: {count}" for label, count in sorted(totals.items()))
    lines.append("\n## Counts per door and seat\n")
    all_labels = sorted(totals)
    lines.append("| door | seat | cells | " + " | ".join(all_labels) + " |")
    lines.append("|---|---|---|" + "---|" * len(all_labels))
    for door, seat in sorted(by_group, key=group_order):
        counter = by_group[(door, seat)]
        cells = sum(
            1
            for record in repark_cells.values()
            if (record["door"], record["seat"]) == (door, seat)
        )
        counts = " | ".join(str(counter.get(label, 0)) for label in all_labels)
        lines.append(f"| {door} | {seat} | {cells} | {counts} |")
    lines.append("\n## Non-EQUAL labels per predicate\n")
    lines.append("| pid | predicate | labels |")
    lines.append("|---|---|---|")
    for pid, text, _ in PREDICATES:
        counter = by_predicate.get(pid, Counter())
        detail = ", ".join(f"{label} {count}" for label, count in sorted(counter.items()))
        lines.append(f"| {pid} | `{markdown_cell(text)}` | {detail or 'all EQUAL'} |")
    lines.extend(information_sections(repark_cells, spark_cells))
    return "\n".join(lines) + "\n"


def information_sections(
    repark_cells: dict[str, dict[str, Any]], spark_cells: dict[str, dict[str, Any]]
) -> list[str]:
    """Return the informational sections: folded predicates and native select against Spark."""
    folded = sorted(folded_predicates(spark_cells))
    lines = ["\n## Predicates Spark folds to an empty relation\n"]
    lines.append(", ".join(f"`{pid}`" for pid in folded) or "none")
    lines.append("\n## Native select against Spark select (information, not a label)\n")
    lines.append("| pid | native spelling | native | Spark |")
    lines.append("|---|---|---|---|")
    for pid, _, _ in PREDICATES:
        native = repark_cells.get(f"native|select|v2-cow|{pid}")
        oracle = spark_cells.get(f"spark-sql|select|v2-cow|{pid}")
        if native is None or oracle is None:
            continue
        both = native["outcome"] == ANSWERED and oracle["outcome"] == ANSWERED
        if both and native["ids"] == oracle["ids"]:
            continue
        lines.append(
            f"| {pid} | `{markdown_cell(native['spelling'])}` | {outcome_text(native)} "
            f"| {outcome_text(oracle)} |"
        )
    return lines


def group_order(pair: tuple[str, str]) -> tuple[int, int]:
    """Order a (door, seat) pair."""
    doors = ("spark-sql", "filter-str", "f-expr", "native")
    return (doors.index(pair[0]), SEATS.index(pair[1]))


def detail_lines(
    repark_cells: dict[str, dict[str, Any]],
    spark_cells: dict[str, dict[str, Any]],
    labels: dict[str, list[str]],
) -> str:
    """Return one line per finding cell."""
    skip = {"EQUAL", "BOTH-REFUSED", "NOT-EXPRESSIBLE", "ORACLE-MISSING", "NATIVE-REFUSED"}
    lines = ["# Finding cells\n"]
    for key, record in sorted(repark_cells.items(), key=lambda item: order_key(item[1])):
        if set(labels[key]) <= skip:
            continue
        oracle = spark_cells.get(key)
        spark_text = "-" if record["door"] == "native" else outcome_text(oracle)
        lines.append(
            f"- {record['pid']} `{record['predicate']}` | {record['seat']} | {record['door']} | "
            f"{record['kind']} | {', '.join(labels[key])} | Spark: {spark_text} | "
            f"RePark: {outcome_text(record)} | "
            f"own select {ids_text((record['pre_select'] or {}).get('ids'))}"
        )
    return "\n".join(lines) + "\n"


def render(arguments: argparse.Namespace) -> int:
    """Read the leg files and write the table, the summary and the detail lines."""
    spark_headers, spark_cells = load_many(arguments.spark)
    repark_headers, repark_cells = load_many(arguments.main)
    labels = label_all(repark_cells, spark_cells)
    out = Path(arguments.tables_dir)
    (out / "envelope-table.md").write_text(
        render_table(repark_cells, spark_cells, labels), encoding="utf-8"
    )
    summary = render_summary(
        repark_cells, spark_cells, labels, {"spark": spark_headers, "repark": repark_headers}
    )
    (out / "summary.md").write_text(summary, encoding="utf-8")
    detail = detail_lines(repark_cells, spark_cells, labels)
    (out / "findings.md").write_text(detail, encoding="utf-8")
    if arguments.detail:
        Path(arguments.detail).write_text(detail, encoding="utf-8")
    totals: Counter[str] = Counter()
    for entry in labels.values():
        totals.update(entry)
    print(dict(sorted(totals.items())))
    return 0


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--spark", nargs="+")
    parser.add_argument("--main", nargs="+")
    parser.add_argument("--tables-dir")
    parser.add_argument("--detail")
    parser.add_argument("--engine", choices=("spark", "repark"))
    parser.add_argument("--label")
    parser.add_argument("--warehouse")
    parser.add_argument("--out")
    parser.add_argument("--only", nargs="+")
    parser.add_argument("--pids", nargs="+")
    parser.add_argument("--ivy-jar")
    parser.add_argument("--wheel")
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run one leg shard, or render the tables when --render is given."""
    arguments = parse_arguments(argv)
    if arguments.render:
        return render(arguments)
    if arguments.plan_only:
        specs = build_plan(arguments.engine or "repark", arguments.only, arguments.pids)
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
    specs = build_plan(arguments.engine, arguments.only, arguments.pids)
    if any(spec["door"] == "native" for spec in specs):
        context["spellings"] = resolve_native_spellings(context, context["doors"]["native"])
    out = Path(arguments.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    outcomes: Counter[str] = Counter()
    with out.open("w", encoding="utf-8") as handle:
        context["header"]["setup_templates"] = setup_templates(context)
        context["header"]["spelling_resolution"] = context.get("spellings")
        write_line(handle, context["header"], context["scrub"])
        for spec in specs:
            record = run_cell(context, spec)
            outcomes[record["outcome"]] += 1
            write_line(handle, record, context["scrub"])
            handle.flush()
    context["close"]()
    print(f"cells: {len(specs)} outcomes: {dict(sorted(outcomes.items()))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
