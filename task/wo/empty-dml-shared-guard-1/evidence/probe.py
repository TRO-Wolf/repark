"""Step 0.3 probe: the empty-DML and struct-comparison matrices on one engine leg.

One invocation runs one leg and writes a leg header line, then one JSON line per cell. The
--render mode reads the three leg files and writes the markdown tables, the label counts and the
detail lines. Measurement only.
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
from pathlib import Path
from typing import Any

ANSWERED = "ANSWERED"
REFUSED = "REFUSED"
PANIC = "PANIC"
SETUP_REFUSED = "SETUP-REFUSED"
ORACLE_MISSING = "ORACLE-MISSING"
MATCHES = "MATCHES-PREDICTION"
DIFFERS = "DIFFERS-FROM-PREDICTION"
UNPREDICTED = "UNPREDICTED"
SPARK_CATALOG = "probe_ice"
REPARK_CATALOG = "ice"
NAMESPACE = "ns"
KINDS = ("v2-cow", "v2-mor", "v3")
WRITE_KEYS = ("write.delete.mode", "write.update.mode")
TABLE_MODES = {
    "v2-cow": ("2", "copy-on-write"),
    "v2-mor": ("2", "merge-on-read"),
    "v3": ("3", "copy-on-write"),
}
MAIN_COLUMNS = "id BIGINT NOT NULL, prim_col INT, flag STRING, st STRUCT<x: INT>, arr ARRAY<INT>"
UUID_COLUMNS = MAIN_COLUMNS + ", uuid_col UUID"
MAIN_SELECT = "id, prim_col, flag, st, arr"
UUID_SELECT = MAIN_SELECT + ", uuid_col"
LINEAGE_SELECT = "id, _row_id, _last_updated_sequence_number"
NATIVE_SEED_ROWS = (
    "(1, 10, 'a', named_struct('x', 1), ARRAY[1]{uuid})",
    "(2, 20, 'b', named_struct('x', 2), ARRAY[2]{uuid})",
    "(3, NULL, 'c', NULL, NULL{uuid})",
)
FACADE_SEED_ROWS = (
    "(1, 10, 'a', named_struct('x', 1), array(1){uuid})",
    "(2, 20, 'b', named_struct('x', 2), array(2){uuid})",
    "(3, NULL, 'c', NULL, NULL{uuid})",
)
UUID_VALUE = "'00000000-0000-0000-0000-00000000000{number}'"
PREDICATES = (
    ("a-false", "false"),
    ("a-null", "NULL"),
    ("b-struct-null", "st = CAST(NULL AS STRUCT<x: INT>)"),
    ("b-arr-and-false", "arr[1] = 1 AND 1 = 0"),
    ("u-required-is-null", "id IS NULL"),
    ("c-one-eq-zero", "1 = 0"),
    ("c-prim-eq-null", "prim_col = NULL"),
    ("c-true", "true"),
    ("c-no-where", None),
)
UUID_PREDICATE = ("b-uuid-null", "uuid_col = NULL")
IDENTITY_CELL = "c-identity-update"
IDENTITY_TEMPLATE = "UPDATE {table} SET flag = flag WHERE id = 1"
WRONG_WRITE = frozenset({"a-false", "a-null", "b-struct-null", "b-arr-and-false", "b-uuid-null"})
NO_CHANGE = frozenset({"c-one-eq-zero", "c-prim-eq-null"})
WHERE_ALL = frozenset({"c-true", "c-no-where"})
STRUCT_CELLS = (
    ("s-int-vs-bigint", "struct(id) = struct(1)"),
    ("s-same-type-diff-name", "struct(id) = struct(CAST(1 AS BIGINT))"),
    ("s-ctrl-self", "struct(id) = struct(id)"),
    ("s-ctrl-named", "named_struct('id', id) = named_struct('id', CAST(1 AS BIGINT))"),
)
STRUCT_ROUTES = ("sql", "filter-str", "f-expr", "typed", "native")
UNION_CELLS = ("u-read", "u-insert")
UNION_ROUTES = ("sql", "native")
STRUCT_SEED = "(1, 'a'), (2, 'b')"
UNION_READ_SQL = (
    "SELECT s FROM (SELECT named_struct('a', 1, 'b', 2) AS s UNION ALL "
    "SELECT named_struct('b', 3, 'a', 4) AS s) q"
)
UNION_INSERT_SQL = (
    "INSERT INTO {table} SELECT named_struct('a', 1, 'b', 2) AS s UNION ALL "
    "SELECT named_struct('b', 3, 'a', 4) AS s"
)
TYPED_TEXT = {
    "s-int-vs-bigint": 'F.struct("id") == F.struct(F.lit(1))',
    "s-same-type-diff-name": 'F.struct("id") == F.struct(F.lit(1).cast("bigint"))',
    "s-ctrl-self": 'F.struct("id") == F.struct("id")',
    "s-ctrl-named": (
        'F.struct(F.col("id").alias("id")) == F.struct(F.lit(1).cast("bigint").alias("id"))'
    ),
}
TYPED_BUILDERS = {
    "s-int-vs-bigint": lambda f: f.struct("id") == f.struct(f.lit(1)),
    "s-same-type-diff-name": lambda f: f.struct("id") == f.struct(f.lit(1).cast("bigint")),
    "s-ctrl-self": lambda f: f.struct("id") == f.struct("id"),
    "s-ctrl-named": lambda f: (
        f.struct(f.col("id").alias("id")) == f.struct(f.lit(1).cast("bigint").alias("id"))
    ),
}
ROW_1A = {"id": 1, "v": "a"}
ROW_2B = {"id": 2, "v": "b"}
ACCEPT_1A = ("accepted, row (1, 'a') inserted again", {"outcome": ANSWERED, "inserted": [ROW_1A]})
ACCEPT_BOTH = (
    "accepted, both rows inserted again",
    {"outcome": ANSWERED, "inserted": [ROW_1A, ROW_2B]},
)
ACCEPT_ROW = ("accepted, row inserted", {"outcome": ANSWERED, "inserted": [ROW_1A]})
REFUSE_TYPE = (
    "refused at analysis (data type mismatch), table unchanged",
    {"outcome": REFUSED},
)
REFUSE_NAMES = (
    "refused at analysis (field names differ), table unchanged; hypothesis, unmeasured",
    {"outcome": REFUSED},
)
STRUCT_PREDICTION = {
    "s-int-vs-bigint": (ACCEPT_1A, REFUSE_TYPE),
    "s-same-type-diff-name": (ACCEPT_ROW, REFUSE_NAMES),
    "s-ctrl-self": (ACCEPT_BOTH, ACCEPT_BOTH),
    "s-ctrl-named": (ACCEPT_1A, ACCEPT_1A),
}
UNION_REPARK_ROWS = [{"s": {"a": 1, "b": 2}}, {"s": {"a": 4, "b": 3}}]
UNION_SPARK_ROWS = [{"s": {"a": 1, "b": 2}}, {"s": {"a": 3, "b": 4}}]
UNION_PREDICTION = {
    "u-read": (
        (
            "rows {a:1,b:2} and {a:4,b:3} (fields paired by name)",
            {"outcome": ANSWERED, "result": UNION_REPARK_ROWS},
        ),
        (
            "rows {a:1,b:2} and {a:3,b:4} (fields paired by position)",
            {"outcome": ANSWERED, "result": UNION_SPARK_ROWS},
        ),
    ),
    "u-insert": (
        (
            "persisted as u-read predicts",
            {"outcome": ANSWERED, "persisted_after": UNION_REPARK_ROWS},
        ),
        (
            "persisted as u-read predicts",
            {"outcome": ANSWERED, "persisted_after": UNION_SPARK_ROWS},
        ),
    ),
}
DOOR_ORDER = ("native", "facade", "sql")
STRUCT_ROUTE_ORDER = ("sql", "filter-str", "f-expr", "typed", "native")
DETAIL_LABELS = frozenset(
    {
        "ROWS-DIFFER",
        "EXTRA-COMMIT",
        "LINEAGE-DIFFER",
        "ACCEPTED-WHERE-SPARK-REFUSED",
        "REFUSED-WHERE-SPARK-ACCEPTED",
        "PANIC",
    }
)


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


def canon(value: Any) -> str:
    """Return a canonical JSON string for comparisons."""
    return json.dumps(value, sort_keys=True, default=str)


def sort_rows(rows: list[Any]) -> list[Any]:
    """Sort rows by their canonical JSON."""
    return sorted(rows, key=canon)


def canon_rows(rows: list[Any] | None) -> list[str]:
    """Return the canonical JSON of each row in sorted order."""
    return [canon(row) for row in sort_rows(rows or [])]


def sha256_of(path: Path) -> str:
    """Return the sha256 hex digest of a file."""
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def props_text(kind: str) -> str:
    """Return the TBLPROPERTIES body for a table kind."""
    version, mode = TABLE_MODES[kind]
    return (
        f"'format-version' = '{version}', 'write.delete.mode' = '{mode}', "
        f"'write.update.mode' = '{mode}'"
    )


def create_sql(door: dict[str, Any], qualified: str, columns: str, kind: str) -> str:
    """Return the CREATE TABLE text a door is given for a table kind."""
    using = " USING iceberg" if door["using_iceberg"] else ""
    clause = ""
    if door["accepts_properties"] or kind != "v2-cow":
        clause = f" TBLPROPERTIES ({props_text(kind)})"
    return f"CREATE TABLE {qualified} ({columns}){using}{clause}"


def seed_sql(door: dict[str, Any], qualified: str, uuid: bool) -> str:
    """Return the single INSERT ... VALUES that seeds a matrix-1 table."""
    templates = NATIVE_SEED_ROWS if door["name"] == "native" else FACADE_SEED_ROWS
    rows = [
        template.format(uuid=f", {UUID_VALUE.format(number=index)}" if uuid else "")
        for index, template in enumerate(templates, start=1)
    ]
    return f"INSERT INTO {qualified} VALUES " + ", ".join(rows)


def execute_sql(door: dict[str, Any], text: str) -> dict[str, Any]:
    """Run one statement on a door and return its Arrow schema and rows."""
    frame = door["sql"](text)
    table = frame.toArrow() if hasattr(frame, "toArrow") else frame.to_arrow()
    return {"schema": str(table.schema), "rows": table.to_pylist()}


def excerpt(text: str, limit: int = 300) -> str:
    """Return the first ``limit`` characters, cut back to a word boundary."""
    if len(text) <= limit:
        return text
    head = text[:limit]
    boundary = head.rfind(" ")
    return head[:boundary] if boundary > 0 else head


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


def metadata_order(path: Path) -> tuple[int, str]:
    """Order metadata files by their leading version number."""
    match = re.match(r"v?(\d+)", path.name)
    return (int(match.group(1)) if match else -1, path.name)


def read_metadata(table_dir: Path) -> dict[str, Any]:
    """Read the newest metadata JSON of a table: snapshots, format and write modes."""
    candidates = sorted((table_dir / "metadata").glob("*.metadata.json"), key=metadata_order)
    if not candidates:
        return {
            "metadata_file": None,
            "format_version": None,
            "snapshot_count": 0,
            "current_snapshot_id": None,
            "write_properties": {},
        }
    newest = candidates[-1]
    document = json.loads(newest.read_text(encoding="utf-8"))
    properties = document.get("properties") or {}
    return {
        "metadata_file": newest.name,
        "format_version": document.get("format-version"),
        "snapshot_count": len(document.get("snapshots") or []),
        "current_snapshot_id": document.get("current-snapshot-id"),
        "write_properties": {key: properties[key] for key in WRITE_KEYS if key in properties},
    }


def list_data_files(table_dir: Path) -> list[str]:
    """List the files under a table's data directory, relative to it."""
    data = table_dir / "data"
    if not data.is_dir():
        return []
    return sorted(str(path.relative_to(data)) for path in data.rglob("*") if path.is_file())


def observe(
    ctx: dict[str, Any],
    door: dict[str, Any],
    qualified: str,
    table_dir: Path,
    select_sql: str,
    with_lineage: bool,
) -> dict[str, Any]:
    """Read a table's metadata, files, rows and, for v3, row lineage."""
    state = read_metadata(table_dir)
    state["persisted"] = execute_sql(door, select_sql)["rows"]
    state["files"] = list_data_files(table_dir)
    state["lineage"] = None
    state["lineage_error"] = None
    if with_lineage:
        rows, error = attempt(
            ctx,
            lambda: execute_sql(door, f"SELECT {LINEAGE_SELECT} FROM {qualified} ORDER BY id")[
                "rows"
            ],
            "lineage",
        )
        state["lineage_error"] = error
        if error is None:
            state["lineage"] = [[row["id"], row["_last_updated_sequence_number"]] for row in rows]
    return state


def snapshot_of(state: dict[str, Any]) -> dict[str, Any]:
    """Return the snapshot identity of a table state."""
    return {"count": state["snapshot_count"], "current": state["current_snapshot_id"]}


def strip_flag(row: dict[str, Any]) -> dict[str, Any]:
    """Return a row without its flag column."""
    return {key: value for key, value in row.items() if key != "flag"}


def classify_rows(before: list[dict[str, Any]], after: list[dict[str, Any]]) -> str:
    """Classify a statement's effect on the seeded rows."""
    if after == before:
        return "unchanged"
    if after == []:
        return "all-deleted"
    same_ids = [row["id"] for row in before] == [row["id"] for row in after]
    if (
        same_ids
        and all(row["flag"] == "z" for row in after)
        and all(strip_flag(b) == strip_flag(a) for b, a in zip(before, after, strict=True))
    ):
        return "all-flag-z"
    return "other"


def lineage_change(
    before_pairs: list[list[Any]] | None, after_pairs: list[list[Any]] | None
) -> tuple[str, list[Any] | None]:
    """Return the lineage class and the ids whose sequence number changed."""
    if before_pairs is None or after_pairs is None:
        return "unreadable", None
    before_map = dict(before_pairs)
    after_map = dict(after_pairs)
    changed = [key for key in sorted(before_map) if after_map.get(key) != before_map[key]]
    if not changed:
        return "none", changed
    if len(changed) == len(before_map):
        return "all", changed
    return "some", changed


def empty_dml_facts(before: dict[str, Any], after: dict[str, Any], kind: str) -> dict[str, Any]:
    """Return the measured facts of one empty-DML cell."""
    lineage, changed = (None, None)
    if kind == "v3":
        lineage, changed = lineage_change(before["lineage"], after["lineage"])
    return {
        "rows": classify_rows(before["persisted"], after["persisted"]),
        "persisted_changed": before["persisted"] != after["persisted"],
        "snapshot_changed": snapshot_of(before) != snapshot_of(after),
        "files_changed": sorted(before["files"]) != sorted(after["files"]),
        "files_added": len(set(after["files"]) - set(before["files"])),
        "lineage": lineage,
        "lineage_changed_ids": changed,
    }


def table_facts(
    before: list[Any], after: list[Any], before_state: dict[str, Any], after_state: dict[str, Any]
) -> dict[str, Any]:
    """Return the facts of one struct-matrix table cell."""
    return {
        "rows": None,
        "persisted_changed": before != after,
        "snapshot_changed": snapshot_of(before_state) != snapshot_of(after_state),
        "files_changed": sorted(before_state["files"]) != sorted(after_state["files"]),
        "files_added": len(set(after_state["files"]) - set(before_state["files"])),
        "lineage": None,
        "lineage_changed_ids": None,
    }


def base_record(ctx: dict[str, Any], spec: dict[str, Any], qualified: str) -> dict[str, Any]:
    """Return a cell record with every field present."""
    return {
        "record": "cell",
        "engine": ctx["engine"],
        "leg": ctx["label"],
        "matrix": spec["matrix"],
        "cell": spec["cell"],
        "verb": spec["verb"],
        "kind": spec["kind"],
        "uuid": spec["uuid"],
        "place": spec["place"],
        "door": spec["door"],
        "join": spec["join"],
        "table": qualified,
        "ddl": None,
        "seed": None,
        "statement": None,
        "outcome": None,
        "error": None,
        "result": None,
        "persisted_before": None,
        "persisted_after": None,
        "snapshot_before": None,
        "snapshot_after": None,
        "files_before": None,
        "files_after": None,
        "lineage_before": None,
        "lineage_after": None,
        "facts": None,
        "prediction": None,
    }


def finish_setup(record: dict[str, Any], error: dict[str, Any]) -> dict[str, Any]:
    """Close a cell whose table could not be created or seeded."""
    record.update(outcome=SETUP_REFUSED, error=error)
    record["prediction"]["match"] = UNPREDICTED
    return record


def finish_oracle(record: dict[str, Any], reason: str) -> dict[str, Any]:
    """Close a cell that has no Spark counterpart."""
    record.update(
        outcome=ORACLE_MISSING,
        error={"class": None, "stage": None, "message": reason, "panic": False},
    )
    record["prediction"]["match"] = UNPREDICTED
    return record


def not_measured() -> dict[str, Any]:
    """Return the prediction stub for a cell that is not measured on this engine."""
    return {"text": "not measured on this engine", "expect": None, "match": None}


def empty_dml_spark(cell: str, verb: str, kind: str) -> tuple[str, dict[str, Any] | None]:
    """Return the Spark prediction for a matrix-1 cell."""
    if cell == IDENTITY_CELL:
        return "measure", None
    text = "no row deleted, no new snapshot" if verb == "DELETE" else "no change, no new snapshot"
    expect = {
        "outcome": ANSWERED,
        "rows": "unchanged",
        "snapshot_changed": False,
        "files_changed": False,
    }
    if kind == "v3":
        expect["lineage"] = "none"
    return text, expect


def empty_dml_repark(
    cell: str, verb: str, kind: str, place: str
) -> tuple[str, dict[str, Any] | None]:
    """Return the RePark prediction for a matrix-1 cell."""
    if cell == IDENTITY_CELL:
        return "unknown", None
    if cell == "u-required-is-null" and verb == "DELETE":
        if place == "native":
            return "no change (metadata-delete route)", {
                "outcome": ANSWERED,
                "rows": "unchanged",
                "snapshot_changed": False,
            }
        return "unknown", None
    if cell in WHERE_ALL:
        if verb == "DELETE":
            return "DELETE removes every row", {"outcome": ANSWERED, "rows": "all-deleted"}
        return "UPDATE sets every flag to 'z'", {"outcome": ANSWERED, "rows": "all-flag-z"}
    if cell in NO_CHANGE:
        return "no change, no new snapshot", {
            "outcome": ANSWERED,
            "rows": "unchanged",
            "snapshot_changed": False,
            "files_changed": False,
        }
    if verb == "DELETE":
        return "every row deleted, new snapshot (wrong write)", {
            "outcome": ANSWERED,
            "rows": "all-deleted",
            "snapshot_changed": True,
        }
    text = "row values unchanged, new snapshot, files rewritten"
    if cell == "u-required-is-null":
        text = "as the UPDATE row above (unprobed inference)"
    expect = {
        "outcome": ANSWERED,
        "rows": "unchanged",
        "snapshot_changed": True,
        "files_changed": True,
    }
    if kind == "v3":
        expect["lineage"] = "all"
        text += "; every row's lineage advances"
    return text, expect


def empty_dml_prediction(engine: str, spec: dict[str, Any]) -> dict[str, Any]:
    """Return the prediction record of a matrix-1 cell on the given engine."""
    if engine == "spark" and spec["cell"] not in WHERE_ALL:
        text, expect = empty_dml_spark(spec["cell"], spec["verb"], spec["kind"])
    else:
        text, expect = empty_dml_repark(spec["cell"], spec["verb"], spec["kind"], spec["place"])
    return {"text": text, "expect": expect, "match": None}


def match_empty_dml(outcome: str, facts: dict[str, Any], expect: dict[str, Any] | None) -> str:
    """Compare one matrix-1 measurement with its prediction."""
    if expect is None or outcome in (SETUP_REFUSED, ORACLE_MISSING):
        return UNPREDICTED
    if expect["outcome"] != outcome:
        return DIFFERS
    measured = {
        "rows": facts["rows"],
        "snapshot_changed": facts["snapshot_changed"],
        "files_changed": facts["files_changed"],
        "lineage": facts["lineage"],
    }
    for key, value in measured.items():
        if key in expect and expect[key] != value:
            return DIFFERS
    return MATCHES


def empty_dml_specs(engine: str, only: str | None) -> list[dict[str, Any]]:
    """Return the matrix-1 cells in run order."""
    places = ("native", "facade") if engine == "repark" else ("sql",)
    specs: list[dict[str, Any]] = []
    for kind in KINDS:
        if only not in (None, kind):
            continue
        for place in places:
            for cell, predicate in PREDICATES:
                for verb in ("DELETE", "UPDATE"):
                    clause = "" if predicate is None else f" WHERE {predicate}"
                    if verb == "DELETE":
                        template = f"DELETE FROM {{table}}{clause}"
                    else:
                        template = f"UPDATE {{table}} SET flag = 'z'{clause}"
                    specs.append(empty_dml_spec(kind, place, cell, verb, False, template))
            specs.append(
                empty_dml_spec(kind, place, IDENTITY_CELL, "UPDATE", False, IDENTITY_TEMPLATE)
            )
            for verb in ("DELETE", "UPDATE"):
                clause = f" WHERE {UUID_PREDICATE[1]}"
                if verb == "DELETE":
                    template = f"DELETE FROM {{table}}{clause}"
                else:
                    template = f"UPDATE {{table}} SET flag = 'z'{clause}"
                specs.append(empty_dml_spec(kind, place, UUID_PREDICATE[0], verb, True, template))
    return specs


def empty_dml_spec(
    kind: str, place: str, cell: str, verb: str, uuid: bool, template: str
) -> dict[str, Any]:
    """Return one matrix-1 cell specification."""
    uuid_tag = "uuid" if uuid else "main"
    return {
        "matrix": "empty-dml",
        "cell": cell,
        "verb": verb,
        "kind": kind,
        "uuid": uuid,
        "place": place,
        "door": place,
        "join": f"empty-dml|{cell}|{verb}|{kind}|{uuid_tag}",
        "table": sanitize(f"t_{cell}_{verb}_{kind}_{uuid_tag}_{place}"),
        "template": template,
    }


def run_empty_dml(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """Run one matrix-1 cell on a fresh table and return its record."""
    door = ctx["doors"][spec["door"]]
    qualified = f"{door['catalog']}.{NAMESPACE}.{spec['table']}"
    record = base_record(ctx, spec, qualified)
    record["prediction"] = empty_dml_prediction(ctx["engine"], spec)
    if ctx["engine"] == "spark" and spec["uuid"]:
        record["prediction"] = not_measured()
        return finish_oracle(record, "Spark SQL cannot declare an Iceberg uuid column")
    columns = UUID_COLUMNS if spec["uuid"] else MAIN_COLUMNS
    select_sql = (
        f"SELECT {UUID_SELECT if spec['uuid'] else MAIN_SELECT} FROM {qualified} ORDER BY id"
    )
    table_dir = door["root"] / NAMESPACE / spec["table"]
    ddl = create_sql(door, qualified, columns, spec["kind"])
    record["ddl"] = ddl
    _, error = attempt(ctx, lambda: execute_sql(door, ddl), "create")
    if error is None:
        seed = seed_sql(door, qualified, spec["uuid"])
        record["seed"] = seed
        _, error = attempt(ctx, lambda: execute_sql(door, seed), "seed")
    if error is not None:
        return finish_setup(record, error)
    with_lineage = spec["kind"] == "v3"
    before = observe(ctx, door, qualified, table_dir, select_sql, with_lineage)
    statement = spec["template"].replace("{table}", qualified)
    record["statement"] = statement
    result, error = attempt(ctx, lambda: execute_sql(door, statement), "sql")
    after = observe(ctx, door, qualified, table_dir, select_sql, with_lineage)
    facts = empty_dml_facts(before, after, spec["kind"])
    record.update(
        outcome=outcome_of(error),
        error=error,
        result=result,
        persisted_before=before["persisted"],
        persisted_after=after["persisted"],
        snapshot_before=snapshot_of(before),
        snapshot_after=snapshot_of(after),
        files_before=before["files"],
        files_after=after["files"],
        lineage_before=before["lineage"],
        lineage_after=after["lineage"],
        facts=facts,
    )
    record["prediction"]["match"] = match_empty_dml(
        record["outcome"], facts, record["prediction"]["expect"]
    )
    return record


def struct_specs(engine: str, only: str | None) -> list[dict[str, Any]]:
    """Return the matrix-2 cells in run order."""
    if only not in (None, "v2-cow"):
        return []
    specs: list[dict[str, Any]] = []
    for cell, predicate in STRUCT_CELLS:
        for route in STRUCT_ROUTES:
            specs.append(struct_spec(engine, cell, route, "INSERT", predicate))
    for cell in UNION_CELLS:
        verb = "SELECT" if cell == "u-read" else "INSERT"
        for route in UNION_ROUTES:
            specs.append(struct_spec(engine, cell, route, verb, None))
    return specs


def route_door(engine: str, route: str) -> str:
    """Return the door a matrix-2 route runs on."""
    if route == "native":
        return "native"
    return "sql" if engine == "spark" else "facade"


def struct_spec(
    engine: str, cell: str, route: str, verb: str, predicate: str | None
) -> dict[str, Any]:
    """Return one matrix-2 cell specification."""
    door = route_door(engine, route)
    table = "" if verb == "SELECT" else sanitize(f"{cell}_{route}_{door}")
    return {
        "matrix": "struct",
        "cell": cell,
        "verb": verb,
        "kind": "v2-cow",
        "uuid": False,
        "place": route,
        "route": route,
        "door": door,
        "predicate": predicate,
        "join": f"struct|{cell}|{route}|v2-cow",
        "table": table,
    }


def struct_prediction(engine: str, spec: dict[str, Any]) -> dict[str, Any]:
    """Return the prediction record of a matrix-2 cell on the given engine."""
    index = 0 if engine == "repark" else 1
    table = UNION_PREDICTION if spec["cell"] in UNION_PREDICTION else STRUCT_PREDICTION
    text, expect = table[spec["cell"]][index]
    return {"text": text, "expect": expect, "match": None}


def match_struct(
    outcome: str,
    expect: dict[str, Any] | None,
    before_rows: list[Any] | None,
    after_rows: list[Any] | None,
    snapshot_changed: bool | None,
    result_rows: list[Any] | None,
) -> str:
    """Compare one matrix-2 measurement with its prediction."""
    if expect is None or outcome == SETUP_REFUSED:
        return UNPREDICTED
    if expect["outcome"] != outcome:
        return DIFFERS
    if "inserted" in expect:
        wanted = canon_rows((before_rows or []) + expect["inserted"])
        if canon_rows(after_rows) != wanted:
            return DIFFERS
    if "persisted_after" in expect and canon_rows(after_rows) != canon_rows(
        expect["persisted_after"]
    ):
        return DIFFERS
    if outcome == REFUSED and (before_rows != after_rows or snapshot_changed):
        return DIFFERS
    if "result" in expect and canon_rows(result_rows) != canon_rows(expect["result"]):
        return DIFFERS
    return MATCHES


def filter_argument(spec: dict[str, Any], functions: Any) -> Any:
    """Return the filter argument a DataFrame route passes to filter()."""
    if spec["route"] == "filter-str":
        return spec["predicate"]
    if spec["route"] == "f-expr":
        return functions.expr(spec["predicate"])
    return TYPED_BUILDERS[spec["cell"]](functions)


def route_text(spec: dict[str, Any], qualified: str) -> str:
    """Return a readable statement text for a DataFrame route."""
    if spec["route"] == "filter-str":
        argument = f'"{spec["predicate"]}"'
    elif spec["route"] == "f-expr":
        argument = f'F.expr("{spec["predicate"]}")'
    else:
        argument = TYPED_TEXT[spec["cell"]]
    return f"table({qualified}).filter({argument}).write.insertInto({qualified})"


def run_union_read(
    ctx: dict[str, Any], spec: dict[str, Any], record: dict[str, Any]
) -> dict[str, Any]:
    """Run the u-read SELECT on the route's door."""
    door = ctx["doors"][spec["door"]]
    record["statement"] = UNION_READ_SQL
    result, error = attempt(ctx, lambda: execute_sql(door, UNION_READ_SQL), "sql")
    record.update(
        outcome=outcome_of(error),
        error=error,
        result=None
        if result is None
        else {"schema": result["schema"], "rows": sort_rows(result["rows"])},
    )
    record["prediction"]["match"] = match_struct(
        record["outcome"],
        record["prediction"]["expect"],
        None,
        None,
        None,
        record["result"]["rows"] if record["result"] else None,
    )
    return record


def run_struct(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """Run one matrix-2 cell and return its record."""
    if ctx["engine"] == "spark" and spec["route"] == "native":
        record = base_record(ctx, spec, "")
        record["prediction"] = not_measured()
        return finish_oracle(record, "the native door's reference is Trino, not Spark")
    door = ctx["doors"][spec["door"]]
    qualified = f"{door['catalog']}.{NAMESPACE}.{spec['table']}" if spec["table"] else ""
    record = base_record(ctx, spec, qualified)
    record["prediction"] = struct_prediction(ctx["engine"], spec)
    if spec["verb"] == "SELECT":
        return run_union_read(ctx, spec, record)
    columns = "s STRUCT<a: INT, b: INT>" if spec["cell"] == "u-insert" else "id BIGINT, v STRING"
    ddl = create_sql(door, qualified, columns, "v2-cow")
    record["ddl"] = ddl
    _, error = attempt(ctx, lambda: execute_sql(door, ddl), "create")
    if error is None and spec["cell"] != "u-insert":
        seed = f"INSERT INTO {qualified} VALUES {STRUCT_SEED}"
        record["seed"] = seed
        _, error = attempt(ctx, lambda: execute_sql(door, seed), "seed")
    if error is not None:
        return finish_setup(record, error)
    table_dir = door["root"] / NAMESPACE / spec["table"]
    select_sql = f"SELECT * FROM {qualified}"
    before = observe(ctx, door, qualified, table_dir, select_sql, False)
    result: Any = None
    if spec["route"] in ("sql", "native"):
        if spec["cell"] == "u-insert":
            statement = UNION_INSERT_SQL.format(table=qualified)
        else:
            statement = (
                f"INSERT INTO {qualified} SELECT * FROM {qualified} WHERE {spec['predicate']}"
            )
        record["statement"] = statement
        result, error = attempt(ctx, lambda: execute_sql(door, statement), "sql")
    else:
        record["statement"] = route_text(spec, qualified)
        frame, error = attempt(
            ctx,
            lambda: (
                door["session"].table(qualified).filter(filter_argument(spec, door["functions"]))
            ),
            "filter",
        )
        if error is None:
            _, error = attempt(ctx, lambda: frame.write.insertInto(qualified), "write")
    after = observe(ctx, door, qualified, table_dir, select_sql, False)
    before["persisted"] = sort_rows(before["persisted"])
    after["persisted"] = sort_rows(after["persisted"])
    facts = table_facts(before["persisted"], after["persisted"], before, after)
    result_view = None
    if result is not None:
        result_view = {"schema": result["schema"], "rows": sort_rows(result["rows"])}
    record.update(
        outcome=outcome_of(error),
        error=error,
        result=result_view,
        persisted_before=before["persisted"],
        persisted_after=after["persisted"],
        snapshot_before=snapshot_of(before),
        snapshot_after=snapshot_of(after),
        files_before=before["files"],
        files_after=after["files"],
        facts=facts,
    )
    record["prediction"]["match"] = match_struct(
        record["outcome"],
        record["prediction"]["expect"],
        before["persisted"],
        after["persisted"],
        facts["snapshot_changed"],
        result_view["rows"] if result_view else None,
    )
    return record


def plan(engine: str, matrix: str, only: str | None) -> list[dict[str, Any]]:
    """Return every cell specification the invocation runs, in order."""
    specs: list[dict[str, Any]] = []
    if matrix in ("empty-dml", "all"):
        specs.extend(empty_dml_specs(engine, only))
    if matrix in ("struct", "all"):
        specs.extend(struct_specs(engine, only))
    return specs


def run_spec(ctx: dict[str, Any], spec: dict[str, Any]) -> dict[str, Any]:
    """Run one cell specification."""
    if spec["matrix"] == "empty-dml":
        return run_empty_dml(ctx, spec)
    return run_struct(ctx, spec)


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
        .appName("probe-0-3")
        .config("spark.ui.enabled", "false")
        .config("spark.local.dir", str(warehouse / "spark-local"))
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
        "name": "sql",
        "sql": session.sql,
        "catalog": SPARK_CATALOG,
        "root": root,
        "session": session,
        "functions": spark_functions,
        "accepts_properties": True,
        "using_iceberg": True,
    }
    execute_sql(door, f"CREATE NAMESPACE IF NOT EXISTS {SPARK_CATALOG}.{NAMESPACE}")
    jar = Path(arguments.ivy_jar)
    header = {
        "record": "leg",
        "engine": "spark",
        "label": arguments.label,
        "spark_version": session.version,
        "jdk_version": session.sparkContext._jvm.System.getProperty("java.version"),
        "iceberg_jar": jar.name,
        "iceberg_jar_sha256": sha256_of(jar),
        "python": sys.version.split()[0],
        "venv": sys.prefix,
    }
    return {
        "engine": "spark",
        "label": arguments.label,
        "scrub": str(warehouse),
        "doors": {"sql": door},
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
    native = PyReparkSession.native()
    native.register_memory_catalog(REPARK_CATALOG, str(native_root))
    native.create_namespace(REPARK_CATALOG, NAMESPACE, str(native_root / NAMESPACE))
    repark._ANSI_NATIVE = native
    facade = (
        ReparkSession.builder.appName("probe-0-3")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    facade.register_memory_catalog(REPARK_CATALOG, str(facade_root))
    doors = {
        "native": {
            "name": "native",
            "sql": repark.sql,
            "catalog": REPARK_CATALOG,
            "root": native_root,
            "session": None,
            "functions": None,
            "accepts_properties": False,
            "using_iceberg": False,
        },
        "facade": {
            "name": "facade",
            "sql": facade.sql,
            "catalog": REPARK_CATALOG,
            "root": facade_root,
            "session": facade,
            "functions": repark_functions,
            "accepts_properties": True,
            "using_iceberg": True,
        },
    }
    context = {
        "engine": "repark",
        "label": arguments.label,
        "scrub": str(warehouse),
        "doors": doors,
    }
    wheel = Path(arguments.wheel)
    context["header"] = {
        "record": "leg",
        "engine": "repark",
        "label": arguments.label,
        "wheel_file": wheel.name,
        "wheel_sha256": sha256_of(wheel),
        "repark_file": repark.__file__,
        "repark_version": importlib.metadata.version("repark"),
        "pyarrow_version": pyarrow.__version__,
        "python": sys.version.split()[0],
        "venv": sys.prefix,
    }
    context["close"] = lambda: None
    facade_location = facade_root / NAMESPACE
    execute_sql(
        doors["facade"],
        f"CREATE NAMESPACE {REPARK_CATALOG}.{NAMESPACE} LOCATION '{facade_location}'",
    )
    return context


def build_context(arguments: argparse.Namespace, warehouse: Path) -> dict[str, Any]:
    """Build the engine context for one leg."""
    if arguments.engine == "spark":
        return build_spark_context(arguments, warehouse)
    return build_repark_context(arguments, warehouse)


def load_cells(path: str) -> list[dict[str, Any]]:
    """Read the cell records of one leg file."""
    cells: list[dict[str, Any]] = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        record = json.loads(line)
        if record.get("record") == "cell":
            cells.append(record)
    return cells


def index_cells(cells: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Index cell records by join key and place."""
    return {(cell["join"], cell["place"]): cell for cell in cells}


def place_rank(join: str, place: str) -> int:
    """Return the display rank of a door or route within its matrix."""
    order = STRUCT_ROUTE_ORDER if join.startswith("struct|") else DOOR_ORDER
    return order.index(place) if place in order else len(order)


def core_labels(spark: dict[str, Any] | None, repark: dict[str, Any] | None) -> list[str]:
    """Return the mechanical verdict labels of one RePark cell against its Spark cell."""
    if repark is None:
        return ["MISSING"]
    if repark["outcome"] == PANIC:
        return ["PANIC"]
    if repark["outcome"] == SETUP_REFUSED:
        return ["SETUP-REFUSED"]
    if spark is None or spark["outcome"] == ORACLE_MISSING:
        return ["ORACLE-MISSING"]
    if spark["outcome"] in (SETUP_REFUSED, PANIC):
        return ["SPARK-NOT-MEASURED"]
    repark_ok = repark["outcome"] == ANSWERED
    spark_ok = spark["outcome"] == ANSWERED
    if not repark_ok and not spark_ok:
        return ["BOTH-REFUSED"]
    if repark_ok and not spark_ok:
        return ["ACCEPTED-WHERE-SPARK-REFUSED"]
    if spark_ok and not repark_ok:
        return ["REFUSED-WHERE-SPARK-ACCEPTED"]
    labels: list[str] = []
    persisted_equal = canon(repark["persisted_after"]) == canon(spark["persisted_after"])
    repark_facts = repark["facts"] or {}
    spark_facts = spark["facts"] or {}
    if not persisted_equal:
        labels.append("ROWS-DIFFER")
    if (
        persisted_equal
        and repark_facts.get("snapshot_changed")
        and not spark_facts.get("snapshot_changed")
    ):
        labels.append("EXTRA-COMMIT")
    if repark_facts.get("lineage_changed_ids") != spark_facts.get("lineage_changed_ids"):
        labels.append("LINEAGE-DIFFER")
    repark_rows = (repark["result"] or {}).get("rows")
    spark_rows = (spark["result"] or {}).get("rows")
    if persisted_equal and canon(repark_rows) != canon(spark_rows):
        labels.append("REPORT-DIFFERS")
    if labels:
        return labels
    if bool(repark_facts.get("snapshot_changed")) == bool(spark_facts.get("snapshot_changed")):
        return ["EQUAL"]
    return ["UNLABELLED-DIFF"]


def short_measure(record: dict[str, Any] | None) -> str:
    """Return a short measured description of one cell."""
    if record is None:
        return "-"
    outcome = record["outcome"]
    if outcome == ORACLE_MISSING:
        return ORACLE_MISSING
    if outcome in (REFUSED, PANIC, SETUP_REFUSED):
        error = record["error"]
        return f"{outcome} ({error['stage']}): {error['class']}"
    parts = [ANSWERED]
    facts = record["facts"] or {}
    if facts.get("rows") is not None:
        parts.append(f"rows {facts['rows']}")
    if facts.get("persisted_changed") is not None:
        parts.append("persisted " + ("changed" if facts["persisted_changed"] else "same"))
    if facts.get("snapshot_changed") is not None:
        parts.append("snapshot " + ("changed" if facts["snapshot_changed"] else "same"))
    if facts.get("files_changed") is not None:
        parts.append("files " + ("changed" if facts["files_changed"] else "same"))
    if facts.get("lineage") is not None:
        parts.append(f"lineage {facts['lineage']}")
    if record["result"] is not None and record["result"]["rows"] is not None:
        parts.append(f"result {len(record['result']['rows'])} row(s)")
    return "; ".join(parts)


def markdown_cell(text: str) -> str:
    """Escape a value for a markdown table cell."""
    return text.replace("|", "/").replace("\n", " ")


def build_row(
    join: str,
    place: str,
    spark: dict[str, Any] | None,
    main: dict[str, Any] | None,
    pypi: dict[str, Any] | None,
) -> dict[str, Any]:
    """Assemble one rendered table row from the three legs."""
    reference = main or pypi
    spark_text = spark["prediction"]["text"] if spark else "no Spark cell"
    kind = reference["kind"] + (" + uuid" if reference["uuid"] else "")
    labels = {}
    for name, repark in (("main", main), ("pypi", pypi)):
        names = core_labels(spark, repark)
        if repark is not None:
            names = [*names, repark["prediction"]["match"]]
        labels[name] = names
    return {
        "join": join,
        "matrix": reference["matrix"],
        "cell": reference["cell"],
        "place": place,
        "verb": reference["verb"],
        "kind": kind,
        "predicted": f"RePark: {reference['prediction']['text']}; Spark: {spark_text}",
        "spark": short_measure(spark),
        "main": short_measure(main),
        "pypi": short_measure(pypi),
        "labels_main": labels["main"],
        "labels_pypi": labels["pypi"],
        "spark_record": spark,
        "main_record": main,
        "pypi_record": pypi,
    }


def markdown_table(title: str, rows: list[dict[str, Any]]) -> str:
    """Render the rows of one matrix as a markdown table."""
    lines = [
        f"# {title}",
        "",
        "Generated by `probe.py --render` from `raw/*.jsonl`; not edited by hand. Measured cells",
        "are short: outcome kind, rows, snapshot, files and lineage, or the refusal stage and",
        "class. The raw JSON lines carry the full detail.",
        "",
        "| cell | door or route | verb | table kind | predicted | Spark measured | "
        "main measured | 1.5.5 measured | labels for main | labels for 1.5.5 |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        cells = [
            row["cell"],
            row["place"],
            row["verb"],
            row["kind"],
            row["predicted"],
            row["spark"],
            row["main"],
            row["pypi"],
            ", ".join(row["labels_main"]),
            ", ".join(row["labels_pypi"]),
        ]
        lines.append("| " + " | ".join(markdown_cell(cell) for cell in cells) + " |")
    return "\n".join(lines) + "\n"


def detail_line(
    row: dict[str, Any], artifact: str, repark: dict[str, Any] | None, labels: list[str]
) -> str:
    """Return one detail line for a cell that carries a flagged label."""
    spark_outcome = row["spark_record"]["outcome"] if row["spark_record"] else "-"
    repark_text = short_measure(repark)
    if repark is not None and repark["error"] and repark["outcome"] != ANSWERED:
        repark_text += f" :: {repark['error']['message'][:160]}"
    return (
        f"{row['matrix']} | {row['cell']} | {row['place']} | {row['verb']} | {row['kind']} | "
        f"{artifact} | spark {spark_outcome} | repark {repark_text} | "
        f"{', '.join(labels)}"
    )


def render(arguments: argparse.Namespace) -> int:
    """Write the two markdown tables, print the label counts and the detail lines."""
    main_cells = load_cells(arguments.main)
    pypi_cells = load_cells(arguments.pypi)
    spark_index = index_cells(load_cells(arguments.spark))
    main_index = index_cells(main_cells)
    pypi_index = index_cells(pypi_cells)
    keys = sorted(
        {(cell["join"], cell["place"]) for cell in main_cells + pypi_cells},
        key=lambda key: (key[0], place_rank(key[0], key[1])),
    )
    rows = []
    for join, place in keys:
        spark_place = "sql" if join.startswith("empty-dml|") else place
        rows.append(
            build_row(
                join,
                place,
                spark_index.get((join, spark_place)),
                main_index.get((join, place)),
                pypi_index.get((join, place)),
            )
        )
    tables_dir = Path(arguments.tables_dir)
    tables_dir.mkdir(parents=True, exist_ok=True)
    empty_rows = [row for row in rows if row["matrix"] == "empty-dml"]
    struct_rows = [row for row in rows if row["matrix"] == "struct"]
    (tables_dir / "empty-dml-table.md").write_text(
        markdown_table("Empty-DML matrix (matrix 1)", empty_rows), encoding="utf-8"
    )
    (tables_dir / "struct-table.md").write_text(
        markdown_table("Struct comparison and UNION pairing (matrix 2)", struct_rows),
        encoding="utf-8",
    )
    counts = {
        artifact: {matrix: Counter() for matrix in ("empty-dml", "struct")}
        for artifact in ("main", "pypi")
    }
    detail: list[str] = []
    for row in rows:
        for artifact, repark_key in (("main", "main_record"), ("pypi", "pypi_record")):
            labels = row[f"labels_{artifact}"]
            counts[artifact][row["matrix"]].update(labels)
            repark = row[repark_key]
            flagged = [label for label in labels if label in DETAIL_LABELS]
            if flagged or DIFFERS in labels:
                detail.append(detail_line(row, artifact, repark, labels))
    for artifact in ("main", "pypi"):
        for matrix in ("empty-dml", "struct"):
            summary = ", ".join(
                f"{label}={count}" for label, count in sorted(counts[artifact][matrix].items())
            )
            print(f"{artifact} {matrix}: {summary}")
    if arguments.detail:
        Path(arguments.detail).write_text("\n".join(detail) + "\n", encoding="utf-8")
    print(f"detail lines: {len(detail)}")
    return 0


def parse_arguments(argv: list[str] | None) -> argparse.Namespace:
    """Parse the command line."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--render", action="store_true")
    parser.add_argument("--spark")
    parser.add_argument("--main")
    parser.add_argument("--pypi")
    parser.add_argument("--tables-dir")
    parser.add_argument("--detail")
    parser.add_argument("--engine", choices=("spark", "repark"))
    parser.add_argument("--label")
    parser.add_argument("--warehouse")
    parser.add_argument("--out")
    parser.add_argument("--matrix", choices=("empty-dml", "struct", "all"), default="all")
    parser.add_argument("--only", choices=KINDS)
    parser.add_argument("--ivy-jar")
    parser.add_argument("--wheel")
    parser.add_argument("--plan-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Run one leg, or render the tables when --render is given."""
    arguments = parse_arguments(argv)
    if arguments.render:
        return render(arguments)
    if arguments.plan_only:
        specs = plan(arguments.engine or "repark", arguments.matrix, arguments.only)
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
    warehouse.mkdir(parents=True)
    context = build_context(arguments, warehouse)
    specs = plan(arguments.engine, arguments.matrix, arguments.only)
    out = Path(arguments.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    outcomes: Counter[str] = Counter()
    header = context.get(
        "header", {"record": "leg", "engine": context["engine"], "label": context["label"]}
    )
    with out.open("w", encoding="utf-8") as handle:
        write_line(handle, header, context["scrub"])
        for spec in specs:
            record = run_spec(context, spec)
            outcomes[record["outcome"]] += 1
            write_line(handle, record, context["scrub"])
            handle.flush()
    context["close"]()
    print(f"cells: {len(specs)} outcomes: {dict(sorted(outcomes.items()))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
