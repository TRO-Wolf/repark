"""Record main's answers for CTAS statements whose body is a write, and the wrappers.

NOT a ``test_`` module: pytest never collects it. It owns the 54 cells of
WRITE-BODY-CARRIERS-1 step 0 on main: the 20 Spark cells (``CREATE TABLE z AS``
and ``CREATE OR REPLACE TABLE z AS`` over the five write bodies, ``z`` new and
existing) on format versions 2 and 3, ``EXPLAIN ANALYZE`` of each body on both
versions, and ``PREPARE`` / ``EXECUTE`` of INSERT and INSERT OVERWRITE on both
versions. Each cell carries the error (exception class, condition, SQLSTATE,
first line, full text) or the answer, plus the snapshot count, row count, rows
and data-file count of every table named, before and after the statement.

Run with the main release module (no build on this box)::

    _record_write_body_carriers_1_main.py [warehouse]

The warehouse is created fresh (removed first) and defaults to a scratch
directory. The run writes ``main-cells.json`` and ``main-cells.md`` into
``task/wo/write-body-carriers-1/evidence/``.
"""

from __future__ import annotations

import datetime
import json
import shutil
import sys
import tempfile
import warnings
from pathlib import Path
from typing import Any

from repark import ReparkSession

CATALOG = "wbc1"
NAMESPACE = "ns"
EVIDENCE = (
    Path(__file__).resolve().parents[3] / "task" / "wo" / "write-body-carriers-1" / "evidence"
)
JSON_NAME = "main-cells.json"
MARKDOWN_NAME = "main-cells.md"
BODIES: dict[str, str] = {
    "insert": "INSERT INTO {t} VALUES (3, 'c')",
    "overwrite": "INSERT OVERWRITE {t} VALUES (10, 'w')",
    "update": "UPDATE {t} SET v = 'x' WHERE id = 1",
    "delete": "DELETE FROM {t} WHERE id = 2",
    "merge": (
        "MERGE INTO {t} USING (SELECT 2 AS id, 'm' AS v UNION ALL SELECT 9 AS id, 'n' AS v) s "
        "ON {t}.id = s.id WHEN MATCHED THEN UPDATE SET v = s.v "
        "WHEN NOT MATCHED THEN INSERT (id, v) VALUES (s.id, s.v)"
    ),
}
CREATORS: dict[str, str] = {
    "create": "CREATE TABLE {z} AS {body}",
    "or_replace": "CREATE OR REPLACE TABLE {z} AS {body}",
}
ZSTATES: tuple[str, ...] = ("z_new", "z_exists")
VERSIONS: tuple[int, ...] = (2, 3)


def error_record(error: Exception) -> dict[str, Any]:
    """Capture one refusal as its class, condition, SQLSTATE and first line."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fields: dict[str, Any] = {}
        for key, attr in (
            ("error_class", "getErrorClass"),
            ("condition", "getCondition"),
            ("sqlstate", "getSqlState"),
        ):
            try:
                fields[key] = getattr(error, attr)()
            except Exception as access_error:
                fields[key] = type(access_error).__name__
    text = str(error)
    fields["first_line"] = next((line.strip() for line in text.splitlines() if line.strip()), "")
    fields["message"] = text[:4000]
    return {"class": type(error).__name__, **fields}


def table_exists(session: ReparkSession, table: str) -> bool:
    """Return whether one table answers a read."""
    try:
        session.sql(f"SELECT 1 AS one FROM {table} LIMIT 1").collect()
    except Exception as error:
        if "not found" in str(error).lower():
            return False
        raise
    return True


def table_state(session: ReparkSession, table: str) -> dict[str, Any]:
    """Return the snapshot, row and data-file counts plus the rows of one table."""
    if not table_exists(session, table):
        return {"exists": False}
    snapshots = session.sql(f"SELECT COUNT(*) AS n FROM {table}.snapshots").collect()[0]["n"]
    rows = [list(row) for row in session.sql(f"SELECT * FROM {table} ORDER BY id").collect()]
    files = session.sql(f"SELECT content, COUNT(*) AS n FROM {table}.files GROUP BY content")
    data_files = sum(row["n"] for row in files.collect() if row["content"] == 0)
    return {
        "exists": True,
        "snapshots": snapshots,
        "row_count": len(rows),
        "rows": rows,
        "data_files": data_files,
    }


def run_statement(session: ReparkSession, statement: str) -> dict[str, Any]:
    """Run one statement and return its answer or its refusal."""
    try:
        answer = session.sql(statement).collect()
    except Exception as error:
        return {"answered": False, **error_record(error)}
    return {"answered": True, "answer_rows": len(answer)}


def seed_table(session: ReparkSession, table: str, version: int, seed: str) -> None:
    """Create one versioned table and seed it."""
    session.sql(f"DROP TABLE IF EXISTS {table}")
    session.sql(
        f"CREATE TABLE {table} (id INT, v STRING) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')"
    )
    session.sql(f"INSERT INTO {table} VALUES {seed}")


def ctas_cell(
    session: ReparkSession, creator: str, body: str, zstate: str, version: int
) -> dict[str, Any]:
    """Seed one CTAS cell, run it, and read both tables back."""
    cell = f"{creator}_{body}_{zstate}_v{version}"
    t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
    z = f"{CATALOG}.{NAMESPACE}.z_{cell}"
    seed_table(session, t, version, "(1, 'a'), (2, 'b')")
    if zstate == "z_exists":
        seed_table(session, z, version, "(100, 'z')")
    else:
        session.sql(f"DROP TABLE IF EXISTS {z}")
    before = {"t": table_state(session, t), "z": table_state(session, z)}
    statement = CREATORS[creator].format(z=z, body=BODIES[body].format(t=t))
    outcome = run_statement(session, statement)
    after = {"t": table_state(session, t), "z": table_state(session, z)}
    return {
        "id": cell,
        "kind": "ctas",
        "creator": creator,
        "body": body,
        "z_state": zstate,
        "version": version,
        "statements": [statement],
        "outcome": outcome,
        "before": before,
        "after": after,
    }


def explain_cell(session: ReparkSession, body: str, version: int) -> dict[str, Any]:
    """Seed one EXPLAIN ANALYZE cell, run it, and read the table back."""
    cell = f"explain_{body}_v{version}"
    t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
    seed_table(session, t, version, "(1, 'a'), (2, 'b')")
    before = {"t": table_state(session, t)}
    statement = f"EXPLAIN ANALYZE {BODIES[body].format(t=t)}"
    outcome = run_statement(session, statement)
    after = {"t": table_state(session, t)}
    return {
        "id": cell,
        "kind": "explain",
        "body": body,
        "version": version,
        "statements": [statement],
        "outcome": outcome,
        "before": before,
        "after": after,
    }


def prepare_cell(session: ReparkSession, body: str, version: int) -> dict[str, Any]:
    """Seed one PREPARE cell, run PREPARE and EXECUTE, and read the table back."""
    cell = f"prepare_{body}_v{version}"
    t = f"{CATALOG}.{NAMESPACE}.t_{cell}"
    name = f"wbc_{cell}"
    seed_table(session, t, version, "(1, 'a'), (2, 'b')")
    before = {"t": table_state(session, t)}
    prepare = f"PREPARE {name} AS {BODIES[body].format(t=t)}"
    execute = f"EXECUTE {name}"
    outcome = {"prepare": run_statement(session, prepare)}
    outcome["execute"] = run_statement(session, execute)
    after = {"t": table_state(session, t)}
    return {
        "id": cell,
        "kind": "prepare",
        "body": body,
        "version": version,
        "statements": [prepare, execute],
        "outcome": outcome,
        "before": before,
        "after": after,
    }


def build_session(warehouse: Path) -> ReparkSession:
    """Return a session over a memory catalog at the warehouse."""
    session = (
        ReparkSession.builder.appName("record-write-body-carriers-1-main")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )
    session.register_memory_catalog(CATALOG, str(warehouse))
    session.sql(f"CREATE NAMESPACE {CATALOG}.{NAMESPACE}")
    return session


def record(warehouse: Path) -> dict[str, Any]:
    """Run every cell on main and return the evidence document."""
    import repark

    shutil.rmtree(warehouse, ignore_errors=True)
    session = build_session(warehouse)
    cells = [
        ctas_cell(session, creator, body, zstate, version)
        for version in VERSIONS
        for creator in CREATORS
        for body in BODIES
        for zstate in ZSTATES
    ]
    cells += [explain_cell(session, body, version) for version in VERSIONS for body in BODIES]
    cells += [
        prepare_cell(session, body, version)
        for version in VERSIONS
        for body in ("insert", "overwrite")
    ]
    module = getattr(repark, "__version__", "unknown")
    session.stop()
    return {
        "provenance": {
            "engine": "repark",
            "repark": module,
            "catalog": "memory",
            "date": datetime.datetime.now(datetime.UTC).date().isoformat(),
        },
        "cells": cells,
    }


def unchanged(cell: dict[str, Any], table: str) -> bool:
    """Return whether one table reads back exactly as before the statement."""
    return cell["before"][table] == cell["after"][table]


def outcome_text(outcome: dict[str, Any]) -> tuple[str, str]:
    """Return the markdown class and first line of one outcome."""
    if outcome.get("answered", True):
        return "answered", ""
    return str(outcome.get("class", "")), str(outcome.get("first_line", ""))


def state_text(state: dict[str, Any]) -> str:
    """Render one table state as its three counts."""
    if not state.get("exists", False):
        return "absent"
    return f"s{state['snapshots']}/r{state['row_count']}/f{state['data_files']}"


def markdown_cell(value: str) -> str:
    """Escape one markdown table cell without changing its text."""
    return value.replace("|", "\\|").replace("\n", " ")


def render_markdown(document: dict[str, Any]) -> str:
    """Render the evidence tables from the recorded document."""
    provenance = document["provenance"]
    lines = [
        "# WRITE-BODY-CARRIERS-1 step 0: main's answer",
        "",
        f"Recorded {provenance['date']} on the main release module "
        f"(repark {provenance['repark']}, memory catalog) by "
        "`python/repark/tests/_record_write_body_carriers_1_main.py`. "
        f"Full texts and row values live in [{JSON_NAME}]({JSON_NAME}); "
        "these tables are a view over it. `s`/`r`/`f` are the snapshot count, "
        "row count and data-file count before `->` after.",
        "",
        "## CTAS cells",
        "",
        "| cell | outcome | first line | t | z |",
        "|---|---|---|---|---|",
    ]
    for cell in document["cells"]:
        if cell["kind"] != "ctas":
            continue
        label, first = outcome_text(cell["outcome"])
        row = [
            cell["id"],
            label,
            first[:160],
            f"{state_text(cell['before']['t'])} -> {state_text(cell['after']['t'])}",
            f"{state_text(cell['before']['z'])} -> {state_text(cell['after']['z'])}",
        ]
        lines.append("| " + " | ".join(markdown_cell(part) for part in row) + " |")
    lines += [
        "",
        "## Wrapper cells",
        "",
        "| cell | outcome | first line | t |",
        "|---|---|---|---|",
    ]
    for cell in document["cells"]:
        if cell["kind"] == "ctas":
            continue
        if cell["kind"] == "prepare":
            prepare = cell["outcome"]["prepare"]
            execute = cell["outcome"]["execute"]
            label = f"prepare={outcome_text(prepare)[0]} execute={outcome_text(execute)[0]}"
            first = outcome_text(prepare)[1] or outcome_text(execute)[1]
        else:
            label, first = outcome_text(cell["outcome"])
        row = [
            cell["id"],
            label,
            first[:160],
            f"{state_text(cell['before']['t'])} -> {state_text(cell['after']['t'])}",
        ]
        lines.append("| " + " | ".join(markdown_cell(part) for part in row) + " |")
    lines.append("")
    moved = sum(
        1
        for cell in document["cells"]
        if any(not unchanged(cell, table) for table in cell["before"])
    )
    lines.append(f"{len(document['cells'])} cells, {moved} with a table that reads back different.")
    lines.append("")
    return "\n".join(lines)


def write_evidence(document: dict[str, Any]) -> None:
    """Write the JSON and the markdown view beside it."""
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / JSON_NAME).write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    (EVIDENCE / MARKDOWN_NAME).write_text(render_markdown(document), encoding="utf-8")


def main(argv: list[str]) -> int:
    """Record every cell and write the evidence files."""
    warehouse = Path(argv[0]) if argv else Path(tempfile.mkdtemp()) / "wh-wbc1-main"
    document = record(warehouse)
    write_evidence(document)
    print(f"main cells recorded={len(document['cells'])} -> {EVIDENCE}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
