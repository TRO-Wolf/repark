"""Round 15 recorder: the mixed-struct write shapes with both operand orders.

``record`` runs every shape on one engine in one zone and writes one JSON
file. It also runs the named cells: the depth-2 NOT NULL UPDATE, the temp view
read after a zone change, ``EXPLAIN UPDATE`` of a zoned struct and ``EXPLAIN
ANALYZE INSERT`` of a mixed union. ``merge`` joins the per-zone Spark files
and one main file into the facade pin's expected file. A named cell lists in
``moved`` the row ids that hold an instant and store Spark's value; every
other row stores main's. ``compare`` counts head cells against main and Spark.

The warehouse, the output paths and Spark's package coordinates come from
arguments or the environment.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import pyarrow as pa

WALL = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
INSTANT = "TIMESTAMP '2026-07-04 11:22:33.654321+00:00'"
NTZ_STRUCT = "STRUCT<v: TIMESTAMP_NTZ, n: INT>"
ZONED_STRUCT = "STRUCT<v: TIMESTAMP, n: INT>"
NS = "ice.ns"
MODES = {
    "cow": "TBLPROPERTIES ('format-version'='2')",
    "mor": (
        "TBLPROPERTIES ('format-version'='2', 'write.delete.mode'='merge-on-read', "
        "'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read')"
    ),
}
WALL_SIDE = f"SELECT id, st FROM {NS}.a WHERE id = 1"
ZONED_SIDE = f"SELECT id, st FROM {NS}.b WHERE id = 2"
ALL_WALLS = f"SELECT id, st FROM {NS}.a"
NO_INSTANT = f"SELECT id, st FROM {NS}.b WHERE id < 0"
FAR_WALLS = f"SELECT id + 9 AS id, st FROM {NS}.a"
FAR_INSTANTS = f"SELECT id + 19 AS id, st FROM {NS}.b"
JOINED = f"FROM {NS}.a a JOIN {NS}.b b ON a.id = b.id"
SET_SOURCE = "WHEN MATCHED THEN UPDATE SET t.st = s.st"
TWO_INSERTS = (
    f"{SET_SOURCE} WHEN NOT MATCHED AND s.id % 2 = 0 THEN INSERT (id, st) VALUES (s.id, s.st) "
    "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id + 100, s.st)"
)


def struct(value: str, number: int) -> str:
    """Render one struct literal."""
    return f"named_struct('v', {value}, 'n', {number})"


def seed() -> list[str]:
    """Create the wall source and the instant source."""
    return [
        f"CREATE TABLE {NS}.a (id INT, st {NTZ_STRUCT}) USING iceberg {MODES['cow']}",
        f"INSERT INTO {NS}.a VALUES (1, {struct(WALL, 1)}), (2, {struct(WALL, 2)})",
        f"CREATE TABLE {NS}.b (id INT, st {ZONED_STRUCT}) USING iceberg {MODES['cow']}",
        f"INSERT INTO {NS}.b VALUES (1, {struct(INSTANT, 101)}), (2, {struct(INSTANT, 102)})",
    ]


def update(value: str) -> str:
    """Render a whole-struct UPDATE of the wide target."""
    return f"UPDATE {{t}} SET st = {value} WHERE id >= 0"


def merge(source: str, clauses: str) -> str:
    """Render a MERGE into the target from one source."""
    return f"MERGE INTO {{t}} t USING {source} s ON t.id = s.id {clauses}"


def union(first: str, second: str) -> str:
    """Render a derived UNION ALL source."""
    return f"({first} UNION ALL {second})"


def shapes() -> dict[str, tuple[str, str, str]]:
    """Return each shape as target layout, wall-first text and instant-first text."""
    built = "named_struct('v', c.v, 'n', 100)"
    return {
        "update-case": (
            "wide",
            update("CASE WHEN id = 2 THEN st ELSE c END"),
            update("CASE WHEN id = 1 THEN c ELSE st END"),
        ),
        "update-case-built": (
            "wide",
            update(f"CASE WHEN id = 2 THEN st ELSE {built} END"),
            update(f"CASE WHEN id = 1 THEN {built} ELSE st END"),
        ),
        "update-case-keeps-every-wall": (
            "wide",
            update(f"CASE WHEN id > 0 THEN st ELSE {built} END"),
            update(f"CASE WHEN id <= 0 THEN {built} ELSE st END"),
        ),
        "update-case-two-when": (
            "wide",
            update("CASE WHEN id = 1 THEN st WHEN id = 2 THEN c END"),
            update("CASE WHEN id = 2 THEN c WHEN id = 1 THEN st END"),
        ),
        "update-case-simple": (
            "wide",
            update("CASE id WHEN 1 THEN st ELSE c END"),
            update("CASE id WHEN 2 THEN c ELSE st END"),
        ),
        "merge-update-case": (
            "wide",
            merge(
                f"{NS}.b",
                "WHEN MATCHED THEN UPDATE SET t.st = CASE WHEN t.id = 1 THEN t.st ELSE s.st END",
            ),
            merge(
                f"{NS}.b",
                "WHEN MATCHED THEN UPDATE SET t.st = CASE WHEN t.id = 2 THEN s.st ELSE t.st END",
            ),
        ),
        "merge-update-union": (
            "wide",
            merge(union(WALL_SIDE, ZONED_SIDE), SET_SOURCE),
            merge(union(ZONED_SIDE, WALL_SIDE), SET_SOURCE),
        ),
        "merge-update-union-keeps-every-wall": (
            "wide",
            merge(union(ALL_WALLS, NO_INSTANT), SET_SOURCE),
            merge(union(NO_INSTANT, ALL_WALLS), SET_SOURCE),
        ),
        "merge-not-matched-by-source": (
            "wide",
            merge(
                f"(SELECT id + 50 AS id FROM {NS}.b)",
                "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET "
                "t.st = CASE WHEN t.id = 1 THEN t.st ELSE t.c END",
            ),
            merge(
                f"(SELECT id + 50 AS id FROM {NS}.b)",
                "WHEN NOT MATCHED BY SOURCE THEN UPDATE SET "
                "t.st = CASE WHEN t.id = 2 THEN t.c ELSE t.st END",
            ),
        ),
        "merge-source-case": (
            "wide",
            merge(
                f"(SELECT a.id, CASE WHEN a.id = 1 THEN a.st ELSE b.st END AS st {JOINED})",
                SET_SOURCE,
            ),
            merge(
                f"(SELECT a.id, CASE WHEN a.id = 2 THEN b.st ELSE a.st END AS st {JOINED})",
                SET_SOURCE,
            ),
        ),
        "merge-update-star": (
            "narrow",
            merge(union(WALL_SIDE, ZONED_SIDE), "WHEN MATCHED THEN UPDATE SET *"),
            merge(union(ZONED_SIDE, WALL_SIDE), "WHEN MATCHED THEN UPDATE SET *"),
        ),
        "merge-second-not-matched": (
            "narrow",
            merge(union(FAR_WALLS, FAR_INSTANTS), TWO_INSERTS),
            merge(union(FAR_INSTANTS, FAR_WALLS), TWO_INSERTS),
        ),
    }


def target_setup(table: str, layout: str, mode: str) -> list[str]:
    """Create one target with two wall rows."""
    if layout == "wide":
        columns = f"id INT, st {NTZ_STRUCT}, c {ZONED_STRUCT}"
        rows = ", ".join(
            f"({key}, {struct(WALL, key)}, {struct(INSTANT, key + 10)})" for key in (1, 2)
        )
    else:
        columns = f"id INT, st {NTZ_STRUCT}"
        rows = ", ".join(f"({key}, {struct(WALL, key)})" for key in (1, 2))
    return [
        f"CREATE TABLE {table} ({columns}) USING iceberg {MODES[mode]}",
        f"INSERT INTO {table} VALUES {rows}",
    ]


def cells() -> list[dict[str, Any]]:
    """Return every shape in both orders and both write modes."""
    out: list[dict[str, Any]] = []
    for mode in MODES:
        for shape, (layout, wall_first, instant_first) in shapes().items():
            for order, text in (("wall-first", wall_first), ("instant-first", instant_first)):
                table = f"{NS}.t{len(out)}"
                out.append(
                    {
                        "id": f"{mode}/{shape}/{order}",
                        "table": table,
                        "pre": target_setup(table, layout, mode),
                        "sql": text.replace("{t}", table),
                    }
                )
    return out


def not_null_cells() -> list[dict[str, Any]]:
    """Return the whole-struct UPDATE into a depth-2 struct with a NOT NULL member."""
    layouts = {
        "inner-required": f"STRUCT<i: {NTZ_STRUCT} NOT NULL, m: INT>",
        "member-required": f"STRUCT<i: {NTZ_STRUCT}, m: INT NOT NULL>",
    }
    built = "named_struct('i', named_struct('v', c, 'n', id), 'm', 5)"
    doors = {
        "update-whole": f"SET st = {built} WHERE id >= 0",
        "update-case": f"SET st = CASE WHEN id = 1 THEN {built} ELSE st END WHERE id >= 0",
    }
    out: list[dict[str, Any]] = []
    for layout, struct_type in layouts.items():
        for door, assignment in doors.items():
            table = f"{NS}.nn{len(out)}"
            out.append(
                {
                    "id": f"not-null/{layout}/{door}",
                    "table": table,
                    "leaf": "st.i",
                    "steps": [
                        [
                            "sql",
                            f"CREATE TABLE {table} (id INT, st {struct_type}, k INT, c TIMESTAMP) "
                            f"USING iceberg {MODES['cow']}",
                        ],
                        [
                            "sql",
                            f"INSERT INTO {table} BY NAME SELECT 0 AS k, "
                            f"named_struct('i', st, 'm', 5) AS st, id, {INSTANT} AS c "
                            f"FROM {NS}.b WHERE id = 1",
                        ],
                        ["sql", f"UPDATE {table} {assignment}"],
                    ],
                }
            )
    return out


def zone_switch_cells() -> list[dict[str, Any]]:
    """Return the temp view planned in one zone and inserted in another."""
    mixed = {
        "union": (f"{ALL_WALLS} UNION ALL SELECT id + 10 AS id, st FROM {NS}.b", [11, 12]),
        "literal": (
            f"SELECT id, CASE WHEN id = 1 THEN {struct(WALL, 7)} ELSE st END AS st FROM {NS}.b",
            [2],
        ),
    }
    out: list[dict[str, Any]] = []
    for name, (select, moved) in mixed.items():
        table = f"{NS}.zs{len(out)}"
        view = f"switched_{name}"
        out.append(
            {
                "id": f"zone-switch/{name}",
                "table": table,
                "leaf": "st",
                "moved": moved,
                "steps": [
                    [
                        "sql",
                        f"CREATE TABLE {table} (id INT, st {NTZ_STRUCT}) "
                        f"USING iceberg {MODES['cow']}",
                    ],
                    ["view", select, view],
                    ["zone", "Asia/Kolkata"],
                    ["sql", f"INSERT INTO {table} SELECT id, st FROM {view}"],
                    ["zone", ""],
                ],
            }
        )
    return out


def explain_cells() -> list[dict[str, Any]]:
    """Return the EXPLAIN UPDATE and EXPLAIN ANALYZE INSERT cells RePark answers."""
    out: list[dict[str, Any]] = []
    values = {"built": "named_struct('v', w, 'n', 1)", "column": "ns"}
    for name, value in values.items():
        table = f"{NS}.xu{len(out)}"
        statement = f"UPDATE {table} SET st = {value} WHERE id >= 0"
        out.append(
            {
                "id": f"explain-update/{name}",
                "table": table,
                "leaf": "st",
                "repark_only": True,
                "steps": [
                    [
                        "sql",
                        f"CREATE TABLE {table} (id INT, st {ZONED_STRUCT}, w TIMESTAMP_NTZ, "
                        f"ns {NTZ_STRUCT}) USING iceberg {MODES['cow']}",
                    ],
                    [
                        "sql",
                        f"INSERT INTO {table} SELECT 1, {struct(INSTANT, 9)}, {WALL}, "
                        f"{struct(WALL, 7)}",
                    ],
                    ["explain", f"EXPLAIN {statement}"],
                    ["sql", statement],
                ],
            }
        )
    sides = {
        "wall-first": f"{ALL_WALLS} UNION ALL SELECT id + 10 AS id, st FROM {NS}.b",
        "instant-first": f"SELECT id + 10 AS id, st FROM {NS}.b UNION ALL {ALL_WALLS}",
    }
    for name, select in sides.items():
        table = f"{NS}.xa{len(out)}"
        out.append(
            {
                "id": f"explain-analyze-insert/{name}",
                "table": table,
                "leaf": "st",
                "repark_only": True,
                "steps": [
                    [
                        "sql",
                        f"CREATE TABLE {table} (id INT, st {NTZ_STRUCT}) "
                        f"USING iceberg {MODES['cow']}",
                    ],
                    ["sql", f"EXPLAIN ANALYZE INSERT INTO {table} {select}"],
                ],
            }
        )
    return out


def named_cells() -> list[dict[str, Any]]:
    """Return the cells a ledger names: equal to main, or main's EXPLAIN text."""
    return not_null_cells() + zone_switch_cells() + explain_cells()


def open_repark(zone: str, warehouse: Path) -> Any:
    """Open one RePark session in the zone with a memory catalog."""
    from repark import ReparkSession

    session = (
        ReparkSession.builder.appName("round15-wall-first")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(warehouse))
    session.sql(f"CREATE NAMESPACE {NS}")
    return session


def open_spark(zone: str, warehouse: Path) -> Any:
    """Open one Spark session with a Hadoop Iceberg catalog."""
    from pyspark.sql import SparkSession

    packages = os.environ.get(
        "SPARK_PACKAGES", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
    )
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("round15-wall-first")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.jars.packages", packages)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", str(warehouse))
    )
    ivy = os.environ.get("SPARK_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}")
    return session


def read_rows(session: Any, table: str, leaf: str = "st") -> list[list[Any]]:
    """Return ``[id, leaf microseconds, n]`` rows sorted by id."""
    frame = session.sql(f"SELECT id, {leaf}.v AS v, {leaf}.n AS n FROM {table}")
    arrow = frame.toArrow()
    ticks = arrow.column("v").cast(pa.int64(), safe=False).to_pylist()
    rows = [
        list(row)
        for row in zip(
            arrow.column("id").to_pylist(), ticks, arrow.column("n").to_pylist(), strict=True
        )
    ]
    rows.sort(key=lambda row: row[0])
    return rows


def run_cell(session: Any, cell: dict[str, Any]) -> dict[str, Any]:
    """Run one cell and return its rows or the first line of its error."""
    record = dict(cell)
    for statement in cell["pre"]:
        session.sql(statement)
    try:
        session.sql(cell["sql"])
        record["ok"] = True
    except Exception as error:
        lines = str(error).strip().splitlines()
        record["ok"] = False
        record["error"] = [error.__class__.__name__, (lines[0] if lines else "")[:300]]
    record["rows"] = read_rows(session, cell["table"])
    return record


def explain_text(session: Any, statement: str) -> str:
    """Return the EXPLAIN rows as text."""
    return "\n".join(str(row) for row in session.sql(statement).toArrow().to_pylist())


def run_step(session: Any, zone: str, step: list[str], record: dict[str, Any]) -> None:
    """Run one step of a named cell."""
    kind = step[0]
    if kind == "sql":
        frame = session.sql(step[1])
        if step[1].startswith("EXPLAIN"):
            frame.toArrow()
    elif kind == "view":
        session.sql(step[1]).createOrReplaceTempView(step[2])
    elif kind == "zone":
        session.conf.set("spark.sql.session.timeZone", step[1] or zone)
    else:
        record["explain"] = explain_text(session, step[1])


def run_named(session: Any, zone: str, cell: dict[str, Any]) -> dict[str, Any]:
    """Run one named cell and return its rows, or the first line of its error."""
    record = dict(cell)
    try:
        for step in cell["steps"]:
            run_step(session, zone, step, record)
        record["ok"] = True
    except Exception as error:
        lines = str(error).strip().splitlines()
        record["ok"] = False
        record["error"] = [error.__class__.__name__, (lines[0] if lines else "")[:300]]
        session.conf.set("spark.sql.session.timeZone", zone)
    record["rows"] = read_rows(session, cell["table"], cell["leaf"])
    return record


def record(engine: str, zone: str, out: Path, warehouse: Path) -> None:
    """Run every cell on one engine in one zone and write the JSON output."""
    opener = open_repark if engine == "repark" else open_spark
    session = opener(zone, warehouse)
    try:
        for statement in seed():
            session.sql(statement)
        records = [run_cell(session, cell) for cell in cells()]
        named = [
            run_named(session, zone, cell)
            for cell in named_cells()
            if engine == "repark" or not cell.get("repark_only")
        ]
        version = str(session.version) if engine == "spark" else "repark"
    finally:
        session.stop()
    payload = {
        "engine": engine,
        "zone": zone,
        "version": version,
        "cells": records,
        "named": named,
    }
    out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n")
    done = sum(1 for item in records + named if item["ok"])
    print(f"{engine} {zone} {version}: {done} ok of {len(records) + len(named)}")


def merge_expected(inputs: list[Path], main: Path, out: Path, measured: str) -> None:
    """Join per-zone Spark files and one main file into the facade pin's expected file."""
    zones: dict[str, Any] = {}
    version = ""
    main_payload = json.loads(main.read_text())
    if main_payload["engine"] != "repark":
        raise SystemExit(f"{main} is not a RePark recording")
    keys = ("id", "table", "leaf", "steps", "moved", "ok", "error", "rows", "explain")
    named = [{key: cell[key] for key in keys if key in cell} for cell in main_payload["named"]]
    for path in inputs:
        payload = json.loads(path.read_text())
        if payload["engine"] != "spark":
            raise SystemExit(f"{path} is not a Spark recording")
        if not all(cell["ok"] for cell in payload["cells"]):
            raise SystemExit(f"{path} holds a refused cell")
        version = payload["version"]
        if payload["zone"] == main_payload["zone"]:
            spark_rows = {cell["id"]: cell["rows"] for cell in payload["named"]}
            for cell in named:
                if cell["id"] in spark_rows:
                    cell["spark_rows"] = spark_rows[cell["id"]]
        zones[payload["zone"]] = [
            {key: cell[key] for key in ("id", "table", "pre", "sql", "rows")}
            for cell in payload["cells"]
        ]
    expected = {
        "recorded": f"Apache Spark {version} with Iceberg 1.11.0, measured {measured}",
        "seed": seed(),
        "zones": zones,
        "main": {"zone": main_payload["zone"], "measured": measured, "named": named},
    }
    out.write_text(json.dumps(expected, indent=1, sort_keys=True) + "\n")
    print(f"{out.name}: {sum(len(found) for found in zones.values())} cells")


def compare(head: Path, main: Path, spark: Path) -> None:
    """Count head cells equal to Spark, and head cells worse than main."""
    loaded = [json.loads(path.read_text()) for path in (head, main, spark)]
    by_id = [{cell["id"]: cell for cell in payload["cells"]} for payload in loaded]
    equal = 0
    worse: list[str] = []
    for key, cell in by_id[0].items():
        spark_rows = by_id[2][key]["rows"]
        equal += cell["rows"] == spark_rows
        main_rows = {row[0]: row for row in by_id[1][key]["rows"]}
        head_rows = {row[0]: row for row in cell["rows"]}
        for row in spark_rows:
            if main_rows.get(row[0]) == row and head_rows.get(row[0]) != row:
                worse.append(f"{key} id {row[0]}")
    zone = loaded[0]["zone"]
    print(f"{zone}: {equal} of {len(by_id[0])} cells equal Spark; {len(worse)} rows worse")
    for line in worse:
        print(f"  worse than main: {line}")


def main() -> None:
    """Parse the command line and run one subcommand."""
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("record")
    run.add_argument("--engine", choices=("repark", "spark"), required=True)
    run.add_argument("--zone", required=True)
    run.add_argument("--out", type=Path, required=True)
    run.add_argument("--warehouse", type=Path, required=True)
    join = commands.add_parser("merge")
    join.add_argument("--out", type=Path, required=True)
    join.add_argument("--measured", required=True)
    join.add_argument("--main", type=Path, required=True)
    join.add_argument("inputs", type=Path, nargs="+")
    check = commands.add_parser("compare")
    check.add_argument("--head", type=Path, required=True)
    check.add_argument("--main", type=Path, required=True)
    check.add_argument("--spark", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "record":
        record(args.engine, args.zone, args.out, args.warehouse)
    elif args.command == "merge":
        merge_expected(args.inputs, args.main, args.out, args.measured)
    else:
        compare(args.head, args.main, args.spark)


if __name__ == "__main__":
    main()
