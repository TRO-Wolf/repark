"""Round 12 joined-head measurement for the two NTZ units.

Measures the coercion verdict's nested-NTZ-target write shapes and the six
F1 mixed-struct statements on one engine in one zone, or merges head, Spark
and main outputs into the decision table.

The warehouse, the output path and Spark's package coordinates come from
arguments or the environment.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

import pyarrow as pa

WALL = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
WALL_SUMMER = "TIMESTAMP_NTZ '2026-07-04 07:22:33.654321'"
INSTANT_NY = "TIMESTAMP_LTZ '2026-01-02 08:04:05.123456+00:00'"
INSTANT_UTC = "TIMESTAMP_LTZ '2026-01-02 03:04:05.123456+00:00'"
INSTANT_JULY = "TIMESTAMP_LTZ '2026-07-04 11:22:33.654321+00:00'"
JULY_SPARK = "TIMESTAMP '2026-07-04 11:22:33.654321+00:00'"
NTZ_STRUCT = "STRUCT<v: TIMESTAMP_NTZ, n: INT>"
LTZ_STRUCT = "STRUCT<v: TIMESTAMP_LTZ, n: INT>"
ZONED_STRUCT = "STRUCT<v: TIMESTAMP, n: INT>"
NS = "ice.ns"


def named_struct(value: str, number: int = 7) -> str:
    """Render one struct literal with an integer member."""
    return f"named_struct('v', {value}, 'n', CAST({number} AS INT))"


def drop_create(name: str, columns: str) -> list[str]:
    """Render drop and create for one Iceberg table."""
    return [
        f"DROP TABLE IF EXISTS {NS}.{name}",
        f"CREATE TABLE {NS}.{name} ({columns}) USING iceberg",
    ]


def make_table(name: str, columns: str, rows: list[str]) -> list[str]:
    """Render drop, create and one insert per row for a test table."""
    statements = drop_create(name, columns)
    for row in rows:
        statements.append(f"INSERT INTO {NS}.{name} SELECT {row}")
    return statements


def base_setup() -> list[str]:
    """Create the wall side and the instant side of the mixed inventory."""
    setup = [f"CREATE NAMESPACE IF NOT EXISTS {NS}"]
    setup += make_table(
        "a",
        f"id INT, st {NTZ_STRUCT}",
        [
            f"1, {named_struct(WALL)}",
            "2, NULL",
            "3, named_struct('v', CAST(NULL AS TIMESTAMP_NTZ), 'n', CAST(8 AS INT))",
            f"4, {named_struct(WALL_SUMMER)}",
        ],
    )
    setup += make_table(
        "b",
        f"id INT, st {LTZ_STRUCT}",
        [
            f"1, {named_struct(INSTANT_NY)}",
            f"2, {named_struct(INSTANT_UTC)}",
            f"3, {named_struct(INSTANT_JULY)}",
            "4, NULL",
        ],
    )
    return setup


def union_sql() -> str:
    """Select the wall side and the instant side as one mixed column."""
    return (
        f"SELECT id, st AS mixed FROM {NS}.a UNION ALL "
        f"SELECT id + 10 AS id, st AS mixed FROM {NS}.b"
    )


def case_sql() -> str:
    """Select a CASE that mixes the wall side with the instant side."""
    return (
        "SELECT a.id AS id, CASE WHEN a.id = 2 THEN b.st ELSE a.st END "
        f"AS mixed FROM {NS}.a a JOIN {NS}.b b ON a.id = b.id"
    )


def coalesce_sql() -> str:
    """Select a COALESCE that mixes the wall side with the instant side."""
    return (
        f"SELECT a.id AS id, COALESCE(a.st, b.st) AS mixed FROM {NS}.a a "
        f"JOIN {NS}.b b ON a.id = b.id"
    )


def ntz_target_cells() -> list[dict[str, Any]]:
    """Return the eight nested-NTZ-target write shapes."""
    target = make_table(
        "tt",
        f"id INT, st {NTZ_STRUCT}, c {LTZ_STRUCT}",
        [
            f"1, {named_struct(WALL)}, {named_struct(INSTANT_JULY)}",
            f"2, {named_struct(WALL)}, {named_struct(INSTANT_JULY)}",
            f"3, NULL, {named_struct(INSTANT_NY)}",
        ],
    )
    read_target = f"SELECT id, st FROM {NS}.tt"
    insert_case = drop_create("wn2", f"id INT, st {NTZ_STRUCT}")
    insert_case.append(f"INSERT INTO {NS}.wn2 {case_sql()}")
    insert_coalesce = drop_create("wn3", f"id INT, st {NTZ_STRUCT}")
    insert_coalesce.append(f"INSERT INTO {NS}.wn3 {coalesce_sql()}")
    overwrite_union = drop_create("wn5", f"id INT, st {NTZ_STRUCT}")
    overwrite_union.append(f"INSERT OVERWRITE {NS}.wn5 {union_sql()}")
    update_case = [
        *target,
        f"UPDATE {NS}.tt SET st = CASE WHEN id = 1 THEN c ELSE st END WHERE id < 3",
    ]
    merge_update = [
        *target,
        f"MERGE INTO {NS}.tt t USING {NS}.b s ON t.id = s.id WHEN MATCHED "
        "THEN UPDATE SET t.st = CASE WHEN s.id = 1 THEN s.st ELSE t.st END",
    ]
    merge_insert = [
        *target,
        f"MERGE INTO {NS}.tt t USING (SELECT a.id + 100 AS id, "
        f"COALESCE(a.st, b.st) AS st FROM {NS}.a a "
        f"JOIN {NS}.b b ON a.id = b.id) s ON t.id = s.id WHEN NOT MATCHED "
        "THEN INSERT (id, st, c) VALUES (s.id, s.st, s.st)",
    ]
    merge_union = [
        *target,
        f"MERGE INTO {NS}.tt t USING (SELECT id + 100 AS id, mixed AS st "
        f"FROM ({union_sql()}) u) s ON t.id = s.id WHEN NOT MATCHED "
        "THEN INSERT (id, st) VALUES (s.id, s.st)",
    ]
    return [
        {"id": "ntz-target/insert-case", "pre": insert_case, "q": f"SELECT id, st FROM {NS}.wn2"},
        {
            "id": "ntz-target/insert-coalesce",
            "pre": insert_coalesce,
            "q": f"SELECT id, st FROM {NS}.wn3",
        },
        {
            "id": "ntz-target/insert-overwrite-union",
            "pre": overwrite_union,
            "q": f"SELECT id, st FROM {NS}.wn5",
        },
        {"id": "ntz-target/update-case-where", "pre": update_case, "q": read_target},
        {"id": "ntz-target/merge-update-case", "pre": merge_update, "q": read_target},
        {"id": "ntz-target/merge-insert-coalesce", "pre": merge_insert, "q": read_target},
        {"id": "ntz-target/merge-source-union", "pre": merge_union, "q": read_target},
        {
            "id": "ntz-target/df-append",
            "pre": drop_create("wn6", f"id INT, st {NTZ_STRUCT}"),
            "df": True,
            "q": f"SELECT id, st FROM {NS}.wn6",
        },
    ]


def f1_cells() -> list[dict[str, Any]]:
    """Return the six mixed-struct statements of the wall unit's F1."""
    wall_one = f"named_struct('v', {WALL}, 'n', 1)"
    wall_two = f"named_struct('v', {WALL}, 'n', 2)"
    seed_t = [
        *drop_create("t", f"id INT, st {NTZ_STRUCT}, k INT, c TIMESTAMP"),
        f"INSERT INTO {NS}.t VALUES (1, {wall_one}, 0, {JULY_SPARK}), "
        f"(2, {wall_two}, 0, {JULY_SPARK})",
    ]
    seed_u = [
        *drop_create("u", f"id INT, st {NTZ_STRUCT}, k INT, c TIMESTAMP"),
        f"INSERT INTO {NS}.u VALUES (1, {wall_one}, 0, {JULY_SPARK}), "
        f"(2, {wall_two}, 0, CAST(NULL AS TIMESTAMP))",
    ]
    seed_zsrc = [
        *drop_create("zsrc", f"id INT, st {ZONED_STRUCT}"),
        f"INSERT INTO {NS}.zsrc VALUES (1, named_struct('v', {JULY_SPARK}, 'n', 1))",
    ]
    seed_nsrc = [
        *drop_create("nsrc", f"id INT, st {NTZ_STRUCT}"),
        f"INSERT INTO {NS}.nsrc VALUES (2, {wall_two})",
    ]
    seed_m = [
        *drop_create("m", f"id INT, st {NTZ_STRUCT}"),
        f"INSERT INTO {NS}.m VALUES "
        "(1, named_struct('v', TIMESTAMP_NTZ '2001-01-01 00:00:00', "
        f"'n', 0)), (2, {wall_two})",
    ]
    seed_w = drop_create("w", f"id INT, st {NTZ_STRUCT}")
    seed_x = drop_create("x", f"id INT, st {NTZ_STRUCT}")
    seed_y = drop_create("y", f"id INT, st {NTZ_STRUCT}")
    return [
        {
            "id": "f1/update-case",
            "pre": [
                *seed_t,
                f"UPDATE {NS}.t SET st = CASE WHEN id = 1 THEN "
                "named_struct('v', c, 'n', 100) ELSE st END WHERE id >= 0",
            ],
            "q": f"SELECT id, st FROM {NS}.t",
        },
        {
            "id": "f1/update-coalesce",
            "pre": [
                *seed_u,
                f"UPDATE {NS}.u SET st = coalesce(CASE WHEN c IS NOT NULL "
                "THEN named_struct('v', c, 'n', 100) END, st) WHERE id >= 0",
            ],
            "q": f"SELECT id, st FROM {NS}.u",
        },
        {
            "id": "f1/insert-union",
            "pre": [
                *seed_zsrc,
                *seed_nsrc,
                *seed_w,
                f"INSERT INTO {NS}.w SELECT id, st FROM {NS}.zsrc UNION ALL "
                f"SELECT id, st FROM {NS}.nsrc",
            ],
            "q": f"SELECT id, st FROM {NS}.w",
        },
        {
            "id": "f1/merge-update-case",
            "pre": [
                *seed_zsrc,
                *seed_m,
                f"MERGE INTO {NS}.m t USING {NS}.zsrc s "
                "ON t.id = s.id OR t.id = s.id + 1 WHEN MATCHED THEN "
                "UPDATE SET t.st = CASE WHEN t.id = 1 THEN s.st ELSE t.st END",
            ],
            "q": f"SELECT id, st FROM {NS}.m",
        },
        {
            "id": "f1/insert-case-subquery",
            "pre": [
                *seed_zsrc,
                *seed_nsrc,
                *seed_x,
                f"INSERT INTO {NS}.x SELECT id, CASE WHEN id = 1 THEN st "
                f"ELSE (SELECT st FROM {NS}.nsrc) END FROM (SELECT id, st "
                f"FROM {NS}.zsrc UNION ALL SELECT 2, st FROM {NS}.zsrc)",
            ],
            "q": f"SELECT id, st FROM {NS}.x",
        },
        {
            "id": "f1/insert-by-name-union",
            "pre": [
                *seed_zsrc,
                *seed_nsrc,
                *seed_y,
                f"INSERT INTO {NS}.y BY NAME SELECT id, st FROM {NS}.zsrc "
                f"UNION ALL SELECT id, st FROM {NS}.nsrc",
            ],
            "q": f"SELECT id, st FROM {NS}.y",
        },
    ]


def open_repark(zone: str, warehouse: Path) -> Any:
    """Open one RePark session in the zone with a memory catalog."""
    from repark.spark import ReparkSession

    session = (
        ReparkSession.builder.appName("round12-join")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(warehouse))
    session.sql(f"CREATE NAMESPACE {NS}").collect()
    return session


def open_spark(zone: str, warehouse: Path) -> Any:
    """Open one Spark session with a Hadoop Iceberg catalog."""
    from pyspark.sql import SparkSession

    packages = os.environ.get(
        "SPARK_PACKAGES", "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"
    )
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("round12-join")
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
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {NS}").collect()
    return session


def canon_rows(table: pa.Table) -> list[list[Any]]:
    """Return ``[id, leaf microseconds, n]`` rows sorted by id."""
    struct_column = table.column("st").combine_chunks()
    ticks = struct_column.field("v").cast(pa.int64(), safe=False).to_pylist()
    numbers = struct_column.field("n").to_pylist()
    valid = struct_column.is_valid().to_pylist()
    ids = table.column("id").to_pylist()
    rows: list[list[Any]] = []
    for key, tick, number, ok in zip(ids, ticks, numbers, valid, strict=True):
        if not ok:
            rows.append([key, None, None])
        else:
            rows.append([key, tick, number])
    rows.sort(key=lambda row: (row[0] is None, row[0]))
    return rows


def redact(text: str, warehouse: str) -> str:
    """Hide machine paths in a recorded error line."""
    clean = text.replace(warehouse, "<WH>")
    clean = clean.replace(tempfile.gettempdir(), "<TMP>")
    clean = clean.replace(str(Path.home()), "<HOME>")
    return clean


def run_cell(session: Any, engine: str, cell: dict[str, Any], warehouse: str) -> dict[str, Any]:
    """Run one cell and return its canonical rows or its error."""
    record: dict[str, Any] = {"id": cell["id"]}
    try:
        for statement in cell["pre"]:
            session.sql(statement).collect()
        if cell.get("df"):
            renamed = session.sql(union_sql()).withColumnRenamed("mixed", "st")
            renamed.writeTo(f"{NS}.wn6").append()
        frame = session.sql(cell["q"])
        table = frame.to_arrow() if engine == "repark" else frame.toArrow()
        record["ok"] = True
        record["rows"] = canon_rows(table)
    except Exception as error:
        text = str(error).strip().splitlines()
        first = text[0] if text else error.__class__.__name__
        record["ok"] = False
        record["error"] = {
            "class": error.__class__.__name__,
            "line": redact(first[:500], warehouse),
        }
    return record


def measure(engine: str, zone: str, out: Path, warehouse: Path) -> None:
    """Run every cell on one engine in one zone and write the JSON output."""
    opener = open_repark if engine == "repark" else open_spark
    session = opener(zone, warehouse)
    try:
        for statement in base_setup():
            session.sql(statement).collect()
        cells = ntz_target_cells() + f1_cells()
        records = [run_cell(session, engine, cell, str(warehouse)) for cell in cells]
        version = session.version if engine == "spark" else "repark-release"
        payload = {
            "meta": {"engine": engine, "zone": zone, "version": str(version)},
            "cells": records,
        }
        out.write_text(json.dumps(payload, indent=1, sort_keys=True) + "\n")
        done = sum(1 for item in records if item["ok"])
        print(f"{engine} {zone} {version}: {done} ok of {len(records)}")
        for record in records:
            if not record["ok"]:
                print(f"  {record['id']}: {record['error']}")
    finally:
        session.stop()


def index_cells(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Index one output file's cells by id."""
    return {cell["id"]: cell for cell in payload["cells"]}


def row_map(cell: dict[str, Any]) -> dict[Any, list[Any]]:
    """Index one cell's rows by id."""
    return {row[0]: [row[1], row[2]] for row in cell.get("rows", [])}


def compare_row(head: list[Any] | None, spark: list[Any] | None, main: list[Any] | None) -> str:
    """Classify one row's three ticks."""
    if head == spark:
        return "same"
    if main == spark:
        return "halt"
    return "hole"


def render_table(
    zone: str, heads: dict[str, Any], sparks: dict[str, Any], mains: dict[str, Any]
) -> tuple[list[str], dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """Render one zone's comparison rows and collect divergences."""
    head_cells = heads.get(zone, {})
    spark_cells = sparks.get(zone, {})
    main_cells = mains.get(zone, {})
    ids = sorted(set(head_cells) | set(spark_cells) | set(main_cells))
    lines = [
        f"## {zone}",
        "",
        "| cell | row | head | spark | main | verdict |",
        "|---|---|---|---|---|---|",
    ]
    zone_rows: dict[str, Any] = {}
    halts: list[dict[str, Any]] = []
    holes: list[dict[str, Any]] = []
    for cid in ids:
        head_cell = head_cells.get(cid, {})
        spark_cell = spark_cells.get(cid, {})
        main_cell = main_cells.get(cid, {})
        head_rows = row_map(head_cell)
        spark_rows = row_map(spark_cell)
        main_rows = row_map(main_cell)
        zone_rows[cid] = {"head": head_rows, "spark": spark_rows, "main": main_rows}
        keys = sorted(set(head_rows) | set(spark_rows) | set(main_rows), key=str)
        for key in keys:
            head_tick = head_rows.get(key)
            spark_tick = spark_rows.get(key)
            main_tick = main_rows.get(key)
            verdict = compare_row(head_tick, spark_tick, main_tick)
            row = {
                "zone": zone,
                "cell": cid,
                "row": key,
                "head": head_tick,
                "spark": spark_tick,
                "main": main_tick,
            }
            if verdict == "halt":
                halts.append(row)
            elif verdict == "hole":
                holes.append(row)
            lines.append(
                f"| {cid} | {key} | {head_tick} | {spark_tick} | {main_tick} | {verdict} |"
            )
    return lines, zone_rows, halts, holes


def summarize(
    head_paths: list[Path],
    spark_paths: list[Path],
    main_path: Path,
    out_json: Path,
    out_md: Path,
) -> int:
    """Merge the three engines into the decision table and count halts."""
    heads = {}
    for path in head_paths:
        payload = json.loads(path.read_text())
        heads[payload["meta"]["zone"]] = index_cells(payload)
    sparks = {}
    for path in spark_paths:
        payload = json.loads(path.read_text())
        sparks[payload["meta"]["zone"]] = index_cells(payload)
    mains = {}
    main_payload = json.loads(main_path.read_text())
    for zone, cells in main_payload["zones"].items():
        mains[zone] = {cell["id"]: cell for cell in cells}
    zones = sorted(set(heads) | set(sparks) | set(mains))
    lines = [
        "# Round 12 joined-head measurement",
        "",
        "Head: the joined branch release module, measured by join_round12. "
        "Spark: live Spark 4.1.2 with Iceberg 1.11, measured by join_round12. "
        "Main: taken from the two verify1 verdicts' recorded outputs (the "
        "coercion per-cell main rows; the F1 wall and instant rows from the "
        "F1 finding, which are zone-independent).",
        "",
    ]
    combined: dict[str, Any] = {"zones": {}, "halt_rows": [], "hole_rows": []}
    for zone in zones:
        rendered, zone_rows, halts, holes = render_table(zone, heads, sparks, mains)
        lines += rendered
        lines.append("")
        combined["zones"][zone] = zone_rows
        combined["halt_rows"] += halts
        combined["hole_rows"] += holes
    halt_count = len(combined["halt_rows"])
    hole_count = len(combined["hole_rows"])
    if halt_count:
        lines += [
            f"Decision: HALT. {halt_count} rows differ from Spark where main equalled Spark.",
            "",
        ]
    else:
        lines += [
            "Decision: continue. 0 rows differ from Spark where main equalled "
            f"Spark; {hole_count} named-hole rows differ where main also differed.",
            "",
        ]
    out_json.write_text(json.dumps(combined, indent=1, sort_keys=True, default=str) + "\n")
    out_md.write_text("\n".join(lines))
    print(f"halt={halt_count} hole={hole_count}")
    return halt_count


def build_parser() -> argparse.ArgumentParser:
    """Build the recorder's argument parser."""
    parser = argparse.ArgumentParser(description="Round 12 joined-head measurement.")
    sub = parser.add_subparsers(dest="mode", required=True)
    run = sub.add_parser("measure", help="Run every cell on one engine in one zone.")
    run.add_argument("--engine", choices=("repark", "spark"), required=True)
    run.add_argument("--zone", required=True)
    run.add_argument("--out", type=Path, required=True)
    run.add_argument("--warehouse", type=Path, default=None)
    total = sub.add_parser("summarize", help="Merge head, Spark and main outputs.")
    total.add_argument("--head", type=Path, nargs="+", required=True)
    total.add_argument("--spark", type=Path, nargs="+", required=True)
    total.add_argument("--main", type=Path, required=True)
    total.add_argument("--out-json", type=Path, required=True)
    total.add_argument("--out-md", type=Path, required=True)
    return parser


def main(argv: list[str]) -> int:
    """Dispatch the recorder's two modes."""
    args = build_parser().parse_args(argv)
    if args.mode == "measure":
        warehouse = args.warehouse or Path(tempfile.mkdtemp(prefix="round12-join-"))
        measure(args.engine, args.zone, args.out, warehouse)
        return 0
    summarize(args.head, args.spark, args.main, args.out_json, args.out_md)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
