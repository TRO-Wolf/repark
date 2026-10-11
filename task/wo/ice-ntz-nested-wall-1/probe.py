"""Measure nested TIMESTAMP_NTZ store doors on RePark and on Spark.

The warehouse, the output path and, for Spark, the ivy directory come from
arguments or from the environment. One line of JSON per cell.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pyarrow as pa

TS_LIT = "TIMESTAMP '2026-01-02 03:04:05.123456+00:00'"
NTZ_LIT = "TIMESTAMP_NTZ '2026-01-02 03:04:05.123456'"
INSTANT_US = 1_767_323_045_123_456
ZONES = ("UTC", "America/New_York", "Asia/Kolkata")
SCHEMAS = ("ntz_nested", "ltz_nested", "ntz_top")
SOURCES = ("ts_literal", "ts_column", "ntz_literal")
SESSION_WALL = {
    "UTC": INSTANT_US,
    "America/New_York": 1_767_305_045_123_456,
    "Asia/Kolkata": 1_767_342_845_123_456,
}
DOORS: tuple[tuple[str, bool], ...] = (
    ("insert_values", False),
    ("insert_select", False),
    ("insert_by_name", False),
    ("insert_overwrite", False),
    ("insert_overwrite_values", False),
    ("insert_replace_where", False),
    ("merge_insert", True),
    ("merge_insert_star", True),
    ("merge_insert_literal", True),
    ("merge_update_whole", True),
    ("merge_update_star", True),
    ("merge_update_whole_literal", True),
    ("update_whole_where", True),
    ("update_whole_nowhere", True),
    ("update_col_where", True),
    ("update_col_nowhere", True),
    ("df_append", False),
    ("df_overwrite_partitions", False),
    ("df_overwrite_condition", False),
    ("df_insert_into", False),
    ("df_insert_into_overwrite", False),
    ("df_save_as_table", False),
    ("merge_update_field", True),
    ("merge_update_field_literal", True),
    ("update_field_where", True),
    ("update_field_nowhere", True),
    ("update_field_col_where", True),
    ("update_field_col_nowhere", True),
)
MOR = (
    "'write.delete.mode'='merge-on-read', "
    "'write.update.mode'='merge-on-read', "
    "'write.merge.mode'='merge-on-read'"
)
SEED_DOORS = {
    "insert_overwrite",
    "insert_overwrite_values",
    "insert_replace_where",
    "merge_update_whole",
    "merge_update_star",
    "merge_update_whole_literal",
    "update_whole_where",
    "update_whole_nowhere",
    "update_col_where",
    "update_col_nowhere",
    "df_overwrite_partitions",
    "df_overwrite_condition",
    "df_insert_into_overwrite",
    "merge_update_field",
    "merge_update_field_literal",
    "update_field_where",
    "update_field_nowhere",
    "update_field_col_where",
    "update_field_col_nowhere",
}


def inner_expr(source: str, place: str) -> str:
    """Return the SQL of one source value at `place` (`bare`, `src` or `s`)."""
    if source == "ts_literal":
        return TS_LIT
    if source == "ntz_literal":
        return NTZ_LIT
    if place == "s":
        return "s.l"
    if place == "src":
        return "l"
    return "(SELECT l FROM ice.ns.src)"


def value_column(schema: str) -> str:
    """Return the stored column name for `schema`."""
    if schema == "ntz_top":
        return "v"
    return "st"


def field_path(schema: str) -> str:
    """Return the leaf path a field assignment writes."""
    if schema == "ntz_top":
        return "v"
    return "st.v"


def packed(schema: str, source: str, place: str) -> str:
    """Return the SQL written into the target column."""
    inner = inner_expr(source, place)
    if schema == "ntz_top":
        return inner
    return f"named_struct('v', {inner}, 'n', 1)"


def null_sql(schema: str) -> str:
    """Return a typed NULL of the target column."""
    if schema == "ntz_top":
        return "CAST(NULL AS TIMESTAMP_NTZ)"
    leaf = "TIMESTAMP_NTZ" if schema == "ntz_nested" else "TIMESTAMP"
    return f"named_struct('v', CAST(NULL AS {leaf}), 'n', 0)"


def source_type(source: str) -> str:
    """Return the SQL type of a sibling column that holds `source`."""
    if source == "ntz_literal":
        return "TIMESTAMP_NTZ"
    return "TIMESTAMP"


def table_ddl(schema: str, source: str, door: str) -> str:
    """Return the column list of one target table."""
    extra = ""
    if "col" in door and door.startswith("update_"):
        extra = f", c {source_type(source)}"
    column = value_column(schema)
    if schema == "ntz_top":
        declared = "TIMESTAMP_NTZ"
    elif schema == "ntz_nested":
        declared = "STRUCT<v: TIMESTAMP_NTZ, n: INT>"
    else:
        declared = "STRUCT<v: TIMESTAMP, n: INT>"
    return f"id INT, {column} {declared}, k INT{extra}"


def projection(schema: str, source: str) -> str:
    """Return a SELECT of one row shaped as the target columns."""
    column = value_column(schema)
    value = packed(schema, source, "src")
    if source == "ts_column":
        return f"SELECT id, {value} AS {column}, 0 AS k FROM ice.ns.src"
    return f"SELECT 1 AS id, {value} AS {column}, 0 AS k"


def error_record(error: Exception) -> dict[str, str]:
    """Return the class, the first line, and the next line of `error`."""
    text = [line.strip() for line in str(error).splitlines() if line.strip()]
    first = text[0] if text else ""
    nxt = text[1] if len(text) > 1 else ""
    return {
        "error_class": type(error).__name__,
        "error_line": first[:500],
        "error_next": nxt[:300],
    }


def ticks_of(table: pa.Table) -> dict[str, Any]:
    """Return physical integer ticks of column `v`."""
    column = table.column("v").combine_chunks()
    arrow_type = str(column.type)
    unit = column.type.unit if pa.types.is_timestamp(column.type) else ""
    values = column.cast(pa.int64(), safe=False).to_pylist()
    scale = {"s": 1_000_000, "ms": 1_000, "us": 1, "ns": 1}
    divisor = 1000 if unit == "ns" else 1
    factor = scale.get(unit, 1)
    normalized: list[int | None] = []
    for value in values:
        if value is None:
            normalized.append(None)
        elif unit == "ns":
            normalized.append(int(value) // divisor)
        else:
            normalized.append(int(value) * factor)
    return {
        "ticks_raw": values,
        "ticks_us": normalized,
        "arrow_type": arrow_type,
        "arrow_unit": unit,
    }


def read_leaf(spark: Any, table: str, schema: str) -> dict[str, Any]:
    """Read the stored leaf through `toArrow` and cast it to int64."""
    leaf = field_path(schema)
    arrow = spark.sql(f"SELECT {leaf} AS v FROM {table}").toArrow()
    return ticks_of(arrow)


def open_repark(zone: str, warehouse: Path) -> Any:
    """Open one RePark session bound to `warehouse`."""
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("ice-ntz-nested-wall-1")
        .config("spark.sql.session.timeZone", zone)
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    return spark


def open_spark(zone: str, warehouse: Path) -> Any:
    """Open one Spark session with a Hadoop Iceberg catalog at `warehouse`."""
    from pyspark.sql import SparkSession

    packages = os.environ.get(
        "SPARK_PACKAGES",
        "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0",
    )
    builder = (
        SparkSession.builder.master("local[4]")
        .appName("ice-ntz-nested-wall-1")
        .config("spark.driver.memory", "4g")
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
    spark = builder.getOrCreate()
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ice.ns")
    return spark


def create_source(spark: Any) -> dict[str, Any]:
    """Write the instant into `ice.ns.src.l` and return the stored tick."""
    spark.sql(
        "CREATE TABLE ice.ns.src (id INT, l TIMESTAMP) USING iceberg "
        "TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql(f"INSERT INTO ice.ns.src VALUES (1, {TS_LIT})")
    arrow = spark.sql("SELECT l AS v FROM ice.ns.src").toArrow()
    return ticks_of(arrow)


def seed_table(spark: Any, table: str, schema: str, source: str, door: str) -> None:
    """Insert one NULL-valued row so an update or overwrite has a target."""
    column = value_column(schema)
    blank = null_sql(schema)
    if door.startswith("update_") and "col" in door:
        if source == "ts_column":
            spark.sql(f"INSERT INTO {table} SELECT 1, {blank}, 0, l FROM ice.ns.src")
        else:
            spark.sql(f"INSERT INTO {table} VALUES (1, {blank}, 0, {inner_expr(source, 'bare')})")
        return
    spark.sql(f"INSERT INTO {table} (id, {column}, k) VALUES (1, {blank}, 0)")


def condition(engine: str) -> Any:
    """Return the `id >= 0` column expression for this engine."""
    if engine == "spark":
        from pyspark.sql.functions import col
    else:
        from repark import functions

        col = functions.col
    return col("id") >= 0


def using_sql(source: str, literal_door: bool) -> str:
    """Return the MERGE USING clause for this source."""
    if source == "ts_column":
        if literal_door:
            return "(SELECT id, l FROM ice.ns.src) s"
        return "ice.ns.src s"
    if literal_door:
        return "(SELECT 1 AS id) s"
    return "ice.ns.src s"


def run_door(spark: Any, engine: str, table: str, schema: str, door: str, source: str) -> None:
    """Run one door. The caller owns the fresh table."""
    column = value_column(schema)
    field = field_path(schema)
    place = "s" if source == "ts_column" else "bare"
    whole = packed(schema, source, place)
    leaf = inner_expr(source, place)
    select = projection(schema, source)
    if door in SEED_DOORS:
        seed_table(spark, table, schema, source, door)
    if door == "insert_values":
        spark.sql(f"INSERT INTO {table} VALUES (1, {packed(schema, source, 'bare')}, 0)")
    elif door == "insert_select":
        spark.sql(f"INSERT INTO {table} {select}")
    elif door == "insert_by_name":
        value = packed(schema, source, "src")
        if source == "ts_column":
            spark.sql(
                f"INSERT INTO {table} BY NAME SELECT 0 AS k, {value} AS {column}, id "
                "FROM ice.ns.src"
            )
        else:
            spark.sql(f"INSERT INTO {table} BY NAME SELECT 0 AS k, {value} AS {column}, 1 AS id")
    elif door == "insert_overwrite":
        spark.sql(f"INSERT OVERWRITE {table} {select}")
    elif door == "insert_overwrite_values":
        spark.sql(f"INSERT OVERWRITE {table} VALUES (1, {packed(schema, source, 'bare')}, 0)")
    elif door == "insert_replace_where":
        spark.sql(f"INSERT INTO {table} REPLACE WHERE id >= 0 {select}")
    elif door == "merge_insert":
        spark.sql(
            f"MERGE INTO {table} t USING {using_sql(source, False)} ON t.id = s.id "
            f"WHEN NOT MATCHED THEN INSERT (id, {column}, k) VALUES (s.id, {whole}, 0)"
        )
    elif door == "merge_insert_star":
        spark.sql(
            f"MERGE INTO {table} t USING ({select}) s ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
        )
    elif door == "merge_insert_literal":
        written = packed(schema, source, "s" if source == "ts_column" else "bare")
        spark.sql(
            f"MERGE INTO {table} t USING {using_sql(source, True)} ON t.id = s.id "
            f"WHEN NOT MATCHED THEN INSERT (id, {column}, k) VALUES (s.id, {written}, 0)"
        )
    elif door == "merge_update_whole":
        spark.sql(
            f"MERGE INTO {table} t USING {using_sql(source, False)} ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET t.{column} = {whole}"
        )
    elif door == "merge_update_star":
        spark.sql(
            f"MERGE INTO {table} t USING ({select}) s ON t.id = s.id WHEN MATCHED THEN UPDATE SET *"
        )
    elif door == "merge_update_whole_literal":
        written = packed(schema, source, "s" if source == "ts_column" else "bare")
        spark.sql(
            f"MERGE INTO {table} t USING {using_sql(source, True)} ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET t.{column} = {written}"
        )
    elif door == "update_whole_where":
        spark.sql(f"UPDATE {table} SET {column} = {packed(schema, source, 'bare')} WHERE id >= 0")
    elif door == "update_whole_nowhere":
        spark.sql(f"UPDATE {table} SET {column} = {packed(schema, source, 'bare')}")
    elif door == "merge_update_field":
        spark.sql(
            f"MERGE INTO {table} t USING {using_sql(source, False)} ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET t.{field} = {leaf}"
        )
    elif door == "merge_update_field_literal":
        written = inner_expr(source, "s" if source == "ts_column" else "bare")
        spark.sql(
            f"MERGE INTO {table} t USING {using_sql(source, True)} ON t.id = s.id "
            f"WHEN MATCHED THEN UPDATE SET t.{field} = {written}"
        )
    elif door == "update_field_where":
        spark.sql(f"UPDATE {table} SET {field} = {inner_expr(source, 'bare')} WHERE id >= 0")
    elif door == "update_field_nowhere":
        spark.sql(f"UPDATE {table} SET {field} = {inner_expr(source, 'bare')}")
    elif door == "update_field_col_where":
        spark.sql(f"UPDATE {table} SET {field} = c WHERE id >= 0")
    elif door == "update_field_col_nowhere":
        spark.sql(f"UPDATE {table} SET {field} = c")
    elif door == "df_append":
        spark.sql(select).writeTo(table).append()
    elif door == "df_overwrite_partitions":
        spark.sql(select).writeTo(table).overwritePartitions()
    elif door == "df_overwrite_condition":
        spark.sql(select).writeTo(table).overwrite(condition(engine))
    elif door == "df_insert_into":
        spark.sql(select).write.insertInto(table)
    elif door == "df_insert_into_overwrite":
        spark.sql(select).write.insertInto(table, overwrite=True)
    elif door == "df_save_as_table":
        spark.sql(select).write.mode("append").saveAsTable(table)
    else:
        raise RuntimeError(f"unknown door {door}")


def packed_sibling(schema: str) -> str:
    """Return a struct or scalar built from the sibling column `c`."""
    if schema == "ntz_top":
        return "c"
    return "named_struct('v', c, 'n', 1)"


def run_door_fixed(
    spark: Any, engine: str, table: str, schema: str, door: str, source: str
) -> None:
    """Run one door, with sibling-column updates written from `c`."""
    if door in {"update_col_where", "update_col_nowhere"}:
        column = value_column(schema)
        written = packed_sibling(schema)
        if door in SEED_DOORS:
            seed_table(spark, table, schema, source, door)
        where = " WHERE id >= 0" if door.endswith("_where") else ""
        spark.sql(f"UPDATE {table} SET {column} = {written}{where}")
        return
    run_door(spark, engine, table, schema, door, source)


def measure_cell(
    spark: Any,
    engine: str,
    schema: str,
    zone: str,
    door: str,
    mode: str,
    source: str,
    index: int,
) -> dict[str, Any]:
    """Create one table, run one door, and return the cell."""
    table = f"ice.ns.t{index}"
    props = f", {MOR}" if mode == "mor" else ""
    record: dict[str, Any] = {
        "engine": engine,
        "schema": schema,
        "zone": zone,
        "door": door,
        "mode": mode,
        "source": source,
        "table": table,
    }
    try:
        spark.sql(
            f"CREATE TABLE {table} ({table_ddl(schema, source, door)}) USING iceberg "
            f"TBLPROPERTIES ('format-version'='2'{props})"
        )
        run_door_fixed(spark, engine, table, schema, door, source)
        record.update(read_leaf(spark, table, schema))
    except Exception as error:
        record.update(error_record(error))
    return record


def measure_zone(
    engine: str,
    zone: str,
    warehouse: Path,
    schemas: tuple[str, ...],
    doors: tuple[tuple[str, bool], ...],
    sources: tuple[str, ...],
    handle: Any,
) -> int:
    """Measure every selected cell of one zone. Return how many were written."""
    opener = open_spark if engine == "spark" else open_repark
    spark = opener(zone, warehouse)
    count = 0
    try:
        source_cell = {
            "kind": "source",
            "engine": engine,
            "zone": zone,
        }
        try:
            source_cell.update(create_source(spark))
        except Exception as error:
            source_cell.update(error_record(error))
        handle.write(json.dumps(source_cell, sort_keys=True) + "\n")
        handle.flush()
        index = 0
        for schema in schemas:
            for door, row_level in doors:
                modes = ("cow", "mor") if row_level else ("cow",)
                for mode in modes:
                    for source in sources:
                        index += 1
                        cell = measure_cell(spark, engine, schema, zone, door, mode, source, index)
                        handle.write(json.dumps(cell, sort_keys=True) + "\n")
                        count += 1
                        if count % 25 == 0:
                            handle.flush()
                            print(
                                f"{engine} {zone} {count}",
                                file=sys.stderr,
                                flush=True,
                            )
        handle.flush()
    finally:
        spark.stop()
    return count


def parse_list(text: str, allowed: tuple[str, ...]) -> tuple[str, ...]:
    """Return `text` split on commas, checked against `allowed`."""
    if not text:
        return allowed
    chosen = tuple(part.strip() for part in text.split(",") if part.strip())
    unknown = [part for part in chosen if part not in allowed]
    if unknown:
        raise SystemExit(f"unknown name: {', '.join(unknown)}")
    return chosen


def parse_doors(text: str) -> tuple[tuple[str, bool], ...]:
    """Return the door rows named by a comma list, or every door."""
    if not text:
        return DOORS
    wanted = {part.strip() for part in text.split(",") if part.strip()}
    chosen = tuple(row for row in DOORS if row[0] in wanted)
    found = {row[0] for row in chosen}
    missing = wanted - found
    if missing:
        raise SystemExit(f"unknown door: {', '.join(sorted(missing))}")
    return chosen


def command_run(arguments: argparse.Namespace) -> None:
    """Measure the inventory and append JSON lines to the output."""
    warehouse = Path(arguments.warehouse)
    warehouse.mkdir(parents=True, exist_ok=True)
    zones = parse_list(arguments.zones, ZONES)
    output = Path(arguments.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    if not arguments.child and len(zones) > 1:
        for zone in zones:
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "run",
                "--engine",
                arguments.engine,
                "--warehouse",
                str(warehouse),
                "--output",
                str(output),
                "--zones",
                zone,
                "--schemas",
                arguments.schemas,
                "--sources",
                arguments.sources,
                "--doors",
                arguments.doors,
                "--child",
            ]
            completed = subprocess.run(command, check=False)
            if completed.returncode != 0:
                raise SystemExit(completed.returncode)
        return
    schemas = parse_list(arguments.schemas, SCHEMAS)
    sources = parse_list(arguments.sources, SOURCES)
    doors = parse_doors(arguments.doors)
    with output.open("a", encoding="utf-8") as handle:
        for zone in zones:
            zone_dir = warehouse / zone.replace("/", "_")
            zone_dir.mkdir(parents=True, exist_ok=True)
            measure_zone(arguments.engine, zone, zone_dir, schemas, doors, sources, handle)


def tick_text(cell: dict[str, Any]) -> str:
    """Return one cell's comparable stored value."""
    if "error_class" in cell:
        nxt = cell.get("error_next") or ""
        extra = f" / {nxt}" if nxt else ""
        return f"ERROR {cell['error_class']}: {cell.get('error_line', '')}{extra}"
    ticks = cell.get("ticks_us")
    if not isinstance(ticks, list) or not ticks:
        return "EMPTY"
    shown = ",".join("null" if value is None else str(value) for value in ticks)
    return shown


def command_summarize(arguments: argparse.Namespace) -> None:
    """Write the door-by-door comparison of the two JSONL files."""
    repark = load_cells(Path(arguments.repark))
    spark = load_cells(Path(arguments.spark))
    keys = sorted(set(repark) | set(spark))
    lines = [
        "# Nested TIMESTAMP_NTZ doors, RePark at this commit against Spark 4.1.2",
        "",
        "Ticks are microseconds, read by `toArrow()` and cast to int64.",
        "A refusal is the error class and the first line of the text.",
        "`same` means the two ticks, or the two refusals, are equal.",
        "",
        "| schema | door | mode | zone | source | RePark | Spark | same or differs |",
        "|---|---|---|---|---|---|---|---|",
    ]
    differ = 0
    for key in keys:
        left = tick_text(repark.get(key, {}))
        right = tick_text(spark.get(key, {}))
        mark = "same" if left == right else "differs"
        if mark == "differs":
            differ += 1
        schema, door, mode, zone, source = key
        lines.append(
            f"| {schema} | {door} | {mode} | {zone} | {source} | {left} | {right} | {mark} |"
        )
    lines.append("")
    lines.append(f"Rows: {len(keys)}. Differ: {differ}.")
    lines.append("")
    Path(arguments.summary).write_text("\n".join(lines), encoding="utf-8")
    print(f"rows {len(keys)} differ {differ}", file=sys.stderr)


def load_cells(path: Path) -> dict[tuple[str, str, str, str, str], dict[str, Any]]:
    """Return measured cells keyed by schema, door, mode, zone and source."""
    found: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        cell = json.loads(line)
        if cell.get("kind") == "source":
            continue
        key = (cell["schema"], cell["door"], cell["mode"], cell["zone"], cell["source"])
        found[key] = cell
    return found


def build_parser() -> argparse.ArgumentParser:
    """Return the probe's argument parser."""
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    run = sub.add_parser("run")
    run.add_argument("--engine", choices=("repark", "spark"), required=True)
    run.add_argument("--warehouse", required=True)
    run.add_argument("--output", required=True)
    run.add_argument("--zones", default="")
    run.add_argument("--schemas", default="")
    run.add_argument("--sources", default="")
    run.add_argument("--doors", default="")
    run.add_argument("--child", action="store_true")
    run.set_defaults(func=command_run)
    summary = sub.add_parser("summarize")
    summary.add_argument("--repark", required=True)
    summary.add_argument("--spark", required=True)
    summary.add_argument("--summary", required=True)
    summary.set_defaults(func=command_summarize)
    return parser


def main() -> None:
    """Run the selected subcommand."""
    arguments = build_parser().parse_args()
    arguments.func(arguments)


if __name__ == "__main__":
    main()
