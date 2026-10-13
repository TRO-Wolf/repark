"""ICE-TSNS-CREATE-DOOR-1: the create-door matrix, and the recorder of what a build answers."""

from __future__ import annotations

import argparse
import itertools
import json
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa

from repark import ReparkSession

FIXTURE = Path(__file__).with_name("ice_tsns_create_door_1_main.json")
SEED_TEXT = "2026-01-02 03:04:05.123456789"
SEED_NANOS = 1_767_323_045_123_456_789
ZONES = ("UTC", "America/New_York")
COLUMNS = ("c", "cz")
NANO_TYPES = {"c": "timestamp_ns", "cz": "timestamptz_ns"}
MICRO_TYPES = {"c": "TIMESTAMP_NTZ", "cz": "TIMESTAMP"}
ARROW_TYPES = {"c": pa.timestamp("ns"), "cz": pa.timestamp("ns", tz="UTC")}
ASKS = ("none", "cast", "setting")
SETTING_KEY = "repark.sql.allowImplicitTimestampNanos"
V3 = "TBLPROPERTIES ('format-version' = '3')"
EVOLVING = "TBLPROPERTIES ('format-version' = '3', 'write.spark.accept-any-schema' = 'true')"
EVOLVING_V2 = "TBLPROPERTIES ('write.spark.accept-any-schema' = 'true')"
SEED = "ice.ns.seed"
MICRO = "ice.ns.micro"
FORMS = {
    "ctas": "CTAS",
    "create_or_replace": "RTAS",
    "replace": "RTAS",
    "write_to_create": "CTAS",
    "write_to_create_or_replace": "RTAS",
    "write_to_replace": "RTAS",
    "save_create": "CTAS",
    "save_overwrite": "RTAS",
    "save_overwrite_schema": "RTAS",
    "save_merge_schema": "mergeSchema append",
    "write_to_append_merge_schema": "mergeSchema append",
    "merge_schema_evolution": "MERGE WITH SCHEMA EVOLUTION",
}
DOORS = tuple(FORMS)
REPLACED = ("replace", "write_to_replace", "save_overwrite", "save_overwrite_schema")
EVOLVED = ("save_merge_schema", "write_to_append_merge_schema")
PASS3_SOURCES = ("scol", "sarrow", "scast")
PASS3_DOORS = (
    "ctas",
    "create_or_replace",
    "replace",
    "temp_view",
    "write_to_create",
    "write_to_create_or_replace",
    "write_to_replace",
)
PLAIN = (
    "ctas",
    "create_or_replace",
    "write_to_create",
    "save_create",
    "save_overwrite",
    "save_append",
    "save_merge_schema",
    "merge_schema_evolution",
    "error_exists",
    "error_replace_missing",
    "error_unresolved_column",
    "error_too_many_columns",
)
V2_DOORS = (
    "ctas_plain",
    "cast_ns",
    "cor",
    "ddl",
    "addcol_v2base",
    "tempview",
    "like",
    "cache",
    "replace_missing",
    "wtocreate",
    "saveastable",
    "wtoreplace",
    "saveoverwrite",
    "overwriteschema",
    "mergeschema",
    "wtoappendmerge",
    "savepath",
    "evolving",
    "evolving_cast",
)
VIEW_NAMES = itertools.count()


def open_session(zone: str, warehouse: Path, setting: bool = False) -> Any:
    """Open a v3-enabled session in ``zone`` with the nanosecond and microsecond seeds written."""
    builder = (
        ReparkSession.builder.appName("ice-tsns-create-door-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", zone)
    )
    if setting:
        builder = builder.config(SETTING_KEY, "true")
    spark = builder.getOrCreate()
    spark.register_memory_catalog("ice", str(warehouse))
    spark.sql("CREATE NAMESPACE ice.ns")
    spark.sql(f"CREATE TABLE {SEED} (id INT, c timestamp_ns, cz timestamptz_ns) USING iceberg {V3}")
    spark.sql(
        f"INSERT INTO {SEED} VALUES (1, CAST('{SEED_TEXT}' AS timestamp_ns), "
        f"CAST('{SEED_TEXT}+00:00' AS timestamptz_ns)), (2, NULL, NULL)"
    )
    spark.sql(f"CREATE TABLE {MICRO} (id INT, c TIMESTAMP_NTZ, cz TIMESTAMP) USING iceberg")
    spark.sql(
        f"INSERT INTO {MICRO} VALUES (1, TIMESTAMP_NTZ '{SEED_TEXT[:-3]}', "
        f"TIMESTAMP '{SEED_TEXT[:-3]}+00:00'), (2, NULL, NULL)"
    )
    return spark


def arrow_view(spark: Any) -> str:
    """Register the seed rows as a capsule Arrow view and return its name."""
    name = f"create_door_arrow_{next(VIEW_NAMES)}"
    table = pa.table(
        {
            "id": pa.array([1, 2], pa.int32()),
            "c": pa.array([SEED_NANOS, None], ARROW_TYPES["c"]),
            "cz": pa.array([SEED_NANOS, None], ARROW_TYPES["cz"]),
        }
    )
    spark._ensure_alive().register_arrow_stream_as_temp_view(name, table)
    return name


def select_sql(ask: str, column: str) -> str:
    """Return the query of one matrix cell: the plain column, or a written cast of it."""
    if ask == "cast":
        return f"SELECT id, CAST({column} AS {NANO_TYPES[column]}) AS {column} FROM {SEED}"
    return f"SELECT id, {column} FROM {SEED}"


def frame_of(spark: Any, ask: str, column: str) -> Any:
    """Return the DataFrame of one matrix cell, the cast written through ``selectExpr``."""
    if ask == "cast":
        cast = f"CAST({column} AS {NANO_TYPES[column]}) AS {column}"
        return spark.table(SEED).selectExpr("id", cast)
    return spark.table(SEED).select("id", column)


def prepare_door(spark: Any, door: str, table: str) -> None:
    """Create the table a replacing or evolving door needs before it runs."""
    if door in REPLACED:
        spark.sql(f"CREATE TABLE {table} (id INT, keepcol INT) USING iceberg {V3}")
    elif door in EVOLVED:
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg {EVOLVING}")
    elif door == "merge_schema_evolution":
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg {V3}")


def run_door(spark: Any, door: str, table: str, select: str, frame: Any) -> None:
    """Run one create door over ``select`` (SQL doors) or ``frame`` (DataFrame doors)."""
    if door == "ctas":
        spark.sql(f"CREATE TABLE {table} USING iceberg {V3} AS {select}")
    elif door == "create_or_replace":
        spark.sql(f"CREATE OR REPLACE TABLE {table} USING iceberg {V3} AS {select}")
    elif door == "replace":
        spark.sql(f"REPLACE TABLE {table} USING iceberg AS {select}")
    elif door == "temp_view":
        view = f"create_door_view_{next(VIEW_NAMES)}"
        frame.createOrReplaceTempView(view)
        spark.sql(f"CREATE TABLE {table} USING iceberg {V3} AS SELECT * FROM {view}")
    elif door == "write_to_create":
        frame.writeTo(table).tableProperty("format-version", "3").create()
    elif door == "write_to_create_or_replace":
        frame.writeTo(table).tableProperty("format-version", "3").createOrReplace()
    elif door == "write_to_replace":
        frame.writeTo(table).replace()
    elif door == "save_create":
        frame.write.saveAsTable(table)
    elif door == "save_overwrite":
        frame.write.mode("overwrite").saveAsTable(table)
    elif door == "save_overwrite_schema":
        frame.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
    elif door == "save_append":
        frame.write.mode("append").saveAsTable(table)
    elif door == "save_merge_schema":
        frame.write.mode("append").option("mergeSchema", "true").saveAsTable(table)
    elif door == "write_to_append_merge_schema":
        frame.writeTo(table).option("mergeSchema", "true").append()
    elif door == "merge_schema_evolution":
        spark.sql(
            f"MERGE WITH SCHEMA EVOLUTION INTO {table} t USING ({select}) s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
        )
    else:
        raise ValueError(door)


def table_files(warehouse: Path, table: str) -> list[str]:
    """Return every file under the table's directory, relative to it."""
    root = warehouse / "ns" / table.rsplit(".", 1)[1]
    return sorted(str(path.relative_to(root)) for path in root.rglob("*") if path.is_file())


def table_state(spark: Any, warehouse: Path, table: str) -> dict[str, Any] | None:
    """Return the stored format version, Iceberg types, Arrow types and int64 rows."""
    root = warehouse / "ns" / table.rsplit(".", 1)[1] / "metadata"
    files = sorted(root.glob("*.metadata.json"), key=metadata_version)
    if not files:
        return None
    metadata = json.loads(files[-1].read_text(encoding="utf-8"))
    current = metadata["current-schema-id"]
    schema = next(entry for entry in metadata["schemas"] if entry["schema-id"] == current)
    names = [field["name"] for field in schema["fields"]]
    order = "id" if "id" in names else f"{names[0]} NULLS LAST"
    read = spark.sql(f"SELECT * FROM {table} ORDER BY {order}").to_arrow()
    return {
        "format_version": metadata["format-version"],
        "schema": [[field["name"], field["type"]] for field in schema["fields"]],
        "arrow": [str(field.type) for field in read.schema],
        "rows": [read.column(name).cast(pa.int64()).to_pylist() for name in read.schema.names],
    }


def metadata_version(path: Path) -> int:
    """Return the sequence number a metadata file name starts with."""
    return int(path.name.split("-", 1)[0])


def error_cell(error: Exception, warehouse: Path) -> dict[str, Any]:
    """Return the class, condition and text of ``error`` with the warehouse path masked."""
    condition = getattr(error, "getCondition", None)
    return {
        "class": type(error).__name__,
        "condition": condition() if callable(condition) else None,
        "text": str(error).replace(str(warehouse), "<warehouse>"),
    }


def measure(spark: Any, warehouse: Path, table: str, action: Callable[[], None]) -> dict[str, Any]:
    """Run ``action`` and return its error, if any, with the table's state and file list."""
    before = table_files(warehouse, table)
    cell: dict[str, Any] = {}
    try:
        action()
    except Exception as error:
        cell["error"] = error_cell(error, warehouse)
        cell["files_unchanged"] = table_files(warehouse, table) == before
    cell["table"] = table_state(spark, warehouse, table)
    return cell


def matrix_cell(spark: Any, warehouse: Path, door: str, column: str, ask: str) -> dict[str, Any]:
    """Measure one cell of the 12-door matrix."""
    table = f"ice.ns.m_{door}_{column}"
    prepare_door(spark, door, table)
    select = select_sql(ask, column)
    frame = frame_of(spark, ask, column)
    return measure(spark, warehouse, table, lambda: run_door(spark, door, table, select, frame))


def pass3_select(spark: Any, source: str) -> str:
    """Return the two-column query of a census pass 3 source."""
    if source == "scol":
        return f"SELECT c, cz FROM {SEED}"
    if source == "sarrow":
        return f"SELECT c, cz FROM {arrow_view(spark)}"
    return (
        f"SELECT CAST('{SEED_TEXT}' AS timestamp_ns) AS c, "
        f"CAST('{SEED_TEXT}+00:00' AS timestamptz_ns) AS cz"
    )


def pass3_cell(spark: Any, warehouse: Path, source: str, door: str) -> dict[str, Any]:
    """Measure one census pass 3 cell."""
    table = f"ice.ns.p_{source}_{door}"
    prepare_door(spark, door, table)
    select = pass3_select(spark, source)
    frame = spark.sql(select)
    return measure(spark, warehouse, table, lambda: run_door(spark, door, table, select, frame))


def plain_cell(spark: Any, warehouse: Path, name: str) -> dict[str, Any]:
    """Measure one statement that has no nanosecond output, over the microsecond source."""
    table = f"ice.ns.plain_{name}"
    select = f"SELECT id, c FROM {MICRO}"
    frame = spark.table(MICRO).select("id", "c")
    if name == "error_exists":
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
        statement = f"CREATE TABLE {table} USING iceberg AS {select}"
        return measure(spark, warehouse, table, lambda: spark.sql(statement))
    if name == "error_replace_missing":
        statement = f"REPLACE TABLE {table} USING iceberg AS {select}"
        return measure(spark, warehouse, table, lambda: spark.sql(statement))
    if name == "error_unresolved_column":
        statement = f"CREATE TABLE {table} USING iceberg AS SELECT id, absent FROM {MICRO}"
        return measure(spark, warehouse, table, lambda: spark.sql(statement))
    if name == "error_too_many_columns":
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg")
        return measure(
            spark,
            warehouse,
            table,
            lambda: frame.write.mode("append").option("mergeSchema", "true").saveAsTable(table),
        )
    if name == "save_append":
        spark.sql(f"CREATE TABLE {table} (id INT, c TIMESTAMP_NTZ) USING iceberg")
    else:
        prepare_door(spark, name, table)
    return measure(spark, warehouse, table, lambda: run_door(spark, name, table, select, frame))


def v2_cell(spark: Any, warehouse: Path, door: str) -> dict[str, Any]:
    """Measure one door of the census v2 refusal list, re-seated on native nanosecond sources."""
    table = f"ice.ns.v2_{door}"
    pre = f"ice.ns.v2_pre_{door}"
    frame = spark.table(SEED).select("id", "c")
    wide = frame.withColumn("c2", frame["c"])
    statements: dict[str, list[str]] = {
        "ctas_plain": [f"CREATE TABLE {table} USING iceberg AS SELECT c FROM {SEED}"],
        "cast_ns": [
            f"CREATE TABLE {table} USING iceberg AS SELECT CAST(c AS timestamp_ns) AS c FROM {SEED}"
        ],
        "cor": [f"CREATE OR REPLACE TABLE {table} USING iceberg AS SELECT c FROM {SEED}"],
        "ddl": [f"CREATE TABLE {table} (id INT, c timestamp_ns) USING iceberg"],
        "addcol_v2base": [
            f"CREATE TABLE {table} (id INT) USING iceberg",
            f"ALTER TABLE {table} ADD COLUMN c2 timestamp_ns",
        ],
        "like": [f"CREATE TABLE {table} LIKE {SEED}"],
        "cache": [f"CACHE TABLE {table} AS SELECT c FROM {SEED}"],
        "replace_missing": [f"REPLACE TABLE {table} USING iceberg AS SELECT c FROM {SEED}"],
    }
    actions: dict[str, Callable[[], Any]] = {
        "tempview": lambda: spark.sql(
            f"CREATE TABLE {table} USING iceberg AS SELECT id, c FROM {arrow_view(spark)}"
        ),
        "wtocreate": lambda: frame.writeTo(table).create(),
        "saveastable": lambda: frame.write.saveAsTable(table),
        "wtoreplace": lambda: frame.writeTo(pre).replace(),
        "saveoverwrite": lambda: frame.write.mode("overwrite").saveAsTable(pre),
        "overwriteschema": lambda: (
            wide.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(pre)
        ),
        "mergeschema": lambda: (
            wide.write.mode("append").option("mergeSchema", "true").saveAsTable(pre)
        ),
        "wtoappendmerge": lambda: wide.writeTo(pre).option("mergeSchema", "true").append(),
        "savepath": lambda: frame.write.format("iceberg").save(str(warehouse / "v2-path")),
    }
    if door in statements:
        for statement in statements[door][:-1]:
            spark.sql(statement)
        last = statements[door][-1]
        return measure(spark, warehouse, table, lambda: spark.sql(last))
    if door in ("evolving", "evolving_cast"):
        spark.sql(f"CREATE TABLE {table} (id INT) USING iceberg {EVOLVING_V2}")
        asked = frame_of(spark, "cast" if door == "evolving_cast" else "none", "c")
        writer = asked.writeTo(table).option("mergeSchema", "true")
        return measure(spark, warehouse, table, writer.append)
    if door not in ("tempview", "wtocreate", "saveastable", "savepath"):
        spark.sql(f"CREATE TABLE {pre} (id INT, c TIMESTAMP) USING iceberg")
        return measure(spark, warehouse, pre, actions[door])
    return measure(spark, warehouse, table, actions[door])


def in_session(
    zone: str, setting: bool, cell: Callable[..., dict[str, Any]], *arguments: str
) -> dict[str, Any]:
    """Run ``cell`` over ``arguments`` in a fresh session and warehouse."""
    with tempfile.TemporaryDirectory(prefix="tsns-create-door-") as directory:
        warehouse = Path(directory)
        spark = open_session(zone, warehouse, setting)
        try:
            return cell(spark, warehouse, *arguments)
        finally:
            spark.stop()


def record() -> dict[str, dict[str, Any]]:
    """Measure every section of the fixture on the installed build."""
    sections: dict[str, dict[str, Any]] = {"matrix": {}, "pass3": {}, "plain": {}, "v2": {}}
    for zone in ZONES:
        for door, column, ask in itertools.product(DOORS, COLUMNS, ("none", "cast")):
            key = f"{zone}|{door}|{column}|{ask}"
            sections["matrix"][key] = in_session(zone, False, matrix_cell, door, column, ask)
        for source, door in itertools.product(PASS3_SOURCES, PASS3_DOORS):
            key = f"{zone}|{source}|{door}"
            sections["pass3"][key] = in_session(zone, False, pass3_cell, source, door)
        for name in PLAIN:
            sections["plain"][f"{zone}|{name}"] = in_session(zone, False, plain_cell, name)
        for door in V2_DOORS:
            sections["v2"][f"{zone}|{door}"] = in_session(zone, False, v2_cell, door)
    return sections


def main() -> None:
    """Record the installed build's answers into the fixture or ``--output``."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("commit")
    parser.add_argument("--output", type=Path, default=FIXTURE)
    arguments = parser.parse_args()
    document = {"recorded_at_commit": arguments.commit, "seed": SEED_TEXT, **record()}
    arguments.output.write_text(
        json.dumps(document, sort_keys=True, indent=1) + "\n", encoding="utf-8"
    )
    print({name: len(cells) for name, cells in document.items() if isinstance(cells, dict)})


if __name__ == "__main__":
    main()
