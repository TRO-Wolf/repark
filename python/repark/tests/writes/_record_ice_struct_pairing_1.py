"""Record the ICE-STRUCT-PAIRING-1 door x shape matrix on Spark 4.1.2 or on a RePark module.

NOT a ``test_`` module: pytest never collects it. Every cell creates a table with one struct
column, seeds ``(0, (10, 20))``, writes ``(1, 2)`` through one door in one field shape and
reads the table back. Groups: ``v2`` and ``v3`` (16 doors x 4 containers x 8 shapes, format
versions 2 and 3), ``cs`` (``spark.sql.caseSensitive=true``, shapes ``same``, ``case`` and
``case_reordered``, format version 2) and ``added`` (the cells step 0 measured beside the
matrix). The test module imports the door table and the shapes from here, so the statements
live once.

Run with a PySpark 4.1.2 interpreter that resolves the Iceberg runtime GAV from
``_oracle_pins`` (``REPARK_ORACLE_IVY`` optionally points ``spark.jars.ivy`` at a warm cache),
or with an interpreter whose ``repark`` module is the build to record::

    _record_ice_struct_pairing_1.py spark ice_struct_pairing_1_spark_oracle.json
    _record_ice_struct_pairing_1.py repark ice_struct_pairing_1_main.json
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

FIELDS = {
    "same": (("a", 1), ("b", 2)),
    "reordered": (("b", 2), ("a", 1)),
    "renamed": (("a", 1), ("q", 2)),
    "renamed_first": (("q", 1), ("b", 2)),
    "extra": (("a", 1), ("b", 2), ("c", 3)),
    "missing": (("a", 1),),
    "case": (("A", 1), ("B", 2)),
    "case_reordered": (("B", 2), ("A", 1)),
}
CASE_FIELDS = ("same", "case", "case_reordered")
CONTAINERS = {
    "top": ("struct<a: INT, b: INT>", "{s}", "named_struct('a', 10, 'b', 20)"),
    "nested": (
        "struct<o: struct<a: INT, b: INT>, n: INT>",
        "named_struct('o', {s}, 'n', 9)",
        "named_struct('o', named_struct('a', 10, 'b', 20), 'n', 90)",
    ),
    "array": (
        "array<struct<a: INT, b: INT>>",
        "array({s})",
        "array(named_struct('a', 10, 'b', 20))",
    ),
    "map": (
        "map<string, struct<a: INT, b: INT>>",
        "map('k', {s})",
        "map('k', named_struct('a', 10, 'b', 20))",
    ),
}
BY_POSITION = (
    "insert",
    "insert_values",
    "insert_overwrite",
    "replace_where",
    "df_insert_into",
    "df_insert_into_overwrite",
)
BY_NAME = (
    "insert_by_name",
    "merge_insert",
    "merge_insert_star",
    "merge_update",
    "merge_update_star",
    "update",
    "update_no_where",
    "df_append",
    "df_overwrite_partitions",
    "df_save_append",
)
DOOR_ORDER = BY_POSITION + BY_NAME
UPDATING = ("merge_update", "merge_update_star", "update", "update_no_where")
REPLACING = ("insert_overwrite", "replace_where", "df_insert_into_overwrite")
UNLABELLED = (
    "merge_insert",
    "merge_insert_star",
    "merge_update",
    "merge_update_star",
    "update",
    "update_no_where",
)
GROUPS = {"v2": ("2", "false"), "v3": ("3", "false"), "cs": ("2", "true")}
NAMESPACE = {"v2": "v2", "v3": "v3", "cs": "cs", "added": "added"}


def struct_sql(field: str) -> str:
    pairs = ", ".join(f"'{name}', {value}" for name, value in FIELDS[field])
    return f"named_struct({pairs})"


def sql_door(template: str) -> Callable[[Any, str, str], None]:
    def run(spark: Any, table: str, src: str) -> None:
        spark.sql(template.format(t=table, s=src))

    return run


def frame(spark: Any, src: str) -> Any:
    return spark.sql(f"SELECT 1 AS id, {src} AS st")


def df_append(spark: Any, table: str, src: str) -> None:
    frame(spark, src).writeTo(table).append()


def df_overwrite_partitions(spark: Any, table: str, src: str) -> None:
    frame(spark, src).writeTo(table).overwritePartitions()


def df_insert_into(spark: Any, table: str, src: str) -> None:
    frame(spark, src).write.insertInto(table)


def df_insert_into_overwrite(spark: Any, table: str, src: str) -> None:
    frame(spark, src).write.insertInto(table, overwrite=True)


def df_save_append(spark: Any, table: str, src: str) -> None:
    frame(spark, src).write.mode("append").saveAsTable(table)


DOORS: dict[str, Callable[[Any, str, str], None]] = {
    "insert": sql_door("INSERT INTO {t} SELECT 1, {s}"),
    "insert_values": sql_door("INSERT INTO {t} VALUES (1, {s})"),
    "insert_by_name": sql_door("INSERT INTO {t} BY NAME SELECT {s} AS st, 1 AS id"),
    "insert_overwrite": sql_door("INSERT OVERWRITE {t} SELECT 1, {s}"),
    "replace_where": sql_door("INSERT INTO {t} REPLACE WHERE id >= 0 SELECT 1, {s}"),
    "merge_insert": sql_door(
        "MERGE INTO {t} t USING (SELECT 1 AS id, {s} AS st) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)"
    ),
    "merge_insert_star": sql_door(
        "MERGE INTO {t} t USING (SELECT 1 AS id, {s} AS st) s ON t.id = s.id "
        "WHEN NOT MATCHED THEN INSERT *"
    ),
    "merge_update": sql_door(
        "MERGE INTO {t} t USING (SELECT 0 AS id, {s} AS st) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET t.st = s.st"
    ),
    "merge_update_star": sql_door(
        "MERGE INTO {t} t USING (SELECT 0 AS id, {s} AS st) s ON t.id = s.id "
        "WHEN MATCHED THEN UPDATE SET *"
    ),
    "update": sql_door("UPDATE {t} SET st = {s} WHERE id = 0"),
    "update_no_where": sql_door("UPDATE {t} SET st = {s}"),
    "df_append": df_append,
    "df_overwrite_partitions": df_overwrite_partitions,
    "df_insert_into": df_insert_into,
    "df_insert_into_overwrite": df_insert_into_overwrite,
    "df_save_append": df_save_append,
}

TOP = "struct<a: INT, b: INT>"
TOP_SEED = "named_struct('a', 10, 'b', 20)"


def functions_of(spark: Any) -> Any:
    if type(spark).__module__.startswith("pyspark"):
        from pyspark.sql import functions

        return functions
    from repark import functions

    return functions


def added_overwrite_condition(spark: Any, table: str, field: str) -> None:
    condition = functions_of(spark).expr("id >= 0")
    frame(spark, struct_sql(field)).writeTo(table).overwrite(condition)


def added_merge_schema(spark: Any, table: str, field: str) -> None:
    frame(spark, struct_sql(field)).writeTo(table).option("merge-schema", "true").append()


def added_by_name(spark: Any, table: str, src: str) -> None:
    spark.sql(f"INSERT INTO {table} BY NAME SELECT 1 AS id, {src} AS st")


def added_required(spark: Any, table: str, _field: str) -> None:
    spark.sql(f"INSERT INTO {table} SELECT 1 AS id, CAST(NULL AS {TOP}) AS st")


ADDED: dict[str, tuple[str, Callable[[Any, str], None]]] = {
    "overwrite_condition_reordered": (
        TOP,
        lambda spark, table: added_overwrite_condition(spark, table, "reordered"),
    ),
    "overwrite_condition_renamed": (
        TOP,
        lambda spark, table: added_overwrite_condition(spark, table, "renamed"),
    ),
    "merge_schema_extra": (TOP, lambda spark, table: added_merge_schema(spark, table, "extra")),
    "by_name_case_twins": (
        TOP,
        lambda spark, table: added_by_name(spark, table, "named_struct('a', 1, 'A', 2)"),
    ),
    "by_name_recased": (
        TOP,
        lambda spark, table: added_by_name(spark, table, struct_sql("case")),
    ),
    "required_struct_null": (
        f"{TOP} NOT NULL",
        lambda spark, table: added_required(spark, table, ""),
    ),
}


def group_fields(group: str) -> tuple[str, ...]:
    return CASE_FIELDS if group == "cs" else tuple(FIELDS)


def table_name(group: str, door: str, container: str, field: str) -> str:
    return f"ice.{NAMESPACE[group]}.{door}__{container}__{field}"


def table_label(table: str) -> str:
    return ".".join(f"`{part}`" for part in table.split("."))


def open_repark(warehouse: str) -> Any:
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("ice-struct-pairing-1")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.register_memory_catalog("ice", warehouse)
    return spark


def open_spark(warehouse: str) -> Any:
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("ice-struct-pairing-1-oracle")
        .config("spark.driver.memory", "3g")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "hadoop")
        .config("spark.sql.catalog.ice.warehouse", warehouse)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
    )
    ivy = os.environ.get("REPARK_ORACLE_IVY")
    if ivy:
        builder = builder.config("spark.jars.ivy", ivy)
    return builder.getOrCreate()


def plain(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {k: plain(v) for k, v in value.asDict().items()}
    if isinstance(value, dict):
        return {str(k): plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple):
        return [plain(v) for v in value]
    return value


def read_rows(spark: Any, table: str) -> list[list[Any]]:
    rows = spark.sql(f"SELECT id, st FROM {table} ORDER BY id").collect()
    return [[plain(row[0]), plain(row[1])] for row in rows]


def one_line(error: BaseException) -> str:
    return " ".join(str(error).split())


def create_seeded(spark: Any, table: str, column: str, seed: str, version: str) -> None:
    spark.sql(
        f"CREATE TABLE {table} (id INT, st {column}) USING iceberg "
        f"TBLPROPERTIES ('format-version' = '{version}')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (0, {seed})")


def outcome(spark: Any, table: str, write: Callable[[], None]) -> dict[str, Any]:
    try:
        write()
    except Exception as error:
        return {"error": one_line(error)}
    try:
        return {"rows": read_rows(spark, table)}
    except Exception as error:
        return {"read_error": one_line(error)}


def matrix_cell(
    spark: Any, group: str, version: str, door: str, container: str, field: str
) -> dict[str, Any]:
    column, wrap, seed = CONTAINERS[container]
    table = table_name(group, door, container, field)
    create_seeded(spark, table, column, seed, version)
    src = wrap.format(s=struct_sql(field))
    return outcome(spark, table, lambda: DOORS[door](spark, table, src))


def added_cell(spark: Any, name: str) -> dict[str, Any]:
    column, write = ADDED[name]
    table = f"ice.{NAMESPACE['added']}.{name}"
    create_seeded(spark, table, column, TOP_SEED, "2")
    return outcome(spark, table, lambda: write(spark, table))


def record_group(spark: Any, group: str) -> dict[str, Any]:
    version, case_sensitive = GROUPS.get(group, ("2", "false"))
    spark.conf.set("spark.sql.caseSensitive", case_sensitive)
    spark.sql(f"CREATE NAMESPACE ice.{NAMESPACE[group]}")
    cells: dict[str, Any] = {}
    if group == "added":
        for name in ADDED:
            cells[name] = added_cell(spark, name)
            print(group, name, json.dumps(cells[name])[:160], flush=True)
        return cells
    for door in DOOR_ORDER:
        for container in CONTAINERS:
            for field in group_fields(group):
                key = f"{door}|{container}|{field}"
                cells[key] = matrix_cell(spark, group, version, door, container, field)
                print(group, key, json.dumps(cells[key])[:160], flush=True)
    return cells


def engine_banner(engine: str) -> str:
    if engine == "spark":
        import pyspark

        return f"pyspark {pyspark.__version__}"
    import repark

    return f"repark {getattr(repark, '__version__', 'unknown')}"


def dump(recorded: dict[str, Any]) -> str:
    groups = [group for group in recorded if group != "engine"]
    lines = ["{", f' "engine": {json.dumps(recorded["engine"])},']
    for index, group in enumerate(groups):
        lines.append(f" {json.dumps(group)}: {{")
        cells = [
            f"  {json.dumps(key)}: {json.dumps(recorded[group][key], sort_keys=True)}"
            for key in sorted(recorded[group])
        ]
        lines.append(",\n".join(cells))
        lines.append(" }" + ("," if index < len(groups) - 1 else ""))
    lines.append("}")
    return "\n".join(lines) + "\n"


def main() -> None:
    engine, out = sys.argv[1], Path(sys.argv[2])
    warehouse = tempfile.mkdtemp(prefix=f"ice-struct-pairing-1-{engine}-")
    spark = open_spark(warehouse) if engine == "spark" else open_repark(warehouse)
    recorded: dict[str, Any] = {"engine": engine_banner(engine)}
    try:
        for group in (*GROUPS, "added"):
            recorded[group] = record_group(spark, group)
    finally:
        if engine == "spark":
            spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
    out.write_text(dump(recorded), encoding="utf-8")


if __name__ == "__main__":
    main()
