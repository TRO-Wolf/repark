from __future__ import annotations

import functools
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _record_ice_struct_pairing_1 as base
import _record_ice_struct_pairing_1_round3 as round3

NAMESPACE = "xr"
TOP = base.TOP
TOP_SEED = base.TOP_SEED
TABLE_COLUMNS = "id INT, v INT, st struct<a: INT, b: INT>"
TABLE_ROWS = "(0, 10, named_struct('a', 10, 'b', 20)), (3, 30, named_struct('a', 30, 'b', 40))"
ANSI_TABLE = (
    "SELECT CAST(0 AS INT) AS id, CAST(10 AS INT) AS v, "
    "named_struct('a', CAST(10 AS INT), 'b', CAST(20 AS INT)) AS st UNION ALL "
    "SELECT CAST(3 AS INT), CAST(30 AS INT), "
    "named_struct('a', CAST(30 AS INT), 'b', CAST(40 AS INT))"
)
SOURCES: dict[str, tuple[str, str, str]] = {
    "reordered": (
        "struct<b: INT, a: INT>",
        "named_struct('b', 2, 'a', 1)",
        "named_struct('b', CAST(2 AS INT), 'a', CAST(1 AS INT))",
    ),
    "in_order": (
        TOP,
        "named_struct('a', 1, 'b', 2)",
        "named_struct('a', CAST(1 AS INT), 'b', CAST(2 AS INT))",
    ),
}
UPDATE_COLUMNS: dict[str, tuple[str, str]] = {
    "int": ("v", "in_order"),
    "struct_reordered": ("st", "reordered"),
    "struct_in_order": ("st", "in_order"),
}
UPDATE_PREDICATES = {
    "where_true": " WHERE true",
    "where_in_list": " WHERE id IN (0, 3)",
    "no_where": "",
    "where_id_eq_0": " WHERE id = 0",
}
UPDATE_VALUES = {
    "subquery": "(SELECT {c} FROM {{t}}_src)",
    "subquery_with_where": "(SELECT {c} FROM {{t}}_src WHERE id = 1)",
}
UPDATE_DOORS = ("spark", "ansi")
IDENTITY_PREDICATES = ("where_id_eq_0",)


class Rows(NamedTuple):
    column: str
    seed: str
    statement: str
    case_sensitive: str = "false"


def ns(*pairs: tuple[str, str]) -> str:
    return round3.ns(*pairs)


BA = ns(("b", "2"), ("a", "1"))
BA_SECOND = ns(("b", "8"), ("a", "7"))
AB_SECOND = ns(("a", "7"), ("b", "8"))
XY = ns(("x", "7"), ("y", "8"))
NESTED = "struct<o: struct<a: INT, b: INT>, n: INT>"
NESTED_SEED = "named_struct('o', named_struct('a', 10, 'b', 20), 'n', 90)"
ARRAY = "array<struct<a: INT, b: INT>>"
ARRAY_SEED = "array(named_struct('a', 10, 'b', 20))"
MAP = "map<string, struct<a: INT, b: INT>>"
MAP_SEED = "map('k', named_struct('a', 10, 'b', 20))"
SPELLINGS = {
    "nocol": "INSERT INTO {{t}} VALUES (1, {first}), (2, {second})",
    "collist_in_order": "INSERT INTO {{t}} (id, st) VALUES (1, {first}), (2, {second})",
    "collist_swapped": "INSERT INTO {{t}} (st, id) VALUES ({first}, 1), ({second}, 2)",
}


def spelled(shape: str, first: str, second: str) -> dict[str, Rows]:
    return {
        f"{spelling}|{shape}": Rows(TOP, TOP_SEED, template.format(first=first, second=second))
        for spelling, template in SPELLINGS.items()
    }


ROWS: dict[str, Rows] = {
    **spelled("names_differ", BA, XY),
    **spelled("names_agree_reordered_from_table", BA, BA_SECOND),
    **spelled("names_agree_in_table_order", ns(("a", "1"), ("b", "2")), AB_SECOND),
    **spelled("names_differ_first_in_table_order", ns(("a", "1"), ("b", "2")), XY),
    "nocol|rows_order_differ": Rows(
        TOP, TOP_SEED, f"INSERT INTO {{t}} VALUES (1, {BA}), (2, {AB_SECOND})"
    ),
    "nocol|rows_case_differ": Rows(
        TOP,
        TOP_SEED,
        f"INSERT INTO {{t}} VALUES (1, {ns(('B', '2'), ('a', '1'))}), (2, {BA_SECOND})",
    ),
    "nocol|rows_case_differ_case_sensitive": Rows(
        TOP,
        TOP_SEED,
        f"INSERT INTO {{t}} VALUES (1, {ns(('B', '2'), ('a', '1'))}), (2, {BA_SECOND})",
        "true",
    ),
    "nocol|field_count_differs": Rows(
        TOP, TOP_SEED, f"INSERT INTO {{t}} VALUES (1, {BA}), (2, {ns(('b', '8'))})"
    ),
    "nocol|third_row_differs": Rows(
        TOP,
        TOP_SEED,
        f"INSERT INTO {{t}} VALUES (1, {BA}), (2, {BA_SECOND}), (3, {XY})",
    ),
    "nocol|null_row": Rows(TOP, TOP_SEED, f"INSERT INTO {{t}} VALUES (1, {BA}), (2, NULL)"),
    "nocol|leaf_types_widen": Rows(
        TOP,
        TOP_SEED,
        f"INSERT INTO {{t}} VALUES (1, {BA}), (2, {ns(('b', 'CAST(8 AS BIGINT)'), ('a', '7'))})",
    ),
    "nocol|nested_names_differ": Rows(
        NESTED,
        NESTED_SEED,
        "INSERT INTO {t} VALUES "
        f"(1, {ns(('o', BA), ('n', '9'))}), (2, {ns(('o', XY), ('n', '9'))})",
    ),
    "nocol|nested_names_agree": Rows(
        NESTED,
        NESTED_SEED,
        "INSERT INTO {t} VALUES "
        f"(1, {ns(('o', BA), ('n', '9'))}), (2, {ns(('o', BA_SECOND), ('n', '9'))})",
    ),
    "nocol|array_names_differ": Rows(
        ARRAY, ARRAY_SEED, f"INSERT INTO {{t}} VALUES (1, array({BA})), (2, array({XY}))"
    ),
    "nocol|map_value_names_differ": Rows(
        MAP, MAP_SEED, f"INSERT INTO {{t}} VALUES (1, map('k', {BA})), (2, map('k', {XY}))"
    ),
    "nocol|leaf_types_differ": Rows(
        TOP,
        TOP_SEED,
        "INSERT INTO {t} VALUES (1, named_struct('a', 1, 'b', 2)), "
        "(2, named_struct('a', 'x', 'b', 8))",
    ),
    "overwrite_nocol|names_differ": Rows(
        TOP, TOP_SEED, f"INSERT OVERWRITE {{t}} VALUES (1, {BA}), (2, {XY})"
    ),
    "select_union|names_differ": Rows(
        TOP, TOP_SEED, f"INSERT INTO {{t}} SELECT 1, {BA} UNION ALL SELECT 2, {XY}"
    ),
}


def update_key(door: str, column: str, where: str, form: str) -> str:
    return f"{door}|{column}|{where}|{form}"


def update_keys() -> list[tuple[str, str, str, str]]:
    return [
        (door, column, where, form)
        for door in UPDATE_DOORS
        for column in UPDATE_COLUMNS
        for where in UPDATE_PREDICATES
        for form in UPDATE_VALUES
    ]


def update_statement(column: str, where: str, form: str) -> str:
    name = UPDATE_COLUMNS[column][0]
    value = UPDATE_VALUES[form].format(c=name)
    return f"UPDATE {{t}} SET {name} = {value}{UPDATE_PREDICATES[where]}"


class Door(NamedTuple):
    run: Callable[[str], Any]
    ansi: bool


def facade_run(spark: Any, sql: str) -> Any:
    return spark.sql(sql).collect()


def native_run(native: Any, alive: dict[str, bool], sql: str) -> Any:
    from repark.spark.dataframe import DataFrame

    return DataFrame(native.sql(sql), native, alive).collect()


def update_rows(door: Door, table: str) -> list[list[Any]]:
    rows = door.run(f"SELECT id, v, st FROM {table} ORDER BY id")
    return [[base.plain(row[0]), base.plain(row[1]), base.plain(row[2])] for row in rows]


def seed_update(door: Door, table: str, source: str) -> None:
    source_type, source_value, ansi_value = SOURCES[source]
    if door.ansi:
        door.run(f"CREATE TABLE {table} AS {ANSI_TABLE}")
        door.run(
            f"CREATE TABLE {table}_src AS SELECT CAST(1 AS INT) AS id, CAST(7 AS INT) AS v, "
            f"{ansi_value} AS st"
        )
        return
    door.run(f"CREATE TABLE {table} ({TABLE_COLUMNS}) USING iceberg")
    door.run(f"INSERT INTO {table} VALUES {TABLE_ROWS}")
    door.run(f"CREATE TABLE {table}_src (id INT, v INT, st {source_type}) USING iceberg")
    door.run(f"INSERT INTO {table}_src VALUES (1, 7, {source_value})")


def update_table(door: str, column: str, where: str, form: str) -> str:
    return f"ice.{NAMESPACE}.u__{door}__{column}__{where}__{form}"


def update_cell(doors: dict[str, Door], key: tuple[str, str, str, str]) -> dict[str, Any]:
    door_name, column, where, form = key
    door = doors[door_name]
    table = update_table(*key)
    try:
        seed_update(door, table, UPDATE_COLUMNS[column][1])
    except Exception as error:
        return {"setup_error": base.one_line(error)}
    statement = update_statement(column, where, form).format(t=table)
    try:
        door.run(statement)
    except Exception as error:
        return {"error": base.one_line(error), "after": update_rows(door, table)}
    return {"rows": update_rows(door, table)}


def rows_table(key: str) -> str:
    return f"ice.{NAMESPACE}.r__{key.replace('|', '__')}"


def rows_cell(spark: Any, key: str) -> dict[str, Any]:
    cell = ROWS[key]
    table = rows_table(key)
    write = functools.partial(spark.sql, cell.statement.format(t=table))
    run = functools.partial(seeded_rows_cell, spark, table, cell, write)
    return round3.with_case(spark, cell.case_sensitive, run)


def seeded_rows_cell(
    spark: Any, table: str, cell: Rows, write: Callable[[], Any]
) -> dict[str, Any]:
    try:
        base.create_seeded(spark, table, cell.column, cell.seed, "2")
    except Exception as error:
        return {"setup_error": base.one_line(error)}
    return round3.recorded(spark, table, cell.column, write)


def open_doors(spark: Any, engine: str, warehouse: str) -> dict[str, Door]:
    facade = Door(functools.partial(facade_run, spark), ansi=False)
    if engine == "spark":
        return {"spark": facade, "ansi": facade}
    from repark._native import PyReparkSession

    native = PyReparkSession.native()
    native.register_memory_catalog("ice", warehouse)
    ansi = Door(functools.partial(native_run, native, {"alive": True}), ansi=True)
    return {"spark": facade, "ansi": ansi}


def record_update(doors: dict[str, Door]) -> dict[str, Any]:
    return {update_key(*key): update_cell(doors, key) for key in update_keys()}


def record_rows(spark: Any) -> dict[str, Any]:
    return {key: rows_cell(spark, key) for key in ROWS}


def create_namespaces(spark: Any, doors: dict[str, Door], warehouse: str) -> None:
    spark.sql(f"CREATE NAMESPACE ice.{NAMESPACE}")
    if doors["ansi"].ansi:
        location = Path(warehouse) / f"{NAMESPACE}-ansi"
        location.mkdir(parents=True, exist_ok=True)
        try:
            doors["ansi"].run(f"CREATE SCHEMA ice.{NAMESPACE} WITH (location = '{location}')")
        except Exception as error:
            if "exists" not in str(error).lower():
                raise


def main() -> None:
    engine, out = sys.argv[1], Path(sys.argv[2])
    existing: dict[str, Any] = json.loads(out.read_text("utf-8")) if out.exists() else {}
    warehouse = tempfile.mkdtemp(prefix=f"ice-struct-pairing-1-x-{engine}-")
    spark = base.open_spark(warehouse) if engine == "spark" else base.open_repark(warehouse)
    existing.setdefault("engine", base.engine_banner(engine))
    try:
        doors = open_doors(spark, engine, warehouse)
        create_namespaces(spark, doors, warehouse)
        existing["x_update"] = record_update(doors)
        print("x_update", len(existing["x_update"]), flush=True)
        existing["x_rows"] = record_rows(spark)
        print("x_rows", len(existing["x_rows"]), flush=True)
    finally:
        if engine == "spark":
            spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
    out.write_text(base.dump(existing), encoding="utf-8")


if __name__ == "__main__":
    main()
