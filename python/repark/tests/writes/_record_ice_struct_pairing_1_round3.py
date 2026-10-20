"""Record the ICE-STRUCT-PAIRING-1 build-round-3 cells on Spark 4.1.2 or on a RePark module.

NOT a ``test_`` module: pytest never collects it. It extends ``_record_ice_struct_pairing_1``
with three groups that verifier 1 measured beside the matrix (2026-10-14):

* ``r3_leaf``: a struct whose paired leaf Spark's store assignment refuses or accepts, through
  every door of the matrix and both ``INSERT OVERWRITE … PARTITION`` spellings (F-01, F-06).
* ``r3_part``: the shapes of verifier 1's probe through both ``PARTITION`` spellings, format
  versions 2 and 3, ``spark.sql.caseSensitive`` both ways for the case shapes (F-08, the
  carve-out of ruling 58).
* ``r3_misc``: the column-list ``VALUES`` door (F-02), non-ASCII case-insensitive names
  (F-03), ``UPDATE`` from a scalar subquery (F-04), the case-sensitive ``UPDATE`` refusal
  (F-05) and an explicit ``CAST`` of a struct on five doors.

Every cell creates its table, seeds row 0, writes row 1 through one statement and reads the
table back; a refusal also records the table as it stands after the refusal. The groups are
merged into an existing recording, so the matrix groups are never re-recorded by this script::

    _record_ice_struct_pairing_1_round3.py spark ice_struct_pairing_1_spark_oracle.json
    _record_ice_struct_pairing_1_round3.py repark ice_struct_pairing_1_main.json
"""

from __future__ import annotations

import datetime
import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any, NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

import _record_ice_struct_pairing_1 as base

TOP = base.TOP
TOP_SEED = base.TOP_SEED
TYPED = "struct<a: BIGINT, b: STRING>"
TYPED_SEED = "named_struct('a', CAST(10 AS BIGINT), 'b', 'x')"
TWO_DEEP = "struct<o: struct<p: struct<a: INT, b: INT>, n: INT>>"
TWO_DEEP_SEED = "named_struct('o', named_struct('p', named_struct('a', 10, 'b', 20), 'n', 90))"
MAP_KEY = "map<struct<a: INT, b: INT>, INT>"
MAP_KEY_SEED = "map(named_struct('a', 10, 'b', 20), 7)"
REQUIRED = "struct<a: INT NOT NULL, b: INT>"
REORDERED = "named_struct('b', 2, 'a', 1)"
E_ACUTE = "struct<`é`: INT, b: INT>"
E_ACUTE_SEED = "named_struct('é', 10, 'b', 20)"
SHARP_S = "struct<`ß`: INT, b: INT>"
SHARP_S_SEED = "named_struct('ß', 10, 'b', 20)"
CAPITAL_SHARP_S = "struct<`ẞ`: INT, b: INT>"
CAPITAL_SHARP_S_SEED = "named_struct('ẞ', 10, 'b', 20)"
CAST_STRUCT = f"CAST(named_struct('a', '5', 'b', 'y') AS {TYPED})"
NAMESPACE = "r3"


def ns(*pairs: tuple[str, str]) -> str:
    return "named_struct(" + ", ".join(f"'{name}', {value}" for name, value in pairs) + ")"


STR5_REORDERED = ns(("b", "'5'"), ("a", "1"))
LEAF_CASES: dict[str, tuple[str, str, str]] = {
    "str5_to_bigint_pos": (TYPED, TYPED_SEED, STR5_REORDERED),
    "bool_to_int_pos": (
        "struct<a: INT, b: BOOLEAN>",
        "named_struct('a', 10, 'b', false)",
        ns(("b", "true"), ("a", "1")),
    ),
    "str_to_date_pos": (
        "struct<a: DATE, b: STRING>",
        "named_struct('a', DATE'2000-01-01', 'b', 'x')",
        ns(("b", "'2020-01-02'"), ("a", "DATE'2021-01-01'")),
    ),
    "str5_to_bigint_inorder": (TYPED, TYPED_SEED, ns(("a", "'5'"), ("b", "'y'"))),
    "bool_to_int_inorder": (
        "struct<a: INT, b: BOOLEAN>",
        "named_struct('a', 10, 'b', false)",
        ns(("a", "true"), ("b", "true")),
    ),
    "str5_to_bigint_renamed_pos": (TYPED, TYPED_SEED, ns(("q", "'5'"), ("r", "'y'"))),
    "str5_in_array_pos": (
        f"array<{TYPED}>",
        f"array({TYPED_SEED})",
        f"array({STR5_REORDERED})",
    ),
    "str5_in_map_pos": (
        f"map<string, {TYPED}>",
        f"map('k', {TYPED_SEED})",
        f"map('k', {STR5_REORDERED})",
    ),
    "typed_reordered": (TYPED, TYPED_SEED, ns(("b", "'y'"), ("a", "1"))),
    "typed_widen_reordered": (
        "struct<a: BIGINT, b: INT>",
        "named_struct('a', CAST(10 AS BIGINT), 'b', 20)",
        REORDERED,
    ),
    "two_deep_reordered": (
        TWO_DEEP,
        TWO_DEEP_SEED,
        ns(("o", ns(("n", "9"), ("p", REORDERED)))),
    ),
    "array_nested_reordered": (
        "array<struct<a: INT, o: struct<x: INT, y: INT>>>",
        "array(named_struct('a', 10, 'o', named_struct('x', 30, 'y', 40)))",
        f"array({ns(('o', ns(('y', '4'), ('x', '3'))), ('a', '1'))})",
    ),
}

PART_SHAPES: dict[str, tuple[str, str, str]] = {
    "reordered": (TOP, TOP_SEED, REORDERED),
    "renamed": (TOP, TOP_SEED, ns(("a", "1"), ("q", "2"))),
    "missing": (TOP, TOP_SEED, ns(("a", "1"))),
    "extra_mid": (TOP, TOP_SEED, ns(("a", "1"), ("c", "3"), ("b", "2"))),
    "case_mixed_reordered": (TOP, TOP_SEED, ns(("B", "2"), ("a", "1"))),
    "twins_plus_b": (TOP, TOP_SEED, ns(("a", "1"), ("b", "2"), ("A", "3"))),
    "typed_reordered": LEAF_CASES["typed_reordered"],
    "typed_widen_reordered": LEAF_CASES["typed_widen_reordered"],
    "two_deep_reordered": LEAF_CASES["two_deep_reordered"],
    "two_deep_inner_reordered": (
        TWO_DEEP,
        TWO_DEEP_SEED,
        ns(("o", ns(("p", REORDERED), ("n", "9")))),
    ),
    "two_deep_renamed": (
        TWO_DEEP,
        TWO_DEEP_SEED,
        ns(("o", ns(("p", ns(("a", "1"), ("z", "2"))), ("n", "9")))),
    ),
    "two_deep_extra": (
        TWO_DEEP,
        TWO_DEEP_SEED,
        ns(("o", ns(("p", ns(("a", "1"), ("b", "2"), ("c", "3"))), ("n", "9")))),
    ),
    "array_nested_reordered": LEAF_CASES["array_nested_reordered"],
    "map_value_reordered": (
        "map<string, struct<a: INT, b: INT>>",
        "map('k', named_struct('a', 10, 'b', 20))",
        f"map('k', {REORDERED})",
    ),
    "map_key_reordered": (MAP_KEY, MAP_KEY_SEED, f"map({REORDERED}, 5)"),
    "map_key_renamed": (MAP_KEY, MAP_KEY_SEED, f"map({ns(('a', '1'), ('q', '2'))}, 5)"),
    "map_in_array_in_struct": (
        "struct<l: array<map<string, struct<a: INT, b: INT>>>>",
        "named_struct('l', array(map('k', named_struct('a', 10, 'b', 20))))",
        ns(("l", f"array(map('k', {REORDERED}))")),
    ),
    "null_field_reordered": (TOP, TOP_SEED, ns(("b", "CAST(NULL AS INT)"), ("a", "1"))),
    "null_struct": (TOP, TOP_SEED, "CAST(NULL AS struct<b: INT, a: INT>)"),
    "required_reordered": (REQUIRED, TOP_SEED, REORDERED),
    "required_fed_null": (REQUIRED, TOP_SEED, ns(("b", "2"), ("a", "CAST(NULL AS INT)"))),
    "required_fed_null_pos": (REQUIRED, TOP_SEED, ns(("a", "CAST(NULL AS INT)"), ("b", "2"))),
    "same_order": (TOP, TOP_SEED, ns(("a", "1"), ("b", "2"))),
    "two_deep_same": (
        TWO_DEEP,
        TWO_DEEP_SEED,
        ns(("o", ns(("p", ns(("a", "1"), ("b", "2"))), ("n", "9")))),
    ),
}
CASE_SHAPES: dict[str, tuple[str, str, str]] = {
    "case": (TOP, TOP_SEED, ns(("A", "1"), ("B", "2"))),
    "case_reordered": (TOP, TOP_SEED, ns(("B", "2"), ("A", "1"))),
    "case_mixed_reordered": PART_SHAPES["case_mixed_reordered"],
    "twins_plus_b": PART_SHAPES["twins_plus_b"],
    "twins": (TOP, TOP_SEED, ns(("a", "1"), ("A", "2"))),
    "case_two_deep": (
        TWO_DEEP,
        TWO_DEEP_SEED,
        ns(("o", ns(("p", ns(("B", "2"), ("A", "1"))), ("n", "9")))),
    ),
    "case_map_key": (MAP_KEY, MAP_KEY_SEED, f"map({ns(('B', '2'), ('a', '1'))}, 5)"),
    "case_array": (
        "array<struct<a: INT, b: INT>>",
        "array(named_struct('a', 10, 'b', 20))",
        f"array({ns(('b', '2'), ('A', '1'))})",
    ),
}
NAMED_ALIKE = ("same_order", "two_deep_same", "required_fed_null_pos")


def part_static(spark: Any, table: str, src: str) -> None:
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (p = 'x') SELECT 1, {src}")


def part_dynamic(spark: Any, table: str, src: str) -> None:
    spark.sql(f"INSERT OVERWRITE {table} PARTITION (p) SELECT 1, {src}, 'x'")


PART_DOORS: dict[str, Callable[[Any, str, str], None]] = {
    "part_static": part_static,
    "part_dynamic": part_dynamic,
}
LEAF_DOORS = (*base.DOOR_ORDER, *PART_DOORS)


class Misc(NamedTuple):
    column: str
    seed: str
    write: Callable[[Any, str], None]
    version: str = "2"
    case_sensitive: str = "false"
    source: tuple[str, str] | None = None


def sql(template: str) -> Callable[[Any, str], None]:
    def run(spark: Any, table: str) -> None:
        spark.sql(template.format(t=table))

    return run


def frame_write(src: str, write: Callable[[Any, str], None]) -> Callable[[Any, str], None]:
    def run(spark: Any, table: str) -> None:
        write(base.frame(spark, src), table)

    return run


def frame_append(frame: Any, table: str) -> None:
    frame.writeTo(table).append()


def frame_save_append(frame: Any, table: str) -> None:
    frame.write.mode("append").saveAsTable(table)


def frame_overwrite_partitions(frame: Any, table: str) -> None:
    frame.writeTo(table).overwritePartitions()


E_ACUTE_REORDERED = "named_struct('b', 2, 'É', 1)"
TWINS = "named_struct('a', 1, 'A', 2)"
TWINS_PLUS_B = "named_struct('a', 1, 'b', 2, 'A', 3)"
MERGE_INSERT = (
    "MERGE INTO {{t}} t USING (SELECT 1 AS id, {s} AS st) s ON t.id = s.id "
    "WHEN NOT MATCHED THEN INSERT (id, st) VALUES (s.id, s.st)"
)
MERGE_UPDATE = (
    "MERGE INTO {{t}} t USING (SELECT 0 AS id, {s} AS st) s ON t.id = s.id "
    "WHEN MATCHED THEN UPDATE SET t.st = s.st"
)


def case_update_cells() -> dict[str, Misc]:
    cells = {}
    for version in ("2", "3"):
        for door, where in (("update", " WHERE id = 0"), ("update_no_where", "")):
            for shape, src in (("twins", TWINS), ("twins_plus_b", TWINS_PLUS_B)):
                cells[f"cs_{door}_{shape}_v{version}"] = Misc(
                    TOP, TOP_SEED, sql(f"UPDATE {{t}} SET st = {src}{where}"), version, "true"
                )
    return cells


MISC: dict[str, Misc] = {
    "values_collist_swapped": Misc(
        TOP, TOP_SEED, sql(f"INSERT INTO {{t}} (st, id) VALUES ({REORDERED}, 1)")
    ),
    "values_collist_swapped_renamed": Misc(
        TOP, TOP_SEED, sql("INSERT INTO {t} (st, id) VALUES (named_struct('a', 1, 'q', 2), 1)")
    ),
    "values_collist_partial": Misc(
        TOP, TOP_SEED, sql(f"INSERT INTO {{t}} (st) VALUES ({REORDERED})")
    ),
    "values_collist_swapped_two_rows": Misc(
        TOP,
        TOP_SEED,
        sql(f"INSERT INTO {{t}} (st, id) VALUES ({REORDERED}, 1), ({REORDERED}, 2)"),
    ),
    "values_collist_in_order": Misc(
        TOP, TOP_SEED, sql(f"INSERT INTO {{t}} (id, st) VALUES (1, {REORDERED})")
    ),
    "values_collist_swapped_leaf": Misc(
        TYPED,
        TYPED_SEED,
        sql("INSERT INTO {t} (st, id) VALUES (named_struct('b', '5', 'a', 1), 1)"),
    ),
    "overwrite_values_collist_swapped": Misc(
        TOP, TOP_SEED, sql(f"INSERT OVERWRITE {{t}} (st, id) VALUES ({REORDERED}, 1)")
    ),
    "e_acute_insert_by_name": Misc(
        E_ACUTE,
        E_ACUTE_SEED,
        sql(f"INSERT INTO {{t}} BY NAME SELECT {E_ACUTE_REORDERED} AS st, 1 AS id"),
    ),
    "e_acute_insert_by_name_in_order": Misc(
        E_ACUTE,
        E_ACUTE_SEED,
        sql("INSERT INTO {t} BY NAME SELECT named_struct('É', 1, 'b', 2) AS st, 1 AS id"),
    ),
    "e_acute_df_append": Misc(E_ACUTE, E_ACUTE_SEED, frame_write(E_ACUTE_REORDERED, frame_append)),
    "e_acute_df_save_append": Misc(
        E_ACUTE, E_ACUTE_SEED, frame_write(E_ACUTE_REORDERED, frame_save_append)
    ),
    "e_acute_df_overwrite_partitions": Misc(
        E_ACUTE, E_ACUTE_SEED, frame_write(E_ACUTE_REORDERED, frame_overwrite_partitions)
    ),
    "e_acute_merge_insert": Misc(
        E_ACUTE, E_ACUTE_SEED, sql(MERGE_INSERT.format(s=E_ACUTE_REORDERED))
    ),
    "e_acute_merge_update": Misc(
        E_ACUTE, E_ACUTE_SEED, sql(MERGE_UPDATE.format(s=E_ACUTE_REORDERED))
    ),
    "e_acute_update": Misc(
        E_ACUTE,
        E_ACUTE_SEED,
        sql(f"UPDATE {{t}} SET st = {E_ACUTE_REORDERED} WHERE id = 0"),
    ),
    "e_acute_case_sensitive": Misc(
        E_ACUTE,
        E_ACUTE_SEED,
        sql("INSERT INTO {t} BY NAME SELECT named_struct('É', 1, 'b', 2) AS st, 1 AS id"),
        case_sensitive="true",
    ),
    "sharp_s_from_capital": Misc(
        SHARP_S,
        SHARP_S_SEED,
        sql("INSERT INTO {t} BY NAME SELECT named_struct('ẞ', 1, 'b', 2) AS st, 1 AS id"),
    ),
    "capital_sharp_s_from_small": Misc(
        CAPITAL_SHARP_S,
        CAPITAL_SHARP_S_SEED,
        sql("INSERT INTO {t} BY NAME SELECT named_struct('b', 2, 'ß', 1) AS st, 1 AS id"),
    ),
    "sharp_s_from_ss": Misc(
        SHARP_S,
        SHARP_S_SEED,
        sql("INSERT INTO {t} BY NAME SELECT named_struct('SS', 1, 'b', 2) AS st, 1 AS id"),
    ),
    "update_subquery_reordered": Misc(
        TOP,
        TOP_SEED,
        sql("UPDATE {t} SET st = (SELECT st FROM {t}_src) WHERE id = 0"),
        source=("struct<b: INT, a: INT>", REORDERED),
    ),
    "update_subquery_in_order": Misc(
        TOP,
        TOP_SEED,
        sql("UPDATE {t} SET st = (SELECT st FROM {t}_src) WHERE id = 0"),
        source=(TOP, "named_struct('a', 1, 'b', 2)"),
    ),
    "explicit_cast_values": Misc(
        TYPED, TYPED_SEED, sql(f"INSERT INTO {{t}} VALUES (1, {CAST_STRUCT})")
    ),
    "explicit_cast_select": Misc(
        TYPED, TYPED_SEED, sql(f"INSERT INTO {{t}} SELECT 1, {CAST_STRUCT}")
    ),
    "explicit_cast_overwrite": Misc(
        TYPED, TYPED_SEED, sql(f"INSERT OVERWRITE {{t}} SELECT 1, {CAST_STRUCT}")
    ),
    "explicit_cast_by_name": Misc(
        TYPED,
        TYPED_SEED,
        sql(f"INSERT INTO {{t}} BY NAME SELECT {CAST_STRUCT} AS st, 1 AS id"),
    ),
    "explicit_cast_df_append": Misc(TYPED, TYPED_SEED, frame_write(CAST_STRUCT, frame_append)),
    **case_update_cells(),
}


def stored(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: stored(item) for key, item in value.items()}
    if isinstance(value, list):
        return [stored(item) for item in value]
    if isinstance(value, datetime.date):
        return value.isoformat()
    return value


def read_rows(spark: Any, table: str, column: str) -> list[list[Any]]:
    value = "map_entries(st)" if column.startswith("map<struct") else "st"
    rows = spark.sql(f"SELECT id, {value} FROM {table} ORDER BY id").collect()
    return [[stored(base.plain(row[0])), stored(base.plain(row[1]))] for row in rows]


def recorded(spark: Any, table: str, column: str, write: Callable[[], None]) -> dict[str, Any]:
    try:
        write()
    except Exception as error:
        refused: dict[str, Any] = {"error": base.one_line(error)}
        try:
            refused["after"] = read_rows(spark, table, column)
        except Exception as read_error:
            refused["after"] = f"read_error {base.one_line(read_error)}"
        return refused
    try:
        return {"rows": read_rows(spark, table, column)}
    except Exception as error:
        return {"read_error": base.one_line(error)}


def create(spark: Any, table: str, column: str, seed: str, version: str, part: bool) -> None:
    if not part:
        base.create_seeded(spark, table, column, seed, version)
        return
    spark.sql(
        f"CREATE TABLE {table} (id INT, st {column}, p STRING) USING iceberg "
        f"PARTITIONED BY (p) TBLPROPERTIES ('format-version' = '{version}')"
    )
    spark.sql(f"INSERT INTO {table} VALUES (0, {seed}, 'x')")


def door_cell(
    spark: Any, table: str, door: str, shape: tuple[str, str, str], version: str
) -> dict[str, Any]:
    column, seed, src = shape
    part = door in PART_DOORS
    try:
        create(spark, table, column, seed, version, part)
    except Exception as error:
        return {"setup_error": base.one_line(error)}
    write = PART_DOORS[door] if part else base.DOORS[door]
    return recorded(spark, table, column, lambda: write(spark, table, src))


def leaf_key(door: str, case: str) -> str:
    return f"{door}|{case}"


def leaf_table(door: str, case: str) -> str:
    return f"ice.{NAMESPACE}.leaf__{door}__{case}"


def part_keys() -> list[tuple[str, str, str, str]]:
    keys = []
    for version in ("2", "3"):
        for door in PART_DOORS:
            keys.extend((version, "false", door, shape) for shape in PART_SHAPES)
            for case_sensitive in ("false", "true"):
                keys.extend((version, case_sensitive, door, f"C_{shape}") for shape in CASE_SHAPES)
    return keys


def part_key(version: str, case_sensitive: str, door: str, shape: str) -> str:
    return f"v{version}|{'cs' if case_sensitive == 'true' else 'ci'}|{door}|{shape}"


def part_shape(shape: str) -> tuple[str, str, str]:
    return CASE_SHAPES[shape[2:]] if shape.startswith("C_") else PART_SHAPES[shape]


def part_table(version: str, case_sensitive: str, door: str, shape: str) -> str:
    sensitivity = "cs" if case_sensitive == "true" else "ci"
    return f"ice.{NAMESPACE}.part__{door}__{shape}__v{version}__{sensitivity}"


def misc_table(name: str) -> str:
    return f"ice.{NAMESPACE}.misc__{name}"


def with_case(spark: Any, case_sensitive: str, run: Callable[[], dict[str, Any]]) -> dict[str, Any]:
    spark.conf.set("spark.sql.caseSensitive", case_sensitive)
    try:
        return run()
    finally:
        spark.conf.set("spark.sql.caseSensitive", "false")


def leaf_cell(spark: Any, door: str, case: str) -> dict[str, Any]:
    return door_cell(spark, leaf_table(door, case), door, LEAF_CASES[case], "2")


def part_cell(
    spark: Any, version: str, case_sensitive: str, door: str, shape: str
) -> dict[str, Any]:
    table = part_table(version, case_sensitive, door, shape)
    return with_case(
        spark,
        case_sensitive,
        lambda: door_cell(spark, table, door, part_shape(shape), version),
    )


def misc_cell(spark: Any, name: str) -> dict[str, Any]:
    cell = MISC[name]
    table = misc_table(name)

    def run() -> dict[str, Any]:
        try:
            base.create_seeded(spark, table, cell.column, cell.seed, cell.version)
            if cell.source is not None:
                spark.sql(f"CREATE TABLE {table}_src (id INT, st {cell.source[0]}) USING iceberg")
                spark.sql(f"INSERT INTO {table}_src VALUES (1, {cell.source[1]})")
        except Exception as error:
            return {"setup_error": base.one_line(error)}
        return recorded(spark, table, cell.column, lambda: cell.write(spark, table))

    return with_case(spark, cell.case_sensitive, run)


def record_leaf(spark: Any) -> dict[str, Any]:
    return {
        leaf_key(door, case): leaf_cell(spark, door, case)
        for door in LEAF_DOORS
        for case in LEAF_CASES
    }


def record_part(spark: Any) -> dict[str, Any]:
    return {part_key(*key): part_cell(spark, *key) for key in part_keys()}


def record_misc(spark: Any) -> dict[str, Any]:
    return {name: misc_cell(spark, name) for name in MISC}


GROUPS: dict[str, Callable[[Any], dict[str, Any]]] = {
    "r3_leaf": record_leaf,
    "r3_part": record_part,
    "r3_misc": record_misc,
}


def main() -> None:
    engine, out = sys.argv[1], Path(sys.argv[2])
    chosen = sys.argv[3:] or list(GROUPS)
    existing: dict[str, Any] = json.loads(out.read_text("utf-8")) if out.exists() else {}
    warehouse = tempfile.mkdtemp(prefix=f"ice-struct-pairing-1-r3-{engine}-")
    spark = base.open_spark(warehouse) if engine == "spark" else base.open_repark(warehouse)
    existing.setdefault("engine", base.engine_banner(engine))
    try:
        spark.sql(f"CREATE NAMESPACE ice.{NAMESPACE}")
        for group in chosen:
            existing[group] = GROUPS[group](spark)
            print(group, len(existing[group]), flush=True)
    finally:
        if engine == "spark":
            spark.stop()
        shutil.rmtree(warehouse, ignore_errors=True)
    out.write_text(base.dump(existing), encoding="utf-8")


if __name__ == "__main__":
    main()
