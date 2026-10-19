from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _record_ice_struct_pairing_1 as recorder
import pyarrow.parquet as pq
import pytest

from repark.spark.session import _reset_active_session_for_tests

HERE = Path(__file__).resolve().parent
ORACLE = json.loads((HERE / "ice_struct_pairing_1_spark_oracle.json").read_text("utf-8"))
MAIN = json.loads((HERE / "ice_struct_pairing_1_main.json").read_text("utf-8"))
TARGET = ("a", "b")
PATH_PREFIX = {
    "top": ("st",),
    "nested": ("st", "o"),
    "array": ("st", "element"),
    "map": ("st", "value"),
}
L1_DOORS = ("insert", "replace_where", "df_insert_into", "df_append", "df_save_append")
L2_DOORS = ("update_no_where",)
MATRIX_GROUPS = ("v2", "v3", "cs")
ADDED_RULES = {
    "overwrite_condition_reordered": ("df_overwrite_condition", "reordered"),
    "overwrite_condition_renamed": ("df_overwrite_condition", "renamed"),
    "merge_schema_extra": ("df_append", "extra"),
    "by_name_case_twins": ("insert_by_name", "case_twins"),
    "by_name_recased": ("insert_by_name", "case"),
}
TWINS = (("a", 1), ("A", 2))
R014_RAW = (
    "datafusion engine error: Arrow error: Invalid argument error: "
    "arguments need to have the same data type"
)
L2_RAW_CELLS = (
    ("v2", "update_no_where", "array", "case"),
    ("v2", "update_no_where", "array", "case_reordered"),
    ("v3", "update_no_where", "array", "case"),
    ("v3", "update_no_where", "array", "case_reordered"),
)


def in_l1(door: str, container: str, field: str) -> bool:
    if field not in ("extra", "missing"):
        return False
    if door == "insert_values":
        return True
    return door in L1_DOORS and container in ("array", "map")


def in_l2(door: str) -> bool:
    return door in L2_DOORS


def by_name(door: str) -> bool:
    return door in recorder.BY_NAME or door == "df_overwrite_condition"


def quoted(path: tuple[str, ...]) -> str:
    return ".".join(f"`{part}`" for part in path)


def names(fields: list[str]) -> str:
    return ", ".join(f"`{name}`" for name in fields)


def pair_by_position(
    source: tuple[tuple[str, int], ...], path: tuple[str, ...]
) -> tuple[str, str] | tuple[int, int]:
    if len(source) > len(TARGET):
        extra = [name for name, _ in source[len(TARGET) :]]
        return (
            "EXTRA_STRUCT_FIELDS",
            f"Cannot write extra fields {names(extra)} to the struct {quoted(path)}",
        )
    if len(source) < len(TARGET):
        return (
            "STRUCT_MISSING_FIELDS",
            f"Struct {quoted(path)} missing fields: {names(list(TARGET[len(source) :]))}",
        )
    return source[0][1], source[1][1]


def pair_by_name(
    source: tuple[tuple[str, int], ...], path: tuple[str, ...], case_sensitive: bool
) -> tuple[str, str] | tuple[int, int]:
    def alike(name: str, target: str) -> bool:
        return name == target if case_sensitive else name.lower() == target.lower()

    values = []
    for target in TARGET:
        found = [value for name, value in source if alike(name, target)]
        if not found:
            return (
                "CANNOT_FIND_DATA",
                f"Cannot find data for the output column {quoted((*path, target))}",
            )
        if len(found) > 1:
            return (
                "AMBIGUOUS_COLUMN_NAME",
                f"Ambiguous column name in the input data {quoted((*path, target))}",
            )
        values.append(found[0])
    if len(source) > len(TARGET):
        used = [name for name, _ in source if any(alike(name, target) for target in TARGET)]
        extra = [name for name, _ in source if name not in used]
        return (
            "EXTRA_STRUCT_FIELDS",
            f"Cannot write extra fields {names(extra)} to the struct {quoted(path)}",
        )
    return values[0], values[1]


def wrapped(container: str, a: int, b: int, n: int) -> Any:
    pair = {"a": a, "b": b}
    if container == "nested":
        return {"n": n, "o": pair}
    if container == "array":
        return [pair]
    if container == "map":
        return {"k": pair}
    return pair


def expected_rows(door: str, container: str, a: int, b: int) -> list[list[Any]]:
    written = wrapped(container, a, b, 9)
    if door in recorder.UPDATING:
        return [[0, written]]
    if door in (*recorder.REPLACING, "df_overwrite_partitions", "df_overwrite_condition"):
        return [[1, written]]
    return [[0, wrapped(container, 10, 20, 90)], [1, written]]


def rule(
    group: str,
    door: str,
    container: str,
    source: tuple[tuple[str, int], ...],
    table: str,
) -> dict[str, Any]:
    path = PATH_PREFIX[container]
    if by_name(door):
        paired = pair_by_name(source, path, case_sensitive=group == "cs")
    else:
        paired = pair_by_position(source, path)
    if isinstance(paired[0], str):
        label = "``" if door in recorder.UNLABELLED else recorder.table_label(table)
        return {
            "error": f"[INCOMPATIBLE_DATA_FOR_TABLE.{paired[0]}] Cannot write incompatible "
            f"data for the table {label}: {paired[1]}. SQLSTATE: KD000"
        }
    return {"rows": expected_rows(door, container, int(paired[0]), int(paired[1]))}


def matrix_rule(group: str, door: str, container: str, field: str) -> dict[str, Any]:
    table = recorder.table_name(group, door, container, field)
    return rule(group, door, container, recorder.FIELDS[field], table)


def added_rule(name: str) -> dict[str, Any]:
    door, field = ADDED_RULES[name]
    source = TWINS if field == "case_twins" else recorder.FIELDS[field]
    return rule("added", door, "top", source, f"ice.added.{name}")


def answers(expected: dict[str, Any], actual: dict[str, Any]) -> bool:
    if "rows" in expected:
        return actual.get("rows") == expected["rows"]
    return expected["error"] in actual.get("error", "")


def matrix_keys(group: str) -> list[tuple[str, str, str, str]]:
    return [
        (group, door, container, field)
        for door in recorder.DOOR_ORDER
        for container in recorder.CONTAINERS
        for field in recorder.group_fields(group)
    ]


ALL_CELLS = [key for group in MATRIX_GROUPS for key in matrix_keys(group)]
RULED_CELLS = [key for key in ALL_CELLS if not in_l1(*key[1:]) and not in_l2(key[1])]
L1_CELLS = [key for key in ALL_CELLS if in_l1(*key[1:])]
L2_CELLS = [key for key in ALL_CELLS if in_l2(key[1])]
L2_REFUSED_CELLS = [key for key in L2_CELLS if "error" in matrix_rule(*key)]
L2_MAIN_CELLS = [key for key in L2_CELLS if key not in L2_REFUSED_CELLS and key not in L2_RAW_CELLS]
PARQUET_DOORS = [door for door in recorder.DOOR_ORDER if not in_l2(door)]


def cell_id(key: tuple[str, ...]) -> str:
    return "|".join(key)


def run_cell(spark: Any, key: tuple[str, str, str, str]) -> dict[str, Any]:
    group, door, container, field = key
    version, case_sensitive = recorder.GROUPS[group]
    spark.conf.set("spark.sql.caseSensitive", case_sensitive)
    try:
        return recorder.matrix_cell(spark, group, version, door, container, field)
    finally:
        spark.conf.set("spark.sql.caseSensitive", "false")


def file_count(spark: Any, table: str) -> int:
    return len(spark.sql(f"SELECT file_path FROM {table}.files").collect())


def parquet_rows(spark: Any, door: str) -> list[list[Any]]:
    table = f"ice.parquet.{door}"
    recorder.create_seeded(spark, table, recorder.TOP, recorder.TOP_SEED, "2")
    recorder.DOORS[door](spark, table, recorder.struct_sql("reordered"))
    files = [row[0] for row in spark.sql(f"SELECT file_path FROM {table}.files").collect()]
    stored = []
    for file in files:
        frame = pq.read_table(file.removeprefix("file://"))
        stored.append(["names", frame.schema.field("st").type.names])
        stored.extend([row["id"], row["st"]] for row in frame.to_pylist())
    return stored


@pytest.fixture(scope="module")
def replayed(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    _reset_active_session_for_tests()
    warehouse = tmp_path_factory.mktemp("ice-struct-pairing-1")
    spark = recorder.open_repark(str(warehouse))
    for group in (*MATRIX_GROUPS, "added", "parquet"):
        spark.sql(f"CREATE NAMESPACE ice.{recorder.NAMESPACE.get(group, group)}")
    cells = {key: run_cell(spark, key) for key in ALL_CELLS}
    files = {key: file_count(spark, recorder.table_name(*key)) for key in L1_CELLS}
    added = {name: recorder.added_cell(spark, name) for name in recorder.ADDED}
    parquet = {}
    for door in PARQUET_DOORS:
        try:
            parquet[door] = parquet_rows(spark, door)
        except Exception as error:
            parquet[door] = [["error", recorder.one_line(error)]]
    return {"cells": cells, "files": files, "added": added, "parquet": parquet}


@pytest.mark.parametrize("key", ALL_CELLS, ids=cell_id)
def test_the_rule_reproduces_spark_in_every_cell(key: tuple[str, str, str, str]) -> None:
    group, door, container, field = key
    oracle = ORACLE[group][f"{door}|{container}|{field}"]
    assert answers(matrix_rule(*key), oracle), oracle


@pytest.mark.parametrize("name", sorted(ADDED_RULES))
def test_the_rule_reproduces_spark_in_every_added_cell(name: str) -> None:
    assert answers(added_rule(name), ORACLE["added"][name]), ORACLE["added"][name]


@pytest.mark.parametrize("key", RULED_CELLS, ids=cell_id)
def test_the_engine_answers_the_rule(replayed: dict[str, Any], key: tuple[str, ...]) -> None:
    actual = replayed["cells"][key]
    expected = matrix_rule(*key)
    assert answers(expected, actual), f"expected {expected}, engine {actual}"


@pytest.mark.parametrize("name", sorted(ADDED_RULES))
def test_the_engine_answers_the_rule_in_every_added_cell(
    replayed: dict[str, Any], name: str
) -> None:
    actual = replayed["added"][name]
    expected = added_rule(name)
    assert answers(expected, actual), f"expected {expected}, engine {actual}"


@pytest.mark.parametrize("key", L1_CELLS, ids=cell_id)
def test_l1_refuses_and_writes_no_file(replayed: dict[str, Any], key: tuple[str, ...]) -> None:
    group, door, container, field = key
    assert "error" in replayed["cells"][key], replayed["cells"][key]
    assert "error" in ORACLE[group][f"{door}|{container}|{field}"]
    assert replayed["files"][key] == 1


@pytest.mark.parametrize("key", L2_REFUSED_CELLS, ids=cell_id)
def test_l2_refuses_as_spark_where_spark_refuses(
    replayed: dict[str, Any], key: tuple[str, ...]
) -> None:
    actual = replayed["cells"][key]
    expected = matrix_rule(*key)
    assert answers(expected, actual), f"expected {expected}, engine {actual}"


@pytest.mark.parametrize("key", L2_MAIN_CELLS, ids=cell_id)
def test_l2_answers_as_main(replayed: dict[str, Any], key: tuple[str, ...]) -> None:
    group, door, container, field = key
    assert replayed["cells"][key] == MAIN[group][f"{door}|{container}|{field}"]


@pytest.mark.parametrize("key", L2_RAW_CELLS, ids=cell_id)
def test_l2_case_cells_keep_an_internal_refusal(
    replayed: dict[str, Any], key: tuple[str, ...]
) -> None:
    group, door, container, field = key
    assert "rows" in ORACLE[group][f"{door}|{container}|{field}"]
    assert replayed["cells"][key] == {"error": R014_RAW}


def test_the_required_field_cell_refuses_as_main(replayed: dict[str, Any]) -> None:
    actual = replayed["added"]["required_struct_null"]
    assert actual == MAIN["added"]["required_struct_null"]
    assert "error" in ORACLE["added"]["required_struct_null"]


@pytest.mark.parametrize("door", PARQUET_DOORS)
def test_the_stored_fields_are_read_from_the_parquet_file(
    replayed: dict[str, Any], door: str
) -> None:
    table = f"ice.parquet.{door}"
    expected = rule("v2", door, "top", recorder.FIELDS["reordered"], table)["rows"]
    stored = replayed["parquet"][door]
    assert all(row == ["names", list(TARGET)] for row in stored if row[0] == "names"), stored
    rows = sorted((row for row in stored if row[0] != "names"), key=lambda row: row[0])
    assert rows == expected, stored
