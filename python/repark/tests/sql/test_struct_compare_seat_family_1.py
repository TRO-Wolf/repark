"""STRUCT-COMPARE-SEAT-FAMILY-1: DataFrame doors pair joins, BETWEEN, CASE and nullif by position"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions
from repark.spark.session.session_state import _promote_active

pytestmark = pytest.mark.xdist_group("test_struct_compare_seat_family_1")

CELLS_PATH = Path(__file__).parent / "data" / "struct_compare_seat_family_cells.jsonl"
INTERNAL = "__repark_struct_by_position"
A = "named_struct('a', 1, 'b', 2)"
FORMS = {
    "between": ("{x} BETWEEN {y} AND {y}", lambda x, y: x.between(y, y)),
    "not_between": ("{x} NOT BETWEEN {y} AND {y}", None),
    "case1": ("CASE {x} WHEN {y} THEN true ELSE false END", None),
    "case2": ("CASE {x} WHEN {y} THEN true WHEN " + A + " THEN false ELSE NULL END", None),
    "nullif_lr": ("nullif({x}, {y})", lambda x, y: functions.nullif(x, y)),
    "nullif_rl": ("nullif({y}, {x})", lambda x, y: functions.nullif(y, x)),
}
NAMED_FORMS = [
    "s = named_struct('b', 1, 'a', 2)",
    "s BETWEEN named_struct('b', 1, 'a', 2) AND named_struct('b', 1, 'a', 2)",
    "s NOT BETWEEN named_struct('b', 1, 'a', 2) AND named_struct('b', 1, 'a', 2)",
    "CASE s WHEN named_struct('b', 1, 'a', 2) THEN 1 ELSE 0 END",
    "nullif(s, named_struct('b', 1, 'a', 2))",
    "nullif(named_struct('b', 1, 'a', 2), s)",
]


def load() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    lines = CELLS_PATH.read_text(encoding="utf-8").splitlines()
    return json.loads(lines[0]), [json.loads(line) for line in lines[1:]]


HEADER, CELLS = load()
VIEWS, NAME_CELLS = HEADER["views"], HEADER["names"]
FRAME_CELLS = [cell for cell in CELLS if cell.get("door") in ("df_typed", "df_str")]


def plain(value: Any, data_type: Any) -> Any:
    if value is None:
        return None
    kind = type(data_type).__name__
    if kind == "StructType":
        items = list(value.values()) if isinstance(value, dict) else list(value)
        return [
            plain(item, field.dataType)
            for item, field in zip(items, data_type.fields, strict=False)
        ]
    if kind == "ArrayType":
        return [plain(item, data_type.elementType) for item in value]
    if kind == "MapType":
        pairs = (
            [plain(key, data_type.keyType), plain(item, data_type.valueType)]
            for key, item in value.items()
        )
        return sorted(pairs, key=repr)
    return value


def wrapped(text: str, wrap: str | None) -> str:
    if wrap == "isnull":
        return f"({text}) IS NULL"
    if wrap == "notnull":
        return f"({text}) IS NOT NULL"
    return text


def join_frame(spark: Any, op: dict[str, Any]) -> Any:
    on = op["form"] == "on"
    left = spark.table(op["L"]).alias("l") if on else spark.table(op["L"])
    right = spark.table(op["R"]).alias("r") if on else spark.table(op["R"])
    key = op["key"]
    if on:
        frame = left.join(right, functions.col("l." + key) == functions.col("r." + key), op["how"])
    else:
        frame = left.join(right, [key] if op["form"] == "list" else key, op["how"])
    if op["proj"] == "star":
        return frame.select("*")
    if op["proj"] == "key":
        return frame.select(key)
    if on:
        return frame.select(*[functions.col(name) for name in op["pairs"]])
    return frame.select(*op["pairs"])


def compare_frame(spark: Any, op: dict[str, Any]) -> Any:
    base = spark.table(op["L"]).alias("l")
    ids = ["l.lid"]
    if op["R"]:
        base = base.crossJoin(spark.table(op["R"]).alias("r"))
        ids.append("r.rid")
    text = wrapped(FORMS[op["form"]][0].format(x="l.s", y=op["rhs"]), op["wrap"])
    if op["door"] == "df_typed":
        rhs = functions.col(op["rhs"]) if op["R"] else functions.expr(op["rhs"])
        cond = FORMS[op["form"]][1](functions.col("l.s"), rhs)
        if op["wrap"] == "isnull":
            cond = cond.isNull()
        elif op["wrap"] == "notnull":
            cond = cond.isNotNull()
        if op["detail"] == "typed_select":
            return base.select(*[functions.col(name) for name in ids], cond.alias("x"))
        return base.filter(cond).select(*[functions.col(name) for name in ids])
    if op["detail"] == "str_filter":
        return base.filter(text).select(*ids)
    if op["detail"] == "str_select_expr":
        return base.selectExpr(*ids, f"{text} AS x")
    return base.select(*[functions.col(name) for name in ids], functions.expr(text).alias("x"))


def frame_of(spark: Any, op: dict[str, Any]) -> Any:
    if op["kind"] == "df_join":
        return join_frame(spark, op)
    if op["kind"] == "cmp":
        return compare_frame(spark, op)
    return (
        spark.table(op["L"]).alias("l").lateralJoin(spark.table(op["R"]).alias("r"), on=[op["key"]])
    )


def outcome(spark: Any, cell: dict[str, Any]) -> tuple[str, Any]:
    spark.conf.set("spark.sql.caseSensitive", "true" if cell["case"] == "sensitive" else "false")
    try:
        frame = frame_of(spark, cell["op"])
        columns = cell["expect"].get("columns", [])
        index = [frame.columns.index(name) for name in columns]
        fields = frame.schema.fields
        rows = [[plain(row[i], fields[i].dataType) for i in index] for row in frame.collect()]
        return "rows", sorted(rows, key=json.dumps)
    except BaseException as error:
        if type(error).__name__ == "PanicException":
            return "panic", str(error)
        return "error", str(error)


def own_session(previous: Any) -> Any:
    if previous is None:
        return ReparkSession.builder.appName("struct-compare-seat-family-1").getOrCreate()
    return previous.newSession()


@pytest.fixture(scope="module")
def doors() -> dict[str, Any]:
    previous = ReparkSession.getActiveSession()
    spark = own_session(previous)
    assert spark is not previous
    try:
        for _name, statement in VIEWS:
            spark.sql(statement).collect()
        cells = {cell["id"]: outcome(spark, cell) for cell in FRAME_CELLS}
        spark.conf.set("spark.sql.caseSensitive", "false")
        names = {text: column_names(spark, text) for text in NAMED_FORMS}
        spark.sql(HEADER["names_view"]).collect()
        named = {cell["id"]: measured_names(spark, cell) for cell in NAME_CELLS}
        dup = outcome_of_duplicate_nullif(spark)
        return {"cells": cells, "names": names, "named": named, "dup": dup}
    finally:
        spark.stop()
        if previous is not None:
            _promote_active(previous)


def column_names(spark: Any, text: str) -> list[str]:
    frame = spark.table("lk")
    return [
        *frame.selectExpr(text).columns,
        *frame.select(functions.expr(text)).columns,
        *spark.sql(f"SELECT {text} FROM lk").columns,
    ]


def named_frame(spark: Any, cell: dict[str, Any]) -> Any:
    if cell["door"] == "sql":
        return spark.sql(f"SELECT {cell['text']} FROM nm")
    table = spark.table("nm")
    if cell["door"] == "select_expr":
        return table.selectExpr(cell["text"])
    if cell["door"] == "f_expr":
        return table.select(functions.expr(cell["text"]))
    s, t = functions.col("s"), functions.col("t")
    if cell["form"] == "between":
        return table.select(s.between(t, t))
    if cell["form"] == "not_between":
        return table.select(~s.between(t, t))
    return table.select(functions.nullif(s, t))


def measured_names(spark: Any, cell: dict[str, Any]) -> list[str]:
    try:
        return list(named_frame(spark, cell).columns)
    except BaseException as error:
        return [f"{type(error).__name__}: {error}"]


def outcome_of_duplicate_nullif(spark: Any) -> list[Any]:
    frame = spark.table("ldup").alias("l").crossJoin(spark.table("rrev").alias("r"))
    text = "nullif(l.s, r.s) IS NULL"
    selected = frame.select("l.lid", "r.rid", functions.expr(text).alias("x"))
    return sorted((list(row) for row in selected.collect()), key=json.dumps)


def test_the_cells_file_carries_every_dataframe_door_cell() -> None:
    assert len(CELLS) == 2426
    assert len(FRAME_CELLS) == 1195
    assert len(NAME_CELLS) == 18


@pytest.mark.parametrize("cell", FRAME_CELLS, ids=[cell["id"] for cell in FRAME_CELLS])
def test_dataframe_door_cell_matches_spark(doors: dict[str, Any], cell: dict[str, Any]) -> None:
    kind, got = doors["cells"][cell["id"]]
    assert kind != "panic", got
    expect = cell["expect"]
    if kind == "rows" and expect["kind"] == "rows":
        assert got == sorted(expect["rows"], key=json.dumps)
    elif not cell["known"]:
        assert kind == expect["kind"], got


@pytest.mark.parametrize("text", NAMED_FORMS)
def test_generated_names_never_show_the_relabel(doors: dict[str, Any], text: str) -> None:
    names = doors["names"][text]
    assert len(names) == 3
    assert not any(INTERNAL in name for name in names), names


@pytest.mark.parametrize("cell", NAME_CELLS, ids=[cell["id"] for cell in NAME_CELLS])
def test_generated_name_is_spark_exact_where_base_was(
    doors: dict[str, Any], cell: dict[str, Any]
) -> None:
    names = doors["named"][cell["id"]]
    assert len(names) == 1, names
    assert INTERNAL not in names[0], names
    if cell["exact"]:
        assert names == cell["spark"]


def test_bound_nullif_with_duplicate_names_pairs_by_position_without_a_panic(
    doors: dict[str, Any],
) -> None:
    rows = doors["dup"]
    assert [1, 10, True] in rows
    assert [2, 10, False] in rows
