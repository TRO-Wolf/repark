"""STRUCT-COMPARE-BY-POSITION-1: DataFrame doors compare struct operands by position."""

from __future__ import annotations

import datetime
import decimal
import json
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pandas
import pyarrow as pa
import pytest

import repark
from repark import ReparkSession
from repark.spark import functions
from repark.spark.session import _reset_active_session_for_tests

CELLS_PATH = Path(__file__).parent / "data" / "struct_compare_by_position_cells.jsonl"
KIND_PATH = Path(__file__).parent / "data" / "struct_compare_kind_cells.jsonl"
CLASS = re.compile(r"\[([A-Z][A-Z0-9_]*(?:\.[A-Z0-9_]+)*)\]")
PLANNER_CLASS = (
    "a SELECT-list struct clash refuses without Spark's class (STRUCT-COMPARE-PLANNER-CLASS-1)"
)
MAP_JOIN = "a map subscript in a self-join is ambiguous (STRUCT-MAP-SUBSCRIPT-JOIN-1)"
UNIT_C = "unit C (#1283) refuses a reordered unionByName; not this unit"
KNOWN: dict[str, str] = {
    **dict.fromkeys(
        (
            "01159",
            "01164",
            "01165",
            "01178",
            "01183",
            "01184",
            "01197",
            "01202",
            "01203",
            "01216",
            "01221",
            "01222",
            "01235",
            "01240",
            "01241",
            "01254",
            "01259",
            "01260",
            "01273",
            "01278",
            "01279",
            "03981",
            "03986",
            "03987",
            "04000",
            "04005",
            "04006",
            "04019",
            "04024",
            "04025",
            "04057",
            "04062",
            "04063",
            "04076",
            "04081",
            "04082",
            "04095",
            "04100",
            "04101",
            "04133",
            "04138",
            "04139",
            "04152",
            "04157",
            "04158",
            "04171",
            "04176",
            "04177",
        ),
        PLANNER_CLASS,
    ),
    **dict.fromkeys(
        (
            "02498",
            "02517",
            "02536",
            "02555",
            "02574",
            "02593",
            "02612",
            "02631",
            "02650",
            "02669",
        ),
        MAP_JOIN,
    ),
    "04389": UNIT_C,
}
NATIVE_REORDERED = "SELECT named_struct('a', 1, 'b', 2) = named_struct('b', 2, 'a', 1) AS r"


def load() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    lines = CELLS_PATH.read_text(encoding="utf-8").splitlines()
    return json.loads(lines[0])["fixtures"], [json.loads(line) for line in lines[1:]]


FIXTURES, CELLS = load()
DF_CELLS = [cell for cell in CELLS if cell["door"] in ("df_typed", "df_string")]
KIND_CELLS = [json.loads(line) for line in KIND_PATH.read_text(encoding="utf-8").splitlines()]
KIND_DOORS = ("sql_view", "df_typed", "df_string")
KIND_RUNS = [
    (cell, door)
    for cell in KIND_CELLS
    for door in KIND_DOORS
    if (cell["door"] == "df") == (door != "sql_view")
]


def value_of(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {key: value_of(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [value_of(item) for item in value]
    if isinstance(value, dict):
        return {str(key): value_of(item) for key, item in sorted(value.items())}
    if isinstance(value, (datetime.date, datetime.datetime, decimal.Decimal)):
        return str(value)
    return value


def rows_of(frame: Any) -> list[Any]:
    return sorted(([value_of(item) for item in row] for row in frame.collect()), key=json.dumps)


def outcome(spark: Any, wide: str, cell: dict[str, Any]) -> tuple[str, Any]:
    scope = {"F": functions, "w": spark.table(wide), "spark": spark}
    try:
        return "rows", rows_of(eval(cell["stmt"], scope))
    except Exception as error:
        found = CLASS.search(str(error))
        name = type(error).__name__
        return "error", "PANIC" if name == "PanicException" else (found.group(1) if found else name)


def matches(expect: dict[str, Any], got: tuple[str, Any]) -> bool:
    kind, value = got
    if expect["kind"] == "rows":
        return kind == "rows" and value == sorted(expect["rows"], key=json.dumps)
    return kind == "error" and value == expect["class"]


@pytest.fixture(scope="module")
def session(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Any]:
    _reset_active_session_for_tests()
    root = tmp_path_factory.mktemp("warehouse")
    spark = ReparkSession.builder.appName("struct-compare-by-position-1").getOrCreate()
    spark.register_memory_catalog("hd", str(root))
    spark.sql("CREATE NAMESPACE hd.sd").collect()
    create, seed, _ = FIXTURES["W"]
    spark.sql(create.replace("{t}", "hd.sd.w")).collect()
    spark.sql(seed.replace("{t}", "hd.sd.w")).collect()
    yield spark
    spark.stop()
    _reset_active_session_for_tests()


@pytest.fixture(scope="module")
def measured(session: Any) -> dict[str, tuple[str, Any]]:
    outcomes = {}
    for cell in DF_CELLS:
        session.conf.set("spark.sql.caseSensitive", str(cell["case"] == "sensitive").lower())
        session.conf.set("spark.sql.ansi.enabled", str(cell["ansi"] != "off").lower())
        try:
            outcomes[cell["key"]] = outcome(session, "hd.sd.w", cell)
        finally:
            session.conf.set("spark.sql.caseSensitive", "false")
            session.conf.set("spark.sql.ansi.enabled", "true")
    return outcomes


def test_the_cells_file_carries_every_dataframe_door_cell() -> None:
    assert len(CELLS) == 4393
    assert len(DF_CELLS) == 1597
    assert set(KNOWN) <= {cell["key"] for cell in DF_CELLS}


@pytest.mark.parametrize("cell", DF_CELLS, ids=[cell["key"] for cell in DF_CELLS])
def test_dataframe_door_cell_matches_spark(
    measured: dict[str, tuple[str, Any]], cell: dict[str, Any]
) -> None:
    got = measured[cell["key"]]
    assert got != ("error", "PANIC"), cell
    if cell["key"] in KNOWN:
        assert got[0] == "error", f"KNOWN holds refusals only: {KNOWN[cell['key']]}"
        assert not matches(cell["expect"], got), f"now matches; drop {KNOWN[cell['key']]}"
        return
    assert matches(cell["expect"], got), (cell["stmt"], got, cell["expect"])


def ordered(order: str) -> list[str]:
    return ["a", "b"] if order == "AB" else ["b", "a"]


def kind_type(kind: str, element: pa.DataType) -> pa.DataType:
    kinds = {
        "list": pa.list_(element),
        "large": pa.large_list(element),
        "fsl1": pa.list_(element, 1),
        "fsl2": pa.list_(element, 2),
        "view": pa.list_view(element),
    }
    return kinds.get(kind, pa.large_list_view(element))


def side_frame(spark: Any, cell: dict[str, Any], name: str) -> Any:
    side, values = cell[name], cell["values"][name]
    struct = pa.struct([(field, pa.int64()) for field in ordered(side["order"])])
    item = {field: values[field] for field in ordered(side["order"])}
    element, item = (struct, item) if cell["payload"] == "struct" else (pa.list_(struct), [item])
    kind = kind_type(side["kind"], element)
    row = [item] * (2 if side["kind"] == "fsl2" else 1)
    if cell["shape"] == "struct":
        array = pa.array([{"x": row}], type=pa.struct([("x", kind)]))
    elif cell["shape"] == "map" and side["names"] == "std":
        array = pa.array([[("k", row)]], type=pa.map_(pa.string(), kind))
    elif cell["shape"] == "map":
        entries = pa.map_(pa.field("k", pa.string(), nullable=False), pa.field("v", kind))
        array = pa.array([[("k", row)]], type=entries)
    else:
        array = pa.array([row], type=kind)
        if side["enc"] == "ree":
            array = pa.RunEndEncodedArray.from_arrays(pa.array([1], type=pa.int32()), array)
        elif side["enc"] == "dict":
            array = pa.DictionaryArray.from_arrays(pa.array([0], type=pa.int32()), array)
    table = pa.table({"s": array}).to_pandas(types_mapper=pandas.ArrowDtype)
    return spark.createDataFrame(table)


def canon(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): canon(item) for key, item in value.items()}
    if isinstance(value, list):
        if value and all(isinstance(item, tuple) for item in value):
            return {str(key): canon(item) for key, item in value}
        return [canon(item) for item in value]
    return value


def kind_frame(spark: Any, cell: dict[str, Any], door: str) -> Any:
    left, right = side_frame(spark, cell, "L"), side_frame(spark, cell, "R")
    if door == "sql_view":
        left.createOrReplaceTempView("l")
        right.createOrReplaceTempView("r")
        return spark.sql(cell["stmt"])
    left = left.select(functions.col("s").alias("ls"))
    right = right.select(functions.col("s").alias("rs"))
    typed = {
        "eq": functions.col("ls") == functions.col("rs"),
        "in": functions.col("ls").isin(functions.col("rs")),
        "join": functions.col("ls") == functions.col("rs"),
    }
    text = {"eq": "ls = rs", "in": "ls IN (rs)", "join": "ls = rs"}
    predicate = typed[cell["cop"]] if door == "df_typed" else functions.expr(text[cell["cop"]])
    if cell["cop"] == "join":
        return left.join(right, predicate).select(functions.col("ls").alias("x"))
    return left.crossJoin(right).select(predicate.alias("x"))


def kind_outcome(spark: Any, cell: dict[str, Any], door: str) -> tuple[str, Any]:
    try:
        rows = kind_frame(spark, cell, door).toArrow().to_pylist()
    except Exception as error:
        return "refused", "PANIC" if type(error).__name__ == "PanicException" else str(error)
    return "rows", sorted(json.dumps(canon(row), sort_keys=True, default=str) for row in rows)


@pytest.fixture(scope="module")
def kind_measured(session: Any) -> dict[str, tuple[str, Any]]:
    return {f"{cell['cell']}-{door}": kind_outcome(session, cell, door) for cell, door in KIND_RUNS}


@pytest.mark.parametrize(
    ("cell", "door"), KIND_RUNS, ids=[f"{cell['cell']}-{door}" for cell, door in KIND_RUNS]
)
def test_every_list_kind_and_encoding_answers_as_its_plain_list_twin_on_spark(
    kind_measured: dict[str, tuple[str, Any]], cell: dict[str, Any], door: str
) -> None:
    kind, value = kind_measured[f"{cell['cell']}-{door}"]
    assert value != "PANIC", cell["cell"]
    expect = cell["expect"]
    if expect["kind"] == "refused":
        assert kind == "refused", (cell["cell"], expect["card"], value)
        return
    assert (kind, value) == ("rows", expect["rows"]), (cell["cell"], cell["stmt"], value)


def test_native_door_still_pairs_by_name() -> None:
    frame = repark.sql(NATIVE_REORDERED)
    assert [row[0] for row in frame.collect()] == [True]
