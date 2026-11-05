from __future__ import annotations

import json
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as sf

CELLS = Path(__file__).parent / "data" / "ansi_subscript_field_cells.jsonl"
V2 = " USING iceberg TBLPROPERTIES ('format-version'='2')"
FIXTURES = {
    "AR": (
        "CREATE TABLE {t} (id INT, l ARRAY<INT>, m MAP<STRING, INT>)" + V2,
        "INSERT INTO {t} VALUES (1, array(10, 20, 30), map('k', 1)), (2, array(40), map('k', 2))",
    ),
    "LI": (
        "CREATE TABLE {t} (id INT, l ARRAY<INT>, i INT)" + V2,
        "INSERT INTO {t} VALUES (1, array(10, 20, 30), 0), (2, array(40), 0)",
    ),
    "U": ("CREATE TABLE {t} (id INT, v INT)" + V2, "INSERT INTO {t} VALUES (100, 100)"),
    "S1": (
        "CREATE TABLE {t} (id INT, s STRUCT<a: INT, b: STRING>)" + V2,
        "INSERT INTO {t} VALUES (1, named_struct('a', 1, 'b', 'x')), "
        "(2, named_struct('a', 7, 'b', 'y')), (3, NULL)",
    ),
}
CONFS = {
    "ansi": ("true", "false"),
    "legacy": ("false", "false"),
    "case_sensitive": ("true", "true"),
}
IN_UNIT = (
    "INVALID_ARRAY_INDEX",
    "INVALID_ARRAY_INDEX_IN_ELEMENT_AT",
    "INVALID_INDEX_OF_ZERO",
    "AMBIGUOUS_REFERENCE_TO_FIELDS",
)
OUTSIDE_UNIT = {
    ("ansi", "get_past"): "the `get` routine is not resolved on the facade",
    ("ansi", "dup_star"): "nested rule D4: star over a struct refuses",
    ("ansi", "case_single"): "nested rule D13 ns_case: a case-different single field refuses",
    ("ansi", "dml_delete_case_guard"): "DataFusion rewrites a boolean CASE guard to AND and OR",
    ("ansi", "f2_alias_mixed"): "nested rule D7: collecting a struct with duplicate names refuses",
    ("ansi", "f2_w_delete_alias_dup"): "card C1: a DELETE WHERE chain refuses with the dot text",
}


def _cells() -> list[dict[str, Any]]:
    return [json.loads(line) for line in CELLS.read_text().splitlines()]


def _cell_value(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {key: _cell_value(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_cell_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _cell_value(item) for key, item in sorted(value.items())}
    return value


def _rows(spark: ReparkSession, sql: str) -> list[list[Any]]:
    rows = [[_cell_value(value) for value in row] for row in spark.sql(sql).collect()]
    return sorted(rows, key=json.dumps)


def _frame_rows(frame: Any) -> list[list[Any]]:
    rows = [[_cell_value(value) for value in row] for row in frame.collect()]
    return sorted(rows, key=json.dumps)


def _error_class(text: str) -> str:
    return text[1 : text.index("]")] if text.startswith("[") else ""


def _row_independent(text: str) -> str:
    return text.split(" The array has ")[0]


def _assert_refusal(want: str, raised: BaseException) -> None:
    if _error_class(want) in IN_UNIT:
        assert _row_independent(want) in str(raised)


@pytest.fixture
def door(tmp_path: Path) -> Iterator[ReparkSession]:
    session = (
        ReparkSession.builder.appName("ansi-subscript-field")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path / "spark-warehouse"))
        .config("spark.sql.catalog.asf.type", "hadoop")
        .config("spark.sql.catalog.asf.warehouse", str(tmp_path / "asf"))
        .getOrCreate()
    )
    yield session
    session.stop()


CELL_ROWS = _cells()


def test_the_cell_table_holds_every_enumerated_case() -> None:
    assert len(CELL_ROWS) == 137
    assert sum(row.get("door") == "dataframe" for row in CELL_ROWS) == 14
    assert {(row["conf"], row["name"]) for row in CELL_ROWS} >= set(OUTSIDE_UNIT)
    refusals = [row for row in CELL_ROWS if "error" in row["spark"]]
    assert sum(_error_class(row["spark"]["error"]) in IN_UNIT for row in refusals) == 69


@pytest.mark.parametrize(
    "cell",
    [
        row
        for row in CELL_ROWS
        if (row["conf"], row["name"]) not in OUTSIDE_UNIT and row.get("door") != "dataframe"
    ],
    ids=lambda row: f"{row['conf']}-{row['name']}",
)
def test_the_spark_door_answers_as_spark_does(door: ReparkSession, cell: dict[str, Any]) -> None:
    ansi, case_sensitive = CONFS[cell["conf"]]
    table = "asf.ns.t"
    door.conf.set("spark.sql.ansi.enabled", "true")
    door.conf.set("spark.sql.caseSensitive", "false")
    door.sql("CREATE NAMESPACE IF NOT EXISTS asf.ns").collect()
    for statement in FIXTURES[cell["fixture"]]:
        door.sql(statement.format(t=table)).collect()
    door.conf.set("spark.sql.ansi.enabled", ansi)
    door.conf.set("spark.sql.caseSensitive", case_sensitive)
    sql = cell["stmt"].format(t=table)
    want = cell["spark"]
    if "error" in want:
        with pytest.raises(Exception) as raised:
            _rows(door, sql)
        _assert_refusal(want["error"], raised.value)
    elif "spark_readback" in cell:
        door.sql(sql).collect()
    else:
        assert _rows(door, sql) == want["rows"]
    if "spark_readback" in cell:
        door.conf.set("spark.sql.caseSensitive", "false")
        assert _rows(door, f"SELECT * FROM {table}") == cell["spark_readback"]


def _array_frame(door: ReparkSession) -> Any:
    return door.createDataFrame([(1, [10, 20, 30], 0), (2, [40], 0)], "id INT, l ARRAY<INT>, i INT")


DATAFRAME_CASES: dict[str, Callable[[Any], Any]] = {
    "f1_df_sub_litcol": lambda df: df.select(sf.col("l")[sf.lit(9)]),
    "f1_df_sub_expr": lambda df: df.select(sf.col("l")[sf.col("i") + 9]),
    "f1_df_sub_negcol": lambda df: df.select(sf.col("l")[sf.lit(-1)]),
    "f1_df_getitem_expr": lambda df: df.select(sf.col("l").getItem(sf.col("i") + 9)),
    "f1_df_getitem_litcol": lambda df: df.select(sf.col("l").getItem(sf.lit(9))),
    "f1_df_sub_col_in": lambda df: df.select(sf.col("l")[sf.col("i")]),
    "f1_df_filter_col": lambda df: df.filter(sf.col("l")[sf.col("i") + 9].isNull()),
    "f1_df_get_col": lambda df: df.select(sf.get(sf.col("l"), sf.col("i") + 9)),
    "f1_df_tea_col": lambda df: df.select(sf.try_element_at(sf.col("l"), sf.col("i") + 9)),
    "f1_legacy_df_sub_expr": lambda df: df.limit(1).select(sf.col("l")[sf.col("i") + 9]),
    "f1_legacy_df_getitem_expr": lambda df: df.limit(1).select(
        sf.col("l").getItem(sf.col("i") + 9)
    ),
    "f1_w_insert_into": lambda df: df.select("id", sf.col("l").getItem(sf.col("i") + 9).alias("v")),
    "f1_w_insert_into_sub": lambda df: df.select("id", sf.col("l")[sf.lit(9)].alias("v")),
}


@pytest.mark.parametrize(
    "cell",
    [row for row in CELL_ROWS if row.get("door") == "dataframe"],
    ids=lambda row: f"{row['conf']}-{row['name']}",
)
def test_the_dataframe_door_answers_as_spark_does(
    door: ReparkSession, cell: dict[str, Any]
) -> None:
    ansi, _ = CONFS[cell["conf"]]
    door.conf.set("spark.sql.ansi.enabled", ansi)
    want = cell["spark"]
    if cell["name"] == "f1_df_map_col_key":
        frame = door.sql("SELECT map('k', 1) AS m").select(sf.col("m")[sf.lit("z")])
        assert _frame_rows(frame) == want["rows"]
        return
    frame = DATAFRAME_CASES[cell["name"]](_array_frame(door))
    if "spark_readback" in cell:
        door.sql("CREATE NAMESPACE IF NOT EXISTS asf.ns").collect()
        for statement in FIXTURES["U"]:
            door.sql(statement.format(t="asf.ns.t")).collect()
        with pytest.raises(Exception) as raised:
            frame.write.insertInto("asf.ns.t")
        _assert_refusal(want["error"], raised.value)
        assert _rows(door, "SELECT * FROM asf.ns.t") == cell["spark_readback"]
    elif "error" in want:
        with pytest.raises(Exception) as raised:
            _frame_rows(frame)
        _assert_refusal(want["error"], raised.value)
    else:
        assert _frame_rows(frame) == want["rows"]
