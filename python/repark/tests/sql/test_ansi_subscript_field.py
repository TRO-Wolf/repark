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
    "MX": (
        "CREATE TABLE {t} (id INT, v INT, l ARRAY<INT>, m MAP<STRING, INT>, "
        "s STRUCT<x: INT, y: INT>)" + V2,
        "INSERT INTO {t} VALUES (1, 1, array(10, 20), map('a', 7), named_struct('x', 8, 'y', 9))",
    ),
    "NX": (
        "CREATE TABLE {t} (id INT, k STRING, ls ARRAY<STRUCT<a: INT>>, "
        "n STRUCT<a: MAP<STRING, INT>, b: STRUCT<x: INT>>, m MAP<STRING, INT>)" + V2,
        "INSERT INTO {t} VALUES (1, 'a', array(named_struct('a', 3)), "
        "named_struct('a', map('x', 4), 'b', named_struct('x', 5)), map('a', 7, 'B', 8))",
    ),
    "UV": ("CREATE TABLE {t} (id INT, v INT)" + V2, "INSERT INTO {t} VALUES (1, 1), (2, 2)"),
    "SA": (
        "CREATE TABLE {t} (id INT, s STRUCT<a: INT>)" + V2,
        "INSERT INTO {t} VALUES (0, named_struct('a', 0))",
    ),
    "SB": (
        "CREATE TABLE {t} (id INT, s STRUCT<a: INT, b: INT>)" + V2,
        "INSERT INTO {t} VALUES (0, named_struct('a', 1, 'b', 2))",
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
    ("ansi", "f4_name_array_fields"): "an array-of-struct field path refuses on the engine",
    ("ansi", "f4_name_col_key"): "card: SQL m[k] with a column key on a table refuses",
    ("ansi", "f4_name_map_null_key"): "a NULL map key refuses on the engine",
    ("ansi", "f4_w_delete_named"): "card C1: a DELETE WHERE chain refuses with the extracted text",
    ("ansi", "f5_eq_string"): "a struct pair of different field types answers (question)",
    ("ansi", "f6_getitem_str"): "a map value by string key keeps the key name (question)",
    ("ansi", "f6_sub_str"): "a map value by string key keeps the key name (question)",
    ("ansi", "f6_amb_key"): "a map value by string key keeps the key name (question)",
    ("ansi", "f6_w_amb_key"): "a map value by string key keeps the key name (question)",
    ("ansi", "f6_dot_str"): "a dotted struct path string refuses on the DataFrame door",
    ("ansi", "f6_dot_str_arg"): "a dotted struct path string refuses on the DataFrame door",
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
    assert len(CELL_ROWS) == 240
    assert sum(row.get("door") == "dataframe" for row in CELL_ROWS) == 42
    assert {(row["conf"], row["name"]) for row in CELL_ROWS} >= set(OUTSIDE_UNIT)
    refusals = [row for row in CELL_ROWS if "error" in row["spark"]]
    assert sum(_error_class(row["spark"]["error"]) in IN_UNIT for row in refusals) == 81


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


def _struct_frame(door: ReparkSession) -> Any:
    return door.sql(
        "SELECT 1 AS id, 1 AS v, 'a' AS k, array(10, 20) AS l, map('a', 7) AS m, "
        "named_struct('x', 8, 'y', 9) AS s"
    )


def _col2() -> Any:
    return sf.struct(sf.lit(1).alias("col2"), sf.lit(2)).getField("col2")


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
    "f6_literals": lambda df: df.select(sf.struct(sf.lit(1), sf.lit(2)).alias("r")),
    "f6_mixed": lambda df: df.select(sf.struct("id", sf.lit(1)).alias("r")),
    "f6_cols": lambda df: df.select(sf.struct(sf.col("id"), "v").alias("r")),
    "f6_dfcol": lambda df: df.select(sf.struct(df["id"], df.v).alias("r")),
    "f6_alias": lambda df: df.select(sf.struct(sf.lit(1).alias("a"), sf.lit(2)).alias("r")),
    "f6_name": lambda df: df.select(sf.struct(sf.lit(1).name("z")).alias("r")),
    "f6_expr": lambda df: df.select(sf.struct(sf.col("id") + 1, sf.abs("v")).alias("r")),
    "f6_cast": lambda df: df.select(sf.struct(sf.col("id").cast("long")).alias("r")),
    "f6_qualified": lambda df: df.alias("t").select(sf.struct(sf.col("t.id")).alias("r")),
    "f6_getfield": lambda df: df.select(sf.struct(sf.col("s").getField("x")).alias("r")),
    "f6_getitem_str": lambda df: df.select(sf.struct(sf.col("m").getItem("a")).alias("r")),
    "f6_sub_str": lambda df: df.select(sf.struct(sf.col("m")["a"]).alias("r")),
    "f6_getitem_int": lambda df: df.select(sf.struct(sf.col("l").getItem(0)).alias("r")),
    "f6_getitem_col": lambda df: df.select(sf.struct(sf.col("m")[sf.col("k")]).alias("r")),
    "f6_dot_str": lambda df: df.select(sf.struct(sf.col("s.x")).alias("r")),
    "f6_dot_str_arg": lambda df: df.select(sf.struct("s.x").alias("r")),
    "f6_attr": lambda df: df.select(sf.struct(df.s.x).alias("r")),
    "f6_nested": lambda df: df.select(sf.struct(sf.struct(sf.lit(1))).alias("r")),
    "f6_getf": lambda df: df.select(sf.struct(sf.lit(1), sf.lit(2)).getField("col2")),
    "f6_getf_mixed": lambda df: df.select(sf.struct("id", sf.lit(5)).getField("col2")),
    "f6_amb": lambda df: df.select(_col2()),
    "f6_amb_cols": lambda df: df.select(sf.struct("id", "id").getField("id")),
    "f6_amb_key": lambda df: df.select(
        sf.struct(sf.col("m")["a"], sf.lit(2).alias("a")).getField("a")
    ),
    "f6_unaliased": lambda df: df.select(sf.struct(sf.lit(1), "id")),
    "f6_eq": lambda df: df.select(sf.struct("id", "v") == sf.struct(sf.lit(1), sf.lit(1))),
    "f6_w_amb": lambda df: df.select("id", _col2().alias("v")),
    "f6_w_ok": lambda df: df.select(
        "id", sf.struct(sf.lit(1), sf.lit(2)).getField("col2").alias("v")
    ),
    "f6_w_amb_key": lambda df: df.select(
        "id", sf.struct(sf.col("m")["a"], sf.lit(2).alias("a")).getField("a").alias("v")
    ),
}


@pytest.mark.parametrize(
    "cell",
    [
        row
        for row in CELL_ROWS
        if row.get("door") == "dataframe" and (row["conf"], row["name"]) not in OUTSIDE_UNIT
    ],
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
    source = _struct_frame(door) if cell["name"].startswith("f6_") else _array_frame(door)
    frame = DATAFRAME_CASES[cell["name"]](source)
    if "spark_readback" in cell:
        door.sql("CREATE NAMESPACE IF NOT EXISTS asf.ns").collect()
        for statement in FIXTURES[cell.get("fixture", "U")]:
            door.sql(statement.format(t="asf.ns.t")).collect()
        if "error" in want:
            with pytest.raises(Exception) as raised:
                frame.write.insertInto("asf.ns.t")
            _assert_refusal(want["error"], raised.value)
        else:
            frame.write.insertInto("asf.ns.t")
        assert _rows(door, "SELECT * FROM asf.ns.t") == cell["spark_readback"]
    elif "error" in want:
        with pytest.raises(Exception) as raised:
            _frame_rows(frame)
        _assert_refusal(want["error"], raised.value)
    else:
        assert _frame_rows(frame) == want["rows"]
