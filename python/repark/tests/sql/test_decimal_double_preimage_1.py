from __future__ import annotations

import itertools
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as sf
from repark.spark.session import _reset_active_session_for_tests

K2 = "__repark_decimal_to_double__"
MODES = {
    "cow": (
        "'format-version' = '2', 'write.delete.mode' = 'copy-on-write', "
        "'write.update.mode' = 'copy-on-write', 'write.merge.mode' = 'copy-on-write'"
    ),
    "mor": (
        "'format-version' = '2', 'write.delete.mode' = 'merge-on-read', "
        "'write.update.mode' = 'merge-on-read', 'write.merge.mode' = 'merge-on-read'"
    ),
}
TOP38 = "99999999999999999999.999999999999999999"
ROWS = [
    (1, "0", "0"),
    (2, "0.01", "0.000000000000000001"),
    (3, "-0.01", "-0.000000000000000001"),
    (4, "0.50", "0.5"),
    (5, "1.47", "1.000000000000000100"),
    (6, "99999999.99", TOP38),
    (7, "-99999999.99", "-" + TOP38),
    (8, None, None),
    (9, "4.50", "4.5"),
    (10, "-1.47", "1.0000000000000004"),
]
CONSTANTS = {
    "half": "0.5",
    "exact": {"d10": "1.47", "d38": "1.0000000000000004"},
    "between": {"d10": "0.005", "d38": "2.5E-19"},
    "between_neg": {"d10": "-0.005", "d38": "-2.5E-19"},
    "pzero": "0.0",
    "nzero": "-0.0",
    "nan": "NaN",
    "pinf": "Infinity",
    "ninf": "-Infinity",
    "top": {"d10": "99999999.99", "d38": "1.0E20"},
    "above": {"d10": "1.0E9", "d38": "1.0E21"},
    "below": {"d10": "-1.0E9", "d38": "-1.0E21"},
}
PER_ROW = {"nan", "pinf", "ninf", "top", "above"}
NULL_TEST_KEEPS_K2 = {"pzero", "nzero"}
OPS = {
    "eq": "=",
    "ne": "<>",
    "lt": "<",
    "le": "<=",
    "gt": ">",
    "ge": ">=",
    "dist": "IS DISTINCT FROM",
    "ns": "<=>",
}
LISTS = {
    "in3": "{c} IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE))",
    "in4": (
        "{c} IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE), "
        "CAST('0.005' AS DOUBLE))"
    ),
    "in3_zero": "{c} IN (CAST('-0.0' AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(0.005 AS DOUBLE))",
    "notin3": "{c} NOT IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE))",
    "notin4": (
        "{c} NOT IN (CAST(0.5 AS DOUBLE), CAST(4.5 AS DOUBLE), CAST(-0.01 AS DOUBLE), "
        "CAST('0.005' AS DOUBLE))"
    ),
    "between": "{c} BETWEEN CAST(0.005 AS DOUBLE) AND CAST(4.5 AS DOUBLE)",
    "between_zero": "{c} BETWEEN CAST('-0.0' AS DOUBLE) AND CAST(0.5 AS DOUBLE)",
    "not_between": "{c} NOT BETWEEN CAST(-0.005 AS DOUBLE) AND CAST(0.5 AS DOUBLE)",
}
LISTS_KEEPING_K2 = {"in4", "notin4"}
FRAMES = {
    "df_gt": lambda f: f.filter(sf.col("d10") > 4.5),
    "df_eq": lambda f: f.filter(sf.col("d10") == 4.5),
    "df_lt_half": lambda f: f.filter(sf.col("d10") < 0.5),
    "df_d38_ge": lambda f: f.filter(sf.col("d38") >= 0.5),
    "df_d38_eq_nzero": lambda f: f.filter(sf.col("d38") == -0.0),
}
DML = {
    "delete": "DELETE FROM {t} WHERE {c} = {k}",
    "update": "UPDATE {t} SET id = id + 100 WHERE {c} = {k}",
    "merge_on": (
        "MERGE INTO {t} t USING (SELECT 1 AS sid) s ON t.{c} = {k} "
        "WHEN MATCHED THEN UPDATE SET id = t.id + 100"
    ),
}
DML_CONSTANTS = {
    "half": "CAST(0.5 AS DOUBLE)",
    "nzero": "CAST('-0.0' AS DOUBLE)",
    "between": "CAST(0.005 AS DOUBLE)",
}
SPARK_TRUTH = {
    "d10/half": (
        "FFFTFFFNFF FFFTFFFNFF TTTFTTTNTT TTTFTTTNTT TTTFFFTNFT FFFFTTFNTF TTTTFFTNFT FFFTTTFNTF "
        "FFFFTTFNTF TTTFFFTNFT FFFTTTFNTF TTTTFFTNFT TTTFTTTTTT TTTFTTTTTT FFFTFFFFFF FFFTFFFFFF"
    ),
    "d10/exact": (
        "FFFFTFFNFF FFFFTFFNFF TTTTFTTNTT TTTTFTTNTT TTTTFFTNFT FFFFFTFNTF TTTTTFTNFT FFFFTTFNTF "
        "FFFFFTFNTF TTTTFFTNFT FFFFTTFNTF TTTTTFTNFT TTTTFTTTTT TTTTFTTTTT FFFFTFFFFF FFFFTFFFFF"
    ),
    "d10/between": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TFTFFFTNFT FTFTTTFNTF TFTFFFTNFT FTFTTTFNTF "
        "FTFTTTFNTF TFTFFFTNFT FTFTTTFNTF TFTFFFTNFT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d10/between_neg": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT FFTFFFTNFT TTFTTTFNTF FFTFFFTNFT TTFTTTFNTF "
        "TTFTTTFNTF FFTFFFTNFT TTFTTTFNTF FFTFFFTNFT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d10/pzero": (
        "TFFFFFFNFF TFFFFFFNFF FTTTTTTNTT FTTTTTTNTT FFTFFFTNFT FTFTTTFNTF TFTFFFTNFT TTFTTTFNTF "
        "FTFTTTFNTF FFTFFFTNFT TTFTTTFNTF TFTFFFTNFT FTTTTTTTTT FTTTTTTTTT TFFFFFFFFF TFFFFFFFFF"
    ),
    "d10/nzero": (
        "TFFFFFFNFF TFFFFFFNFF FTTTTTTNTT FTTTTTTNTT FFTFFFTNFT FTFTTTFNTF TFTFFFTNFT TTFTTTFNTF "
        "FTFTTTFNTF FFTFFFTNFT TTFTTTFNTF TFTFFFTNFT FTTTTTTTTT FTTTTTTTTT TFFFFFFFFF TFFFFFFFFF"
    ),
    "d10/nan": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF "
        "FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d10/pinf": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF "
        "FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d10/ninf": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT "
        "TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d10/top": (
        "FFFFFTFNFF FFFFFTFNFF TTTTTFTNTT TTTTTFTNTT TTTTTFTNTT FFFFFFFNFF TTTTTTTNTT FFFFFTFNFF "
        "FFFFFFFNFF TTTTTFTNTT FFFFFTFNFF TTTTTTTNTT TTTTTFTTTT TTTTTFTTTT FFFFFTFFFF FFFFFTFFFF"
    ),
    "d10/above": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF "
        "FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d10/below": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT "
        "TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d38/half": (
        "FFFTFFFNFF FFFTFFFNFF TTTFTTTNTT TTTFTTTNTT TTTFFFTNFF FFFFTTFNTT TTTTFFTNFF FFFTTTFNTT "
        "FFFFTTFNTT TTTFFFTNFF FFFTTTFNTT TTTTFFTNFF TTTFTTTTTT TTTFTTTTTT FFFTFFFFFF FFFTFFFFFF"
    ),
    "d38/exact": (
        "FFFFFFFNFT FFFFFFFNFT TTTTTTTNTF TTTTTTTNTF TTTTTFTNFF FFFFFTFNTF TTTTTFTNFT FFFFFTFNTT "
        "FFFFFTFNTF TTTTTFTNFF FFFFFTFNTT TTTTTFTNFT TTTTTTTTTF TTTTTTTTTF FFFFFFFFFT FFFFFFFFFT"
    ),
    "d38/between": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TFTFFFTNFF FTFTTTFNTT TFTFFFTNFF FTFTTTFNTT "
        "FTFTTTFNTT TFTFFFTNFF FTFTTTFNTT TFTFFFTNFF TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d38/between_neg": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT FFTFFFTNFF TTFTTTFNTT FFTFFFTNFF TTFTTTFNTT "
        "TTFTTTFNTT FFTFFFTNFF TTFTTTFNTT FFTFFFTNFF TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d38/pzero": (
        "TFFFFFFNFF TFFFFFFNFF FTTTTTTNTT FTTTTTTNTT FFTFFFTNFF FTFTTTFNTT TFTFFFTNFF TTFTTTFNTT "
        "FTFTTTFNTT FFTFFFTNFF TTFTTTFNTT TFTFFFTNFF FTTTTTTTTT FTTTTTTTTT TFFFFFFFFF TFFFFFFFFF"
    ),
    "d38/nzero": (
        "TFFFFFFNFF TFFFFFFNFF FTTTTTTNTT FTTTTTTNTT FFTFFFTNFF FTFTTTFNTT TFTFFFTNFF TTFTTTFNTT "
        "FTFTTTFNTT FFTFFFTNFF TTFTTTFNTT TFTFFFTNFF FTTTTTTTTT FTTTTTTTTT TFFFFFFFFF TFFFFFFFFF"
    ),
    "d38/nan": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF "
        "FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d38/pinf": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF "
        "FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d38/ninf": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT "
        "TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d38/top": (
        "FFFFFTFNFF FFFFFTFNFF TTTTTFTNTT TTTTTFTNTT TTTTTFTNTT FFFFFFFNFF TTTTTTTNTT FFFFFTFNFF "
        "FFFFFFFNFF TTTTTFTNTT FFFFFTFNFF TTTTTTTNTT TTTTTFTTTT TTTTTFTTTT FFFFFTFFFF FFFFFTFFFF"
    ),
    "d38/above": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF "
        "FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
    "d38/below": (
        "FFFFFFFNFF FFFFFFFNFF TTTTTTTNTT TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT "
        "TTTTTTTNTT FFFFFFFNFF TTTTTTTNTT FFFFFFFNFF TTTTTTTTTT TTTTTTTTTT FFFFFFFFFF FFFFFFFFFF"
    ),
}
SPARK_LISTS = {
    "frame/df_gt": [6],
    "frame/df_eq": [9],
    "frame/df_lt_half": [1, 2, 3, 7, 10],
    "frame/df_d38_ge": [4, 5, 6, 9, 10],
    "frame/df_d38_eq_nzero": [1],
    "list/d10/in3": [3, 4, 9],
    "list/d10/in4": [3, 4, 9],
    "list/d10/in3_zero": [1, 9],
    "list/d10/notin3": [1, 2, 5, 6, 7, 10],
    "list/d10/notin4": [1, 2, 5, 6, 7, 10],
    "list/d10/between": [2, 4, 5, 9],
    "list/d10/between_zero": [1, 2, 4],
    "list/d10/not_between": [3, 5, 6, 7, 9, 10],
    "list/d38/in3": [4, 9],
    "list/d38/in4": [4, 9],
    "list/d38/in3_zero": [1, 9],
    "list/d38/notin3": [1, 2, 3, 5, 6, 7, 10],
    "list/d38/notin4": [1, 2, 3, 5, 6, 7, 10],
    "list/d38/between": [4, 5, 9, 10],
    "list/d38/between_zero": [1, 2, 4],
    "list/d38/not_between": [5, 6, 7, 9, 10],
}
SPARK_DML = {
    "cow/d10/delete/half": [1, 2, 3, 5, 6, 7, 8, 9, 10],
    "cow/d10/delete/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10],
    "cow/d10/delete/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "cow/d10/update/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "cow/d10/update/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "cow/d10/update/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "cow/d10/merge_on/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "cow/d10/merge_on/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "cow/d10/merge_on/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "cow/d38/delete/half": [1, 2, 3, 5, 6, 7, 8, 9, 10],
    "cow/d38/delete/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10],
    "cow/d38/delete/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "cow/d38/update/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "cow/d38/update/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "cow/d38/update/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "cow/d38/merge_on/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "cow/d38/merge_on/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "cow/d38/merge_on/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d10/delete/half": [1, 2, 3, 5, 6, 7, 8, 9, 10],
    "mor/d10/delete/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d10/delete/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d10/update/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "mor/d10/update/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "mor/d10/update/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d10/merge_on/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "mor/d10/merge_on/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "mor/d10/merge_on/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d38/delete/half": [1, 2, 3, 5, 6, 7, 8, 9, 10],
    "mor/d38/delete/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d38/delete/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d38/update/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "mor/d38/update/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "mor/d38/update/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    "mor/d38/merge_on/half": [1, 2, 3, 5, 6, 7, 8, 9, 10, 104],
    "mor/d38/merge_on/nzero": [2, 3, 4, 5, 6, 7, 8, 9, 10, 101],
    "mor/d38/merge_on/between": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
}
COUNTER = itertools.count()


@pytest.fixture(params=["true", "false"])
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    _reset_active_session_for_tests()
    session = (
        ReparkSession.builder.appName("test-decimal-double-preimage-1")
        .config("spark.sql.ansi.enabled", request.param)
        .getOrCreate()
    )
    session.register_memory_catalog("ice", str(tmp_path / "wh"))
    session.sql("CREATE NAMESPACE ice.ns").collect()
    yield session
    session.stop()
    _reset_active_session_for_tests()


def decimal(value: str | None, column: str) -> str:
    if value is None:
        return "NULL"
    return f"CAST('{value}' AS DECIMAL({'10,2' if column == 'd10' else '38,18'}))"


def make_table(spark: Any, mode: str = "cow") -> str:
    name = f"ice.ns.e{next(COUNTER)}"
    spark.sql(
        f"CREATE TABLE {name} (id INT, d10 DECIMAL(10,2), d38 DECIMAL(38,18)) USING iceberg "
        f"TBLPROPERTIES ({MODES[mode]})"
    ).collect()
    rows = ", ".join(f"({i}, {decimal(a, 'd10')}, {decimal(b, 'd38')})" for i, a, b in ROWS)
    spark.sql(f"INSERT INTO {name} VALUES {rows}").collect()
    return name


def constant_of(name: str, column: str) -> str:
    raw = CONSTANTS[name]
    return raw[column] if isinstance(raw, dict) else raw


def predicates(column: str, other: str) -> dict[str, str]:
    out = {}
    for name, op in OPS.items():
        out[f"{name}/r"] = f"{column} {op} {other}"
        if name != "dist":
            out[f"{name}/l"] = f"{other} {op} {column}"
    return out


def truth(cell: str) -> dict[str, str]:
    words = SPARK_TRUTH[cell].split()
    keys = [f"{name}/{side}" for name in OPS for side in ("r", "l")]
    return dict(zip(keys, words, strict=True))


def letters(rows: list[Any]) -> str:
    return "".join("N" if r[1] is None else ("T" if r[1] else "F") for r in rows)


def ids(spark: Any, sql: str) -> list[int]:
    return sorted(int(r[0]) for r in spark.sql(sql).collect())


def plan_of(spark: Any, sql: str) -> str:
    return " ".join(str(r[-1]) for r in spark.sql("EXPLAIN " + sql).collect())


@pytest.mark.parametrize("cell", sorted(SPARK_TRUTH))
def test_a_decimal_against_a_double_constant_answers_as_spark(spark: Any, cell: str) -> None:
    column, name = cell.split("/")
    table = make_table(spark)
    literal = f"CAST('{constant_of(name, column)}' AS DOUBLE)"
    held_table = f"ice.ns.k{next(COUNTER)}"
    spark.sql(f"CREATE TABLE {held_table} (c DOUBLE) USING iceberg").collect()
    spark.sql(f"INSERT INTO {held_table} VALUES ({literal})").collect()
    want = truth(cell)
    folded = predicates(column, literal)
    held = predicates(f"e.{column}", "h.c")
    projected = ", ".join(f"{p} AS `{k}`" for k, p in folded.items())
    rows = sorted(spark.sql(f"SELECT id, {projected} FROM {table}").collect(), key=lambda r: r[0])
    held_projected = ", ".join(f"{p} AS `{k}`" for k, p in held.items())
    held_rows = sorted(
        spark.sql(f"SELECT e.id, {held_projected} FROM {table} e, {held_table} h").collect(),
        key=lambda r: r[0],
    )
    for index, key in enumerate(folded, start=1):
        got = letters([(r[0], r[index]) for r in rows])
        assert got == want[key], f"{cell} {key}: {got}"
        assert letters([(r[0], r[index]) for r in held_rows]) == got, f"{cell} {key} held"
        where = ids(spark, f"SELECT id FROM {table} WHERE {folded[key]}")
        assert where == [i + 1 for i, c in enumerate(want[key]) if c == "T"], f"{cell} {key}"


@pytest.mark.parametrize("cell", sorted(SPARK_TRUTH))
def test_a_bounded_double_constant_plans_no_per_row_conversion(spark: Any, cell: str) -> None:
    column, name = cell.split("/")
    table = make_table(spark)
    literal = f"CAST('{constant_of(name, column)}' AS DOUBLE)"
    for key, predicate in predicates(column, literal).items():
        plan = plan_of(spark, f"SELECT id FROM {table} WHERE {predicate}")
        kept = name in PER_ROW or (name in NULL_TEST_KEEPS_K2 and key[:2] in {"di", "ns"})
        assert (K2 in plan) == kept, f"{cell} {key}: {plan}"


@pytest.mark.parametrize("cell", sorted(SPARK_LISTS))
def test_lists_ranges_and_frames_answer_as_spark(spark: Any, cell: str) -> None:
    table = make_table(spark)
    family, *rest = cell.split("/")
    if family == "frame":
        frame = FRAMES[rest[0]](spark.table(table))
        assert sorted(int(r[0]) for r in frame.collect()) == SPARK_LISTS[cell]
        return
    column, name = rest
    sql = f"SELECT id FROM {table} WHERE {LISTS[name].format(c=column)}"
    assert ids(spark, sql) == SPARK_LISTS[cell]


@pytest.mark.parametrize("name", sorted(LISTS))
def test_short_lists_and_ranges_plan_no_per_row_conversion(spark: Any, name: str) -> None:
    table = make_table(spark)
    for column in ("d10", "d38"):
        sql = f"SELECT id FROM {table} WHERE {LISTS[name].format(c=column)}"
        assert (K2 in plan_of(spark, sql)) == (name in LISTS_KEEPING_K2), sql


def test_a_dataframe_filter_against_a_python_float_plans_no_per_row_conversion(
    spark: Any, capsys: pytest.CaptureFixture[str]
) -> None:
    table = make_table(spark)
    capsys.readouterr()
    spark.table(table).filter(sf.col("d10") > 4.5).explain(True)
    assert K2 not in capsys.readouterr().out


@pytest.mark.parametrize("cell", sorted(SPARK_DML))
def test_dml_against_a_double_constant_answers_as_spark(spark: Any, cell: str) -> None:
    mode, column, kind, name = cell.split("/")
    table = make_table(spark, mode)
    statement = DML[kind].format(t=table, c=column, k=DML_CONSTANTS[name])
    spark.sql(statement).collect()
    assert ids(spark, f"SELECT id FROM {table}") == SPARK_DML[cell]


@pytest.mark.parametrize("mode", sorted(MODES))
@pytest.mark.parametrize("kind", ["delete", "update"])
def test_a_delete_or_update_against_a_double_constant_plans_no_per_row_conversion(
    spark: Any, mode: str, kind: str
) -> None:
    table = make_table(spark, mode)
    for column in ("d10", "d38"):
        for constant in DML_CONSTANTS.values():
            statement = DML[kind].format(t=table, c=column, k=constant)
            assert K2 not in plan_of(spark, statement), statement
