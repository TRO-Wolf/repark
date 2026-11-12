from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from repark import ReparkSession
from repark.spark import functions
from repark.spark.session.session_state import _promote_active

pytestmark = pytest.mark.slow

GROUP = "test_listview_inner_join_key_1"

CELLS_PATH = Path(__file__).parent / "data" / "listview_inner_join_key_1_cells.jsonl"
CATALOG = "lvjk_cat"
TABLE_PROPERTIES = (
    "'format-version'='2', 'write.delete.mode'='copy-on-write', "
    "'write.update.mode'='copy-on-write', 'write.merge.mode'='copy-on-write'"
)
USING_PAIR = "SELECT lid, rid FROM lv_l JOIN lv_r USING (k)"
SQL_FORMS = {
    "sql_using": USING_PAIR,
    "using_pair": USING_PAIR,
    "using_star": "SELECT * FROM lv_l JOIN lv_r USING (k)",
    "using_alias": "SELECT lid, rid FROM lv_l l JOIN lv_r r USING (k)",
    "using_left": "SELECT lid, rid FROM lv_l LEFT JOIN lv_r USING (k)",
    "natural": "SELECT lid, rid FROM lv_l NATURAL JOIN (SELECT rid, k FROM lv_r) r",
}
ROUTE_TYPES = {"LV_I64": pa.list_view(pa.int64()), "LLV_I64": pa.large_list_view(pa.int64())}
LEFT_ONLY = ("left_semi", "left_anti")


def load() -> tuple[dict[str, Any], list[dict[str, Any]]]:
    lines = CELLS_PATH.read_text(encoding="utf-8").splitlines()
    return json.loads(lines[0]), [json.loads(line) for line in lines[1:]]


HEADER, CELLS = load()
BIG_CELLS = [cell for cell in CELLS if cell["views"].get("ds") == "big"]
FAST_CELLS = [cell for cell in CELLS if cell not in BIG_CELLS]


def norm(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "asDict"):
        value = value.asDict()
    if isinstance(value, dict):
        return [norm(value[key]) for key in sorted(value)]
    if isinstance(value, (list, tuple)):
        return [norm(item) for item in value]
    return value


def outcome(collect: Callable[[], list[Any]]) -> dict[str, Any]:
    try:
        rows = collect()
    except BaseException as error:
        kind = "panic" if type(error).__name__ == "PanicException" else "error"
        return {"kind": kind, "message": str(error)}
    lines = sorted(json.dumps(norm(row), sort_keys=True, default=str) for row in rows)
    digest = hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]
    return {"kind": "rows", "n": len(lines), "sha": digest, "sample": lines[:4]}


def side_sql(kind: str, rows: list[list[Any]], left: bool) -> str:
    letters = HEADER["letters"][kind]
    if left:
        selects = (
            f"SELECT CAST({lid} AS BIGINT) AS lid, {letters[letter]} AS k, "
            f"CAST({idv} AS BIGINT) AS id, CAST({j} AS INT) AS j"
            for lid, letter, idv, j in rows
        )
    else:
        selects = (
            f"SELECT CAST({rid} AS BIGINT) AS rid, {letters[letter]} AS k, "
            f"CAST({j} AS INT) AS j, '{rv}' AS rv"
            for rid, letter, j, rv in rows
        )
    return " UNION ALL ".join(selects)


def kind_views(spark: Any, kind: str, dataset: str) -> None:
    left, right = HEADER["datasets"][dataset]
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW lv_l AS {side_sql(kind, left, True)}").collect()
    spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW lv_r AS {side_sql(kind, right, False)}").collect()


def route_array(kind: str, values: list[Any]) -> Any:
    if kind == "REE_I64":
        return pc.run_end_encode(pa.array(values, pa.int64()))
    if kind == "UNION_I64":
        type_ids = pa.array([0] * len(values), pa.int8())
        return pa.UnionArray.from_sparse(type_ids, [pa.array(values, pa.int64())], ["a"])
    return pa.array(values, ROUTE_TYPES[kind])


def route_views(spark: Any, kind: str) -> None:
    left, right = HEADER["datasets"]["base"]
    values = HEADER["routes"][kind]
    left_key = pandas.Series(pandas.arrays.ArrowExtensionArray(route_array(kind, values["l"])))
    right_key = pandas.Series(pandas.arrays.ArrowExtensionArray(route_array(kind, values["r"])))
    left_frame = pandas.DataFrame(
        {
            "lid": [row[0] for row in left],
            "k": left_key,
            "id": [row[2] for row in left],
            "j": [row[3] for row in left],
        }
    )
    right_frame = pandas.DataFrame(
        {
            "rid": [row[0] for row in right],
            "k": right_key,
            "j": [row[2] for row in right],
            "rv": [row[3] for row in right],
        }
    )
    spark.createDataFrame(left_frame).createOrReplaceTempView("lv_l")
    spark.createDataFrame(right_frame).createOrReplaceTempView("lv_r")


def key_frame(spark: Any, form: str, how: str) -> Any:
    left = spark.table("lv_l")
    right = spark.table("lv_r")
    columns = ["lid"] if how in LEFT_ONLY else ["lid", "rid"]
    if form == "df_str":
        return left.join(right, "k", how).select(*columns)
    if form == "df_list":
        return left.join(right, ["k"], how).select(*columns)
    if form == "df_two":
        return left.join(right, ["k", "j"], how).select(*columns)
    if form in ("df_expr", "df_expr_and"):
        condition = functions.col("l.k") == functions.col("r.k")
        if form == "df_expr_and":
            condition = condition & (functions.col("l.id") > 0)
        return left.alias("l").join(right.alias("r"), condition, how).select(*columns)
    if form == "agg":
        joined = left.join(right, "k", how)
        return joined.agg(
            functions.count(functions.lit(1)).alias("c"), functions.sum("lid").alias("s")
        )
    if form == "lateral_str":
        return left.lateralJoin(right, "k", how).select(*columns)
    if form == "lateral_list":
        return left.lateralJoin(right, ["k"], how).select(*columns)
    return spark.sql(SQL_FORMS[form])


def written_rows(spark: Any, form: str, table: str) -> list[Any]:
    spark.sql(f"DROP TABLE IF EXISTS {table}").collect()
    spark.sql(
        f"CREATE TABLE {table} (lid BIGINT, rid BIGINT) USING iceberg "
        f"TBLPROPERTIES ({TABLE_PROPERTIES})"
    ).collect()
    if form == "ins":
        spark.sql(
            f"INSERT INTO {table} SELECT l.lid, r.rid FROM lv_l l JOIN lv_r r ON l.k = r.k"
        ).collect()
    else:
        joined = spark.table("lv_l").join(spark.table("lv_r"), "k", "inner")
        joined.select("lid", "rid").writeTo(table).append()
    return spark.sql(f"SELECT lid, rid FROM {table}").collect()


def view_key(cell: dict[str, Any]) -> tuple[str, ...]:
    views = cell["views"]
    if "family" in views:
        return ("family",)
    if "route" in views:
        return ("route", views["kind"])
    return ("kind", views["kind"], views["ds"])


def build_views(spark: Any, key: tuple[str, ...]) -> None:
    if key[0] == "family":
        for name, statement in HEADER["family"]:
            spark.sql(f"CREATE OR REPLACE TEMPORARY VIEW {name} AS {statement}").collect()
    elif key[0] == "route":
        route_views(spark, key[1])
    else:
        kind_views(spark, key[1], key[2])


def cell_outcome(spark: Any, cell: dict[str, Any], index: int) -> dict[str, Any]:
    form, how = cell["form"], cell["how"]
    if form in ("wt", "ins"):
        table = f"{CATALOG}.lv.t{index}"
        return outcome(lambda: written_rows(spark, form, table))
    if form == "join_str":
        views = cell["views"]
        left, right = spark.table(views["family"]), spark.table(views["right"])
        return outcome(lambda: left.join(right, "s", how).select("lid", "rid").collect())
    return outcome(lambda: key_frame(spark, form, how).collect())


def reached_type(spark: Any) -> str:
    try:
        return str(spark.table("lv_l").toArrow().schema.field("k").type)
    except BaseException as error:
        return f"{type(error).__name__}: {error}"


def own_session(previous: Any) -> Any:
    if previous is None:
        return ReparkSession.builder.appName("listview-inner-join-key-1").getOrCreate()
    return previous.newSession()


def measure(spark: Any) -> dict[str, Any]:
    cells: dict[str, Any] = {}
    reached: dict[str, str] = {}
    groups: dict[tuple[str, ...], list[tuple[int, dict[str, Any]]]] = {}
    for index, cell in enumerate(FAST_CELLS):
        groups.setdefault(view_key(cell), []).append((index, cell))
    for key, members in groups.items():
        build_views(spark, key)
        if key[0] == "route":
            reached[f"pandas:{key[1]}"] = reached_type(spark)
        elif key[0] == "kind" and key[2] == "base":
            reached[key[1]] = reached_type(spark)
        for index, cell in members:
            cells[cell["id"]] = cell_outcome(spark, cell, index)
    return {"cells": cells, "reached": reached}


def big_outcome(cell: dict[str, Any]) -> dict[str, Any]:
    previous = ReparkSession.getActiveSession()
    spark = own_session(previous)
    assert spark is not previous
    try:
        build_views(spark, view_key(cell))
        return cell_outcome(spark, cell, 0)
    finally:
        spark.stop()
        if previous is not None:
            _promote_active(previous)


@pytest.fixture(scope="module")
def measured(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    previous = ReparkSession.getActiveSession()
    spark = own_session(previous)
    assert spark is not previous
    try:
        spark.register_memory_catalog(CATALOG, str(tmp_path_factory.mktemp("warehouse")))
        spark.sql(f"CREATE NAMESPACE {CATALOG}.lv").collect()
        return measure(spark)
    finally:
        spark.stop()
        if previous is not None:
            _promote_active(previous)


def assert_matches(got: dict[str, Any], expect: dict[str, Any]) -> None:
    assert got["kind"] != "panic", got
    if expect["kind"] == "rows":
        assert got["kind"] == "rows", got
        assert (got["n"], got["sha"]) == (expect["n"], expect["sha"]), got
    else:
        assert got["kind"] == "error", got
        assert f"[{expect['class']}]" in got["message"], got


@pytest.mark.xdist_group(GROUP)
def test_the_cells_file_carries_the_tables_dataframe_and_using_cells() -> None:
    sections: dict[str, int] = {}
    for cell in CELLS:
        section = "|".join(cell["id"].split("|")[:2])
        sections[section] = sections.get(section, 0) + 1
    assert len(CELLS) == 283
    assert len(BIG_CELLS) == 13
    assert sections == {
        "s0|join": 169,
        "s0|agg": 13,
        "s0|dml": 14,
        "i1|variant": 32,
        "i1|lateral": 17,
        "i1|route": 4,
        "i1|mixed": 24,
        "i1|using": 10,
    }


@pytest.mark.xdist_group(GROUP)
@pytest.mark.parametrize("kind", sorted(HEADER["reached"]))
def test_each_key_reaches_the_measured_arrow_type(measured: dict[str, Any], kind: str) -> None:
    assert measured["reached"][kind] == HEADER["reached"][kind]


@pytest.mark.xdist_group(GROUP)
@pytest.mark.parametrize("cell", FAST_CELLS, ids=[cell["id"] for cell in FAST_CELLS])
def test_cell_matches_its_oracle(measured: dict[str, Any], cell: dict[str, Any]) -> None:
    assert_matches(measured["cells"][cell["id"]], cell["expect"])


@pytest.mark.parametrize(
    "cell",
    [
        pytest.param(
            cell,
            id=cell["id"],
            marks=pytest.mark.xdist_group(f"{GROUP}_big_{cell['views']['kind']}"),
        )
        for cell in BIG_CELLS
    ],
)
def test_big_dataset_cell_matches_its_oracle(cell: dict[str, Any]) -> None:
    assert_matches(big_outcome(cell), cell["expect"])
