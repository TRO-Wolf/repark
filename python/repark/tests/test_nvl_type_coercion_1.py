from __future__ import annotations

import datetime
import json
import re
import time
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

import repark.spark.functions as functions
from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)

RAISES = (
    AnalysisException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)

DATA = json.loads(Path(__file__).with_name("test_nvl_type_coercion_1_spark.json").read_text())

BASE_SQL = (
    "SELECT DATE '2024-01-01' AS d, TIMESTAMP '2024-01-01 05:00:00' AS ts, "
    "CAST('2024-01-01 05:00:00' AS TIMESTAMP_NTZ) AS ntz, 1 AS i, 's' AS s, "
    "CAST(1.5 AS DECIMAL(10,2)) AS dec, 1.5 AS dbl, TRUE AS b, "
    "STRUCT(1 AS a, 'x' AS bb) AS st, ARRAY(1,2) AS arr"
)

FRAME_OPS = {
    "nvl_d_ts": functions.nvl("d", "ts"),
    "ifnull_d_ts": functions.ifnull("d", "ts"),
    "nvl_d_ntz": functions.nvl("d", "ntz"),
    "nvl_ts_ntz": functions.nvl("ts", "ntz"),
    "nvl_i_s": functions.nvl("i", "s"),
    "nvl_s_d": functions.nvl("s", "d"),
    "nvl_dec_i": functions.nvl("dec", "i"),
    "nvl2_d_ts": functions.nvl2("b", "d", "ts"),
    "nvl2_xnull": functions.nvl2(functions.lit(None), "d", "ts"),
    "nullif_d_ts": functions.nullif("d", "ts"),
    "nullif_i_dbl": functions.nullif("i", "dbl"),
    "coalesce_d_ts": functions.coalesce(functions.col("d"), functions.col("ts")),
    "zeroifnull_i": functions.zeroifnull("i"),
    "zeroifnull_dec": functions.zeroifnull("dec"),
    "zeroifnull_s": functions.zeroifnull("s"),
    "nullifzero_i": functions.nullifzero("i"),
    "nullifzero_s": functions.nullifzero("s"),
}

COLLECT_UTC = [cell for cell in DATA["collect"] if cell["session"] == "UTC"]
COLLECT_NY = [cell for cell in DATA["collect"] if cell["session"] == "NY"]
GUARD_UTC = [cell for cell in DATA["coalesce_guards"] if cell["session"] == "UTC"]
GUARD_NY = [cell for cell in DATA["coalesce_guards"] if cell["session"] == "NY"]


@pytest.fixture
def utc() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def ny() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-ny")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


@pytest.fixture
def kolkata() -> ReparkSession:
    return (
        ReparkSession.builder.appName("nvl-type-coercion-1-kolkata")
        .config("spark.sql.session.timeZone", "Asia/Kolkata")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


def check_select(session: ReparkSession, cell: dict[str, Any]) -> None:
    expect = cell["expect"]
    if "error" in expect:
        if expect["error"] is None:
            with pytest.raises(RAISES):
                session.sql(cell["sql"]).collect()
        else:
            with pytest.raises(RAISES, match=re.escape(expect["error"])):
                session.sql(cell["sql"]).collect()
        return
    rows = session.sql(cell["sql"]).collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    if "t" in expect:
        assert row["t"] == expect["t"]
    if "v" in expect:
        assert row["v"] == expect["v"]


def normalize(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {key: normalize(item) for key, item in value.asDict().items()}
    if isinstance(value, dict):
        return {str(key): normalize(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize(item) for item in value]
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, datetime.date):
        return value.isoformat()
    if isinstance(value, (Decimal, float, int)):
        return str(value)
    return value


def check_collect(session: ReparkSession, cell: dict[str, Any]) -> None:
    typed = session.sql(f"SELECT typeof({cell['expr']}) AS t").collect()
    assert typed[0].asDict()["t"] == cell["t"]
    rows = session.sql(f"SELECT {cell['expr']} AS v").collect()
    assert len(rows) == 1
    assert normalize(rows[0].asDict()["v"]) == cell["v"]


def check_facade(session: ReparkSession, cell: dict[str, Any]) -> None:
    base = session.sql(BASE_SQL)
    expect = cell["expect"]
    if "error" in expect:
        with pytest.raises(RAISES):
            base.select(FRAME_OPS[cell["op"]].alias("v")).selectExpr(
                "typeof(v) AS t", "CAST(v AS STRING) AS s"
            ).collect()
        return
    rows = (
        base.select(FRAME_OPS[cell["op"]].alias("v"))
        .selectExpr("typeof(v) AS t", "CAST(v AS STRING) AS s")
        .collect()
    )
    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["t"] == expect["t"]
    assert row["s"] == expect["s"]


def check_guard(session: ReparkSession, cell: dict[str, Any]) -> None:
    recorded = cell["repark"]
    if "error" in recorded:
        with pytest.raises(RAISES):
            session.sql(cell["sql"]).collect()
        return
    rows = session.sql(cell["sql"]).collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["t"] == recorded["t"]
    assert row["v"] == recorded["v"]


def test_nvl_date_timestamp_headline(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT typeof(nvl(DATE '2024-01-01', TIMESTAMP '2024-01-01 05:00:00')) AS t"
    ).collect()
    assert rows[0].asDict()["t"] == "timestamp"


@pytest.mark.parametrize("cell", DATA["select_utc"], ids=[c["key"] for c in DATA["select_utc"]])
def test_select_utc(utc: ReparkSession, cell: dict[str, Any]) -> None:
    check_select(utc, cell)


@pytest.mark.parametrize("cell", DATA["select_ny"], ids=[c["key"] for c in DATA["select_ny"]])
def test_select_ny(ny: ReparkSession, cell: dict[str, Any]) -> None:
    check_select(ny, cell)


@pytest.mark.parametrize("cell", COLLECT_UTC, ids=[c["key"] for c in COLLECT_UTC])
def test_collect_utc(utc: ReparkSession, cell: dict[str, Any]) -> None:
    check_collect(utc, cell)


@pytest.mark.parametrize("cell", COLLECT_NY, ids=[c["key"] for c in COLLECT_NY])
def test_collect_ny(ny: ReparkSession, cell: dict[str, Any]) -> None:
    check_collect(ny, cell)


@pytest.mark.parametrize("cell", DATA["facade"], ids=[c["op"] for c in DATA["facade"]])
def test_facade_typeof(utc: ReparkSession, cell: dict[str, Any]) -> None:
    check_facade(utc, cell)


@pytest.mark.parametrize("cell", DATA["view"], ids=[c["key"] for c in DATA["view"]])
def test_view_typeof(utc: ReparkSession, cell: dict[str, Any]) -> None:
    utc.sql(BASE_SQL).selectExpr(f"{cell['expr']} AS v").createOrReplaceTempView(
        f"vv_{cell['key'].split('/')[1]}"
    )
    rows = utc.sql(
        f"SELECT typeof(v) AS t, CAST(v AS STRING) AS s FROM vv_{cell['key'].split('/')[1]}"
    ).collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    assert row["t"] == cell["t"]
    assert row["s"] == cell["s"]


@pytest.mark.parametrize("cell", GUARD_UTC, ids=[c["key"] for c in GUARD_UTC])
def test_coalesce_guard_utc(utc: ReparkSession, cell: dict[str, Any]) -> None:
    check_guard(utc, cell)


@pytest.mark.parametrize("cell", GUARD_NY, ids=[c["key"] for c in GUARD_NY])
def test_coalesce_guard_ny(ny: ReparkSession, cell: dict[str, Any]) -> None:
    check_guard(ny, cell)


def test_insert_flow(tmp_path: Path) -> None:
    session = (
        ReparkSession.builder.appName("nvl-type-coercion-1-ins")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.warehouse.dir", str(tmp_path / "wh"))
        .getOrCreate()
    )
    try:
        session.sql("CREATE TABLE ts_col (x TIMESTAMP) USING parquet")
        session.sql("CREATE TABLE str_col (x STRING) USING parquet")
        for statement in DATA["insert"]["statements"]:
            if statement["raises"]:
                with pytest.raises(RAISES):
                    session.sql(statement["sql"]).collect()
            else:
                session.sql(statement["sql"]).collect()
        ts_rows = session.sql("SELECT CAST(x AS STRING) AS s FROM ts_col ORDER BY s").collect()
        assert [row.asDict() for row in ts_rows] == DATA["insert"]["ts_read"]
        str_rows = session.sql("SELECT x AS s FROM str_col ORDER BY s").collect()
        assert [row.asDict() for row in str_rows] == DATA["insert"]["str_read"]
    finally:
        session.stop()


@pytest.mark.parametrize("cell", DATA["divergence"], ids=[c["key"] for c in DATA["divergence"]])
def test_known_divergence(utc: ReparkSession, cell: dict[str, Any]) -> None:
    if cell["repark"] == "raises":
        with pytest.raises(RAISES):
            utc.sql(cell["sql"]).collect()
        return
    rows = utc.sql(cell["sql"]).collect()
    assert len(rows) == 1
    row = rows[0].asDict()
    assert row[cell["col"]] == cell["repark"]


VN2_MICROS_NY = 1704474000000000
VN2_MICROS_KOLKATA = 1704436200000000

VN2_MICROS_SQL = (
    "SELECT unix_micros(nvl(CAST(NULL AS TIMESTAMP), '2024-01-05 12:00:00')) AS m, "
    "unix_micros(nvl2(1, '2024-01-05 12:00:00', CAST(NULL AS TIMESTAMP))) AS n"
)

VN1_PROBE_VIEW = (
    "CREATE OR REPLACE TEMPORARY VIEW t AS SELECT * FROM VALUES "
    "(1, 1, 0, 'a', ' 7 ', 1.5D, CAST(1 AS BIGINT)), "
    "(2, NULL, 2, NULL, 'abc', -0.0D, NULL), "
    "(3, 3, 0, 'c', '1.5', CAST('NaN' AS DOUBLE), CAST(3 AS BIGINT)), "
    "(4, NULL, 5, 'd', '2024-01-05 12:00:00', 0.0D, NULL) "
    "AS t(id, x, y, s, txt, f, k)"
)

VN3_WIDENINGS = [
    ("CAST(NULL AS BIGINT)", "' 7 '", "bigint", "7"),
    ("CAST(NULL AS DOUBLE)", "' 1.5 '", "double", "1.5"),
    ("CAST(NULL AS DOUBLE)", "'1.5d'", "double", "1.5"),
    ("CAST(NULL AS DATE)", "' 2024-01-05 '", "date", "2024-01-05"),
    ("CAST(NULL AS DATE)", "'2024-01-05T00:00'", "date", "2024-01-05"),
    ("CAST(NULL AS DATE)", "'2024'", "date", "2024-01-01"),
    ("CAST(NULL AS TIMESTAMP)", "'2024-1-5 1:2:3'", "timestamp", "2024-01-05 01:02:03"),
]

VN3_IDS = ["bigint_ws", "dbl_ws", "dbl_dsuffix", "date_ws", "date_T", "date_year", "ts_short"]


def test_vn1_nvl_skips_divide_by_zero(utc: ReparkSession) -> None:
    rows = utc.sql("SELECT CAST(nvl(1, 1/0) AS STRING) AS v").collect()
    assert [row.asDict() for row in rows] == [{"v": "1.0"}]


def test_vn1_nvl2_skips_divide_by_zero(utc: ReparkSession) -> None:
    rows = utc.sql("SELECT CAST(nvl2(1, 2, 1/0) AS STRING) AS v").collect()
    assert [row.asDict() for row in rows] == [{"v": "2.0"}]


def test_vn1_nvl_skips_overflow_under_ansi(utc: ReparkSession) -> None:
    rows = utc.sql("SELECT CAST(nvl(1, 2147483647 + 1) AS STRING) AS v").collect()
    assert [row.asDict() for row in rows] == [{"v": "1"}]


def test_vn1_nvl_skips_failed_assert(utc: ReparkSession) -> None:
    rows = utc.sql("SELECT CAST(nvl(1, assert_true(false)) AS STRING) AS v").collect()
    assert [row.asDict() for row in rows] == [{"v": "1"}]


def test_vn1_nvl_column_form_skips_divide_by_zero(utc: ReparkSession) -> None:
    utc.sql(VN1_PROBE_VIEW).collect()
    rows = utc.sql(
        "SELECT id, CAST(nvl(x, 10 / y) AS STRING) AS v FROM t WHERE id IN (1,3) ORDER BY id"
    ).collect()
    assert [row.asDict() for row in rows] == [{"id": 1, "v": "1.0"}, {"id": 3, "v": "3.0"}]


def test_vn1_nvl_dataframe_form_skips_divide_by_zero(utc: ReparkSession) -> None:
    utc.sql(VN1_PROBE_VIEW).collect()
    fallback = functions.lit(10) / functions.col("y")
    rows = (
        utc.table("t")
        .where("id in (1,3)")
        .select("id", functions.nvl("x", fallback).cast("string").alias("v"))
        .orderBy("id")
        .collect()
    )
    assert [row.asDict() for row in rows] == [{"id": 1, "v": "1.0"}, {"id": 3, "v": "3.0"}]


def test_vn2_new_york_reads_session_zone(ny: ReparkSession) -> None:
    rows = ny.sql(VN2_MICROS_SQL).collect()
    assert [row.asDict() for row in rows] == [{"m": VN2_MICROS_NY, "n": VN2_MICROS_NY}]


def test_vn2_kolkata_reads_session_zone(kolkata: ReparkSession) -> None:
    rows = kolkata.sql(VN2_MICROS_SQL).collect()
    assert [row.asDict() for row in rows] == [{"m": VN2_MICROS_KOLKATA, "n": VN2_MICROS_KOLKATA}]


def test_vn2_iceberg_store_reads_session_zone(tmp_path: Path) -> None:
    session = (
        ReparkSession.builder.appName("nvl-type-coercion-1-vn2-ice")
        .config("spark.sql.session.timeZone", "America/New_York")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    try:
        session.register_memory_catalog("sc", tmp_path)
        session.sql("CREATE NAMESPACE sc.ns")
        session.sql("CREATE TABLE sc.ns.vn2 (id INT, ts TIMESTAMP) USING iceberg")
        session.sql(
            "INSERT INTO sc.ns.vn2 SELECT 1, nvl(CAST(NULL AS TIMESTAMP), '2024-01-05 12:00:00')"
        )
        rows = session.sql("SELECT unix_micros(ts) AS m, CAST(ts AS STRING) AS s FROM sc.ns.vn2")
        assert [row.asDict() for row in rows.collect()] == [
            {"m": VN2_MICROS_NY, "s": "2024-01-05 12:00:00"}
        ]
    finally:
        session.stop()


@pytest.mark.parametrize(("typed_null", "text", "want_t", "want_v"), VN3_WIDENINGS, ids=VN3_IDS)
def test_vn3_string_widens_like_spark(
    utc: ReparkSession, typed_null: str, text: str, want_t: str, want_v: str
) -> None:
    rows = utc.sql(
        f"SELECT typeof(nvl({typed_null}, {text})) AS t, "
        f"CAST(nvl({typed_null}, {text}) AS STRING) AS v"
    ).collect()
    assert [row.asDict() for row in rows] == [{"t": want_t, "v": want_v}]


def test_vn3_on_refuses_cast_invalid_input(utc: ReparkSession) -> None:
    with pytest.raises(RAISES, match="CAST_INVALID_INPUT"):
        utc.sql(
            "SELECT typeof(nvl(CAST(NULL AS BOOLEAN), 'on')) AS t, "
            "CAST(nvl(CAST(NULL AS BOOLEAN), 'on') AS STRING) AS v"
        ).collect()


def test_vn3_typeof_nvl_abc_int_is_bigint(utc: ReparkSession) -> None:
    rows = utc.sql("SELECT typeof(nvl('abc', 2)) AS t").collect()
    assert [row.asDict() for row in rows] == [{"t": "bigint"}]


VN3_RAND_CASE = "CASE WHEN rand() < 0.5 THEN NULL ELSE 1 END"
VN3_NULLABLE_FALLBACK = "CASE WHEN id < 0 THEN NULL ELSE 2 END"

VN3_CASTBACK = [
    (
        "CAST(1.23456789012345678 AS DECIMAL(38,17))",
        "1.5D",
        Decimal("1.23456789012345678"),
        "decimal(38,17)",
    ),
    ("9007199254740993L", "1.5D", 9007199254740993, "bigint"),
    ("'01'", "5", "01", "string"),
    ("'1.50'", "2.5D", "1.50", "string"),
    ("'TRUE'", "false", "TRUE", "string"),
    ("'2020-1-1'", "DATE '2021-01-01'", "2020-1-1", "string"),
    ("'2020-01-01'", "TIMESTAMP '2021-01-01 00:00:00'", "2020-01-01", "string"),
]

VN3_CASTBACK_IDS = [
    "decimal_double",
    "bigint_double",
    "str_int",
    "str_double",
    "str_bool",
    "str_date",
    "str_ts",
]


def check_vn3_no_null_balanced(rows: list[Any], one_value: Any, other_value: Any) -> None:
    assert len(rows) == 20000
    nulls = sum(1 for row in rows if row["v"] is None)
    assert nulls == 0
    ones = sum(1 for row in rows if row["v"] == one_value)
    assert 0.45 <= ones / len(rows) <= 0.55
    others = sum(1 for row in rows if row["v"] == other_value)
    assert ones + others == len(rows)


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
def test_vn3_select_distributes_once(utc: ReparkSession, fn: str) -> None:
    rows = utc.sql(
        f"SELECT {fn}({VN3_RAND_CASE}, {VN3_NULLABLE_FALLBACK}) AS v FROM range(20000)"
    ).collect()
    check_vn3_no_null_balanced(rows, 1, 2)


def test_vn3_zeroifnull_select_distributes_once(utc: ReparkSession) -> None:
    rows = utc.sql(f"SELECT zeroifnull({VN3_RAND_CASE}) AS v FROM range(20000)").collect()
    check_vn3_no_null_balanced(rows, 1, 0)


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
def test_vn3_where_sees_single_evaluation(utc: ReparkSession, fn: str) -> None:
    rows = utc.sql(
        f"SELECT count(*) AS c FROM range(20000) "
        f"WHERE {fn}({VN3_RAND_CASE}, {VN3_NULLABLE_FALLBACK}) = 1"
    ).collect()
    count = rows[0].asDict()["c"]
    assert 9000 <= count <= 11000


def test_vn3_zeroifnull_where_sees_single_evaluation(utc: ReparkSession) -> None:
    rows = utc.sql(
        f"SELECT count(*) AS c FROM range(20000) WHERE zeroifnull({VN3_RAND_CASE}) = 1"
    ).collect()
    count = rows[0].asDict()["c"]
    assert 9000 <= count <= 11000


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
def test_vn3_group_by_sees_single_evaluation(utc: ReparkSession, fn: str) -> None:
    rows = utc.sql(
        "SELECT v, count(*) AS c FROM "
        f"(SELECT {fn}({VN3_RAND_CASE}, {VN3_NULLABLE_FALLBACK}) AS v FROM range(20000)) "
        "GROUP BY v ORDER BY v"
    ).collect()
    tallies = {row["v"]: row["c"] for row in rows}
    assert set(tallies) == {1, 2}
    assert 9000 <= tallies[1] <= 11000
    assert 9000 <= tallies[2] <= 11000


def test_vn3_zeroifnull_group_by_sees_single_evaluation(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT v, count(*) AS c FROM "
        f"(SELECT zeroifnull({VN3_RAND_CASE}) AS v FROM range(20000)) "
        "GROUP BY v ORDER BY v"
    ).collect()
    tallies = {row["v"]: row["c"] for row in rows}
    assert set(tallies) == {0, 1}
    assert 9000 <= tallies[1] <= 11000
    assert 9000 <= tallies[0] <= 11000


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
def test_vn3_lag_sees_single_evaluation(utc: ReparkSession, fn: str) -> None:
    rows = utc.sql(
        "SELECT count(*) AS c FROM "
        f"(SELECT lag({fn}({VN3_RAND_CASE}, {VN3_NULLABLE_FALLBACK})) "
        "OVER (ORDER BY id) AS w FROM range(20000)) WHERE w IS NULL"
    ).collect()
    assert [row.asDict() for row in rows] == [{"c": 1}]


def test_vn3_zeroifnull_lag_sees_single_evaluation(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT count(*) AS c FROM "
        f"(SELECT lag(zeroifnull({VN3_RAND_CASE})) OVER (ORDER BY id) AS w "
        "FROM range(20000)) WHERE w IS NULL"
    ).collect()
    assert [row.asDict() for row in rows] == [{"c": 1}]


@pytest.mark.parametrize("fn", [functions.nvl, functions.ifnull], ids=["nvl", "ifnull"])
def test_vn3_dataframe_distributes_once(utc: ReparkSession, fn: Any) -> None:
    base = utc.sql("SELECT id FROM range(20000)")
    rand_case = functions.when(functions.rand() < 0.5, None).otherwise(1)
    fallback = functions.when(functions.col("id") < 0, None).otherwise(2)
    rows = base.select(fn(rand_case, fallback).alias("v")).collect()
    check_vn3_no_null_balanced(rows, 1, 2)


@pytest.mark.parametrize("fn", ["nvl", "ifnull"])
def test_vn3_literal_fallback_collects_without_raising(utc: ReparkSession, fn: str) -> None:
    rows = utc.sql(f"SELECT {fn}({VN3_RAND_CASE}, 2) AS v FROM range(20000)").collect()
    check_vn3_no_null_balanced(rows, 1, 2)


@pytest.mark.parametrize(
    ("first", "second", "want_v", "want_t"), VN3_CASTBACK, ids=VN3_CASTBACK_IDS
)
def test_vn3_nullif_volatile_returns_original(
    utc: ReparkSession, first: str, second: str, want_v: Any, want_t: str
) -> None:
    rows = utc.sql(f"SELECT nullif(CASE WHEN rand() < 2 THEN {first} END, {second}) AS v").collect()
    assert len(rows) == 1
    assert rows[0].asDict()["v"] == want_v
    typed = utc.sql(
        f"SELECT typeof(nullif(CASE WHEN rand() < 2 THEN {first} END, {second})) AS t"
    ).collect()
    assert typed[0].asDict()["t"] == want_t


def test_vn3_depth_12_widened_nests_plan_and_run_fast(utc: ReparkSession) -> None:
    widened = "x"
    doubled = "x"
    stringed = "s"
    zeroed = "x"
    for depth in range(12):
        widened = f"nullif({widened}, {100 + depth}L)"
        doubled = f"nullif({doubled}, {100 + depth}.5D)"
        stringed = f"nullif({stringed}, {100 + depth})"
        zeroed = f"nullifzero({zeroed})"
    queries = [
        f"SELECT id, {widened} AS v FROM (SELECT 1 AS id, 1 AS x)",
        f"SELECT id, {doubled} AS v FROM (SELECT 1 AS id, 1 AS x)",
        f"SELECT id, {stringed} AS v FROM (SELECT 1 AS id, '5' AS s)",
        f"SELECT id, {zeroed} AS v FROM (SELECT 1 AS id, 1 AS x)",
    ]
    for query in queries:
        utc.sql("EXPLAIN " + query).collect()
        started = time.perf_counter()
        utc.sql("EXPLAIN " + query).collect()
        assert time.perf_counter() - started < 2.0
        started = time.perf_counter()
        utc.sql(query).collect()
        assert time.perf_counter() - started < 2.0


def test_vn3_nvl_chain_shows_no_nvl_cast(utc: ReparkSession) -> None:
    rows = utc.sql("EXPLAIN SELECT nvl(nvl(nvl(id, 1), 2), 3) AS v FROM range(10)").collect()
    text = "\n".join(str(row.asDict()) for row in rows)
    assert "__repark_nvl_cast" not in text


def test_vn2_nullif_rand_evaluates_once(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT count(*) AS c FROM "
        "(SELECT nullif(CASE WHEN rand() < 0.5 THEN 1 ELSE 2 END, 1) AS v "
        "FROM range(20000)) WHERE v = 1"
    ).collect()
    assert [row.asDict() for row in rows] == [{"c": 0}]


def test_vn2_nullif_uuid_evaluates_once(utc: ReparkSession) -> None:
    rows = utc.sql(
        "SELECT count(*) AS c FROM "
        "(SELECT nullif(CASE WHEN substr(uuid(), 1, 1) < '8' THEN 1 ELSE 2 END, 1) AS v "
        "FROM range(20000)) WHERE v = 1"
    ).collect()
    assert [row.asDict() for row in rows] == [{"c": 0}]


def test_vn2_depth_12_nests_plan_fast(utc: ReparkSession) -> None:
    nested_nvl = "x"
    nested_nullif = "x"
    for depth in range(12):
        nested_nvl = f"nvl({nested_nvl},{depth})"
        nested_nullif = f"nullif({nested_nullif},{100 + depth})"
    queries = [
        f"SELECT id, {nested_nvl} AS v FROM (SELECT 1 AS id, 1 AS x)",
        f"SELECT id, {nested_nullif} AS v FROM (SELECT 1 AS id, 1 AS x)",
    ]
    for query in queries:
        utc.sql("EXPLAIN " + query).collect()
        started = time.perf_counter()
        utc.sql("EXPLAIN " + query).collect()
        assert time.perf_counter() - started < 1.0


def test_vn2_rollup_over_nvl(utc: ReparkSession) -> None:
    utc.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn2_rollup_t AS SELECT * FROM VALUES "
        "(1,'g1'),(2,'g1'),(3,'g2'),(4,'g2'),(5,NULL) AS t(id,g)"
    ).collect()
    rows = utc.sql(
        "SELECT nvl(g, 'z') AS gg, count(*) AS c FROM vn2_rollup_t GROUP BY ROLLUP(nvl(g, 'z'))"
    ).collect()
    assert {(row["gg"], row["c"]) for row in rows} == {
        ("g1", 2),
        ("g2", 2),
        ("z", 1),
        (None, 5),
    }


def test_vn2_cube_over_ifnull(utc: ReparkSession) -> None:
    utc.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn2_cube_t AS SELECT * FROM VALUES "
        "(1,'g1'),(2,'g1'),(3,'g2'),(4,'g2'),(5,NULL) AS t(id,g)"
    ).collect()
    rows = utc.sql(
        "SELECT ifnull(g, 'z') AS gg, count(*) AS c FROM vn2_cube_t GROUP BY CUBE(ifnull(g, 'z'))"
    ).collect()
    assert {(row["gg"], row["c"]) for row in rows} == {
        ("g1", 2),
        ("g2", 2),
        ("z", 1),
        (None, 5),
    }


def test_vn2_rollup_over_nullif(utc: ReparkSession) -> None:
    utc.sql(
        "CREATE OR REPLACE TEMPORARY VIEW vn2_rollup_n_t AS SELECT * FROM VALUES "
        "(1,'g1'),(2,'g1'),(3,'g2'),(4,'g2'),(5,NULL) AS t(id,g)"
    ).collect()
    rows = utc.sql(
        "SELECT nullif(g, 'g1') AS gg, count(*) AS c FROM vn2_rollup_n_t "
        "GROUP BY ROLLUP(nullif(g, 'g1'))"
    ).collect()
    assert {(row["gg"], row["c"]) for row in rows} == {
        ("g2", 2),
        (None, 3),
        (None, 5),
    }
