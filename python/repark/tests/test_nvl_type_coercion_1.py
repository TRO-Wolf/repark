from __future__ import annotations

import datetime
import json
import re
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
