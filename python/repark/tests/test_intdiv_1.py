from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark import functions as fn
from repark.errors import ParseException

ORACLE_PATH: Path = Path(__file__).with_name("intdiv_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]
_FRAMES: dict[str, dict[str, Any]] = _ORACLE["dataframe"]
_GROUPS: tuple[str, ...] = tuple(dict.fromkeys(key.rsplit("/", 1)[0] for key in _CELLS))


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("intdiv-1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse / "spark-warehouse"))
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )
    for statement in _ORACLE["setup"]:
        session.sql(statement).collect()
    return session


def _norm(value: Any) -> Any:
    if hasattr(value, "asDict"):
        return {key: _norm(item) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _answer(frame: Any, ordered: bool) -> dict[str, Any]:
    rows = [_norm(list(row)) for row in frame.collect()]
    return {
        "cols": [[field.name, field.dataType.simpleString()] for field in frame.schema.fields],
        "rows": rows if ordered else sorted(rows, key=repr),
    }


def _refusal(session: ReparkSession, sql: str) -> dict[str, Any]:
    try:
        session.sql(sql).collect()
    except Exception as error:
        condition = getattr(error, "getCondition", None)
        sql_state = getattr(error, "getSqlState", None)
        return {
            "condition": condition() if callable(condition) else None,
            "sql_state": sql_state() if callable(sql_state) else None,
            "message": str(error).splitlines()[0],
        }
    return {"answered": True}


def _mismatch(session: ReparkSession, key: str) -> str | None:
    cell = _CELLS[key]
    if cell.get("statement"):
        session.sql(cell["sql"]).collect()
        return None
    if "refusal_text_waived" in cell:
        got = _refusal(session, cell["sql"])
        return None if "answered" not in got else f"{key}: answered instead of refusing"
    if "condition" in cell:
        got = _refusal(session, cell["sql"])
        head_ok = cell["message_head"] in got.get("message", "")
        cond_ok = got.get("condition") == cell["condition"] or "condition_waived" in cell
        return None if head_ok and cond_ok else f"{key}: {got}"
    got = _answer(session.sql(cell["sql"]), bool(cell.get("ordered")))
    want = {"cols": cell["cols"], "rows": cell["rows"]}
    return None if got == want else f"{key}: {got} != {want}"


@pytest.mark.parametrize("group", _GROUPS)
def test_division_scopes_replay_spark(group: str, tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        keys = [key for key in _CELLS if key.rsplit("/", 1)[0] == group]
        misses = [miss for key in keys if (miss := _mismatch(session, key)) is not None]
    finally:
        session.stop()
    assert misses == []


def _frames(session: ReparkSession) -> dict[str, Any]:
    base = session.sql("SELECT id, id / 2 AS h FROM sc.ns.t")
    table = session.sql("SELECT id, i FROM sc.ns.t")
    int_half = session.sql("SELECT i / 2 AS h FROM sc.ns.t")
    return {
        "df/select_mul2": base.select((fn.col("h") * 2).alias("v")),
        "df/selectexpr_add1": base.selectExpr("h + 1 AS v", "typeof(h + 1) AS t"),
        "df/filter": base.filter("h + 1 = 1.5").select("id", "h"),
        "df/withcol": base.withColumn("v", fn.col("h") + 1).select("v"),
        "df/flat_int_add1": table.select((fn.col("i") / 2 + 1).alias("v")),
        "df/flat_big_mul2": table.select((fn.col("id") / 2 * 2).alias("v")),
        "df/int_sum": int_half.agg(fn.sum("h").alias("v")),
        "df/int_derived_add1": int_half.select((fn.col("h") + 1).alias("v")),
    }


def test_div_operator_refusal_pinned(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        with pytest.raises(ParseException, match="No infix parser"):
            session.sql("SELECT id div 2 AS v, typeof(id div 2) AS t FROM sc.ns.t").collect()
    finally:
        session.stop()


def test_dataframe_door_replays_spark(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        frames = _frames(session)
        got = {key: _answer(frame, False) for key, frame in frames.items()}
    finally:
        session.stop()
    assert got == _FRAMES
