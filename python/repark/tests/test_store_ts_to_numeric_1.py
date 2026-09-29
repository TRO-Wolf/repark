from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from repark import ReparkSession

ORACLE_PATH: Path = Path(__file__).with_name("store_ts_to_numeric_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
_CELLS: dict[str, dict[str, Any]] = _ORACLE["cells"]


def _open(warehouse: Path) -> ReparkSession:
    session = (
        ReparkSession.builder.appName("store-ts-to-numeric-1")
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
    if isinstance(value, (list, tuple)):
        return [_norm(item) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _write(session: ReparkSession, door: str, cell: dict[str, Any]) -> dict[str, Any]:
    try:
        if door == "df_append":
            session.sql(cell["source"]).writeTo(cell["table"]).append()
        elif door == "df_insertinto":
            session.sql(cell["source"]).write.insertInto(cell["table"])
        else:
            session.sql(cell["sql"]).collect()
    except Exception as error:
        condition = getattr(error, "getCondition", None)
        sql_state = getattr(error, "getSqlState", None)
        return {
            "refused": True,
            "condition": condition() if callable(condition) else None,
            "sql_state": sql_state() if callable(sql_state) else None,
            "message": str(error).splitlines()[0],
        }
    return {"refused": False}


def _mismatch(session: ReparkSession, key: str) -> str | None:
    cell = _CELLS[key]
    got = _write(session, key.split("/", 1)[0], cell)
    rows = sorted(
        [_norm(list(row)) for row in session.sql(cell["readback"]).collect()],
        key=repr,
    )
    if "reset" in cell:
        session.sql(cell["reset"]).collect()
    if got["refused"] != cell["refused"]:
        return f"{key}: {got} but Spark refused={cell['refused']}"
    if cell["refused"] and (
        got["condition"] != cell["condition"]
        or got["sql_state"] != cell["sql_state"]
        or not got["message"].endswith(cell["message"])
    ):
        return f"{key}: {got} != {cell['message']}"
    if rows != cell["rows"]:
        return f"{key}: rows {rows} != {cell['rows']}"
    return None


def test_store_assignment_cells_replay_spark(tmp_path: Path) -> None:
    session = _open(tmp_path)
    try:
        misses = [miss for key in _CELLS if (miss := _mismatch(session, key)) is not None]
    finally:
        session.stop()
    assert misses == []
