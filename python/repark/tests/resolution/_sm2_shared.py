from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from repark import ReparkSession
from repark.spark import functions as spark_functions
from repark.spark.dataframe import DataFrame


def _open(warehouse: Path, app: str) -> ReparkSession:
    return (
        ReparkSession.builder.appName(app)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.catalog.sc.type", "hadoop")
        .config("spark.sql.catalog.sc.warehouse", str(warehouse / "sc"))
        .getOrCreate()
    )


def _self_join(session: ReparkSession) -> DataFrame:
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    return left.alias("l").join(
        left.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id")
    )


def _mixed_join(session: ReparkSession) -> DataFrame:
    left = session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "s", "v"])
    right = session.createDataFrame([(1, "x"), (3, "y")], ["id", "t"])
    return left.alias("l").join(
        right.alias("r"), spark_functions.col("l.id") == spark_functions.col("r.id")
    )


def _refusal_of(action: Callable[[], Any]) -> Exception:
    try:
        action()
    except Exception as refused:
        return refused
    raise AssertionError("expected the action to refuse")


def _condition_of(error: Exception) -> str:
    getter = getattr(error, "getCondition", None)
    if callable(getter):
        try:
            return str(getter() or "")
        except Exception:
            return ""
    return ""


def _sql_state_of(error: Exception) -> str:
    getter = getattr(error, "getSqlState", None)
    if callable(getter):
        try:
            return str(getter() or "")
        except Exception:
            return ""
    return ""


def _expected_dup_message(name: str) -> str:
    return (
        f"[COLUMN_ALREADY_EXISTS] The column `{name}` already exists. "
        "Choose another name or rename the existing column. SQLSTATE: 42711"
    )


def _assert_no_twin_bytes(root: Path) -> None:
    for path in sorted(root.rglob("*")):
        if path.is_file():
            content = path.read_bytes()
            assert b"__repark_" not in content, str(path)
