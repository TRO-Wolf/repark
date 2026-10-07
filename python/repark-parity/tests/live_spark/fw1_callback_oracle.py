"""FOREACH-WRAP-1 oracle: how Spark 4.1.2 raises a user callback's exception."""

from __future__ import annotations

import json
import os
import sys
import tempfile
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from importlib.util import find_spec
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

SECRET = "S3cr3tPw"
URL = "http://u:" + SECRET + "@h/x"
MESSAGE = "fetch failed for " + URL
OUT_ENV = "FW1_OUT"
WAREHOUSE_ENV = "FW1_WAREHOUSE"
EXPECTED_CELLS = 3
HEAD_MARKER = ": Job aborted"


def raise_on_row(row: Any) -> None:
    raise ValueError(MESSAGE)


def raise_on_partition(rows: Iterator[Any]) -> None:
    raise ValueError(MESSAGE)


def raise_on_frame(frame: DataFrame) -> DataFrame:
    raise ValueError(MESSAGE)


def define_cells(frame: DataFrame) -> dict[str, Callable[[], object]]:
    return {
        "FW1-foreach": lambda: frame.foreach(raise_on_row),
        "FW1-foreachPartition": lambda: frame.foreachPartition(raise_on_partition),
        "FW1-transform": lambda: frame.transform(raise_on_frame),
    }


def call_or_none(target: Any, name: str) -> Any:
    call = getattr(target, name, None)
    if call is None:
        return None
    try:
        return call()
    except Exception:
        return None


def qualified(value: object) -> str:
    return type(value).__module__ + "." + type(value).__qualname__


def java_chain(error: BaseException) -> list[str]:
    chain: list[str] = []
    java_error = getattr(error, "java_exception", None)
    while java_error is not None and len(chain) < 8:
        chain.append(str(java_error.getClass().getName()))
        java_error = java_error.getCause()
    return chain


def python_chain(error: BaseException) -> list[str]:
    chain: list[str] = []
    link = error.__cause__
    while link is not None and len(chain) < 8:
        chain.append(qualified(link))
        link = link.__cause__
    return chain


def user_lines(text: str) -> list[str]:
    return [line.replace(SECRET, "$SECRET") for line in text.splitlines() if SECRET in line]


def message_head(text: str) -> str:
    cut = text.find(HEAD_MARKER)
    head = text if cut < 0 else text[:cut]
    return head.replace(SECRET, "$SECRET")


def error_of(error: BaseException) -> dict[str, Any]:
    java_error = getattr(error, "java_exception", None)
    targets = (error,) if java_error is None else (error, java_error)
    error_class: Any = None
    for target in targets:
        if error_class is None:
            error_class = call_or_none(target, "getErrorClass")
        if error_class is None:
            error_class = call_or_none(target, "getCondition")
    text = str(error)
    return {
        "class": qualified(error),
        "context": None if error.__context__ is None else qualified(error.__context__),
        "error_class": error_class,
        "has_get_error_class": hasattr(error, "getErrorClass"),
        "is_original": isinstance(error, ValueError) and text == MESSAGE,
        "java_chain": java_chain(error),
        "message_head": message_head(text),
        "mro": [klass.__name__ for klass in type(error).__mro__],
        "python_cause_chain": python_chain(error),
        "secret_in_repr": SECRET in repr(error),
        "secret_in_str": SECRET in text,
        "user_lines": sorted(set(user_lines(text))),
    }


def record_all(frame: DataFrame) -> dict[str, dict[str, Any]]:
    recorded: dict[str, dict[str, Any]] = {}
    for cell_id, action in define_cells(frame).items():
        try:
            action()
        except Exception as error:
            entry: dict[str, Any] = {"cell": cell_id, "error": error_of(error)}
        else:
            entry = {"cell": cell_id, "error": None}
        recorded[cell_id] = entry
        print(json.dumps(entry, sort_keys=True), flush=True)
    return recorded


def build_session(warehouse: Path) -> SparkSession:
    from pyspark.sql import SparkSession

    os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"
    return (
        SparkSession.builder.master("local[1]")
        .appName("fw1-callback-oracle")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def fresh_dir() -> Path:
    configured = os.environ.get(WAREHOUSE_ENV)
    path = Path(configured) if configured else Path(tempfile.mkdtemp(prefix="fw1-warehouse-"))
    path.mkdir(parents=True, exist_ok=True)
    if any(path.iterdir()):
        raise SystemExit(WAREHOUSE_ENV + "=" + str(path) + " must be empty")
    return path.resolve()


def main() -> int:
    if find_spec("pyspark") is None:
        print("SKIP fw1_callback_oracle: pyspark is not installed", file=sys.stderr, flush=True)
        return 0
    import pyspark

    warehouse = fresh_dir()
    out = Path(os.environ.get(OUT_ENV) or Path(__file__).with_suffix(".json"))
    spark = build_session(warehouse)
    spark.sparkContext.setLogLevel("OFF")
    try:
        frame = spark.createDataFrame([(1, 1.0), (2, 2.0)], ["k", "v"]).coalesce(1)
        recorded = record_all(frame)
        document = {
            "cells": list(recorded.values()),
            "date": datetime.now(UTC).date().isoformat(),
            "master": "local[1]",
            "message": "fetch failed for http://u:$SECRET@h/x",
            "spark": pyspark.__version__,
        }
    finally:
        spark.stop()
    if len(recorded) != EXPECTED_CELLS:
        raise SystemExit("expected " + str(EXPECTED_CELLS) + " cells, got " + str(len(recorded)))
    out.write_text(json.dumps(document, indent=1, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
