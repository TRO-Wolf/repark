"""SQL-EPOCH-CONSTRUCTORS-1 — ``timestamp_seconds``/``millis``/``micros`` equal Spark 4.1.2.

Every cell reads its expected answer from ``sql_epoch_constructors_1_spark_oracle.json``:
76 inputs x session zones ``UTC`` / ``America/New_York`` x ANSI off / on, each with the
``unix_micros`` value or Spark's error condition and message on the SQL door and the
DataFrame door, plus the ``timestamp`` result type. The pins run each cell on the facade
``spark.sql`` door as a literal and through ``F.timestamp_*`` over a column, on the Arrow
path (``timestamp[us, tz=UTC]``, value and type). The native ``repark.sql`` door carries no
Spark built-ins, so it keeps refusing the three names. The live tier re-derives every cell
from live Spark and asserts the committed fixture still matches.

pins: sql-epoch-constructors-1/C-002, C-003, C-004, C-005, C-006, C-007, C-009
"""

from __future__ import annotations

import json
import os
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_sql_epoch_constructors_1 import comparable, record

from repark import ReparkSession
from repark import sql as native_sql
from repark.errors import AnalysisException, PySparkException
from repark.spark import functions as spark_functions

_HERE = Path(__file__).resolve().parent
_ORACLE: dict[str, Any] = json.loads(
    (_HERE / "sql_epoch_constructors_1_spark_oracle.json").read_text(encoding="utf-8")
)
_CELLS: list[dict[str, Any]] = _ORACLE["cells"]
_FUNCTIONS: tuple[str, ...] = ("timestamp_seconds", "timestamp_millis", "timestamp_micros")
_GROUPS: tuple[tuple[str, str], ...] = (
    ("UTC", "false"),
    ("UTC", "true"),
    ("America/New_York", "false"),
    ("America/New_York", "true"),
)
_LTZ = pa.timestamp("us", tz="UTC")
_GOT_TYPE = re.compile(r'has the type "([^"]+)"')


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A fresh facade session; each test sets the zone and the ANSI mode it needs."""
    session = ReparkSession.builder.appName("sql-epoch-constructors-1").getOrCreate()
    yield session


def _configure(session: ReparkSession, zone: str, ansi: str) -> None:
    """Set the session zone and the ANSI mode for one cell group."""
    session.conf.set("spark.sql.session.timeZone", zone)
    session.conf.set("spark.sql.ansi.enabled", ansi)


def _group(zone: str, ansi: str) -> list[dict[str, Any]]:
    """Return the fixture cells of one zone and ANSI mode, in recording order."""
    return [cell for cell in _CELLS if cell["zone"] == zone and cell["ansi"] == ansi]


def _run_leg(session: ReparkSession, door: str, function: str, expression: str) -> Any:
    """Run one epoch-constructor leg; return the frame or the raised exception."""
    try:
        if door == "sql":
            return session.sql(f"SELECT {function}({expression}) AS v")
        frame = session.sql(f"SELECT {expression} AS v")
        return frame.select(getattr(spark_functions, function)("v").alias("v"))
    except Exception as error:
        return error


def _frame_answer(frame: Any) -> tuple[Any, Any, Any]:
    """Read one answered frame as its micros value, Arrow type and facade type."""
    table = frame.to_arrow()
    arrow_type = table.schema.field("v").type
    if pa.types.is_timestamp(arrow_type):
        value = table.column("v").cast(pa.int64()).to_pylist()[0]
    else:
        value = table.column("v").to_pylist()[0]
    facade_type = frame.schema["v"].dataType.simpleString()
    return value, arrow_type, facade_type


def _refusal_matches(function: str, expected: dict[str, Any], actual: Any) -> bool:
    """Check one RePark refusal against the recorded Spark error."""
    if not isinstance(actual, Exception):
        return False
    if expected.get("error") == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE":
        if type(actual) is not AnalysisException:
            return False
        if (
            actual.getCondition() != expected["error"]
            or actual.getSqlState() != expected["sqlstate"]
        ):
            return False
        required = "NUMERIC" if function == "timestamp_seconds" else "INTEGRAL"
        if f'The first parameter requires the "{required}" type' not in str(actual):
            return False
        match = _GOT_TYPE.search(expected["message"])
        if match is None:
            return False
        return f'has the type "{match.group(1)}"' in str(actual)
    if type(actual) is not PySparkException:
        return False
    if actual.getCondition() or actual.getSqlState():
        return False
    return expected["message"] in str(actual)


def _leg_mismatches(session: ReparkSession, zone: str, ansi: str, door: str, key: str) -> list[Any]:
    """Collect the legs of one door whose answer differs from the oracle."""
    mismatches: list[Any] = []
    for cell in _group(zone, ansi):
        for function in _FUNCTIONS:
            expected = cell[function][key]
            actual = _run_leg(session, door, function, cell["expr"])
            if "us" in expected:
                if isinstance(actual, Exception):
                    mismatches.append((cell["input"], function, str(actual)[:160]))
                    continue
                try:
                    value, arrow_type, facade_type = _frame_answer(actual)
                except Exception as error:
                    mismatches.append((cell["input"], function, str(error)[:160]))
                    continue
                if value != expected["us"] or arrow_type != _LTZ or facade_type != "timestamp":
                    mismatches.append((cell["input"], function, value, str(arrow_type)))
                continue
            if not isinstance(actual, Exception):
                try:
                    _frame_answer(actual)
                except Exception as error:
                    actual = error
            if not _refusal_matches(function, expected, actual):
                rendered = actual if not isinstance(actual, Exception) else str(actual)[:160]
                mismatches.append((cell["input"], function, rendered))
    return mismatches


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_sql_door_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """The Spark SQL door answers every oracle cell, value and refusal.

    pins: sql-epoch-constructors-1/C-002, C-003, C-009
    """
    _configure(spark, zone, ansi)
    assert _leg_mismatches(spark, zone, ansi, "sql", "sql") == []


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_dataframe_door_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """The DataFrame door answers every oracle cell, value and refusal.

    pins: sql-epoch-constructors-1/C-004, C-005, C-009
    """
    _configure(spark, zone, ansi)
    assert _leg_mismatches(spark, zone, ansi, "df", "df") == []


@pytest.mark.parametrize("function", _FUNCTIONS)
def test_native_door_refuses_the_spark_spellings(spark: ReparkSession, function: str) -> None:
    """The native door carries no Spark built-ins and refuses the three names.

    pins: sql-epoch-constructors-1/C-006
    """
    _configure(spark, "UTC", "false")
    with pytest.raises(Exception, match="UNRESOLVED_ROUTINE") as caught:
        native_sql(f"SELECT {function}(1) AS v").to_arrow()
    assert type(caught.value) is AnalysisException


@pytest.mark.skipif(
    os.environ.get("REPARK_PARITY_LIVE") != "1",
    reason="REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)",
)
def test_live_oracle_matches_committed_fixture(spark_engine: Any) -> None:
    """Live Spark 4.1.2 re-derives every fixture cell (drift detector).

    pins: sql-epoch-constructors-1/C-007
    """
    session = spark_engine.session
    zone = session.conf.get("spark.sql.session.timeZone")
    ansi = session.conf.get("spark.sql.ansi.enabled")
    try:
        live = record(session)
    finally:
        session.conf.set("spark.sql.session.timeZone", zone)
        session.conf.set("spark.sql.ansi.enabled", ansi)
    assert comparable(live) == comparable(_ORACLE)
