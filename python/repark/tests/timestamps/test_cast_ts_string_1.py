"""CAST-TS-STRING-1 — ``CAST(<string> AS TIMESTAMP)`` answers Spark 4.1.2 on every door.

Every cell reads its expected answer from ``cast_ts_string_1_spark_oracle.json``: 151
strings x session zones ``UTC`` / ``America/New_York`` x ANSI off / on, each with the
``unix_micros`` of ``CAST``, ``TRY_CAST`` and ``to_timestamp``, or Spark's error condition
and message. The pins run each cell on the facade ``spark.sql`` door as a literal and as
a temp-view column, on ``Column.cast`` and ``Column.try_cast``, and on ``to_timestamp``.
The Arrow path carries ``timestamp[us, tz=UTC]``. A time-only string resolves against
today's date in its zone, so those cells check the local date and time of day instead of
a fixed instant.

The native ``repark.sql`` door has no Spark ``TIMESTAMP`` (LTZ) shape: its ``TIMESTAMP``
is the ANSI zoneless type, so it carries no cell here.

The live tier re-derives every cell from live Spark and asserts the committed fixture
still matches.

pins: cast-ts-string-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
"""

from __future__ import annotations

import datetime
import json
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from _record_cast_ts_string_1 import comparable, record, sql_literal, zone_info

from repark import ReparkSession

_HERE = Path(__file__).resolve().parent
_ORACLE: dict[str, Any] = json.loads(
    (_HERE / "cast_ts_string_1_spark_oracle.json").read_text(encoding="utf-8")
)
_CELLS: list[dict[str, Any]] = _ORACLE["cells"]
_GROUPS: tuple[tuple[str, str], ...] = (
    ("UTC", "false"),
    ("UTC", "true"),
    ("America/New_York", "false"),
    ("America/New_York", "true"),
)
_LTZ = pa.timestamp("us", tz="UTC")


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    """A fresh facade session; each test sets the zone and the ANSI mode it needs."""
    session = ReparkSession.builder.appName("cast-ts-string-1").getOrCreate()
    yield session


def _configure(session: ReparkSession, zone: str, ansi: str) -> None:
    """Set the session zone and the ANSI mode for one cell group."""
    session.conf.set("spark.sql.session.timeZone", zone)
    session.conf.set("spark.sql.ansi.enabled", ansi)


def _group(zone: str, ansi: str) -> list[dict[str, Any]]:
    """Return the fixture cells of one zone and ANSI mode, in recording order."""
    return [cell for cell in _CELLS if cell["zone"] == zone and cell["ansi"] == ansi]


def _today_matches(micros: int, wall: dict[str, str]) -> bool:
    """Check a time-only answer against today's date in its zone and the recorded time."""
    instant = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC) + datetime.timedelta(
        microseconds=micros
    )
    local = instant.astimezone(zone_info(wall["zone"]))
    now = datetime.datetime.now(tz=zone_info(wall["zone"])).date()
    days = {now, now - datetime.timedelta(days=1)}
    return local.date() in days and local.strftime("%H:%M:%S.%f") == wall["time"]


def _answer_matches(cell: dict[str, Any], expected: dict[str, Any], actual: Any) -> bool:
    """Compare one RePark answer (micros, ``None`` or an exception) with an oracle entry."""
    if "error" in expected:
        return isinstance(actual, Exception) and expected["message"] in str(actual)
    if isinstance(actual, Exception):
        return False
    if expected["us"] is not None and "today_wall" in cell:
        return actual is not None and _today_matches(actual, cell["today_wall"])
    return actual == expected["us"]


def _run_scalar(session: ReparkSession, sql: str) -> Any:
    """Run a one-row, one-column query; return the value or the raised exception."""
    try:
        table = session.sql(sql).to_arrow()
    except Exception as error:
        return error
    return table.column(0).to_pylist()[0]


def _mismatches(results: list[tuple[dict[str, Any], dict[str, Any], Any]]) -> list[Any]:
    """Collect the cells whose RePark answer differs from the oracle."""
    return [
        (cell["s"], expected, actual if not isinstance(actual, Exception) else str(actual)[:160])
        for cell, expected, actual in results
        if not _answer_matches(cell, expected, actual)
    ]


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_sql_literal_cast_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """A literal ``CAST(s AS TIMESTAMP)`` answers every oracle cell.

    pins: cast-ts-string-1/C-001, C-002, C-003
    """
    _configure(spark, zone, ansi)
    results = [
        (
            cell,
            cell["cast"],
            _run_scalar(spark, f"SELECT unix_micros(CAST({sql_literal(cell['s'])} AS TIMESTAMP))"),
        )
        for cell in _group(zone, ansi)
    ]
    assert _mismatches(results) == []


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_sql_literal_try_cast_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """A literal ``TRY_CAST`` answers NULL wherever Spark's cast fails.

    pins: cast-ts-string-1/C-004
    """
    _configure(spark, zone, ansi)
    results = [
        (
            cell,
            cell["try_cast"],
            _run_scalar(
                spark, f"SELECT unix_micros(TRY_CAST({sql_literal(cell['s'])} AS TIMESTAMP))"
            ),
        )
        for cell in _group(zone, ansi)
    ]
    assert _mismatches(results) == []


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_sql_literal_to_timestamp_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """One-argument ``to_timestamp`` is Spark's cast. pins: cast-ts-string-1/C-005"""
    _configure(spark, zone, ansi)
    results = [
        (
            cell,
            cell["to_timestamp"],
            _run_scalar(spark, f"SELECT unix_micros(to_timestamp({sql_literal(cell['s'])}))"),
        )
        for cell in _group(zone, ansi)
    ]
    assert _mismatches(results) == []


def _frame(spark: ReparkSession, cells: list[dict[str, Any]]) -> Any:
    """Build a two-column frame (row index, string) over the given cells."""
    return spark.createDataFrame(
        [(index, cell["s"]) for index, cell in enumerate(cells)], "i INT, s STRING"
    )


def _column_answers(table: pa.Table) -> list[Any]:
    """Read the cast column of an ``(i, t)`` Arrow table as micros in row-index order."""
    assert table.schema.field("t").type == _LTZ
    ordered = table.sort_by("i")
    return ordered.column("t").cast(pa.int64()).to_pylist()


def _column_results(
    spark: ReparkSession, zone: str, ansi: str, key: str, build: Any
) -> list[tuple[dict[str, Any], dict[str, Any], Any]]:
    """Run one column-path spelling over a group: answering cells in one frame, errors alone."""
    _configure(spark, zone, ansi)
    cells = _group(zone, ansi)
    answering = [cell for cell in cells if "error" not in cell[key]]
    failing = [cell for cell in cells if "error" in cell[key]]
    answers = _column_answers(build(spark, _frame(spark, answering)).to_arrow())
    results = [(cell, cell[key], answer) for cell, answer in zip(answering, answers, strict=True)]
    for cell in failing:
        try:
            build(spark, _frame(spark, [cell])).to_arrow()
        except Exception as error:
            results.append((cell, cell[key], error))
            continue
        results.append((cell, cell[key], "answered"))
    return results


def _column_cast(spark: ReparkSession, frame: Any) -> Any:
    """``Column.cast("timestamp")`` over the string column."""
    return frame.select("i", frame.s.cast("timestamp").alias("t"))


def _column_try_cast(spark: ReparkSession, frame: Any) -> Any:
    """``Column.try_cast("timestamp")`` over the string column."""
    return frame.select("i", frame.s.try_cast("timestamp").alias("t"))


def _view_cast(spark: ReparkSession, frame: Any) -> Any:
    """``CAST(s AS TIMESTAMP)`` in ``spark.sql`` over a temp view column."""
    frame.createOrReplaceTempView("cast_ts_string_1_view")
    return spark.sql("SELECT i, CAST(s AS TIMESTAMP) AS t FROM cast_ts_string_1_view")


def _view_try_cast(spark: ReparkSession, frame: Any) -> Any:
    """``TRY_CAST(s AS TIMESTAMP)`` in ``spark.sql`` over a temp view column."""
    frame.createOrReplaceTempView("cast_ts_string_1_view")
    return spark.sql("SELECT i, TRY_CAST(s AS TIMESTAMP) AS t FROM cast_ts_string_1_view")


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_column_cast_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """``Column.cast("timestamp")`` answers every cell as ``timestamp[us, UTC]``.

    pins: cast-ts-string-1/C-006
    """
    assert _mismatches(_column_results(spark, zone, ansi, "cast", _column_cast)) == []


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_column_try_cast_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """``Column.try_cast("timestamp")`` answers NULL where Spark fails.

    pins: cast-ts-string-1/C-004, C-006
    """
    assert _mismatches(_column_results(spark, zone, ansi, "try_cast", _column_try_cast)) == []


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_sql_view_column_cast_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """``CAST`` over a view column runs the same kernel as a literal.

    pins: cast-ts-string-1/C-001, C-006
    """
    assert _mismatches(_column_results(spark, zone, ansi, "cast", _view_cast)) == []


@pytest.mark.parametrize("zone,ansi", _GROUPS)
def test_sql_view_column_try_cast_matches_spark(spark: ReparkSession, zone: str, ansi: str) -> None:
    """``TRY_CAST`` over a view column answers NULL where Spark fails.

    pins: cast-ts-string-1/C-004, C-006
    """
    assert _mismatches(_column_results(spark, zone, ansi, "try_cast", _view_try_cast)) == []


@pytest.mark.parametrize("spelling", ["CAST", "TRY_CAST"])
def test_sql_literal_cast_is_microsecond_utc(spark: ReparkSession, spelling: str) -> None:
    """The literal casts produce Spark's ``TIMESTAMP`` Arrow type. pins: cast-ts-string-1/C-006"""
    _configure(spark, "America/New_York", "true")
    table = spark.sql(f"SELECT {spelling}('2999-01-01' AS TIMESTAMP) AS t").to_arrow()
    assert table.schema.field("t").type == _LTZ
    assert table.column("t").cast(pa.int64()).to_pylist() == [32472162000000000]


@pytest.mark.skipif(
    os.environ.get("REPARK_PARITY_LIVE") != "1",
    reason="REPARK_PARITY_LIVE != 1 — live Spark cell skipped (routine CI is JVM-free)",
)
def test_live_oracle_matches_committed_fixture(spark_engine: Any) -> None:
    """Live Spark 4.1.2 re-derives every fixture cell (drift detector).

    pins: cast-ts-string-1/C-007
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
