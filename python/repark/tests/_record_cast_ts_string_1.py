"""Re-derive the CAST-TS-STRING-1 Spark oracle from live PySpark 4.1.2.

Every string in ``STRINGS`` is cast to ``TIMESTAMP`` under the session zones ``UTC`` and
``America/New_York`` with ``spark.sql.ansi.enabled`` false and true. Each cell records
``unix_micros(CAST(s AS TIMESTAMP))``, ``unix_micros(TRY_CAST(s AS TIMESTAMP))`` and
``unix_micros(to_timestamp(s))``, or the error condition and the first message line.
Only ``unix_micros`` crosses into Python, so no PySpark ``datetime`` conversion can fail
on a year outside Python's range. The string goes in as a named parameter, so no SQL
escaping touches it.

A time-only string resolves against today's date in its zone, so its cell also carries
``today_wall``: the zone Spark used and the local time of day. The pins check the local
date against the test day instead of a fixed instant.

Run with a PySpark 4.1.2 interpreter::

    SPARK_LOCAL_IP=127.0.0.1 python python/repark/tests/_record_cast_ts_string_1.py

``--check`` compares a fresh recording against the committed fixture and exits non-zero
on drift. Not collected by pytest.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_HERE = Path(__file__).resolve().parent
FIXTURE = _HERE / "cast_ts_string_1_spark_oracle.json"
ZONES: tuple[str, ...] = ("UTC", "America/New_York")
ANSI_MODES: tuple[str, ...] = ("false", "true")

MEASURED_STRINGS: tuple[str, ...] = (
    "2020",
    "2020-06",
    "2020-6",
    "2020-06-01",
    "2020-6-1",
    "2020-06-01 10:00",
    "2020-06-01 1:2",
    "2020-06-01 1:2:3",
    "2020-06-01T10:00:00",
    "2020-06-01 10:00:00.123456",
    "2020-06-01 10:00:00.123456789",
    "2020-06-01 10:00:00.1",
    "2020-06-01 10:00:00Z",
    "2020-06-01T10:00:00Z",
    "2020-06-01 10:00:00+02:00",
    "2020-06-01 10:00:00+0200",
    "2020-06-01 10:00:00 UTC",
    "2020-06-01 10:00:00 America/New_York",
    "2020-06-01 10:00:00-07",
    "2020-06-01Z",
    "2020-06-01T00:00Z",
    "2999-01-01",
    "2999-01-01 00:00:00",
    "9999-12-31 23:59:59.999999",
    "0001-01-01",
    "1582-10-10",
    "1969-12-31 23:59:59.999999",
    " 2020-06-01 ",
    "2020-06-01  10:00",
    "20200601",
    "2020/06/01",
    "2020-13-01",
    "2020-02-30",
    "2020-06-01 24:00:00",
    "2020-06-01 10:60",
    "+2020-06-01",
    "-0044-03-15",
    "12345-01-01",
    "",
    "T10:00",
    "10:00:00",
    "2020-06-01T",
    "2020-06-01 10",
    "2020-06-01T10",
    "2026-03-08 02:30:00",
    "2026-11-01 01:30:00",
)

EXTENDED_STRINGS: tuple[str, ...] = (
    "2020-06-01 10:00:00.",
    "2020-06-01 10:00:00.12",
    "2020-06-01 10:00:00.123",
    "2020-06-01 10:00:00.1234",
    "2020-06-01 10:00:00.12345",
    "2020-06-01 10:00:00.1234567",
    "2020-06-01 10:00:00.12345678",
    "2020-06-01 10:00:00.1234567891",
    "2020-06-01 10:00:00.000001",
    "2020-06-01 10:00:00.1.2",
    "2020-06-01 10:00:00.x",
    "2020-06-01 10:00:00+02",
    "2020-06-01 10:00:00+2",
    "2020-06-01 10:00:00+2:00",
    "2020-06-01 10:00:00+02:0",
    "2020-06-01 10:00:00+2:3",
    "2020-06-01 10:00:00+0230",
    "2020-06-01 10:00:00+02:30:15",
    "2020-06-01 10:00:00+023015",
    "2020-06-01 10:00:00-00:00",
    "2020-06-01 10:00:00+18:00",
    "2020-06-01 10:00:00+19:00",
    "2020-06-01 10:00:00 +01:00",
    "2020-06-01 10:00:00 UTC+1",
    "2020-06-01 10:00:00 UTC+01:00",
    "2020-06-01 10:00:00 GMT-5",
    "2020-06-01 10:00:00 GMT",
    "2020-06-01 10:00:00 UT",
    "2020-06-01 10:00:00 UT+3",
    "2020-06-01 10:00:00 GMT0",
    "2020-06-01 10:00:00 EST",
    "2020-06-01 10:00:00 PST",
    "2020-06-01 10:00:00 CST",
    "2020-06-01 10:00:00 IST",
    "2020-06-01 10:00:00 Etc/GMT+5",
    "2020-06-01 10:00:00 Europe/Paris",
    "2020-06-01 10:00:00 utc",
    "2020-06-01 10:00:00z",
    "2020-06-01 10:00:00 Mars/Phobos",
    "2020-06-01 10:00:00.5Z",
    "2020-06-01T10:00:00.123456789+01:00",
    "2020-06-01T10:00:00.1 Asia/Tokyo",
    "2020-06-01 10:00 UTC",
    "2020-06-01 10:00:00   ",
    "2020-06-01T10:00:00.",
    "2020-06-01t10:00",
    "2020-06-01T10:00:00T",
    "\t2020-06-01\n",
    "\x0b2020-06-01",
    "\x1f2020-06-01\x7f",
    "\u00a02020-06-01",
    "2020-06-01\u3000",
    " T10:00",
    "-2020-06-01",
    "+12345-01-01",
    "123456-01-01",
    "1234567-01-01",
    "-290308-12-21 19:59:05.224192",
    "-290308-12-21 19:59:05.224191",
    "+294247-01-10 04:00:54.775807",
    "+294247-01-10 04:00:54.775808",
    "262143-01-01",
    "0000-01-01",
    "-0000-06-01",
    "+T10:00",
    "-10:00",
    "10:00",
    "1:2",
    "10:00:00.5",
    "10",
    "10:00:00Z",
    "T10",
    "T10:00:00+01:00",
    "24:00",
    "2020-06-31",
    "2020-02-29",
    "2021-02-29",
    "1900-02-29",
    "2000-02-29",
    "2020-00-01",
    "2020-06-00",
    "2020-06-01 23:59:60",
    "2020-06-01 10:00:0",
    "2020-06-01 10:0:0",
    "2020-06-001",
    "02020-06-01",
    "20-06-01",
    "2020-",
    "2020-06-",
    "2020--01",
    "2026-03-08 02:00:00",
    "2026-03-08 02:59:59.999999",
    "2026-03-08 03:00:00",
    "2026-11-01 01:00:00",
    "2026-11-01 01:59:59",
    "2026-11-01 02:00:00",
    "2026-03-08 02:30:00 America/New_York",
    "2026-11-01 01:30:00 America/New_York",
    "2026-11-01 01:30:00 EST",
    "2026-11-01 01:30:00 -04:00",
    "2999-07-01 12:00:00",
    "2999-07-01 12:00:00 America/New_York",
    "2100-07-01 12:00:00",
    "1800-01-01 00:00:00",
    "1883-11-18 12:00:00",
)

STRINGS: tuple[str, ...] = MEASURED_STRINGS + EXTENDED_STRINGS

TIME_ONLY_ZONES: dict[str, str | None] = {
    "T10:00": None,
    "10:00:00": None,
    "10:00": None,
    "1:2": None,
    "10:00:00.5": None,
    "10:00:00Z": "UTC",
    "T10:00:00+01:00": "+01:00",
    "T10": None,
}

_CAST_SQL = "SELECT unix_micros(CAST(:v AS TIMESTAMP)) AS us"
_TRY_SQL = "SELECT unix_micros(TRY_CAST(:v AS TIMESTAMP)) AS us"
_TO_TIMESTAMP_SQL = "SELECT unix_micros(to_timestamp(:v)) AS us"


def zone_info(zone: str) -> datetime.tzinfo:
    """Return the tzinfo for a region id or a ``+hh:mm`` offset.

    Args:
        zone: An IANA region id or a signed ``hh:mm`` offset.

    Returns:
        The matching ``tzinfo``.
    """
    if zone[0] in "+-":
        sign = -1 if zone[0] == "-" else 1
        hours, minutes = zone[1:].split(":")
        return datetime.timezone(sign * datetime.timedelta(hours=int(hours), minutes=int(minutes)))
    return ZoneInfo(zone)


def today_wall(micros: int, zone: str) -> dict[str, str]:
    """Split an instant into its zone's local date and time of day.

    Args:
        micros: Microseconds since the epoch.
        zone: The zone that resolved the time-only string.

    Returns:
        The zone, the local ISO date, and the local ``HH:MM:SS.ffffff`` time of day.
    """
    instant = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC) + datetime.timedelta(
        microseconds=micros
    )
    local = instant.astimezone(zone_info(zone))
    return {
        "zone": zone,
        "date": local.date().isoformat(),
        "time": local.strftime("%H:%M:%S.%f"),
    }


def run_micros(session: Any, sql: str, value: str) -> dict[str, Any]:
    """Run one parameterized query and capture its micros or its error.

    Args:
        session: A live PySpark session.
        sql: The query with a ``:v`` parameter.
        value: The string bound to ``:v``.

    Returns:
        ``{"us": int | None}`` on success, else the error condition and first line.
    """
    try:
        row = session.sql(sql, args={"v": value}).collect()[0]
    except Exception as error:
        condition = getattr(error, "getCondition", lambda: None)()
        return {"error": condition, "message": str(error).split("\n")[0].strip()}
    return {"us": row["us"]}


def record_cell(session: Any, zone: str, ansi: str, value: str) -> dict[str, Any]:
    """Record the CAST, TRY_CAST and to_timestamp answers for one string.

    Args:
        session: A live PySpark session already set to ``zone`` and ``ansi``.
        zone: The session zone.
        ansi: ``"true"`` or ``"false"``.
        value: The string under test.

    Returns:
        One fixture cell.
    """
    cell: dict[str, Any] = {"zone": zone, "ansi": ansi, "s": value}
    cell["cast"] = run_micros(session, _CAST_SQL, value)
    cell["try_cast"] = run_micros(session, _TRY_SQL, value)
    cell["to_timestamp"] = run_micros(session, _TO_TIMESTAMP_SQL, value)
    micros = cell["try_cast"].get("us")
    if value in TIME_ONLY_ZONES and micros is not None:
        cell["today_wall"] = today_wall(micros, TIME_ONLY_ZONES[value] or zone)
    return cell


def record(session: Any) -> dict[str, Any]:
    """Record every cell against a live PySpark session.

    Args:
        session: A live PySpark 4.1.2 session.

    Returns:
        The fixture document.
    """
    cells: list[dict[str, Any]] = []
    for zone in ZONES:
        for ansi in ANSI_MODES:
            session.conf.set("spark.sql.session.timeZone", zone)
            session.conf.set("spark.sql.ansi.enabled", ansi)
            cells.extend(record_cell(session, zone, ansi, value) for value in STRINGS)
    return {
        "banner": session.version,
        "recorder": "python/repark/tests/_record_cast_ts_string_1.py",
        "cells": cells,
    }


def comparable(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Drop the recording-day fields so two recordings compare on value.

    Args:
        document: A fixture document.

    Returns:
        The cells without instants and dates that depend on the recording day.
    """
    stable: list[dict[str, Any]] = []
    for cell in document["cells"]:
        if "today_wall" in cell:
            wall = cell["today_wall"]
            stable.append(
                {
                    "s": cell["s"],
                    "zone": cell["zone"],
                    "ansi": cell["ansi"],
                    "wall": [wall["zone"], wall["time"]],
                }
            )
            continue
        stable.append(cell)
    return stable


def main() -> int:
    """Record the oracle, or compare a fresh recording with the committed fixture.

    Returns:
        The process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--out", type=Path, default=FIXTURE)
    arguments = parser.parse_args()
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    document = record(session)
    if arguments.check:
        committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
        if comparable(committed) != comparable(document):
            print("cast-ts-string-1 oracle drift", file=sys.stderr)
            return 1
        print(f"cast-ts-string-1 oracle matches ({len(document['cells'])} cells)")
        return 0
    arguments.out.write_text(json.dumps(document, indent=1) + "\n", encoding="utf-8")
    print(f"wrote {len(document['cells'])} cells to {arguments.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
