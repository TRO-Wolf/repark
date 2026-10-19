"""Re-derive the ZONE-HORIZON-RENDER-1 Spark oracle from live PySpark 4.1.2.

The grid is session zone x year x probe x function. Six zones: ``America/New_York``,
``Australia/Sydney``, ``Australia/Lord_Howe`` (a 30-minute shift), ``Asia/Kolkata`` (no
shift), ``UTC`` and the fixed offset ``-08:00``. Five years: 2099 (the control), 2100,
2104 (a leap year), 2500 and 9999. Each zone and year carries a January and a July
instant, and a shifting zone adds the last second before and the first instant of each of
its two transitions that year.

An instant cell spells its instant as ``TIMESTAMP '<wall clock> UTC'``, the same instant
under every session zone, so only the instant to wall-clock direction is under test. A wall
cell spells a wall clock as text, so the other direction is measured beside it; a shifting
zone adds one wall clock inside the gap and one inside the overlap. Every expression is
cast to ``STRING`` in SQL, so no Python ``datetime`` conversion touches an answer. A frame
cell writes all the instants of one zone as CSV (with a header) or JSON and records the
sorted file lines.

The probes are computed once with ``zoneinfo`` and stored in the fixture beside one answer
per cell in recording order, so a re-run rebuilds the same statements from the committed
probes and never recomputes one.

Run with a PySpark 4.1.2 interpreter::

    SPARK_LOCAL_IP=127.0.0.1 python python/repark/tests/_record_zone_horizon_render_1.py

``--check`` re-runs the committed statements and exits non-zero on drift. ``--engine
repark`` runs them on the facade and counts the cells that differ from the fixture;
``--out`` saves those cells and ``--residue`` rewrites
``zone_horizon_render_1_residue.json`` from them. Not
collected by pytest.
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
import tempfile
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

_HERE = Path(__file__).resolve().parent
FIXTURE = _HERE / "zone_horizon_render_1_spark_oracle.json"
RESIDUE = _HERE / "zone_horizon_render_1_residue.json"
ZONES: tuple[str, ...] = (
    "America/New_York",
    "Australia/Sydney",
    "Australia/Lord_Howe",
    "Asia/Kolkata",
    "UTC",
    "-08:00",
)
SHIFTING_ZONES: frozenset[str] = frozenset(
    ("America/New_York", "Australia/Sydney", "Australia/Lord_Howe")
)
YEARS: tuple[int, ...] = (2099, 2100, 2104, 2500, 9999)
_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.UTC)
_HALF_HOUR = datetime.timedelta(minutes=30)
_MICROS_PER_SECOND = 1_000_000

_WINDOW = (
    "(SELECT unix_micros(w.{field}) FROM (SELECT window(t, '{width}') AS w "
    "FROM (SELECT {ts} AS t) GROUP BY window(t, '{width}')))"
)

INSTANT_FUNCTIONS: tuple[tuple[str, str], ...] = (
    ("cast_string", "{ts}"),
    ("cast_date", "CAST({ts} AS DATE)"),
    ("cast_ntz", "CAST({ts} AS TIMESTAMP_NTZ)"),
    ("hour", "hour({ts})"),
    ("minute", "minute({ts})"),
    ("second", "second({ts})"),
    ("year", "year({ts})"),
    ("month", "month({ts})"),
    ("dayofmonth", "dayofmonth({ts})"),
    ("dayofweek", "dayofweek({ts})"),
    ("dayofyear", "dayofyear({ts})"),
    ("weekofyear", "weekofyear({ts})"),
    ("quarter", "quarter({ts})"),
    ("weekday", "weekday({ts})"),
    ("last_day", "last_day(CAST({ts} AS DATE))"),
    ("date_format", "date_format({ts}, 'yyyy-MM-dd HH:mm:ss')"),
    ("date_format_hm", "date_format({ts}, 'HH:mm')"),
    ("to_char", "to_char({ts}, 'yyyy-MM-dd HH:mm:ss')"),
    ("date_trunc_hour", "unix_micros(date_trunc('HOUR', {ts}))"),
    ("date_trunc_day", "unix_micros(date_trunc('DAY', {ts}))"),
    ("date_trunc_month", "unix_micros(date_trunc('MONTH', {ts}))"),
    ("date_trunc_year", "unix_micros(date_trunc('YEAR', {ts}))"),
    ("trunc_month", "trunc({ts}, 'MM')"),
    ("extract_hour", "extract(HOUR FROM {ts})"),
    ("extract_minute", "extract(MINUTE FROM {ts})"),
    ("extract_doy", "extract(DOY FROM {ts})"),
    ("date_part_hour", "date_part('HOUR', {ts})"),
    ("date_part_day", "date_part('DAY', {ts})"),
    ("from_utc_timestamp", "unix_micros(from_utc_timestamp({ts}, '{arg_zone}'))"),
    ("to_utc_timestamp", "unix_micros(to_utc_timestamp({ts}, '{arg_zone}'))"),
    ("from_unixtime", "from_unixtime({seconds})"),
    ("from_unixtime_pattern", "from_unixtime({seconds}, 'yyyy-MM-dd HH:mm')"),
    ("timestampadd_hour", "unix_micros(timestampadd(HOUR, 1, {ts}))"),
    ("timestampadd_day", "unix_micros(timestampadd(DAY, 1, {ts}))"),
    ("timestampadd_month", "unix_micros(timestampadd(MONTH, 1, {ts}))"),
    ("interval_day", "unix_micros({ts} + INTERVAL '1' DAY)"),
    ("interval_month", "unix_micros({ts} - INTERVAL '1' MONTH)"),
    ("date_add", "date_add(CAST({ts} AS DATE), 1)"),
    ("timestampdiff_day", "timestampdiff(DAY, {ts}, {ts} + INTERVAL '36' HOUR)"),
    ("window_day_start", _WINDOW.format(field="start", ts="{ts}", width="1 day")),
    ("window_day_end", _WINDOW.format(field="end", ts="{ts}", width="1 day")),
    ("window_hour_start", _WINDOW.format(field="start", ts="{ts}", width="1 hour")),
    ("to_json", "to_json(named_struct('t', {ts}))"),
    ("unix_date", "unix_date(CAST({ts} AS DATE))"),
    ("next_day", "next_day(CAST({ts} AS DATE), 'MON')"),
    ("months_between", "months_between({ts}, TIMESTAMP_NTZ '2099-01-01 00:00:00')"),
)

WALL_FUNCTIONS: tuple[tuple[str, str], ...] = (
    ("literal", "unix_micros(TIMESTAMP '{wall}')"),
    ("cast_from_string", "unix_micros(CAST('{wall}' AS TIMESTAMP))"),
    ("to_timestamp", "unix_micros(to_timestamp('{wall}'))"),
    ("to_timestamp_pattern", "unix_micros(to_timestamp('{wall}', 'yyyy-MM-dd HH:mm:ss'))"),
    ("unix_timestamp", "unix_timestamp('{wall}')"),
    ("to_unix_timestamp", "to_unix_timestamp('{wall}', 'yyyy-MM-dd HH:mm:ss')"),
    ("make_timestamp", "unix_micros(make_timestamp({y}, {mo}, {d}, {h}, {mi}, {s}))"),
    (
        "make_timestamp_zone",
        "unix_micros(make_timestamp({y}, {mo}, {d}, {h}, {mi}, {s}, '{arg_zone}'))",
    ),
    ("ntz_to_ltz", "unix_micros(CAST(TIMESTAMP_NTZ '{wall}' AS TIMESTAMP))"),
    ("to_utc_timestamp_wall", "to_utc_timestamp(TIMESTAMP_NTZ '{wall}', '{arg_zone}')"),
    ("from_utc_timestamp_wall", "from_utc_timestamp(TIMESTAMP_NTZ '{wall}', '{arg_zone}')"),
    ("convert_from_utc", "convert_timezone('UTC', '{arg_zone}', TIMESTAMP_NTZ '{wall}')"),
    ("convert_to_utc", "convert_timezone('{arg_zone}', 'UTC', TIMESTAMP_NTZ '{wall}')"),
    ("round_trip", "TIMESTAMP '{wall}'"),
    ("round_trip_hour", "hour(TIMESTAMP '{wall}')"),
    ("date_to_timestamp", "unix_micros(CAST(DATE '{date}' AS TIMESTAMP))"),
)

FRAME_KINDS: tuple[str, ...] = ("csv", "json")


def zone_info(zone: str) -> datetime.tzinfo:
    """Return the ``tzinfo`` of a region id or a signed ``hh:mm`` offset.

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


def micros_of(instant: datetime.datetime) -> int:
    """Return the microseconds since the epoch of an aware datetime.

    Args:
        instant: An aware datetime.

    Returns:
        Whole microseconds since 1970-01-01 UTC.
    """
    delta = instant - _EPOCH
    return (delta.days * 86_400 + delta.seconds) * _MICROS_PER_SECOND + delta.microseconds


def utc_literal(micros: int) -> str:
    """Spell an instant as a typed literal that names UTC.

    Args:
        micros: Whole-second microseconds since the epoch.

    Returns:
        ``TIMESTAMP '<wall clock> UTC'``, the same instant under every session zone.
    """
    moment = _EPOCH + datetime.timedelta(microseconds=micros)
    return f"TIMESTAMP '{moment.year:04d}-{moment:%m-%d %H:%M:%S} UTC'"


def transitions(zone: str, year: int) -> list[tuple[datetime.datetime, int, int]]:
    """Find the offset changes of one zone inside one UTC year.

    Args:
        zone: An IANA region id.
        year: The UTC year to scan in half-hour steps.

    Returns:
        ``(first instant of the new offset, old offset seconds, new offset seconds)`` per
        change, in order.
    """
    info = zone_info(zone)
    cursor = datetime.datetime(year, 1, 2, tzinfo=datetime.UTC)
    end = datetime.datetime(year, 12, 30, tzinfo=datetime.UTC)
    found: list[tuple[datetime.datetime, int, int]] = []
    before = cursor.astimezone(info).utcoffset()
    while cursor < end:
        cursor += _HALF_HOUR
        after = cursor.astimezone(info).utcoffset()
        if before is not None and after is not None and after != before:
            found.append((cursor, int(before.total_seconds()), int(after.total_seconds())))
        before = after
    return found


def build_probes() -> dict[str, Any]:
    """Compute the probed instants and wall clocks of every zone and year.

    Returns:
        ``{"instants": {zone: {year: [[probe, micros]]}}, "walls": {zone: {year: [[probe,
        text]]}}}``; a shifting zone adds the last second before and the first instant of
        each transition, one wall clock in the middle of its gap and one in the middle of
        its overlap.
    """
    instants: dict[str, Any] = {}
    walls: dict[str, Any] = {}
    for zone in ZONES:
        instants[zone] = {}
        walls[zone] = {}
        for year in YEARS:
            jan = datetime.datetime(year, 1, 15, 12, 34, 56, tzinfo=datetime.UTC)
            jul = datetime.datetime(year, 7, 15, 12, 34, 56, tzinfo=datetime.UTC)
            points = [["jan", micros_of(jan)], ["jul", micros_of(jul)]]
            clocks = [["jan", f"{year:04d}-01-15 12:00:00"], ["jul", f"{year:04d}-07-15 12:00:00"]]
            changes = transitions(zone, year) if zone in SHIFTING_ZONES else []
            for index, (first, before, after) in enumerate(changes, start=1):
                points.append([f"t{index}_before", micros_of(first) - _MICROS_PER_SECOND])
                points.append([f"t{index}_at", micros_of(first)])
                low, high = sorted((before, after))
                middle = first.replace(tzinfo=None) + datetime.timedelta(
                    seconds=low + (high - low) // 2
                )
                clocks.append(
                    [
                        "gap" if after > before else "overlap",
                        f"{middle.year:04d}-{middle:%m-%d %H:%M:%S}",
                    ]
                )
            instants[zone][str(year)] = points
            walls[zone][str(year)] = clocks
    return {"instants": instants, "walls": walls}


def _cell(zone: str, year: int, probe: str, function: str, body: str) -> dict[str, Any]:
    """Build one scalar cell.

    Args:
        zone: The session zone.
        year: The probed year.
        probe: The probe name.
        function: The function name.
        body: The expression under test.

    Returns:
        The cell without its answer.
    """
    return {
        "zone": zone,
        "year": year,
        "probe": probe,
        "fn": function,
        "sql": f"CAST({body} AS STRING)",
    }


def scalar_cells(probes: dict[str, Any], zone: str, year: int) -> list[dict[str, Any]]:
    """Build the scalar cells of one zone and year.

    Args:
        probes: The probe document.
        zone: The session zone.
        year: The probed year.

    Returns:
        The instant cells, then the wall cells.
    """
    arg_zone = "America/New_York" if zone not in SHIFTING_ZONES else zone
    cells: list[dict[str, Any]] = []
    for probe, micros in probes["instants"][zone][str(year)]:
        for function, template in INSTANT_FUNCTIONS:
            body = template.format(
                ts=utc_literal(micros),
                seconds=micros // _MICROS_PER_SECOND,
                arg_zone=arg_zone,
            )
            cells.append(_cell(zone, year, probe, function, body))
    for probe, text in probes["walls"][zone][str(year)]:
        date, clock = text.split(" ")
        y, mo, d = (int(part) for part in date.split("-"))
        h, mi, s = (int(part) for part in clock.split(":"))
        for function, template in WALL_FUNCTIONS:
            body = template.format(
                wall=text, date=date, y=y, mo=mo, d=d, h=h, mi=mi, s=s, arg_zone=arg_zone
            )
            cells.append(_cell(zone, year, f"wall_{probe}", function, body))
    return cells


def frame_cells(probes: dict[str, Any], zone: str) -> list[dict[str, Any]]:
    """Build the frame cells of one zone.

    Args:
        probes: The probe document.
        zone: The session zone.

    Returns:
        One cell per frame kind over every instant of the zone.
    """
    values = ", ".join(
        f"({utc_literal(micros)})"
        for year in YEARS
        for _probe, micros in probes["instants"][zone][str(year)]
    )
    sql = f"SELECT t FROM VALUES {values} AS v(t) ORDER BY t"
    return [
        {"zone": zone, "year": 0, "probe": "all", "fn": kind, "frame": kind, "sql": sql}
        for kind in FRAME_KINDS
    ]


def build_cells(probes: dict[str, Any]) -> list[dict[str, Any]]:
    """Build every cell of the grid, without answers.

    Args:
        probes: The probe document.

    Returns:
        The cells in recording order.
    """
    cells: list[dict[str, Any]] = []
    for zone in ZONES:
        for year in YEARS:
            cells.extend(scalar_cells(probes, zone, year))
        cells.extend(frame_cells(probes, zone))
    return cells


def oracle_cells(document: dict[str, Any]) -> list[dict[str, Any]]:
    """Expand a fixture document into cells that carry Spark's answer.

    Args:
        document: A fixture document.

    Returns:
        The cells in recording order, each with its answer under ``spark``.
    """
    cells = build_cells(document)
    for cell, result in zip(cells, document["answers"], strict=True):
        cell["spark"] = result
    return cells


def _failure(error: Exception) -> dict[str, Any]:
    """Describe a raised error as an answer.

    Args:
        error: The exception one statement raised.

    Returns:
        The error condition when the engine names one, else the exception class.
    """
    condition = getattr(error, "getCondition", lambda: None)()
    return {"error": condition or type(error).__name__}


def run_scalars(session: Any, cells: list[dict[str, Any]]) -> list[Any]:
    """Answer scalar cells, in one statement when every expression succeeds.

    Args:
        session: A session already set to the cells' zone.
        cells: Scalar cells of one zone.

    Returns:
        One answer per cell: the string, ``None``, or an error description.
    """
    if not cells:
        return []
    columns = ", ".join(f"{cell['sql']} AS c{index}" for index, cell in enumerate(cells))
    try:
        row = session.sql(f"SELECT {columns}").collect()[0]
    except Exception:
        if len(cells) == 1:
            try:
                return [session.sql(f"SELECT {cells[0]['sql']} AS c0").collect()[0][0]]
            except Exception as error:
                return [_failure(error)]
        half = len(cells) // 2
        return run_scalars(session, cells[:half]) + run_scalars(session, cells[half:])
    return [row[index] for index in range(len(cells))]


def _written_lines(frame: Any, kind: str) -> list[str]:
    """Write a frame as CSV or JSON and read the part files back.

    Args:
        frame: The one-column timestamp frame.
        kind: ``csv`` or ``json``.

    Returns:
        The written lines, sorted.
    """
    with tempfile.TemporaryDirectory(prefix="zone-horizon-render-1-") as directory:
        target = Path(directory) / kind
        writer = frame.coalesce(1).write.mode("overwrite")
        if kind == "csv":
            writer.option("header", "true").csv(str(target))
        else:
            writer.json(str(target))
        lines: list[str] = []
        for part in sorted(target.iterdir()):
            if part.name[0] not in "._" and part.is_file():
                lines.extend(part.read_text(encoding="utf-8").splitlines())
        return sorted(lines)


def run_frame(session: Any, cell: dict[str, Any]) -> Any:
    """Answer one frame cell.

    Args:
        session: A session already set to the cell's zone.
        cell: A frame cell.

    Returns:
        The written file lines, or an error description.
    """
    try:
        return _written_lines(session.sql(cell["sql"]), cell["frame"])
    except Exception as error:
        return _failure(error)


def answer(session: Any, cells: list[dict[str, Any]]) -> list[Any]:
    """Run every cell on one engine.

    Args:
        session: A PySpark or facade session.
        cells: Cells in recording order.

    Returns:
        One answer per cell, in order.
    """
    answers: list[Any] = [None] * len(cells)
    for zone in dict.fromkeys(cell["zone"] for cell in cells):
        session.conf.set("spark.sql.session.timeZone", zone)
        indexed = [(index, cell) for index, cell in enumerate(cells) if cell["zone"] == zone]
        scalars = [(index, cell) for index, cell in indexed if "frame" not in cell]
        for year in dict.fromkeys(cell["year"] for _index, cell in scalars):
            group = [(index, cell) for index, cell in scalars if cell["year"] == year]
            results = run_scalars(session, [cell for _index, cell in group])
            for pair, result in zip(group, results, strict=True):
                answers[pair[0]] = result
        for index, cell in indexed:
            if "frame" in cell:
                answers[index] = run_frame(session, cell)
    return answers


def record(session: Any, probes: dict[str, Any] | None = None) -> dict[str, Any]:
    """Record the fixture document from live Spark.

    Args:
        session: A live PySpark 4.1.2 session.
        probes: The committed probes to re-run; fresh ones when omitted.

    Returns:
        The fixture document: the probes and one answer per cell, in recording order.
    """
    source = probes or build_probes()
    zone = session.conf.get("spark.sql.session.timeZone")
    try:
        answers = answer(session, build_cells(source))
    finally:
        session.conf.set("spark.sql.session.timeZone", zone)
    return {
        "banner": session.version,
        "recorder": "python/repark/tests/_record_zone_horizon_render_1.py",
        "instants": source["instants"],
        "walls": source["walls"],
        "answers": answers,
    }


def cell_key(cell: dict[str, Any]) -> str:
    """Name one cell.

    Args:
        cell: A cell of the grid.

    Returns:
        ``zone|year|probe|function``.
    """
    return f"{cell['zone']}|{cell['year']}|{cell['probe']}|{cell['fn']}"


def differing(document: dict[str, Any], answers: list[Any]) -> list[dict[str, Any]]:
    """List the cells one engine answers differently from the fixture.

    Args:
        document: A fixture document.
        answers: One answer per fixture cell.

    Returns:
        The differing cells, each with the engine's answer under ``got``.
    """
    return [
        {**cell, "got": result}
        for cell, result in zip(oracle_cells(document), answers, strict=True)
        if result != cell["spark"]
    ]


def dump(document: dict[str, Any]) -> str:
    """Serialise a fixture document with one answer per line.

    Args:
        document: A fixture document.

    Returns:
        The JSON text.
    """
    head = {key: value for key, value in document.items() if key != "answers"}
    lines = ",\n".join(json.dumps(result, ensure_ascii=False) for result in document["answers"])
    return json.dumps(head, ensure_ascii=False)[:-1] + ', "answers": [\n' + lines + "\n]}\n"


def _spark_session() -> Any:
    """Start the recording session.

    Returns:
        A local PySpark session with ANSI on.
    """
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


def _repark_session() -> Any:
    """Start a facade session.

    Returns:
        A facade session with ANSI on.
    """
    from repark import ReparkSession

    session = ReparkSession.builder.appName("zone-horizon-render-1").getOrCreate()
    session.conf.set("spark.sql.ansi.enabled", "true")
    return session


def main() -> int:
    """Record the fixture, check it, or compare the facade against it.

    Returns:
        The process exit code.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--engine", choices=("spark", "repark"), default="spark")
    parser.add_argument("--out", type=Path, default=None)
    parser.add_argument("--residue", action="store_true")
    arguments = parser.parse_args()
    if arguments.engine == "repark":
        committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
        cells = differing(committed, answer(_repark_session(), build_cells(committed)))
        if arguments.out is not None:
            arguments.out.write_text(json.dumps(cells, indent=1) + "\n", encoding="utf-8")
        if arguments.residue:
            residue = {
                zone: sorted(cell_key(cell) for cell in cells if cell["zone"] == zone)
                for zone in ZONES
            }
            RESIDUE.write_text(json.dumps(residue, indent=1) + "\n", encoding="utf-8")
        print(f"{len(cells)} of {len(committed['answers'])} cells differ from Spark")
        return 0
    session = _spark_session()
    if arguments.check:
        committed = json.loads(FIXTURE.read_text(encoding="utf-8"))
        if record(session, committed) != {**committed, "banner": session.version}:
            print("zone-horizon-render-1 oracle drift", file=sys.stderr)
            return 1
        print(f"zone-horizon-render-1 oracle matches ({len(committed['answers'])} cells)")
        return 0
    document = record(session)
    target = arguments.out or FIXTURE
    target.write_text(dump(document), encoding="utf-8")
    print(f"wrote {len(document['answers'])} cells to {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
