"""CURRENT-DATE-SESSION-ZONE-1 — ``current_date()`` answers the session-zone date.

Under a zone whose date differs from UTC's, every ``current_date``-rooted spelling answers
that zone's date: the SQL parens and bare spellings, the DataFrame pair, the derived
expressions, the WHERE filter, and the value an INSERT stores. UTC stays the UTC date, and
the value is fixed within one query.

The differing zone is computed at run time: ``Pacific/Kiritimati`` (+14) differs from UTC's
date during UTC 10:00-24:00 and ``Pacific/Pago_Pago`` (-11) during UTC 00:00-11:00, so one
of the two always differs and no skip exists.

pins: current-date-session-zone-1/C-003, C-004
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

ZONE_KEY = "spark.sql.session.timeZone"
ANSI_KEY = "spark.sql.ansi.enabled"
EAST_ZONE = "Pacific/Kiritimati"
WEST_ZONE = "Pacific/Pago_Pago"
EPOCH = date(1970, 1, 1)
BASELINE = date(2026, 1, 1)
CATALOG = "cdsz1"


def _zone_today(zone: str) -> date:
    """Today's date in ``zone`` right now."""
    return datetime.now(ZoneInfo(zone)).date()


def _differing_zone() -> tuple[str, date]:
    """A zone whose date differs from UTC's date now, with that date."""
    utc_today = datetime.now(UTC).date()
    east_today = _zone_today(EAST_ZONE)
    if east_today != utc_today:
        return EAST_ZONE, east_today
    return WEST_ZONE, _zone_today(WEST_ZONE)


def _session(app: str, zone: str, ansi: str, style: str) -> ReparkSession:
    """One session with ``zone`` in force, set at build or by ``conf.set`` after start."""
    builder = ReparkSession.builder.appName(app).config(ANSI_KEY, ansi)
    if style == "build":
        builder = builder.config(ZONE_KEY, zone)
    else:
        builder = builder.config(ZONE_KEY, "UTC")
    session = builder.getOrCreate()
    if style == "set":
        session.conf.set(ZONE_KEY, zone)
    return session


def _single_date(session: ReparkSession, sql: str) -> date:
    """The one DATE value of a single-cell query, asserting the Arrow type."""
    arrow = session.sql(sql).to_arrow()
    assert pa.types.is_date32(arrow.schema.field(0).type)
    value = arrow.column(0).to_pylist()[0]
    assert isinstance(value, date)
    return value


def _single_int(session: ReparkSession, sql: str) -> int:
    """The one integer value of a single-cell query."""
    arrow = session.sql(sql).to_arrow()
    value = arrow.column(0).to_pylist()[0]
    assert isinstance(value, int)
    return value


@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("style", ["build", "set"])
def test_sql_spellings_answer_the_session_zone_date(ansi: str, style: str) -> None:
    """C-003: every SQL spelling rooted in current_date answers the zone date."""
    zone, expected = _differing_zone()
    session = _session(f"cdsz1-sql-{ansi}-{style}", zone, ansi, style)
    try:
        assert _single_date(session, "SELECT current_date()") == expected
        assert _single_date(session, "SELECT current_date") == expected
        assert _single_date(session, "SELECT CAST(current_timestamp() AS DATE)") == expected
        assert _single_date(session, "SELECT to_date(current_timestamp())") == expected
        assert _single_date(session, "SELECT date(now())") == expected
        assert _single_date(session, "SELECT date_add(current_date(), 1)") == expected + timedelta(
            days=1
        )
        parts = (
            session.sql("SELECT year(current_date()), month(current_date()), day(current_date())")
            .to_arrow()
            .to_pylist()[0]
        )
        assert (
            parts["year(current_date())"],
            parts["month(current_date())"],
            parts["day(current_date())"],
        ) == (
            expected.year,
            expected.month,
            expected.day,
        )
        assert (
            _single_int(session, "SELECT datediff(current_date(), DATE '2026-01-01')")
            == (expected - BASELINE).days
        )
        assert _single_date(session, "SELECT trunc(current_date(), 'MM')") == expected.replace(
            day=1
        )
        assert _single_int(session, "SELECT unix_date(current_date())") == (expected - EPOCH).days
    finally:
        session.stop()


@pytest.mark.parametrize("style", ["build", "set"])
def test_dataframe_spellings_answer_the_session_zone_date(style: str) -> None:
    """C-003: F.current_date and F.curdate answer the zone date."""
    zone, expected = _differing_zone()
    session = _session(f"cdsz1-df-{style}", zone, "true", style)
    try:
        base = session.sql("SELECT 1 AS one")
        for column in (F.current_date(), F.curdate()):
            arrow = base.select(column.alias("c")).to_arrow()
            assert pa.types.is_date32(arrow.schema.field(0).type)
            assert arrow.column(0).to_pylist() == [expected]
    finally:
        session.stop()


@pytest.mark.parametrize("style", ["build", "set"])
def test_where_and_insert_use_the_session_zone_date(style: str, tmp_path: Path) -> None:
    """C-003: the WHERE filter and the stored Iceberg value read the zone date."""
    zone, expected = _differing_zone()
    utc_today = datetime.now(UTC).date()
    session = _session(f"cdsz1-write-{style}", zone, "true", style)
    try:
        days = sorted({utc_today.isoformat(), expected.isoformat(), "2026-01-01"})
        values = ", ".join(f"(DATE '{day}')" for day in days)
        view_sql = (
            "CREATE OR REPLACE TEMPORARY VIEW cdsz1_dates AS "
            f"SELECT * FROM (VALUES {values}) AS t(d)"
        )
        session.sql(view_sql)
        arrow = session.sql(
            "SELECT d FROM cdsz1_dates WHERE d = current_date() ORDER BY d"
        ).to_arrow()
        assert arrow.column(0).to_pylist() == [expected]
        warehouse = tmp_path / "wh"
        session.register_memory_catalog(CATALOG, str(warehouse))
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.w")
        session.sql(f"CREATE TABLE {CATALOG}.w.dates (d DATE) USING iceberg")
        session.sql(f"INSERT INTO {CATALOG}.w.dates SELECT current_date()")
        stored = session.sql(f"SELECT d FROM {CATALOG}.w.dates").to_arrow().column(0).to_pylist()
        assert stored == [expected]
    finally:
        session.stop()


def test_value_is_fixed_within_one_query() -> None:
    """C-004: every row and every reference in one query sees the same value."""
    zone, expected = _differing_zone()
    session = _session("cdsz1-stable", zone, "true", "build")
    try:
        rows = (
            session.sql("SELECT current_date() AS a, current_date() AS b, now() AS n1, now() AS n2")
            .to_arrow()
            .to_pylist()
        )
        assert len(rows) == 1
        assert rows[0]["a"] == expected
        assert rows[0]["b"] == expected
        assert rows[0]["n1"] == rows[0]["n2"]
        multi = (
            session.sql("SELECT v, current_date() AS c FROM (VALUES (1), (2), (3)) AS t(v)")
            .to_arrow()
            .column("c")
            .to_pylist()
        )
        assert multi == [expected, expected, expected]
    finally:
        session.stop()


@pytest.mark.parametrize("style", ["build", "set"])
def test_utc_control_matches_the_utc_date(style: str) -> None:
    """C-004: under UTC every spelling answers exactly the UTC date."""
    utc_today = datetime.now(UTC).date()
    session = _session(f"cdsz1-utc-{style}", "UTC", "true", style)
    try:
        assert _single_date(session, "SELECT current_date()") == utc_today
        assert _single_date(session, "SELECT current_date") == utc_today
        assert _single_date(session, "SELECT date_add(current_date(), 1)") == utc_today + timedelta(
            days=1
        )
        arrow = session.sql("SELECT 1 AS one").select(F.current_date().alias("c")).to_arrow()
        assert arrow.column(0).to_pylist() == [utc_today]
        arrow = session.sql("SELECT 1 AS one").select(F.curdate().alias("c")).to_arrow()
        assert arrow.column(0).to_pylist() == [utc_today]
    finally:
        session.stop()
