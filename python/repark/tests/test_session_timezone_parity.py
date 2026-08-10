"""Session-timezone differential rows (gap G1) + temporal edges (gap G16) — H-1a split A.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``) on 2026-08-10, with ``spark.sql.session.timeZone`` set to the
row's own zone. One SQL string per row runs on BOTH engines, so the recipe under test and the
recipe the oracle ran are the same string — nothing here is hand-computed.

**Why most rows are DISCLOSURES, not equalities.** This unit ships the session-timezone
*configuration surface*; the extraction fix is the unit that follows it (the split rule in
``briefs/v2-engine-hardening.md`` H-1a). Today repark extracts timestamp fields in the STORED
zone rather than the session zone, so asserting ``repark == Spark`` here would be red on arrival —
and deleting the rows until the fix lands would hide the class that the census measured as a
four-hour silent offset. So each divergent row pins BOTH halves:

* ``repark`` — repark's actual output today (value AND Arrow type), and
* ``spark`` — the recorded live-Spark output it differs from,

and the row asserts that the two still differ. A row that silently CONVERGES goes RED and forces
the disclosure to be revisited rather than laundered into "parity" — the same discipline
``docs/testing.md`` puts on the live tier's disclosures. When the extraction fix lands, each
divergent row flips to ``repark=None`` (equality) and that flip is the fix's revert-red evidence.

**Rows assert on the Arrow path** (``to_arrow``) through the parity comparator, so schema name,
Arrow type and nullability are part of every assertion — never ``show``.

**Re-deriving the goldens (record mode).** The driver that recorded every ``spark`` half is
committed beside this module, so the "recorded against live PySpark 4.1.2" claim is falsifiable
from inside the repo rather than only from the session that made it::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_session_timezone_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe under the row's own zone, so
the recorded golden and the asserted recipe cannot drift apart. It needs a JVM + ``pyspark``
(``uv sync --extra record``) and is never collected by pytest.

**Entry points.** Every row here goes through the facade ``sql()`` door — over scalar literals for
most rows and over a real tz-aware timestamp COLUMN for the ``column_extract_*`` family. The
four-entry-point matrix the brief mandates (native DataFrame / ANSI door / Spark door / facade)
is **split B's obligation**, claimed as such in the unit ledger's gate table, not silently.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.session import ReparkSession

# The two non-UTC oracle zones. New York exercises a DST-observing zone west of UTC; Tokyo a
# fixed-offset zone east of UTC, so a sign error cannot pass both.
ZONE_NEW_YORK = "America/New_York"
ZONE_TOKYO = "Asia/Tokyo"

SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

# The in-flight fix every disclosure below names, so a reader of a red row knows what flips it.
FIX = "the session-timezone extraction fix (briefs/v2-engine-hardening.md, H-1a split B)"


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


def _one_row(fields: list[tuple[str, pa.DataType, bool]], values: dict[str, object]) -> pa.Table:
    """Build the single-row Arrow table a recorded golden describes (name, type, nullability)."""
    return _table(fields, {name: [values[name]] for name, _, _ in fields})


@dataclass(frozen=True)
class TimeZoneRow:
    """One differential row: a SQL string, the recorded live-Spark table, and repark's own.

    ``repark is None`` means the engines AGREE — the row is a plain equality assertion.
    ``repark is not None`` means the row is a DISCLOSURE: repark's actual output is pinned, and
    a convergence onto the recorded Spark output is detected and reported as one.

    ``needs_column_view`` marks the rows whose SQL reads the tz-aware timestamp COLUMN rather than
    a scalar literal; the runner registers :data:`COLUMN_VIEW` on the session first.
    """

    name: str
    gap: str
    session_time_zone: str
    sql: str
    spark: pa.Table
    repark: pa.Table | None
    note: str
    needs_column_view: bool = False


# ==================================================================================================
# The tz-aware timestamp COLUMN (the column-path rows read from it, on both engines)
# ==================================================================================================

# The brief's recipe is written over a tz-aware timestamp COLUMN, not a scalar literal, because a
# migrated job extracts from a column. These two instants straddle a calendar-year boundary in
# New York (2024-01-01T04:30Z is 2023-12-31 23:30 EST) and a plain hour shift in Tokyo, so one
# small in-memory frame exercises the year/month/day AND hour fields under both zones.
COLUMN_VIEW = "tz_aware_instants"


def _utc(*args: int) -> dt.datetime:
    """A tz-aware UTC instant (what PySpark's Arrow export produces for a TIMESTAMP)."""
    return dt.datetime(*args, tzinfo=dt.UTC)  # type: ignore[arg-type]


COLUMN_INSTANTS: tuple[dt.datetime, ...] = (_utc(2024, 6, 15, 12, 0), _utc(2024, 1, 1, 4, 30))

COLUMN_SQL = (
    "SELECT year(ts) AS year_part, month(ts) AS month_part, dayofmonth(ts) AS day_part, "
    f"hour(ts) AS hour_part FROM {COLUMN_VIEW} ORDER BY ts"
)


def register_column_view(session: object) -> None:
    """Register :data:`COLUMN_VIEW` — a two-row tz-aware TIMESTAMP column — on either engine.

    ``createDataFrame`` + ``createOrReplaceTempView`` are spelled identically in PySpark and in
    the repark facade, so the SAME function prepares the oracle and the engine under test. Schema
    is INFERRED deliberately: both engines then infer an instant-typed TIMESTAMP and export
    ``timestamp[us, tz=UTC]``, which is what makes the column genuinely tz-aware (a DDL
    ``ts timestamp`` string makes repark's column tz-naive and would weaken the row).
    """
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [(instant,) for instant in COLUMN_INSTANTS], ["ts"]
    )
    frame.createOrReplaceTempView(COLUMN_VIEW)


# ==================================================================================================
# Gap G1 — session timezone / tz-aware timestamps
# ==================================================================================================

_INT32 = pa.int32()

G1_ROWS: list[TimeZoneRow] = [
    TimeZoneRow(
        "year_of_instant_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT year(to_timestamp('2024-01-01T04:30:00Z')) AS year_part",
        _one_row([("year_part", _INT32, True)], {"year_part": 2023}),
        _one_row([("year_part", _INT32, True)], {"year_part": 2024}),
        "the instant is 2023-12-31 23:30 in New York, so Spark's year is 2023; repark extracts "
        f"in the stored (UTC) zone and answers 2024. Flipped to equality by {FIX}.",
    ),
    TimeZoneRow(
        "month_of_instant_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT month(to_timestamp('2024-03-01T02:15:00Z')) AS month_part",
        _one_row([("month_part", _INT32, True)], {"month_part": 2}),
        _one_row([("month_part", _INT32, True)], {"month_part": 3}),
        "2024-02-29 21:15 in New York (a leap day) vs 2024-03-01 in the stored zone — the month "
        f"boundary moves with the session zone. Flipped to equality by {FIX}.",
    ),
    TimeZoneRow(
        "day_of_instant_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT dayofmonth(to_timestamp('2024-06-15T03:00:00Z')) AS day_part",
        _one_row([("day_part", _INT32, True)], {"day_part": 14}),
        _one_row([("day_part", _INT32, True)], {"day_part": 15}),
        "the day-partition key a migrated job would write: 14 in the session zone, 15 in the "
        f"stored zone. Flipped to equality by {FIX}.",
    ),
    TimeZoneRow(
        "hour_of_instant_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT hour(to_timestamp('2024-06-15T12:00:00Z')) AS hour_part",
        _one_row([("hour_part", _INT32, True)], {"hour_part": 8}),
        _one_row([("hour_part", _INT32, True)], {"hour_part": 12}),
        "the census's four-hour silent offset, isolated: EDT is UTC-4. Flipped to equality by "
        f"{FIX}.",
    ),
    TimeZoneRow(
        "hour_of_instant_under_tokyo_session",
        "G1",
        ZONE_TOKYO,
        "SELECT hour(to_timestamp('2024-06-15T12:00:00Z')) AS hour_part",
        _one_row([("hour_part", _INT32, True)], {"hour_part": 21}),
        _one_row([("hour_part", _INT32, True)], {"hour_part": 12}),
        "the same instant east of UTC (+9): repark answers 12 under BOTH session zones, which is "
        f"what makes this a session-zone bug rather than an offset-sign bug. Flipped by {FIX}.",
    ),
    TimeZoneRow(
        "year_month_day_of_instant_under_tokyo_session",
        "G1",
        ZONE_TOKYO,
        "SELECT year(to_timestamp('2023-12-31T16:30:00Z')) AS year_part, "
        "month(to_timestamp('2023-12-31T16:30:00Z')) AS month_part, "
        "dayofmonth(to_timestamp('2023-12-31T16:30:00Z')) AS day_part",
        _one_row(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
            ],
            {"year_part": 2024, "month_part": 1, "day_part": 1},
        ),
        _one_row(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
            ],
            {"year_part": 2023, "month_part": 12, "day_part": 31},
        ),
        "all three calendar fields move together across the year boundary (2024-01-01 01:30 in "
        f"Tokyo); repark reports 2023-12-31 for every one. Flipped by {FIX}.",
    ),
    TimeZoneRow(
        "to_timestamp_of_zone_suffixed_string",
        "G1",
        ZONE_NEW_YORK,
        "SELECT to_timestamp('2024-03-10T01:30:00-05:00') AS ts",
        _one_row([("ts", pa.timestamp("us", "UTC"), True)], {"ts": _utc(2024, 3, 10, 6, 30)}),
        _one_row([("ts", pa.timestamp("ns"), True)], {"ts": dt.datetime(2024, 3, 10, 6, 30)}),
        "the INSTANT agrees (06:30Z) — the divergence is the Arrow type on the export path: "
        "Spark's TIMESTAMP is an instant exported as timestamp[us, tz=UTC], repark exports a "
        f"tz-NAIVE timestamp[ns]. A consumer that localizes the column is silently wrong. {FIX} "
        "owns the value half; the export type is part of the same class.",
    ),
    TimeZoneRow(
        "dst_spring_forward_instant_hour",
        "G1",
        ZONE_NEW_YORK,
        "SELECT hour(to_timestamp('2024-03-10T07:00:00Z')) AS hour_part",
        _one_row([("hour_part", _INT32, True)], {"hour_part": 3}),
        _one_row([("hour_part", _INT32, True)], {"hour_part": 7}),
        "the spring-forward instant: 02:00-03:00 local does not exist on 2024-03-10 in New York, "
        f"so Spark answers 3 (EDT) — repark answers the stored 7. Flipped by {FIX}.",
    ),
    TimeZoneRow(
        "dst_fall_back_repeated_local_hour",
        "G1",
        ZONE_NEW_YORK,
        "SELECT hour(to_timestamp('2024-11-03T05:30:00Z')) AS before_part, "
        "hour(to_timestamp('2024-11-03T06:30:00Z')) AS after_part",
        _one_row(
            [("before_part", _INT32, True), ("after_part", _INT32, True)],
            {"before_part": 1, "after_part": 1},
        ),
        _one_row(
            [("before_part", _INT32, True), ("after_part", _INT32, True)],
            {"before_part": 5, "after_part": 6},
        ),
        "fall-back: two distinct instants share local hour 1 (EDT then EST), so Spark answers "
        "(1, 1) — repark answers (5, 6) and never collapses the repeated hour. This is the row a "
        f"dedup-by-hour job depends on. Flipped by {FIX}.",
    ),
    TimeZoneRow(
        "tz_aware_to_naive_round_trip",
        "G1",
        ZONE_NEW_YORK,
        "SELECT CAST(CAST(to_timestamp('2024-06-15T12:00:00Z') AS STRING) AS TIMESTAMP) "
        "AS round_trip",
        _one_row(
            [("round_trip", pa.timestamp("us", "UTC"), True)],
            {"round_trip": _utc(2024, 6, 15, 12, 0)},
        ),
        _one_row(
            [("round_trip", pa.timestamp("ns"), True)],
            {"round_trip": dt.datetime(2024, 6, 15, 12, 0)},
        ),
        "timestamp -> string -> timestamp: Spark renders in the session zone (08:00) and parses "
        "back in the session zone, so the instant survives as timestamp[us, tz=UTC]. repark "
        "renders and re-parses in the stored zone and returns a tz-naive timestamp[ns]; the "
        f"instant survives only because both halves ignore the zone. Flipped by {FIX}.",
    ),
    TimeZoneRow(
        "date_trunc_day_across_a_zone_boundary",
        "G1",
        ZONE_NEW_YORK,
        "SELECT date_trunc('day', to_timestamp('2024-06-15T03:00:00Z')) AS day_start",
        _one_row(
            [("day_start", pa.timestamp("us", "UTC"), True)],
            {"day_start": _utc(2024, 6, 14, 4, 0)},
        ),
        _one_row(
            [("day_start", pa.timestamp("us"), True)],
            {"day_start": dt.datetime(2024, 6, 15, 0, 0)},
        ),
        "the daily-rollup boundary: Spark truncates to local midnight (2024-06-14 00:00 EDT = "
        "04:00Z), repark to UTC midnight of the next day. Value AND type diverge, so a daily "
        f"aggregate lands in the wrong bucket. Flipped by {FIX}.",
    ),
    # ----- the COLUMN family: the same class over a real tz-aware timestamp column ---------------
    # Every row above extracts from a scalar literal. A migrated job extracts from a COLUMN, which
    # is the recipe the brief actually writes ("year/month/day/hour over a tz-aware timestamp
    # column under two non-UTC session zones"). These two rows run that recipe over an in-memory
    # two-row frame registered as a temp view, so the divergence is pinned on data rather than on
    # constant folding — and both engines carry the column as timestamp[us, tz=UTC].
    TimeZoneRow(
        "column_extract_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        COLUMN_SQL,
        _table(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
                ("hour_part", _INT32, True),
            ],
            {
                "year_part": [2023, 2024],
                "month_part": [12, 6],
                "day_part": [31, 15],
                "hour_part": [23, 8],
            },
        ),
        _table(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
                ("hour_part", _INT32, True),
            ],
            {
                "year_part": [2024, 2024],
                "month_part": [1, 6],
                "day_part": [1, 15],
                "hour_part": [4, 12],
            },
        ),
        "the brief's own recipe, over a COLUMN: all four fields of both instants move to the "
        "session zone in Spark (2024-01-01T04:30Z is 2023-12-31 23:30 EST, so even the YEAR "
        "changes), while repark answers the stored zone for every one. The row a partitioned "
        f"write would get wrong for every row of a real table. Flipped to equality by {FIX}.",
        needs_column_view=True,
    ),
    TimeZoneRow(
        "column_extract_under_tokyo_session",
        "G1",
        ZONE_TOKYO,
        COLUMN_SQL,
        _table(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
                ("hour_part", _INT32, True),
            ],
            {
                "year_part": [2024, 2024],
                "month_part": [1, 6],
                "day_part": [1, 15],
                "hour_part": [13, 21],
            },
        ),
        _table(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
                ("hour_part", _INT32, True),
            ],
            {
                "year_part": [2024, 2024],
                "month_part": [1, 6],
                "day_part": [1, 15],
                "hour_part": [4, 12],
            },
        ),
        "the same column east of UTC (+9): the calendar fields happen to AGREE here and only the "
        "hour moves, which is why the pair is recorded — repark's answer is identical under both "
        "zones, so the New York row alone could be misread as an offset-sign bug rather than a "
        f"session-zone one. Flipped to equality by {FIX}.",
        needs_column_view=True,
    ),
]


# ==================================================================================================
# Gap G16 — epoch / DST / temporal edges (pre-1970, year boundary, leap day)
# ==================================================================================================

G16_ROWS: list[TimeZoneRow] = [
    TimeZoneRow(
        "pre_1970_extract_under_new_york_session",
        "G16",
        ZONE_NEW_YORK,
        "SELECT year(to_timestamp('1969-12-31T23:30:00Z')) AS year_part, "
        "month(to_timestamp('1969-12-31T23:30:00Z')) AS month_part, "
        "dayofmonth(to_timestamp('1969-12-31T23:30:00Z')) AS day_part, "
        "hour(to_timestamp('1969-12-31T23:30:00Z')) AS hour_part",
        _one_row(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
                ("hour_part", _INT32, True),
            ],
            {"year_part": 1969, "month_part": 12, "day_part": 31, "hour_part": 18},
        ),
        _one_row(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
                ("hour_part", _INT32, True),
            ],
            {"year_part": 1969, "month_part": 12, "day_part": 31, "hour_part": 23},
        ),
        "a negative epoch instant 30 minutes before 1970: the calendar fields agree, the HOUR "
        f"does not (18 EST vs the stored 23) — sign handling is fine, the zone is not. {FIX}.",
    ),
    TimeZoneRow(
        "pre_1970_timestamp_cast_to_bigint",
        "G16",
        ZONE_NEW_YORK,
        "SELECT CAST(to_timestamp('1969-12-31T23:30:00Z') AS BIGINT) AS epoch_value",
        _one_row([("epoch_value", pa.int64(), True)], {"epoch_value": -1800}),
        _one_row([("epoch_value", pa.int64(), True)], {"epoch_value": -1800000000000}),
        "a SEPARATE divergence this unit surfaced and does NOT fix: casting TIMESTAMP to BIGINT "
        "yields epoch SECONDS in Spark and epoch NANOSECONDS in repark (a 10^9 factor, correctly "
        "signed before 1970). Not a zone bug — a cast-unit bug; recorded here so the temporal "
        "edge is not silently green, and carried to the divergence registry as its own row.",
    ),
    TimeZoneRow(
        "year_boundary_date_trunc_under_tokyo_session",
        "G16",
        ZONE_TOKYO,
        "SELECT date_trunc('year', to_timestamp('2023-12-31T15:00:00Z')) AS year_start",
        _one_row(
            [("year_start", pa.timestamp("us", "UTC"), True)],
            {"year_start": _utc(2023, 12, 31, 15, 0)},
        ),
        _one_row(
            [("year_start", pa.timestamp("us"), True)],
            {"year_start": dt.datetime(2023, 1, 1, 0, 0)},
        ),
        "the instant is 2024-01-01 00:00 in Tokyo, so Spark's year start is that same instant; "
        f"repark truncates the stored 2023-12-31 and lands a whole year earlier. {FIX}.",
    ),
    TimeZoneRow(
        "year_boundary_extract_and_format_under_new_york_session",
        "G16",
        ZONE_NEW_YORK,
        "SELECT year(to_timestamp('2024-01-01T02:00:00Z')) AS year_part, "
        "date_format(to_timestamp('2024-01-01T02:00:00Z'), 'yyyy-MM-dd') AS local_date",
        _one_row(
            [("year_part", _INT32, True), ("local_date", pa.string(), True)],
            {"year_part": 2023, "local_date": "2023-12-31"},
        ),
        _one_row(
            [("year_part", _INT32, True), ("local_date", pa.string(), True)],
            {"year_part": 2024, "local_date": "2024-01-01"},
        ),
        "extraction and RENDERING move together in Spark and stay together in repark — both in "
        f"the wrong zone, so a formatted partition path and an extracted key agree with each "
        f"other and disagree with Spark. {FIX}.",
    ),
    TimeZoneRow(
        "leap_day_extract_under_new_york_session",
        "G16",
        ZONE_NEW_YORK,
        "SELECT month(to_timestamp('2024-02-29T02:00:00Z')) AS month_part, "
        "dayofmonth(to_timestamp('2024-02-29T02:00:00Z')) AS day_part",
        _one_row(
            [("month_part", _INT32, True), ("day_part", _INT32, True)],
            {"month_part": 2, "day_part": 28},
        ),
        _one_row(
            [("month_part", _INT32, True), ("day_part", _INT32, True)],
            {"month_part": 2, "day_part": 29},
        ),
        "the leap day itself: the same instant is 2024-02-28 in New York, so a leap-day filter "
        f"selects different rows on the two engines. {FIX}.",
    ),
    TimeZoneRow(
        "date_extraction_is_session_zone_independent",
        "G16",
        ZONE_NEW_YORK,
        "SELECT year(to_date('2024-02-29')) AS year_part, "
        "month(to_date('2024-02-29')) AS month_part, "
        "dayofmonth(to_date('2024-02-29')) AS day_part",
        _one_row(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
            ],
            {"year_part": 2024, "month_part": 2, "day_part": 29},
        ),
        None,
        "the control row, and an invariant in its own right: a DATE carries no instant, so its "
        "extraction must NOT move with the session zone. Both engines agree today — and the fix "
        "must keep them agreeing, which is exactly what a zone-blind fix would break.",
    ),
    TimeZoneRow(
        "leap_day_date_arithmetic_is_session_zone_independent",
        "G16",
        ZONE_TOKYO,
        "SELECT last_day(to_date('2024-02-01')) AS month_end, "
        "trunc(to_date('2024-02-29'), 'YEAR') AS year_start, "
        "datediff(to_date('2024-03-01'), to_date('2024-02-01')) AS february_days",
        _one_row(
            [
                ("month_end", pa.date32(), True),
                ("year_start", pa.date32(), True),
                ("february_days", _INT32, True),
            ],
            {
                "month_end": dt.date(2024, 2, 29),
                "year_start": dt.date(2024, 1, 1),
                "february_days": 29,
            },
        ),
        None,
        "the second control row: leap-day date arithmetic under a non-UTC session, agreeing on "
        "value AND date32 type. It is the pin that would catch a fix that pushed the session zone "
        "into the DATE path.",
    ),
]

ROWS: list[TimeZoneRow] = [*G1_ROWS, *G16_ROWS]


# ==================================================================================================
# Helpers
# ==================================================================================================


def _session_at(zone: str) -> ReparkSession:
    """A repark session whose session timezone is ``zone`` (resolved at build, engine-validated)."""
    import repark

    return (
        repark.ReparkSession.builder.appName("session-timezone-parity")
        .config(SESSION_TIME_ZONE_KEY, zone)
        .getOrCreate()
    )


def _frames_differ(actual: pa.Table, expected: pa.Table) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected)
    except FrameMismatchError:
        return True
    return False


# ==================================================================================================
# The rows
# ==================================================================================================


def run_row(row: TimeZoneRow, session: object) -> pa.Table:
    """Run one row's recipe on a session (either engine) and return its Arrow output.

    Shared with the record driver so the recipe the oracle ran and the recipe asserted here are
    the same code, not two copies of one string.
    """
    if row.needs_column_view:
        register_column_view(session)
    frame = session.sql(row.sql)  # type: ignore[attr-defined]
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow
    return to_arrow()  # type: ignore[no-any-return]


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_session_timezone_row_matches_spark_or_still_diverges(row: TimeZoneRow) -> None:
    """Every recorded row, on the Arrow path (value AND Arrow type AND nullability).

    Equality rows assert ``repark == Spark``.

    Disclosure rows assert repark's pinned actual output — and when that assertion fails, the
    failure is CLASSIFIED before it is raised, because the two ways it can fail need opposite
    responses. If repark's live output now equals the recorded Spark golden, the engines have
    CONVERGED and the row must be flipped, not deleted; if it matches neither half, that is a
    regression and both halves must be re-derived in record mode. The classification is done on
    ``actual`` — the engine's real output — so an engine change genuinely reaches it. (The
    trailing ``_frames_differ(row.repark, row.spark)`` assertion compares two module constants and
    can therefore only catch a bad EDIT: it is the row-well-formedness guard, not the convergence
    detector, and it is kept for exactly that.)
    """
    session = _session_at(row.session_time_zone)
    actual = run_row(row, session)

    if row.repark is None:
        assert_frames_equal(actual, row.spark)
        return

    try:
        assert_frames_equal(actual, row.repark)
    except FrameMismatchError as mismatch:
        if not _frames_differ(actual, row.spark):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED — repark now produces the RECORDED "
                f"SPARK output, so this disclosure is stale. Do not delete the row: flip it to an "
                f"equality row (repark=None) and record the convergence. {row.note}"
            ) from mismatch
        raise AssertionError(
            f"{row.name}: repark moved OFF its pinned disclosure and does NOT match the recorded "
            f"Spark golden either — this is a regression, not a convergence. Re-derive both "
            f"halves in record mode (see this module's docstring) before touching the pin. "
            f"{row.note}"
        ) from mismatch

    assert _frames_differ(row.repark, row.spark), (
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a disclosure at "
        f"all — either it converged and was half-edited, or the Spark half was pasted over the "
        f"repark half. Flip it to an equality row (repark=None) or re-record it. {row.note}"
    )


def test_session_timezone_row_set_covers_both_gap_budgets() -> None:
    """The pin budget is part of the unit, so the corpus size is pinned, not incidental.

    G1's budget is 10-14 differential rows (13 table rows here — 11 scalar-literal rows plus the
    2 column-path rows — plus the ``current_timestamp`` row below, whose value is nondeterministic
    and so is pinned as its own test); G16's is 6-8.
    """
    g1 = [row for row in ROWS if row.gap == "G1"]
    g16 = [row for row in ROWS if row.gap == "G16"]
    assert len(g1) + 1 == 14, (
        "G1: 11 scalar-literal rows + 2 column-path rows + the current_timestamp row (budget 10-14)"
    )
    assert len(g16) == 7, "G16: pre-1970, year-boundary and leap-day rows (budget 6-8)"
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"
    assert [row for row in ROWS if row.repark is None], (
        "at least one control row must assert plain equality — an all-disclosure corpus cannot "
        "tell a zone-blind engine from a zone-aware one"
    )
    column_rows = [row for row in ROWS if row.needs_column_view]
    assert {row.session_time_zone for row in column_rows} == {ZONE_NEW_YORK, ZONE_TOKYO}, (
        "the brief's recipe is year/month/day/hour over a tz-aware timestamp COLUMN under BOTH "
        "non-UTC session zones — a corpus of scalar literals alone cannot claim it"
    )


def test_current_timestamp_type_and_zone_disclosure() -> None:
    """``current_timestamp`` — the G1 row whose VALUE cannot be pinned, so its TYPE is.

    Recorded live Spark 4.1.2 under ``spark.sql.session.timeZone=America/New_York``:
    ``timestamp[us, tz=UTC]``, ``nullable=False``. repark returns a tz-NAIVE ``timestamp[ns]``,
    also non-nullable — the instant is right, the zone annotation and the unit are not, so a
    consumer that localizes the column silently shifts it.
    """
    session = _session_at(ZONE_NEW_YORK)
    field = session.sql("SELECT current_timestamp() AS now_ts").to_arrow().schema.field("now_ts")

    spark_recorded_type = pa.timestamp("us", "UTC")
    assert field.type == pa.timestamp("ns"), (
        "repark's current_timestamp is tz-naive timestamp[ns] today; if this changed, the "
        f"disclosure below must be revisited. {FIX}"
    )
    assert field.nullable is False, "both engines mark current_timestamp non-nullable"
    assert field.type != spark_recorded_type, (
        "repark and Spark have CONVERGED on current_timestamp's Arrow type — flip this test to "
        "an equality assertion and record the convergence."
    )


def test_session_timezone_conf_is_readable_back_and_defaults_to_utc() -> None:
    """The conf surface: the default, the builder round trip, and `conf.getAll`."""
    import repark

    default_session = repark.ReparkSession.builder.appName("tz-conf").getOrCreate()
    assert default_session.conf.get(SESSION_TIME_ZONE_KEY) == "UTC"
    assert default_session.conf.getAll[SESSION_TIME_ZONE_KEY] == "UTC"
    default_session.stop()

    configured = _session_at(ZONE_TOKYO)
    assert configured.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO
    assert configured.conf.getAll[SESSION_TIME_ZONE_KEY] == ZONE_TOKYO
    configured.stop()


def test_runtime_conf_set_of_the_session_zone_is_accepted_but_not_applied() -> None:
    """A runtime `conf.set` / `conf.unset` of the zone: accepted (drop-in), never a lying read.

    PySpark applies this key at runtime; repark resolves the zone once at session build. Refusing
    the call was tried first and reds a pinned Apache drop-in test
    (`test_create_dataframe_from_pandas_with_dst` sets it through PySpark's own `sql_conf`
    helper), so the call is accepted with a one-time disclosure — and the value is NOT stored, so
    `conf.get` keeps reporting the zone the live engine session actually has.

    The value is also NOT VALIDATED, because validation is the engine's and happens once at build.
    That is a knowing laxness (live Spark raises `[INVALID_CONF_VALUE.TIME_ZONE]` for a garbage
    zone here), so the garbage leg is pinned too — and the warning text is asserted to SAY that
    the value is unvalidated, rather than leaving a caller to infer it from a silent no-op.
    """
    import warnings

    from repark.session import session_time_zone as tz_module

    tz_module._runtime_session_time_zone_warned = False  # re-arm the once-per-process disclosure
    session = _session_at(ZONE_TOKYO)
    disclosure = "accepted for source compatibility but NOT applied"
    with pytest.warns(UserWarning, match=disclosure) as rec:
        session.conf.set(SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK)
    assert "its value is NOT validated" in str(rec[0].message), (
        "the disclosure must say the value is unvalidated, not only unapplied — that is the one "
        "point on which repark is laxer than PySpark here"
    )
    assert session.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO, (
        "an unapplied runtime set must never move the facade away from the engine's real zone"
    )

    # A zone the ENGINE would refuse at build is accepted here, silently after the first warning
    # (the disclosure is once per PROCESS). Pinned so the laxness cannot change unnoticed.
    with warnings.catch_warnings():
        warnings.simplefilter("error", UserWarning)  # a second warning would raise here
        session.conf.set(SESSION_TIME_ZONE_KEY, "Mars/Olympus_Mons")
    assert session.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO, (
        "an invalid runtime zone is neither validated nor stored — conf.get stays on the engine's"
    )

    session.conf.unset(SESSION_TIME_ZONE_KEY)
    assert session.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO, (
        "unset must not tombstone the zone into the default either — the session still has one"
    )
    assert session.conf.getAll[SESSION_TIME_ZONE_KEY] == ZONE_TOKYO
    session.stop()


def test_apache_sql_conf_context_manager_round_trips_the_session_zone() -> None:
    """The drop-in shape that drove the decision, exercised directly.

    PySpark's `sql_conf` helper reads the old value, sets a new one, and restores it. Every step
    must work on repark (accepted, warned, not applied) and the zone must be unchanged at the end
    — that is what keeps the pinned Apache test green.
    """
    import warnings

    import repark
    from repark.session import session_time_zone as tz_module

    tz_module._runtime_session_time_zone_warned = True  # disclosure already made; keep it quiet
    session = _session_at(ZONE_NEW_YORK)
    previous = session.conf.get(SESSION_TIME_ZONE_KEY, None)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", UserWarning)
        session.conf.set(SESSION_TIME_ZONE_KEY, "America/Los_Angeles")
        try:
            assert repark.ReparkSession.getActiveSession() is session
        finally:
            if previous is None:
                session.conf.unset(SESSION_TIME_ZONE_KEY)
            else:
                session.conf.set(SESSION_TIME_ZONE_KEY, previous)
    assert session.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_NEW_YORK
    session.stop()


def test_getorcreate_reuse_with_a_different_zone_warns_and_leaves_the_conf_alone() -> None:
    """The reuse path is the other way a facade conf could drift from the engine's real zone.

    `getOrCreate` folds facade-only builder keys into the live session's runtime conf. The session
    zone is an ENGINE knob fixed at build, so it is excluded from that fold: the active session is
    returned unchanged, the existing engine-knob warning fires, and `conf.get` still reports the
    zone the engine has.
    """
    import repark

    session = _session_at(ZONE_TOKYO)
    with pytest.warns(UserWarning, match="engine knobs are fixed at session build"):
        reused = (
            repark.ReparkSession.builder.appName("session-timezone-parity")
            .config(SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK)
            .getOrCreate()
        )
    assert reused is session, "reuse returns the active session, it does not rebuild"
    assert reused.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO
    session.stop()


def test_getorcreate_reuse_with_an_invalid_zone_warns_and_does_not_raise() -> None:
    """The DELIBERATE laxness on the reuse path (D-A1), pinned rather than described.

    Zone validity needs the engine's zone database, and this repo keeps exactly ONE validator —
    in the engine, at session build. On the reuse path no session is built, so a zone that would
    fail the build is neither validated nor applied: the engine-knob warning fires and `conf.get`
    still reports the live engine session's real zone. Live PySpark 4.1.2 raises here
    (`[INVALID_CONF_VALUE.TIME_ZONE]`), so repark is knowingly laxer on this one path — which is
    why it is a pin. If a future change starts raising (or starts swallowing more), this reds.

    Contrast `test_unknown_session_timezone_fails_loud_at_session_build`: the same garbage value
    on the BUILD path is refused loud.
    """
    import repark

    session = _session_at(ZONE_TOKYO)
    with pytest.warns(UserWarning, match="engine knobs are fixed at session build"):
        reused = (
            repark.ReparkSession.builder.appName("session-timezone-parity")
            .config(SESSION_TIME_ZONE_KEY, "Mars/Olympus_Mons")
            .getOrCreate()
        )
    assert reused is session, "reuse returns the active session; nothing is built, nothing raises"
    assert reused.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO, (
        "an unvalidated, unapplied zone must never move the facade off the engine's real zone"
    )
    assert reused.conf.getAll[SESSION_TIME_ZONE_KEY] == ZONE_TOKYO
    session.stop()


def test_padded_zone_is_normalized_so_conf_get_reports_the_engine_zone() -> None:
    """A padded builder value must not make `conf.get` report a string the engine trimmed away.

    `repark_core::SessionTimeZone::parse` trims before parsing, so the live session holds
    `Asia/Tokyo`. The facade normalizes the same way BEFORE storing (whitespace only — the engine
    stays the sole validator), so the two agree. Without the normalization `conf.get` returns
    `'  Asia/Tokyo \\t'` while the engine holds `Asia/Tokyo`: a facade/engine split-brain on the
    exact surface this unit claims to own.
    """
    import repark

    session = (
        repark.ReparkSession.builder.appName("tz-conf-padded")
        .config(SESSION_TIME_ZONE_KEY, "  Asia/Tokyo \t")
        .getOrCreate()
    )
    assert session.conf.get(SESSION_TIME_ZONE_KEY) == ZONE_TOKYO
    assert session.conf.getAll[SESSION_TIME_ZONE_KEY] == ZONE_TOKYO
    session.stop()


def test_whitespace_only_zone_still_fails_loud_at_session_build() -> None:
    """Normalization must not turn a refusal into a silent default.

    `'   '` trims to the empty string, which the ENGINE refuses naming the key — the same class as
    any other unresolvable zone. If the facade ever "helpfully" dropped a blank value instead, the
    session would build silently on `UTC` and this reds.
    """
    import repark
    from repark.errors import IllegalArgumentException

    with pytest.raises(IllegalArgumentException, match=SESSION_TIME_ZONE_KEY):
        (
            repark.ReparkSession.builder.appName("tz-conf-blank")
            .config(SESSION_TIME_ZONE_KEY, "   ")
            .getOrCreate()
        )


def test_unknown_session_timezone_fails_loud_at_session_build() -> None:
    """An unresolvable zone is refused by the ENGINE at ``getOrCreate`` — never accepted and then
    silently ignored at query time."""
    import repark
    from repark.errors import IllegalArgumentException

    with pytest.raises(IllegalArgumentException, match=SESSION_TIME_ZONE_KEY):
        (
            repark.ReparkSession.builder.appName("tz-conf-bad")
            .config(SESSION_TIME_ZONE_KEY, "Mars/Olympus_Mons")
            .getOrCreate()
        )
