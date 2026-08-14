"""Session-timezone differential rows (gap G1) + temporal edges (gap G16) — H-1a split A.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``) on 2026-08-10, with ``spark.sql.session.timeZone`` set to the
row's own zone. One SQL string per row runs on BOTH engines, so the recipe under test and the
recipe the oracle ran are the same string — nothing here is hand-computed.

**Most rows are now EQUALITY rows, and the flip is the evidence.** Split A shipped the
session-timezone *configuration surface* and recorded this corpus as DISCLOSURES, because repark
then extracted timestamp fields in the STORED zone: asserting ``repark == Spark`` would have been
red on arrival, and deleting the rows until the fix landed would have hidden the class the census
measured as a four-hour silent offset. Split B landed the extraction fix, so **thirteen** of those
disclosures are now plain equality rows (``repark=None``) — and that flip is precisely the
revert-red evidence the testing contract asks for: undo the fix and each one goes red.

**The rows that are still disclosures are a different class, named on each row.** TZ-4 PR-2
flipped the five zoneless-input / NTZ / CAST-str-round-trip rows to equality. Remaining
disclosures (if any) are named in
``test_the_extraction_class_converged_and_the_residue_is_named``. B-TZ-4
(``CAST(ts AS STRING)`` render) is pinned in ``test_timestamp_cast_parity.py``.
TZ-8 ``CAST(ts AS DATE)`` / ``to_date`` / ``datediff`` (rides CAST) rows in this
corpus are equality (R-4); ``last_day`` / ``date_add`` over TIMESTAMP stay residual.

The twelfth **was** ``CAST(TIMESTAMP AS BIGINT)`` returning nanoseconds (registry row TZ-5). It
CONVERGED when :data:`TZ5_FIX` landed and is now an equality row, which is the same revert-red
evidence the extraction flip is. The class's own per-entry-point corpus — SQL door and DataFrame
door, both signs of the floor edge, NULL, and the reverse direction — is
``test_timestamp_cast_parity.py``; this row stays here because it is where the class was first
measured.

A disclosure row pins BOTH halves — repark's actual output (value AND Arrow type) and the
recorded live-Spark output it differs from — and asserts that the two still differ. A row that
silently CONVERGES goes RED and forces the disclosure to be revisited rather than laundered into
"parity", the same discipline ``docs/testing.md`` puts on the live tier's disclosures.

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

**Entry points.** Most rows go through the facade ``sql()`` door — over scalar literals, and over
a real tz-aware timestamp COLUMN for the ``column_extract_*`` family. The
``dataframe_api_extract_*`` rows go through the OTHER facade spelling,
``df.select(F.year(...), ...)``, which crosses PyO3 as a standalone expression with no session
attached and is a distinct user entry point rather than a synonym for the SQL door. Together they
are the **facade** cell of the four-entry-point matrix the brief mandates. The other three cells
are pinned in Rust, against the same instants and the same expectations:

* native DataFrame API and Spark door — ``crates/repark-spark/tests/session_timezone.rs``
* ANSI door — ``crates/repark-sql/tests/session_timezone_ansi_door.rs`` (it lives in that crate
  because the crate-DAG policy allows ``repark-sql -> repark-spark`` as a dev edge and nothing
  the other way).
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.spark.session import ReparkSession

# The two non-UTC oracle zones. New York exercises a DST-observing zone west of UTC; Tokyo a
# fixed-offset zone east of UTC, so a sign error cannot pass both.
ZONE_NEW_YORK = "America/New_York"
ZONE_TOKYO = "Asia/Tokyo"

SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

# The fix that closed the extraction class, named by every row it moved so a reader of a red row
# knows what to look at.
FIX = "the session-timezone extraction fix (briefs/v2-engine-hardening.md, H-1a split B)"
# What an equality row is EVIDENCE for: it was a recorded disclosure until the fix landed, so
# reverting the fix reds it. That is the revert-red half of the testing contract, stated on the
# row rather than promised in a ledger.
REVERT = f"reverting {FIX} reds this row."
# The class the rows that did NOT fully converge still belong to. It is a different mechanism —
# repark's TIMESTAMP Arrow export carries no timezone — and the registry row says why it splits.
TZ4 = "registry row TZ-4 (repark's tz-naive TIMESTAMP Arrow export)"
# The INPUT half of the same representation gap: repark cannot tell a zoneless timestamp literal
# from a zone-suffixed one, because both land as `timestamp[ns]` holding the same ticks.
TZ7 = (
    "registry row TZ-7 (a zoneless TIMESTAMP input is read as UTC, not as a session-zone "
    "wall clock)"
)
# repark spells `TimestampNTZType` but maps it onto the same Arrow type as `TimestampType`.
TZ6 = "registry row TZ-6 (no TIMESTAMP_NTZ distinct from TIMESTAMP)"
# The cast-scaling fix that closed registry row TZ-5, named on the row it flipped to equality.
TZ5_FIX = (
    "the timestamp-cast epoch-seconds fix (task/tz5-cast-seconds-ledger.md; "
    "`repark_functions::timestamp_cast` + the analyzer's `Expr::Cast` arm)"
)
# What a TZ-7 row costs, stated once: these shapes AGREED with Spark before the extraction fix.
TZ7_REGRESSION = (
    "This row was GREEN against Spark before the extraction fix and is RED after it. That is the "
    "disclosed price of reading every TIMESTAMP as an instant (ledger D-B5): repark's planner "
    "gives a zoneless literal and a `…Z` one the SAME Arrow type holding the SAME ticks, so no "
    "rule at the extractor can separate them. Closing it means changing repark's TIMESTAMP "
    "representation, which is TZ-4's unit."
)


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

    ``needs_column_view`` / ``needs_naive_column_view`` / ``needs_ltz_and_ntz_view`` mark the rows
    whose SQL reads a COLUMN rather than a scalar literal; the runner registers the matching view
    on the session first.

    ``entry_point`` selects the facade SPELLING: ``"sql"`` runs ``session.sql(row.sql)``;
    ``"dataframe_api"`` runs :func:`dataframe_api_extraction` and ``sql`` is then documentation of
    the equivalent projection, not a string anything executes.
    """

    name: str
    gap: str
    session_time_zone: str
    sql: str
    spark: pa.Table
    repark: pa.Table | None
    note: str
    needs_column_view: bool = False
    needs_naive_column_view: bool = False
    needs_ltz_and_ntz_view: bool = False
    entry_point: str = "sql"


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


# ----- the NAIVE (zoneless) wall-clock column: the TZ-7 shape a migrated job hits by accident ----
#
# `createDataFrame` over naive `datetime` objects is the ordinary way a Python job builds a
# timestamp column, and BOTH engines type it as a plain default TIMESTAMP — no `TimestampNTZType`
# anywhere. Spark localizes each wall clock in `spark.sql.session.timeZone` (so `hour` reads back
# the digits it was given); repark stores the digits as UTC ticks, so `hour` comes back shifted.
NAIVE_COLUMN_VIEW = "naive_wall_clocks"
NAIVE_WALL_CLOCKS: tuple[dt.datetime, ...] = (
    dt.datetime(2024, 6, 15, 12, 0),
    dt.datetime(2024, 1, 1, 0, 30),
)


def register_naive_column_view(session: object) -> None:
    """Register :data:`NAIVE_COLUMN_VIEW` — a two-row NAIVE-datetime column — on either engine."""
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [(wall_clock,) for wall_clock in NAIVE_WALL_CLOCKS], ["ts"]
    )
    frame.createOrReplaceTempView(NAIVE_COLUMN_VIEW)


# ----- the LTZ / NTZ pair: the TZ-6 shape, measured rather than described -----------------------
LTZ_AND_NTZ_VIEW = "ltz_and_ntz"
LTZ_AND_NTZ_WALL_CLOCK = dt.datetime(2024, 6, 15, 12, 0)


def register_ltz_and_ntz_view(session: object) -> None:
    """Register one row typed EXPLICITLY as ``TimestampType`` beside ``TimestampNTZType``.

    Both engines are handed the identical schema through the identical spelling, so what the row
    measures is whether the engine can tell the two Spark types apart at all. Spark exports them
    as two DIFFERENT Arrow types carrying two different instants; repark exports one type twice.
    """
    types = session.__class__.__module__.split(".")[0]  # "pyspark" or "repark"
    if types == "pyspark":
        from pyspark.sql.types import (
            StructField,
            StructType,
            TimestampNTZType,
            TimestampType,
        )
    else:
        from repark.spark.types import (
            StructField,
            StructType,
            TimestampNTZType,
            TimestampType,
        )
    schema = StructType(
        [
            StructField("ltz", TimestampType(), True),
            StructField("ntz", TimestampNTZType(), True),
        ]
    )
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [(LTZ_AND_NTZ_WALL_CLOCK, LTZ_AND_NTZ_WALL_CLOCK)], schema
    )
    frame.createOrReplaceTempView(LTZ_AND_NTZ_VIEW)


# ----- the DataFrame-API spelling: the OTHER facade entry point --------------------------------
DATAFRAME_API_SPELLING = (
    "df.orderBy('ts').select(F.year('ts'), F.hour('ts'), "
    "F.date_format('ts', 'yyyy-MM-dd HH:mm'), F.date_trunc('day', 'ts'))"
)


def dataframe_api_extraction(session: object) -> pa.Table:
    """The four extractors through ``df.select(F...)`` — spelled ONCE, run on either engine.

    ``F.year(col)`` is a distinct user entry point from ``sql("SELECT year(ts)")``: on repark it
    builds a standalone expression that crosses PyO3 with no ``SessionContext`` attached, which is
    exactly the shape a registration-time session zone would miss. The Rust cell
    (``native_dataframe_api_extracts_in_the_session_zone``) pins the engine-side equivalent; this
    pins the spelling a user actually writes.
    """
    functions = _functions_module(session)
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [(instant,) for instant in COLUMN_INSTANTS], ["ts"]
    )
    projected = frame.orderBy("ts").select(
        functions.year("ts").alias("year_part"),
        functions.hour("ts").alias("hour_part"),
        functions.date_format("ts", "yyyy-MM-dd HH:mm").alias("rendered"),
        functions.date_trunc("day", "ts").alias("day_start"),
    )
    to_arrow = getattr(projected, "to_arrow", None) or projected.toArrow
    return to_arrow()  # type: ignore[no-any-return]


def _functions_module(session: object) -> object:
    """The ``functions`` module belonging to ``session``'s engine — PySpark's or repark's.

    The row must run the SAME recipe on both engines, and the only thing that legitimately differs
    between them is which package the ``F`` namespace comes from.
    """
    if session.__class__.__module__.split(".")[0] == "pyspark":
        from pyspark.sql import functions as spark_functions

        return spark_functions
    from repark.spark.sql import functions as repark_functions

    return repark_functions


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
        None,
        "the instant is 2023-12-31 23:30 in New York, so Spark's year is 2023 — and so is "
        f"repark's. Before {FIX} repark extracted in the stored (UTC) zone and answered 2024; "
        f"{REVERT}",
    ),
    TimeZoneRow(
        "month_of_instant_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT month(to_timestamp('2024-03-01T02:15:00Z')) AS month_part",
        _one_row([("month_part", _INT32, True)], {"month_part": 2}),
        None,
        "2024-02-29 21:15 in New York (a leap day) vs 2024-03-01 in the stored zone — the month "
        f"boundary moves with the session zone, and now moves on both engines. {REVERT}",
    ),
    TimeZoneRow(
        "day_of_instant_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT dayofmonth(to_timestamp('2024-06-15T03:00:00Z')) AS day_part",
        _one_row([("day_part", _INT32, True)], {"day_part": 14}),
        None,
        "the day-partition key a migrated job would write: 14 in the session zone, 15 in the "
        f"stored zone — repark writes 14 now. {REVERT}",
    ),
    TimeZoneRow(
        "hour_of_instant_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT hour(to_timestamp('2024-06-15T12:00:00Z')) AS hour_part",
        _one_row([("hour_part", _INT32, True)], {"hour_part": 8}),
        None,
        "the census's four-hour silent offset, isolated: EDT is UTC-4. This is the single row "
        f"the CRITICAL G1 finding was written about. {REVERT}",
    ),
    TimeZoneRow(
        "hour_of_instant_under_tokyo_session",
        "G1",
        ZONE_TOKYO,
        "SELECT hour(to_timestamp('2024-06-15T12:00:00Z')) AS hour_part",
        _one_row([("hour_part", _INT32, True)], {"hour_part": 21}),
        None,
        "the same instant east of UTC (+9). Before the fix repark answered 12 under BOTH session "
        "zones, which is what made this a session-zone bug rather than an offset-sign bug; the "
        f"pair now moves in opposite directions, as Spark's does. {REVERT}",
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
        None,
        "all three calendar fields move together across the year boundary (2024-01-01 01:30 in "
        f"Tokyo); repark reported 2023-12-31 for every one before the fix. {REVERT}",
    ),
    TimeZoneRow(
        "to_timestamp_of_zone_suffixed_string",
        "G1",
        ZONE_NEW_YORK,
        "SELECT to_timestamp('2024-03-10T01:30:00-05:00') AS ts",
        _one_row([("ts", pa.timestamp("us", "UTC"), True)], {"ts": _utc(2024, 3, 10, 6, 30)}),
        None,
        "the INSTANT agreed already (06:30Z). TZ-4 PR-1 closed the type half: repark now "
        "exports timestamp[us, tz=UTC] like Spark. Flip evidence — revert the to_timestamp "
        "µs+UTC wrap and this row reds.",
    ),
    TimeZoneRow(
        "dst_spring_forward_instant_hour",
        "G1",
        ZONE_NEW_YORK,
        "SELECT hour(to_timestamp('2024-03-10T07:00:00Z')) AS hour_part",
        _one_row([("hour_part", _INT32, True)], {"hour_part": 3}),
        None,
        "the spring-forward instant: 02:00-03:00 local does not exist on 2024-03-10 in New York, "
        "so the answer is 3 (EDT) and not the stored 7. A fixed-offset implementation of the "
        f"session zone would answer 2 here, so this row separates zone-aware from offset-aware. "
        f"{REVERT}",
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
        None,
        "fall-back: two distinct instants share local hour 1 (EDT then EST), so the answer is "
        "(1, 1). repark answered (5, 6) before the fix and never collapsed the repeated hour — "
        f"the row a dedup-by-hour job depends on. {REVERT}",
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
        None,
        "timestamp -> string -> timestamp: B-TZ-4 renders the session-zone wall "
        "(`2024-06-15 08:00:00` under New York); TZ-4 PR-2 localizes that zoneless string "
        "back in the session zone so the instant survives as timestamp[us, tz=UTC].",
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
        None,
        "the daily-rollup boundary. VALUE converged with H-1a; TZ-4 PR-1 closed the type half "
        "(date_trunc now returns timestamp[us, tz=UTC]). Flip evidence — revert the date_trunc "
        "return annotation and this row reds.",
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
        None,
        "the brief's own recipe, over a COLUMN: all four fields of both instants move to the "
        "session zone (2024-01-01T04:30Z is 2023-12-31 23:30 EST, so even the YEAR changes). "
        "repark answered the stored zone for every one before the fix — the row a partitioned "
        f"write would have got wrong for every row of a real table. {REVERT}",
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
        None,
        "the same column east of UTC (+9): the calendar fields happen to AGREE here and only the "
        "hour moves, which is why the pair is recorded — before the fix repark's answer was "
        "identical under both zones, so the New York row alone could have been misread as an "
        f"offset-sign bug rather than a session-zone one. {REVERT}",
        needs_column_view=True,
    ),
    # ----- the ZONELESS-INPUT family: what this unit does NOT fix, measured ----------------------
    # Every row above hands the engine a `…Z`-suffixed string, i.e. the case where "read every
    # TIMESTAMP as a UTC instant" is RIGHT. These rows are the same rule applied where it is
    # WRONG. A corpus that only carried the first kind could not tell the two apart, and that is
    # precisely how the class was over-claimed on its first pass.
    TimeZoneRow(
        "zoneless_timestamp_literal_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT hour(TIMESTAMP '2024-06-15 12:00:00') AS hour_part, "
        "year(TIMESTAMP '2024-01-01 00:30:00') AS year_part, "
        "dayofmonth(TIMESTAMP '2024-01-01 00:30:00') AS day_part, "
        "date_format(TIMESTAMP '2024-06-15 12:00:00', 'yyyy-MM-dd HH:mm') AS rendered",
        _one_row(
            [
                ("hour_part", _INT32, False),
                ("year_part", _INT32, False),
                ("day_part", _INT32, False),
                ("rendered", pa.string(), False),
            ],
            {
                "hour_part": 12,
                "year_part": 2024,
                "day_part": 1,
                "rendered": "2024-06-15 12:00",
            },
        ),
        _one_row(
            [
                ("hour_part", _INT32, True),
                ("year_part", _INT32, True),
                ("day_part", _INT32, True),
                ("rendered", pa.string(), True),
            ],
            {
                "hour_part": 12,
                "year_part": 2024,
                "day_part": 1,
                "rendered": "2024-06-15 12:00",
            },
        ),
        "VALUE-converged TZ-7 (session-zone wall clock). Residual: Spark types these "
        "extractor columns non-null; repark extractors stay nullable — not the TZ-7 class. "
        "Recorded Spark half keeps the live-4.1.2 non-null schema for the record driver.",
    ),
    TimeZoneRow(
        "zoneless_timestamp_input_spellings_under_tokyo_session",
        "G1",
        ZONE_TOKYO,
        "SELECT hour(to_timestamp('2024-06-15 12:00:00')) AS from_to_timestamp, "
        "hour(CAST('2024-06-15 12:00:00' AS TIMESTAMP)) AS from_cast",
        _one_row(
            [("from_to_timestamp", _INT32, True), ("from_cast", _INT32, True)],
            {"from_to_timestamp": 12, "from_cast": 12},
        ),
        None,
        "the other two zoneless spellings, east of UTC. TZ-4 PR-2 localizes both as a Tokyo "
        "wall clock so `hour` reads 12. Both spellings must move TOGETHER. Flip evidence.",
    ),
    TimeZoneRow(
        "naive_datetime_column_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT hour(ts) AS hour_part, year(ts) AS year_part, "
        f"date_format(ts, 'yyyy-MM-dd HH:mm') AS rendered FROM {NAIVE_COLUMN_VIEW} ORDER BY ts",
        _table(
            [
                ("hour_part", _INT32, True),
                ("year_part", _INT32, True),
                ("rendered", pa.string(), True),
            ],
            {
                "hour_part": [0, 12],
                "year_part": [2024, 2024],
                "rendered": ["2024-01-01 00:30", "2024-06-15 12:00"],
            },
        ),
        None,
        "the mainstream COLUMN shape: `createDataFrame` over naive `datetime` objects, typed as "
        "default TIMESTAMP. TZ-4 PR-2 localizes each wall in the session zone so `hour` reads "
        "the digits. Flip evidence.",
        needs_naive_column_view=True,
    ),
    TimeZoneRow(
        "timestamp_ntz_is_indistinguishable_from_timestamp",
        "G1",
        ZONE_NEW_YORK,
        f"SELECT ltz, ntz, hour(ltz) AS ltz_hour, hour(ntz) AS ntz_hour FROM {LTZ_AND_NTZ_VIEW}",
        _one_row(
            [
                ("ltz", pa.timestamp("us", "UTC"), True),
                ("ntz", pa.timestamp("us"), True),
                ("ltz_hour", _INT32, True),
                ("ntz_hour", _INT32, True),
            ],
            {
                "ltz": _utc(2024, 6, 15, 16, 0),
                "ntz": dt.datetime(2024, 6, 15, 12, 0),
                "ltz_hour": 12,
                "ntz_hour": 12,
            },
        ),
        None,
        "the same wall clock declared EXPLICITLY as `TimestampType` beside `TimestampNTZType`. "
        "TZ-4 PR-2: LTZ localizes to 16:00Z (`timestamp[us, tz=UTC]`); NTZ stays naive 12:00; "
        "`hour` reads 12 from both. Flip evidence.",
        needs_ltz_and_ntz_view=True,
    ),
    # ----- the DataFrame-API spelling of the facade cell -----------------------------------------
    TimeZoneRow(
        "dataframe_api_extract_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        DATAFRAME_API_SPELLING,
        _table(
            [
                ("year_part", _INT32, True),
                ("hour_part", _INT32, True),
                ("rendered", pa.string(), True),
                ("day_start", pa.timestamp("us", "UTC"), True),
            ],
            {
                "year_part": [2023, 2024],
                "hour_part": [23, 8],
                "rendered": ["2023-12-31 23:30", "2024-06-15 08:00"],
                "day_start": [_utc(2023, 12, 31, 5, 0), _utc(2024, 6, 15, 4, 0)],
            },
        ),
        None,
        f"the OTHER facade entry point, at the spelling a user writes: `F.year(col)` builds a "
        f"standalone expression with no session attached, so a session zone baked in at "
        f"REGISTRATION would reach `sql()` and miss this path entirely. VALUE converged with "
        f"H-1a; TZ-4 PR-1 closed the date_trunc type half. "
        f"{REVERT}",
        entry_point="dataframe_api",
    ),
    TimeZoneRow(
        "dataframe_api_extract_under_tokyo_session",
        "G1",
        ZONE_TOKYO,
        DATAFRAME_API_SPELLING,
        _table(
            [
                ("year_part", _INT32, True),
                ("hour_part", _INT32, True),
                ("rendered", pa.string(), True),
                ("day_start", pa.timestamp("us", "UTC"), True),
            ],
            {
                "year_part": [2024, 2024],
                "hour_part": [13, 21],
                "rendered": ["2024-01-01 13:30", "2024-06-15 21:00"],
                "day_start": [_utc(2023, 12, 31, 15, 0), _utc(2024, 6, 14, 15, 0)],
            },
        ),
        None,
        f"the same spelling east of UTC, so the DataFrame-API cell is pinned under BOTH non-UTC "
        f"zones rather than under one. VALUE converged with H-1a; TZ-4 PR-1 closed the "
        f"date_trunc type half. {REVERT}",
        entry_point="dataframe_api",
    ),
    # ----- TZ-8: CAST(ts AS DATE) / to_date read the session zone -------------------------------
    TimeZoneRow(
        "timestamp_to_date_cast_and_to_date_under_new_york_session",
        "G1",
        ZONE_NEW_YORK,
        "SELECT CAST(to_timestamp('2024-06-15T03:00:00Z') AS DATE) AS from_cast, "
        "to_date(to_timestamp('2024-06-15T03:00:00Z')) AS from_to_date, "
        "datediff(to_timestamp('2024-06-15T03:00:00Z'), DATE '2024-06-01') AS day_span",
        _one_row(
            [
                ("from_cast", pa.date32(), True),
                ("from_to_date", pa.date32(), True),
                ("day_span", _INT32, True),
            ],
            {
                "from_cast": dt.date(2024, 6, 14),
                "from_to_date": dt.date(2024, 6, 14),
                "day_span": 13,
            },
        ),
        None,
        "TZ-8: the instant is 23:00 EDT on the 14th. CAST and to_date must move TOGETHER. "
        "datediff rides CAST(ts AS DATE) (SparkDateDiff::simplify) so it is 13, not 14. "
        "Recorded Spark 4.1.2 (V-3 handoff + R-4 live table).",
    ),
    TimeZoneRow(
        "timestamp_to_date_cast_and_to_date_under_utc_session",
        "G1",
        "UTC",
        "SELECT CAST(to_timestamp('2024-06-15T03:00:00Z') AS DATE) AS from_cast, "
        "to_date(to_timestamp('2024-06-15T03:00:00Z')) AS from_to_date",
        _one_row(
            [("from_cast", pa.date32(), True), ("from_to_date", pa.date32(), True)],
            {
                "from_cast": dt.date(2024, 6, 15),
                "from_to_date": dt.date(2024, 6, 15),
            },
        ),
        None,
        "TZ-8 UTC control: the session-zone date equals the stored date, so a sign error "
        "cannot hide behind a New York-only pin.",
    ),
    TimeZoneRow(
        "timestamp_to_date_crosses_forward_under_tokyo_session",
        "G1",
        ZONE_TOKYO,
        "SELECT CAST(to_timestamp('2023-12-31T16:30:00Z') AS DATE) AS local_date",
        _one_row([("local_date", pa.date32(), True)], {"local_date": dt.date(2024, 1, 1)}),
        None,
        "TZ-8 east-of-UTC midnight crossing: 01:30 JST on 2024-01-01. The NY row crosses "
        "BACKWARD; this one crosses FORWARD.",
    ),
    TimeZoneRow(
        "ntz_to_date_is_session_zone_independent",
        "G1",
        ZONE_NEW_YORK,
        f"SELECT CAST(ntz AS DATE) AS from_cast, to_date(ntz) AS from_to_date "
        f"FROM {LTZ_AND_NTZ_VIEW}",
        _one_row(
            [("from_cast", pa.date32(), True), ("from_to_date", pa.date32(), True)],
            {
                "from_cast": dt.date(2024, 6, 15),
                "from_to_date": dt.date(2024, 6, 15),
            },
        ),
        None,
        "TZ-8 NTZ: the stored wall is 2024-06-15 12:00 regardless of session zone. "
        "CAST and to_date must not apply New York.",
        needs_ltz_and_ntz_view=True,
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
        None,
        "a negative epoch instant 30 minutes before 1970: the calendar fields always agreed and "
        "the HOUR did not (18 EST vs the stored 23) — sign handling was fine, the zone was not, "
        f"and the zone is what the fix moved. {REVERT}",
    ),
    TimeZoneRow(
        "pre_1970_timestamp_cast_to_bigint",
        "G16",
        ZONE_NEW_YORK,
        "SELECT CAST(to_timestamp('1969-12-31T23:30:00Z') AS BIGINT) AS epoch_value",
        _one_row([("epoch_value", pa.int64(), True)], {"epoch_value": -1800}),
        None,
        "a SEPARATE divergence this unit surfaced and did NOT fix — casting TIMESTAMP to BIGINT "
        "yielded epoch SECONDS in Spark and epoch NANOSECONDS in repark (a 10^9 factor, correctly "
        "signed before 1970). It was recorded here as a disclosure so the temporal edge was not "
        f"silently green, and it CONVERGED when {TZ5_FIX} landed: repark now scales the instant to "
        "seconds under every numeric cast target. This row is now the flip evidence — reverting "
        "that fix reds it. The class's own per-entry-point corpus is "
        "`test_timestamp_cast_parity.py`.",
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
        None,
        "the instant is 2024-01-01 00:00 in Tokyo, so the year start is that same instant — and "
        "repark now returns it, where before the fix it truncated the stored 2023-12-31 and "
        "landed a whole year earlier. VALUE converged with H-1a; TZ-4 PR-1 closed the type half.",
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
        None,
        "extraction and RENDERING move together on both engines now. Before the fix they moved "
        "together in the WRONG zone, which is the nastiest shape this class has: a formatted "
        "partition path and an extracted partition key that agree with each other and disagree "
        f"with Spark are self-consistently wrong. {REVERT}",
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
        None,
        "the leap day itself: the instant is 2024-02-28 in New York, so before the fix a "
        f"leap-day filter selected different rows on the two engines. {REVERT}",
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
        "extraction must NOT move with the session zone. It was green before the fix and is green "
        "after it, UNCHANGED — which is the half of the claim an all-disclosure corpus could "
        "never make. Its Rust sibling "
        "(crates/repark-spark/tests/session_timezone.rs::date_arguments_never_move_with_the_"
        "session_zone) caught a real over-reach during the fix: an earlier draft of the coercion "
        "path was not idempotent under DataFusion's re-analysis and rendered this DATE a day "
        "early.",
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
        "value AND date32 type, and UNCHANGED by the fix. It is the pin that catches a fix that "
        "pushed the session zone into the DATE path.",
    ),
    # ----- COMPOSITION: a DATE or string through `date_trunc` and back into an extractor ---------
    # `date_trunc`'s output is tz-naive, and the extractor coercion reads a tz-naive timestamp as
    # a UTC instant. So `date_trunc` must emit an INSTANT on every path, including the DATE/string
    # path — which is also what Spark does, because its DATE -> TIMESTAMP promotion is a
    # session-zone localization. A first draft of this unit emitted LOCAL wall-clock ticks there
    # instead, and every one of these rows was a whole calendar day wrong. The single-hop DATE
    # control row above cannot see it: it never chains two shims.
    TimeZoneRow(
        "date_trunc_of_a_date_composed_under_new_york_session",
        "G16",
        ZONE_NEW_YORK,
        "SELECT year(date_trunc('day', DATE '2024-01-01')) AS year_part, "
        "month(date_trunc('day', DATE '2024-01-01')) AS month_part, "
        "dayofmonth(date_trunc('day', DATE '2024-01-01')) AS day_part, "
        "hour(date_trunc('day', DATE '2024-01-01')) AS hour_part, "
        "date_format(date_trunc('day', DATE '2024-01-01'), 'yyyy-MM-dd HH:mm') AS rendered",
        _one_row(
            [
                ("year_part", _INT32, True),
                ("month_part", _INT32, True),
                ("day_part", _INT32, True),
                ("hour_part", _INT32, True),
                ("rendered", pa.string(), True),
            ],
            {
                "year_part": 2024,
                "month_part": 1,
                "day_part": 1,
                "hour_part": 0,
                "rendered": "2024-01-01 00:00",
            },
        ),
        None,
        "a DATE truncated to a day and then read back must be that same day at local midnight. "
        "West of UTC a `date_trunc` that emits local wall-clock ticks under a tz-naive type sends "
        "every field here back one calendar day (2023-12-31 19:00) — the daily-rollup key of a "
        "migrated job. Equality on both engines is the claim; the single-hop DATE control row is "
        "green either way, which is why this row exists.",
    ),
    TimeZoneRow(
        "date_trunc_of_a_string_composed_under_tokyo_session",
        "G16",
        ZONE_TOKYO,
        "SELECT year(date_trunc('day', '2024-01-01')) AS year_part, "
        "dayofmonth(date_trunc('day', '2024-01-01')) AS day_part, "
        "date_format(date_trunc('day', '2024-01-01'), 'yyyy-MM-dd HH:mm') AS rendered",
        _one_row(
            [
                ("year_part", _INT32, True),
                ("day_part", _INT32, True),
                ("rendered", pa.string(), True),
            ],
            {"year_part": 2024, "day_part": 1, "rendered": "2024-01-01 00:00"},
        ),
        None,
        "the STRING twin of the row above, east of UTC: a string argument takes the same zone-free "
        "path into `date_trunc`, so both must be localized the same way. The Tokyo half is what "
        "separates 'the promotion is a localization' from 'the promotion happens to be a no-op'.",
    ),
    TimeZoneRow(
        "date_trunc_across_the_fall_back_hour_under_new_york_session",
        "G16",
        ZONE_NEW_YORK,
        "SELECT date_trunc('hour', to_timestamp('2024-11-03T05:30:00Z')) AS before_fall_back, "
        "date_trunc('hour', to_timestamp('2024-11-03T06:30:00Z')) AS after_fall_back, "
        "date_trunc('minute', to_timestamp('2024-11-03T06:30:40Z')) AS truncated_minute",
        _one_row(
            [
                ("before_fall_back", pa.timestamp("us", "UTC"), True),
                ("after_fall_back", pa.timestamp("us", "UTC"), True),
                ("truncated_minute", pa.timestamp("us", "UTC"), True),
            ],
            {
                "before_fall_back": _utc(2024, 11, 3, 5, 0),
                "after_fall_back": _utc(2024, 11, 3, 6, 0),
                "truncated_minute": _utc(2024, 11, 3, 6, 30),
            },
        ),
        None,
        "truncating inside the REPEATED hour. Spark truncates with `ZonedDateTime.truncatedTo`, "
        "which preserves the source instant's offset, so the two distinct instants of local hour "
        "1 stay distinct and the minute row does not move at all. An implementation that "
        "re-resolves the truncated local time to the earliest valid offset collapses the pair "
        "onto 05:00Z and puts the minute row an hour early — which is what the first draft of "
        "this unit did. VALUE converged with H-1a; TZ-4 PR-1 closed the type half.",
    ),
    TimeZoneRow(
        "timestamp_to_date_epoch_under_new_york_session",
        "G16",
        ZONE_NEW_YORK,
        "SELECT CAST(to_timestamp('1970-01-01T00:00:00Z') AS DATE) AS from_cast, "
        "to_date(to_timestamp('1970-01-01T00:00:00Z')) AS from_to_date",
        _one_row(
            [("from_cast", pa.date32(), True), ("from_to_date", pa.date32(), True)],
            {
                "from_cast": dt.date(1969, 12, 31),
                "from_to_date": dt.date(1969, 12, 31),
            },
        ),
        None,
        "TZ-8 epoch edge: 00:00Z is 1969-12-31 19:00 EST. Sign handling of a negative-of-epoch "
        "calendar date, not just a modern DST offset.",
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
    if row.needs_naive_column_view:
        register_naive_column_view(session)
    if row.needs_ltz_and_ntz_view:
        register_ltz_and_ntz_view(session)
    if row.entry_point == "dataframe_api":
        return dataframe_api_extraction(session)
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

    The brief's opening budgets were G1 10-14 and G16 6-8, and the rule is that a size pin moves
    only when the fix FORCES it. It did. An adversarial panel measured three wrong-answer families
    against live Spark 4.1.2 that this corpus was structurally blind to — every original row hands
    the engine a ``…Z``-suffixed string, i.e. only the shapes where reading a TIMESTAMP as a UTC
    instant is right — so the budget is now G1 19 + ``current_timestamp`` and G16 10. What each
    added row buys is named on the row; the counts are pinned here so growth stays a decision.
    """
    g1 = [row for row in ROWS if row.gap == "G1"]
    g16 = [row for row in ROWS if row.gap == "G16"]
    assert len(g1) + 1 == 24, (
        "G1: 11 scalar-literal + 2 column-path + 4 zoneless-input/NTZ + 2 DataFrame-API rows "
        "+ 4 TZ-8 CAST/to_date rows, plus the current_timestamp row"
    )
    assert len(g16) == 11, (
        "G16: pre-1970, year-boundary and leap-day rows, plus 2 date_trunc COMPOSITION rows, "
        "the DST fall-back truncation row, and the TZ-8 epoch CAST/to_date row"
    )
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
    dataframe_rows = [row for row in ROWS if row.entry_point == "dataframe_api"]
    assert {row.session_time_zone for row in dataframe_rows} == {ZONE_NEW_YORK, ZONE_TOKYO}, (
        "the facade cell has TWO user entry points (`sql()` and `df.select(F...)`); pinning only "
        "the first would leave the most-used spelling on a Rust proxy"
    )
    zoneless_names = {
        "zoneless_timestamp_literal_under_new_york_session",
        "zoneless_timestamp_input_spellings_under_tokyo_session",
        "naive_datetime_column_under_new_york_session",
    }
    zoneless_rows = [row for row in ROWS if row.name in zoneless_names]
    assert len(zoneless_rows) == 3, "the three TZ-7 zoneless-input rows must stay in the corpus"
    assert all(
        row.name != "zoneless_timestamp_literal_under_new_york_session" or row.repark is not None
        for row in zoneless_rows
    ), "literal row is value-converged; extractor nullability is a residual disclosure"
    assert all(
        row.name == "zoneless_timestamp_literal_under_new_york_session" or row.repark is None
        for row in zoneless_rows
    ), "to_timestamp / CAST / naive-column TZ-7 rows are equality"


def test_the_extraction_class_converged_and_the_residue_is_named() -> None:
    """The SHAPE of the corpus after the fix, pinned so a later edit cannot quietly reopen it.

    Two failure modes this catches, neither of which the per-row assertions can:

    * an equality row silently reverting to a disclosure (someone "fixes a red row" by pinning
      repark's new wrong answer instead of the engine) — the equality count drops;
    * a disclosure being added back into the EXTRACTION class rather than into the class that
      actually owns it. Every remaining disclosure is named here, one by one, with the registry
      row that keeps it open, so admitting a new one is an edit a reviewer sees.
    """
    equality = {row.name for row in ROWS if row.repark is None}
    disclosures = {row.name for row in ROWS if row.repark is not None}
    assert disclosures == {
        # VALUE-converged TZ-7; residual is extractor nullability (Spark non-null).
        "zoneless_timestamp_literal_under_new_york_session",
    }, (
        "TZ-4 PR-2 flipped TZ-6 / two TZ-7 spellings / CAST-str-round-trip; leftover "
        f"{sorted(disclosures)} must be named here with their registry row"
    )
    assert len(equality) == 33, (
        "28 after TZ-4 PR-2, plus 5 TZ-8 CAST/to_date equality rows (NY, UTC, Tokyo, NTZ, epoch)"
    )


def test_current_timestamp_type_and_zone_disclosure() -> None:
    """``current_timestamp`` — the G1 row whose VALUE cannot be pinned, so its TYPE is.

    Recorded live Spark 4.1.2 under ``spark.sql.session.timeZone=America/New_York``:
    ``timestamp[us, tz=UTC]``, ``nullable=False``. TZ-4 PR-1 aligned SQL ``current_timestamp``
    to that wire type (copy of the ``F.current_timestamp`` µs+UTC cast).
    """
    session = _session_at(ZONE_NEW_YORK)
    field = session.sql("SELECT current_timestamp() AS now_ts").to_arrow().schema.field("now_ts")

    spark_recorded_type = pa.timestamp("us", "UTC")
    assert field.type == spark_recorded_type, (
        f"TZ-4 PR-1: SQL current_timestamp is timestamp[us, tz=UTC] like Spark; got {field.type}"
    )
    assert field.nullable is False, "both engines mark current_timestamp non-nullable"


def test_dataframe_api_timestamp_to_date_reads_the_session_zone() -> None:
    """The OTHER facade spelling of TZ-8: ``F.col.cast('date')`` and ``F.to_date``.

    SQL CAST / ``to_date`` live in :data:`ROWS`. This pin is the PyO3 standalone-expression
    path (no SQL string) under New York, so a registration-time zone would miss it.
    """
    from repark.spark.sql import functions as functions

    session = _session_at(ZONE_NEW_YORK)
    try:
        frame = session.createDataFrame([(_utc(2024, 6, 15, 3, 0),)], ["ts"])
        projected = frame.select(
            functions.col("ts").cast("date").alias("from_cast"),
            functions.to_date("ts").alias("from_to_date"),
        )
        table = projected.to_arrow()
        expected = _one_row(
            [("from_cast", pa.date32(), True), ("from_to_date", pa.date32(), True)],
            {
                "from_cast": dt.date(2024, 6, 14),
                "from_to_date": dt.date(2024, 6, 14),
            },
        )
        assert_frames_equal(table, expected)
    finally:
        session.stop()


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

    from repark.spark.session import session_time_zone as tz_module

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
    from repark.spark.session import session_time_zone as tz_module

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
