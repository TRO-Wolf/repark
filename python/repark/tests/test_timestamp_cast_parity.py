"""``CAST(TIMESTAMP AS <numeric>)`` differential rows — the facade cell of registry row TZ-5.

**The class.** Apache Spark's ``Cast(TimestampType, LongType)`` is the **floor of epoch SECONDS**.
repark stored timestamps as nanosecond ticks and let DataFusion's cast reinterpret the raw value,
so ``CAST(ts AS BIGINT)`` came back a factor of 10⁹ too large — correctly signed, plausibly
shaped, and wrong. The engine fix is ``repark_functions::timestamp_cast`` plus the analyzer's
``Expr::Cast`` arm; this module is the facade evidence for it.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``) on 2026-08-11, with ``spark.sql.session.timeZone`` set to the
row's own zone — the same basis ``test_session_timezone_parity.py`` was recorded on. One recipe
per row runs on BOTH engines, so the recipe under test and the recipe the oracle ran are the same
code; nothing here is hand-computed.

**Why a corpus of its own, beside the timezone one.** The class was first measured as a single
disclosure row inside ``test_session_timezone_parity.py``
(``pre_1970_timestamp_cast_to_bigint``), which is where it stays as the flip evidence. But the
class is **zone-independent** — probed under ``America/New_York``, ``Asia/Tokyo`` and ``UTC``, a
cast reads the instant and never a wall clock — so its own rows do not belong in a corpus whose
budget documents timezone semantics. They live here, and the timezone corpus keeps exactly the one
row that recorded the divergence.

**The floor edge is why half these rows are negative.** Spark uses ``Math.floorDiv``. Truncation
toward zero — what an arrow ``Timestamp(Second)`` cast hop gives, and the plausible way to write
this fix — agrees with Spark on every positive instant and on every whole negative second. It
disagrees only on a **negative fractional** second. So ``-0.5 s → -1`` and ``-1.25 s → -2`` are the
two rows that separate the real fix from the plausible one, and the positive fractional rows are
the other half of that fence, so the fix cannot be "always subtract one".

**Entry points.** Three facade spellings, because a claim tested through one says nothing about
the others (``docs/testing.md`` "Divergence-class claims"):

* ``"sql"`` — ``session.sql("SELECT CAST(ts AS BIGINT)")``, the Spark-dialect door;
* ``"dataframe_api"`` — ``df.select(F.col("ts").cast("long"))`` over a real tz-aware COLUMN, which
  crosses PyO3 as a standalone ``Expr::Cast`` with no SQL string anywhere;
* ``"expr"`` — ``F.expr("CAST(... AS BIGINT)")``, the third spelling a migrated job reaches for.

The engine-side cells are pinned in Rust against the same instants and the same expectations:
``crates/repark-spark/tests/timestamp_cast_seconds.rs`` (Spark door + native ``DataFrame`` API)
and ``crates/repark-sql/tests/timestamp_cast_ansi_door.rs`` (ANSI door).

**Rows assert on the Arrow path** (``to_arrow``) through the parity comparator, so schema name,
Arrow type and nullability are part of every assertion — never ``show``.

**Re-deriving the goldens (record mode).** The driver that recorded every ``spark`` half is
committed beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_timestamp_cast_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe under the row's own zone, so
the recorded golden and the asserted recipe cannot drift apart. It needs a JVM + ``pyspark``
(``uv sync --extra record``) and is never collected by pytest.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.session import ReparkSession

ZONE_NEW_YORK = "America/New_York"
ZONE_TOKYO = "Asia/Tokyo"
ZONE_UTC = "UTC"

SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

# The engine change every equality row below is evidence for: revert it and the row goes red.
FIX = (
    "the timestamp-cast epoch-seconds fix (task/tz5-cast-seconds-ledger.md; "
    "`repark_functions::timestamp_cast` + the analyzer's `Expr::Cast` arm)"
)
REVERT = f"reverting {FIX} reds this row."


def _one_row(fields: list[tuple[str, pa.DataType, bool]], values: dict[str, object]) -> pa.Table:
    """Build the single-row Arrow table a recorded golden describes (name, type, nullability)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array([values[name]], kind) for name, kind, _ in fields}, schema)


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the multi-row Arrow table a recorded golden describes."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


@dataclass(frozen=True)
class TimestampCastRow:
    """One differential row: a recipe, the recorded live-Spark table, and repark's own.

    ``repark is None`` means the engines AGREE — the row is a plain equality assertion.
    ``repark is not None`` means the row is a DISCLOSURE: repark's actual output is pinned, and a
    convergence onto the recorded Spark output is detected and reported as one.

    ``entry_point`` selects the facade SPELLING: ``"sql"`` runs ``session.sql(row.sql)``;
    ``"dataframe_api"`` runs :func:`dataframe_api_cast_projection`; ``"expr"`` runs
    :func:`expr_door_projection`. For the two non-SQL spellings ``sql`` is documentation of the
    equivalent projection, not a string anything executes.
    """

    name: str
    session_time_zone: str
    sql: str
    spark: pa.Table
    repark: pa.Table | None
    note: str
    entry_point: str = "sql"


# ==================================================================================================
# The instants — RFC-3339 strings, so an expectation is checkable without epoch arithmetic
# ==================================================================================================

WHOLE_BEFORE_EPOCH = "1969-12-31T23:30:00Z"  # -1800 s exactly
HALF_SECOND_BEFORE_EPOCH = "1969-12-31T23:59:59.5Z"  # -0.5 s: floor -1, truncation 0
QUARTER_PAST_BEFORE_EPOCH = "1969-12-31T23:59:58.75Z"  # -1.25 s: floor -2, truncation -1
FRACTION_AFTER_EPOCH = "1970-01-01T00:00:00.75Z"  # +0.75 s: floor 0
EPOCH_ZERO = "1970-01-01T00:00:00Z"
MODERN_INSTANT = "2024-06-15T12:00:00Z"
MODERN_WITH_FRACTION = "2024-06-15T12:00:01.999999Z"


def _cast_sql(instant: str, target: str) -> str:
    """``SELECT CAST(<instant> AS <target>) AS epoch_value`` — the SQL-door recipe, spelled once."""
    return f"SELECT CAST(to_timestamp('{instant}') AS {target}) AS epoch_value"


_INT64 = pa.int64()


def _epoch(target_type: pa.DataType, value: object) -> pa.Table:
    """The one-column ``epoch_value`` golden every SQL-door row below describes."""
    return _one_row([("epoch_value", target_type, True)], {"epoch_value": value})


# ==================================================================================================
# The DataFrame-API spelling: `F.col("ts").cast(...)` over a real tz-aware COLUMN
# ==================================================================================================

# `createDataFrame` over tz-aware `datetime` objects is how a Python job builds a timestamp column,
# and BOTH engines infer an instant-typed TIMESTAMP for it. The three instants are the whole
# negative second, the negative FRACTIONAL second (the floor edge), and a modern sub-microsecond
# one, so a single frame exercises both signs of the rounding rule.
DATAFRAME_INSTANTS: tuple[dt.datetime, ...] = (
    dt.datetime(1969, 12, 31, 23, 30, tzinfo=dt.UTC),
    dt.datetime(1969, 12, 31, 23, 59, 59, 500000, tzinfo=dt.UTC),
    dt.datetime(2024, 6, 15, 12, 0, 1, 999999, tzinfo=dt.UTC),
)

DATAFRAME_API_SPELLING = (
    "df.orderBy('ts').select(F.col('ts').cast('long'), F.col('ts').cast('int'), "
    "F.col('ts').cast('double'))"
)


def _functions_module(session: object) -> object:
    """The ``functions`` module belonging to ``session``'s engine — PySpark's or repark's.

    The row must run the SAME recipe on both engines, and the only thing that legitimately differs
    between them is which package the ``F`` namespace comes from.
    """
    if session.__class__.__module__.split(".")[0] == "pyspark":
        from pyspark.sql import functions as spark_functions

        return spark_functions
    from repark.sql import functions as repark_functions

    return repark_functions


def _to_arrow(frame: object) -> pa.Table:
    """``to_arrow`` on repark, ``toArrow`` on PySpark — the export path both engines expose."""
    export = getattr(frame, "to_arrow", None) or frame.toArrow  # type: ignore[attr-defined]
    return export()  # type: ignore[no-any-return]


def dataframe_api_cast_projection(session: object) -> pa.Table:
    """The three numeric cast targets through ``df.select(F.col(...).cast(...))``, on either engine.

    ``F.col("ts").cast("long")`` is a distinct user entry point from ``sql("SELECT CAST(...)")``:
    on repark it builds a standalone ``Expr::Cast`` that crosses PyO3 with no SQL string, which is
    exactly the shape an analyzer rewrite keyed to the SQL planner would miss.
    """
    functions = _functions_module(session)
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [(instant,) for instant in DATAFRAME_INSTANTS], ["ts"]
    )
    projected = frame.orderBy("ts").select(
        functions.col("ts").cast("long").alias("epoch_long"),
        functions.col("ts").cast("int").alias("epoch_int"),
        functions.col("ts").cast("double").alias("epoch_double"),
    )
    return _to_arrow(projected)


EXPR_DOOR_SPELLING = "df.select(F.expr(\"CAST(to_timestamp('…') AS BIGINT)\"))"


def expr_door_projection(session: object) -> pa.Table:
    """The floor edge through ``F.expr`` — the third facade spelling, over a one-row frame.

    The expression is SELF-CONTAINED (it builds its own instant) rather than referencing ``ts``:
    repark's ``F.expr`` resolves a column reference eagerly against an empty schema and refuses
    one, which is an ``F.expr`` binding gap recorded as a residual in the unit ledger — not part
    of this class, and not something a cast corpus should quietly depend on.
    """
    functions = _functions_module(session)
    frame = session.createDataFrame([(1,)], ["one"])  # type: ignore[attr-defined]
    projected = frame.select(
        functions.expr(f"CAST(to_timestamp('{HALF_SECOND_BEFORE_EPOCH}') AS BIGINT)").alias(
            "epoch_value"
        )
    )
    return _to_arrow(projected)


# ==================================================================================================
# The rows
# ==================================================================================================

ROWS: list[TimestampCastRow] = [
    # ----- the charged class: whole instants either side of 1970 --------------------------------
    TimestampCastRow(
        "pre_1970_whole_second_to_bigint",
        ZONE_NEW_YORK,
        _cast_sql(WHOLE_BEFORE_EPOCH, "BIGINT"),
        _epoch(_INT64, -1800),
        None,
        "the row the class was first measured on: repark answered -1800000000000 (nanoseconds) "
        f"where Spark answers -1800 (seconds). {REVERT}",
    ),
    TimestampCastRow(
        "modern_whole_second_to_bigint",
        ZONE_NEW_YORK,
        _cast_sql(MODERN_INSTANT, "BIGINT"),
        _epoch(_INT64, 1718452800),
        None,
        "the same class after 1970 — a sign-only fix would pass the pre-1970 row and fail here. "
        f"{REVERT}",
    ),
    TimestampCastRow(
        "epoch_zero_to_bigint",
        ZONE_NEW_YORK,
        _cast_sql(EPOCH_ZERO, "BIGINT"),
        _epoch(_INT64, 0),
        None,
        "the CONTROL row: zero is the one input a 10^9 scaling error cannot get wrong, so a "
        "corpus without it could not tell a fixed engine from a broken one that returns 0.",
    ),
    # ----- THE floor edge: negative fractional seconds -------------------------------------------
    TimestampCastRow(
        "negative_fractional_second_floors_to_minus_one",
        ZONE_NEW_YORK,
        _cast_sql(HALF_SECOND_BEFORE_EPOCH, "BIGINT"),
        _epoch(_INT64, -1),
        None,
        "the floor edge. Spark uses Math.floorDiv, so half a second BEFORE the epoch is -1; "
        "truncation toward zero — what an arrow Timestamp(Second) cast hop gives — answers 0. "
        f"This row is why the scaling lives in a UDF. {REVERT}",
    ),
    TimestampCastRow(
        "negative_one_and_a_quarter_seconds_floors_to_minus_two",
        ZONE_NEW_YORK,
        _cast_sql(QUARTER_PAST_BEFORE_EPOCH, "BIGINT"),
        _epoch(_INT64, -2),
        None,
        "the floor edge past one whole second: -1.25 s is -2, not -1. A fix that special-cased "
        f"the sub-second range would pass the -0.5 s row and fail this one. {REVERT}",
    ),
    TimestampCastRow(
        "positive_fractional_second_floors_to_zero",
        ZONE_NEW_YORK,
        _cast_sql(FRACTION_AFTER_EPOCH, "BIGINT"),
        _epoch(_INT64, 0),
        None,
        "the other half of the floor fence: after the epoch, floor and truncation agree, so the "
        f"fix cannot be 'always subtract one'. {REVERT}",
    ),
    TimestampCastRow(
        "modern_sub_microsecond_floors_down",
        ZONE_NEW_YORK,
        _cast_sql(MODERN_WITH_FRACTION, "BIGINT"),
        _epoch(_INT64, 1718452801),
        None,
        "a present-day instant with a sub-microsecond fraction floors to the second below — the "
        "case an f64 seconds intermediate cannot be trusted with (f64 resolves ~2e-7 s at this "
        f"magnitude), which is why the integer path uses exact i64 arithmetic. {REVERT}",
    ),
    # ----- NULL ---------------------------------------------------------------------------------
    TimestampCastRow(
        "null_timestamp_to_bigint_is_null",
        ZONE_NEW_YORK,
        "SELECT CAST(CAST(NULL AS TIMESTAMP) AS BIGINT) AS epoch_value",
        _epoch(_INT64, None),
        None,
        "a NULL instant stays NULL and keeps its Int64 type — the input a scaling kernel most "
        f"easily turns into a zero. {REVERT}",
    ),
    # ----- the same-path siblings: narrower integers, floats, decimal ----------------------------
    TimestampCastRow(
        "timestamp_to_int_is_epoch_seconds",
        ZONE_NEW_YORK,
        _cast_sql(WHOLE_BEFORE_EPOCH, "INT"),
        _epoch(pa.int32(), -1800),
        None,
        "a narrower signed integer target shares the class: the rewrite scales first and the "
        "user's width is applied after. repark REFUSED this cast before the fix (DataFusion has "
        f"no direct Timestamp -> Int32), so the row is new surface as well as a fix. {REVERT}",
    ),
    TimestampCastRow(
        "timestamp_to_smallint_is_epoch_seconds",
        ZONE_NEW_YORK,
        _cast_sql(WHOLE_BEFORE_EPOCH, "SMALLINT"),
        _epoch(pa.int16(), -1800),
        None,
        f"the same, one width down — -1800 still fits an Int16. {REVERT}",
    ),
    TimestampCastRow(
        "timestamp_to_double_keeps_the_fraction",
        ZONE_NEW_YORK,
        _cast_sql(HALF_SECOND_BEFORE_EPOCH, "DOUBLE"),
        _epoch(pa.float64(), -0.5),
        None,
        "the float sibling shares the wrong scaling but NOT the floor: Spark keeps the fraction, "
        "so half a second before the epoch is -0.5 and not -1. This is why the fix needs two "
        f"scaling UDFs rather than one. {REVERT}",
    ),
    TimestampCastRow(
        "timestamp_to_float_is_epoch_seconds",
        ZONE_NEW_YORK,
        _cast_sql(WHOLE_BEFORE_EPOCH, "FLOAT"),
        _epoch(pa.float32(), -1800.0),
        None,
        "FLOAT is the narrow float target; before the fix repark answered -1799999979520.0, "
        f"which is the raw nanosecond tick rounded into an f32. {REVERT}",
    ),
    TimestampCastRow(
        "timestamp_to_decimal_keeps_the_fraction",
        ZONE_NEW_YORK,
        _cast_sql(HALF_SECOND_BEFORE_EPOCH, "DECIMAL(20,6)"),
        _epoch(pa.decimal128(20, 6), Decimal("-0.500000")),
        None,
        "the decimal sibling, with the declared precision and scale surviving the rewrite. Spark "
        f"computes its own decimal cast through a double, so the double hop matches it. {REVERT}",
    ),
    # ----- zone independence ---------------------------------------------------------------------
    TimestampCastRow(
        "epoch_seconds_are_zone_independent_under_tokyo",
        ZONE_TOKYO,
        _cast_sql(WHOLE_BEFORE_EPOCH, "BIGINT"),
        _epoch(_INT64, -1800),
        None,
        "a cast reads the INSTANT, never a wall clock, so a session zone east of UTC must give "
        "the identical answer to one west of it. This row is the standing detector for a change "
        f"that wires the session zone into this path by accident. {REVERT}",
    ),
    TimestampCastRow(
        "epoch_seconds_are_zone_independent_under_utc",
        ZONE_UTC,
        _cast_sql(HALF_SECOND_BEFORE_EPOCH, "BIGINT"),
        _epoch(_INT64, -1),
        None,
        f"the floor edge again, under the default zone — three zones, one answer. {REVERT}",
    ),
    # ----- the REVERSE direction: the fence, not a fix --------------------------------------------
    TimestampCastRow(
        "bigint_to_timestamp_reads_seconds",
        ZONE_NEW_YORK,
        "SELECT CAST(-1800L AS TIMESTAMP) AS ts_value",
        _one_row(
            [("ts_value", pa.timestamp("us", "UTC"), False)],
            {"ts_value": dt.datetime(1969, 12, 31, 23, 30, tzinfo=dt.UTC)},
        ),
        None,
        "the REVERSE direction already read SECONDS (TZ-5 fence). TZ-4 PR-1 closed the type half: "
        "CAST(<integer> AS TIMESTAMP) is now timestamp[us, tz=UTC] like Spark. Flip evidence — "
        "revert the ns-naive wrap and this row reds.",
    ),
    # ----- the DataFrame door --------------------------------------------------------------------
    TimestampCastRow(
        "dataframe_api_cast_under_new_york_session",
        ZONE_NEW_YORK,
        DATAFRAME_API_SPELLING,
        _table(
            [
                ("epoch_long", _INT64, True),
                ("epoch_int", pa.int32(), True),
                ("epoch_double", pa.float64(), True),
            ],
            {
                "epoch_long": [-1800, -1, 1718452801],
                "epoch_int": [-1800, -1, 1718452801],
                "epoch_double": [-1800.0, -0.5, 1718452801.999999],
            },
        ),
        None,
        "the DataFrame door over a real tz-aware COLUMN, all three numeric targets at once, with "
        "the floor edge in row two. This spelling crosses PyO3 as a standalone Expr::Cast with no "
        f"SQL string, so a SQL-only fix would leave it wrong. {REVERT}",
        entry_point="dataframe_api",
    ),
    TimestampCastRow(
        "dataframe_api_cast_under_tokyo_session",
        ZONE_TOKYO,
        DATAFRAME_API_SPELLING,
        _table(
            [
                ("epoch_long", _INT64, True),
                ("epoch_int", pa.int32(), True),
                ("epoch_double", pa.float64(), True),
            ],
            {
                "epoch_long": [-1800, -1, 1718452801],
                "epoch_int": [-1800, -1, 1718452801],
                "epoch_double": [-1800.0, -0.5, 1718452801.999999],
            },
        ),
        None,
        "the same DataFrame recipe under the other zone: the door AND the zone are varied "
        f"independently, so neither can be the thing carrying the answer. {REVERT}",
        entry_point="dataframe_api",
    ),
    # ----- the F.expr door -----------------------------------------------------------------------
    TimestampCastRow(
        "expr_door_floor_edge",
        ZONE_NEW_YORK,
        EXPR_DOOR_SPELLING,
        _epoch(_INT64, -1),
        None,
        "the third facade spelling on the hardest input (the negative fractional second). "
        f"{REVERT}",
        entry_point="expr",
    ),
]


# ==================================================================================================
# Helpers
# ==================================================================================================


def _session_at(zone: str) -> ReparkSession:
    """A repark session whose session timezone is ``zone`` (resolved at build, engine-validated)."""
    import repark

    return (
        repark.ReparkSession.builder.appName("timestamp-cast-parity")
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


def run_row(row: TimestampCastRow, session: object) -> pa.Table:
    """Run one row's recipe on a session (either engine) and return its Arrow output.

    Shared with the record driver so the recipe the oracle ran and the recipe asserted here are
    the same code, not two copies of one string.
    """
    if row.entry_point == "dataframe_api":
        return dataframe_api_cast_projection(session)
    if row.entry_point == "expr":
        return expr_door_projection(session)
    frame = session.sql(row.sql)  # type: ignore[attr-defined]
    return _to_arrow(frame)


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_timestamp_cast_row_matches_spark_or_still_diverges(row: TimestampCastRow) -> None:
    """Every recorded row, on the Arrow path (value AND Arrow type AND nullability).

    Equality rows assert ``repark == Spark``. Disclosure rows assert repark's pinned actual
    output, and classify a failure before raising it: a convergence onto the recorded Spark
    golden must be flipped rather than deleted, and a move off BOTH halves is a regression whose
    two halves need re-recording. The classification is done on ``actual`` — the engine's real
    output — so an engine change genuinely reaches it.
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


def test_the_class_is_covered_per_entry_point_and_per_edge() -> None:
    """The SHAPE of the corpus, pinned so a later edit cannot quietly hollow the class out.

    ``docs/testing.md`` "Divergence-class claims": a claim quantifies over divergence classes
    crossed with user entry points, and is only as true as its weakest untested cell. The
    assertions below are what stop this corpus from decaying into "one representative case".
    """
    assert len({row.name for row in ROWS}) == len(ROWS), "row names are unique"

    entry_points = {row.entry_point for row in ROWS}
    assert entry_points == {"sql", "dataframe_api", "expr"}, (
        "all three facade spellings must be present — the DataFrame door crosses PyO3 as a bare "
        "Expr::Cast and is exactly the cell a SQL-only fix would leave wrong"
    )

    # The floor edge, both signs, at the SQL door AND the DataFrame door.
    negative_fractional = [
        row
        for row in ROWS
        if row.entry_point == "sql"
        and (HALF_SECOND_BEFORE_EPOCH in row.sql or QUARTER_PAST_BEFORE_EPOCH in row.sql)
    ]
    assert len(negative_fractional) >= 3, (
        "the negative FRACTIONAL second is the only input that separates Spark's floor from "
        "truncation toward zero; a corpus that loses it stops testing the fix"
    )
    positive_fractional = [
        row
        for row in ROWS
        if row.entry_point == "sql"
        and (FRACTION_AFTER_EPOCH in row.sql or MODERN_WITH_FRACTION in row.sql)
    ]
    assert positive_fractional, (
        "a positive fractional row must stay, so the fix cannot degenerate into 'subtract one'"
    )

    # Every numeric target family the engine rewrite claims.
    for target in ("BIGINT", "INT", "SMALLINT", "DOUBLE", "FLOAT", "DECIMAL(20,6)"):
        assert [row for row in ROWS if f"AS {target})" in row.sql], (
            f"the class names {target} as a cast target; pin it or stop claiming it"
        )

    # Zone independence, measured rather than asserted in prose.
    zones = {row.session_time_zone for row in ROWS if row.entry_point == "sql"}
    assert zones == {ZONE_NEW_YORK, ZONE_TOKYO, ZONE_UTC}, (
        "the class is zone-independent; three zones (one DST-observing, one fixed-offset east of "
        "UTC, and UTC itself) are what make that a measurement"
    )
    dataframe_zones = {row.session_time_zone for row in ROWS if row.entry_point == "dataframe_api"}
    assert dataframe_zones == {ZONE_NEW_YORK, ZONE_TOKYO}, (
        "the door and the zone are varied independently, so neither can be the thing carrying "
        "the answer"
    )

    # Instant-producer type residue closed in TZ-4 PR-1; this corpus is all equality.
    disclosures = {row.name for row in ROWS if row.repark is not None}
    assert disclosures == set(), (
        "TZ-4 PR-1 flipped the reverse-direction type disclosure to equality. A new disclosure "
        "here means the fix regressed or a new class was found; either way it is an edit a "
        "reviewer must see"
    )
