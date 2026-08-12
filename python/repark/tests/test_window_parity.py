"""Window-function differential corpus (H-2 gap G5) — G5 frames never compared to live Spark.

**Oracle.** Every ``spark`` table below was RECORDED in record mode against live PySpark 4.1.2
(zulu-17, ``master("local[2]")``, ``spark.sql.ansi.enabled=true``,
``spark.sql.shuffle.partitions=2``) on 2026-08-11. One recipe per row runs on BOTH engines, so the
recipe under test and the recipe the oracle ran are the same code — nothing here is hand-computed.

**Why some rows may be DISCLOSURES.** When the engines agree on value AND Arrow type AND
nullability the row is a plain equality (``repark is None``). When they honestly disagree the row
pins BOTH halves and asserts the divergence still holds. A silent CONVERGENCE goes red and forces
the disclosure to be revisited rather than laundered into "parity" — the same discipline
``docs/testing.md`` puts on the live tier's disclosures. When a G5 fix lands, each divergent row
flips to ``repark=None`` (equality) and that flip is the fix's revert-red evidence.

**The default-frame trap is first-class.** With ``ORDER BY``, Spark's default frame is
``RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`` (peers included). An aggregate-over-window
with ties MUST be pinned — the classic silent divergence when an engine silently uses ROWS.

**Rows assert on the Arrow path** (``to_arrow`` / Spark ``toArrow``) through the parity
comparator, so schema name, Arrow type and nullability are part of every content assertion —
never ``show``.

**Determinism.** Every row's window ``ORDER BY`` either forms a total order (unique key, or
``k, id`` tie-breaker) OR the assertion is order-insensitive by construction and the measured
columns are peer-determined (rank / dense_rank / default-RANGE sum over peers). A flaky golden is
a CP-7 finding.

**Re-deriving the goldens (record mode).** The driver that recorded every ``spark`` half is
committed beside this module::

    JAVA_HOME=/usr/lib/jvm/zulu-17-amd64 SPARK_LOCAL_IP=127.0.0.1 \\
        PYTHONPATH=python/repark-parity/src \\
        .venv/bin/python python/repark/tests/_record_window_goldens.py

It imports ``ROWS`` from THIS module and runs each row's own recipe, so the recorded golden and the
asserted recipe cannot drift apart. Needs a JVM + ``pyspark`` (``uv sync --extra record``); never
collected by pytest. ``--emit`` prints paste-ready table constructors.

**Entry points.** Facade ``sql()`` is primary. The ``df_api_*`` rows go through the DataFrame-API
``Window.partitionBy(...).orderBy(...)`` spelling (CP-11) via the same dual-engine recipe helper.
The claim is scoped to the facade surface (sql + DataFrame API); native / ANSI doors are not in
this corpus.

**In-flight fix named by every disclosure** so a red row points at what flips it.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

import pyarrow as pa
import pytest

from repark_parity import FrameMismatchError, assert_frames_equal

if TYPE_CHECKING:
    from repark.session import ReparkSession

# Named so every disclosure's note can cite the same future work without inventing per-row fix IDs.
FIX_G5 = (
    "the window-frame / ranking / offset parity fix "
    "(briefs/v2-engine-hardening.md, gap G5; DECLARE candidacy if the ruling is disclosure-only)"
)
# Shared lead-in for the SQL-door ranking TYPE disclosures (uint64 vs Spark int32).
TYPE_DISC = (
    "VALUE matches Spark; TYPE diverges: SQL-door ranking returns Arrow uint64 "
    "(DataFusion UInt64) vs Spark int32 (IntegerType). DF-API row_number casts to "
    "IntegerType (see df_api equality). Flipped by "
)

# Budget floors/ceilings pinned by test_window_row_set_covers_gap_budgets (not incidental).
G5_BUDGET_MIN = 20
# Raised 20-28 -> 20-45 by G5b, which appends the temporal-RANGE family (15 rows) to W-4's
# 27. The ceiling is a shape pin, not a cap on honest coverage; the equality floor and the
# disclosure ceiling below are what keep the corpus from degenerating.
G5_BUDGET_MAX = 45
# Corpus cannot degenerate to all-disclosures: at least this many plain equalities, and at most
# this many disclosures among the differential ROWS.
MIN_EQUALITY_ROWS = 6
MAX_DISCLOSURE_ROWS = 22


# ==================================================================================================
# Arrow helpers
# ==================================================================================================


def _table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build the Arrow table a recorded golden describes (name, type, nullability, then values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


def _one_row(fields: list[tuple[str, pa.DataType, bool]], values: dict[str, object]) -> pa.Table:
    """Build the single-row Arrow table a recorded golden describes."""
    return _table(fields, {name: [values[name]] for name, _, _ in fields})


# ==================================================================================================
# Shared seed data (VALUES / createDataFrame) — total-order friendly where needed
# ==================================================================================================

# Five-row window input: ties on (grp, k) so default RANGE peers are observable; unique `id`
# provides a total-order tie-breaker when the row needs one.
SEED_ROWS: list[tuple[int, str, int, int]] = [
    (1, "A", 1, 10),
    (2, "A", 1, 20),  # peer of id=1 on k within A
    (3, "A", 2, 30),
    (4, "B", 1, 40),
    (5, "B", 3, 50),
]
SEED_COLUMNS = ["id", "grp", "k", "v"]

SEED_VIEW = "win_seed"
NULL_SEED_VIEW = "win_null_seed"

# SQL rows read from the registered temp view (createDataFrame seed) so seed column
# Arrow types match the DataFrame-API rows (int64) and the corpus measures WINDOW behaviour,
# not VALUES literal-inference noise (Spark VALUES → int32 non-null; repark VALUES → int64 null).
SEED_VALUES_SQL = f"FROM {SEED_VIEW}"

# NULL-bearing seed for NULLS FIRST/LAST + lag/lead NULL handling.
NULL_SEED_VALUES_SQL = f"FROM {NULL_SEED_VIEW}"

# --- G5b temporal-RANGE seeds -------------------------------------------------------------------
# Timestamp seed: a 12-hour pair (ids 1/2) makes a sub-day interval observable, a 24-hour gap
# (id 3) separates HOUR from DAY, and a tie on `ts` (ids 4/5) makes peer handling observable.
TEMPORAL_TS_VIEW = "win_ts_seed"
TEMPORAL_DATE_VIEW = "win_date_seed"
TEMPORAL_NULL_TS_VIEW = "win_ts_null_seed"

TEMPORAL_TS_ROWS: list[tuple[int, dt.datetime, int]] = [
    (1, dt.datetime(2024, 1, 1, 0, 0, 0), 10),
    (2, dt.datetime(2024, 1, 1, 12, 0, 0), 20),
    (3, dt.datetime(2024, 1, 2, 0, 0, 0), 30),
    (4, dt.datetime(2024, 1, 4, 0, 0, 0), 40),
    (5, dt.datetime(2024, 1, 4, 0, 0, 0), 50),
]
TEMPORAL_DATE_ROWS: list[tuple[int, dt.date, int]] = [
    (1, dt.date(2024, 1, 1), 10),
    (2, dt.date(2024, 1, 2), 20),
    (3, dt.date(2024, 1, 4), 30),
]
TEMPORAL_NULL_TS_ROWS: list[tuple[int, dt.datetime | None, int]] = [
    (1, dt.datetime(2024, 1, 1), 10),
    (2, None, 20),
    (3, dt.datetime(2024, 1, 2), 30),
    (4, None, 40),
]

TEMPORAL_TS_SQL = f"FROM {TEMPORAL_TS_VIEW}"
TEMPORAL_DATE_SQL = f"FROM {TEMPORAL_DATE_VIEW}"
TEMPORAL_NULL_TS_SQL = f"FROM {TEMPORAL_NULL_TS_VIEW}"

# Every temporal row carries this family so `run_row` knows to register the three seeds above and
# the budget test can name-gate the family.
TEMPORAL_FAMILY = "temporal_range"

# Named so every temporal disclosure cites the same follow-up rather than inventing per-row IDs.
FIX_G5B = (
    "the temporal-RANGE residual follow-up (G5b ledger task/g5b-temporal-range-ledger.md "
    "section 6 handoff; each row names the exact spelling or bound class it is waiting on)"
)


# ==================================================================================================
# Row shape
# ==================================================================================================


@dataclass(frozen=True)
class WindowRow:
    """One differential row: a window recipe + recorded Spark half + repark half.

    ``repark is None`` and ``spark is not None`` and no raise flags → plain EQUALITY
    (``repark == Spark``).

    ``repark is not None`` and ``spark is not None`` → DISCLOSURE: repark's actual output is pinned
    and a convergence onto the recorded Spark output is detected and reported as one.

    ``spark_raises`` / ``repark_raises`` mark refuse-class rows. The raising side's table is
    ``None``; the non-raising side pins its Arrow half.

    ``entry_point`` selects the facade SPELLING: ``"sql"`` runs ``session.sql(row.sql)``;
    ``"dataframe_api"`` runs the named :data:`DF_RECIPES` helper (``sql`` is documentation of the
    equivalent projection, not a string anything executes for that row).
    """

    name: str
    family: str
    sql: str
    spark: pa.Table | None
    repark: pa.Table | None
    note: str
    entry_point: Literal["sql", "dataframe_api"] = "sql"
    df_recipe: str | None = None
    spark_raises: str | None = None
    repark_raises: str | None = None
    # When True, pin row order (ORDER BY under test). Default False = sort-all-columns compare.
    order_sensitive: bool = False

    def is_equality(self) -> bool:
        """True when the row asserts plain repark == Spark (no raise flags, no repark pin)."""
        return (
            self.repark is None
            and self.spark is not None
            and self.spark_raises is None
            and self.repark_raises is None
        )

    def is_disclosure(self) -> bool:
        """True when the row pins a known divergence (table disclosure or raise-class split)."""
        return not self.is_equality()


# ==================================================================================================
# Dual-engine helpers (shared with the record driver)
# ==================================================================================================


def _functions_module(session: object) -> object:
    """The ``functions`` module belonging to ``session``'s engine — PySpark's or repark's."""
    if session.__class__.__module__.split(".")[0] == "pyspark":
        from pyspark.sql import functions as spark_functions

        return spark_functions
    from repark.sql import functions as repark_functions

    return repark_functions


def _window_class(session: object) -> type:
    """The ``Window`` class belonging to ``session``'s engine."""
    if session.__class__.__module__.split(".")[0] == "pyspark":
        from pyspark.sql import Window as SparkWindow

        return SparkWindow
    from repark import Window as ReparkWindow

    return ReparkWindow


def register_seed_view(session: object) -> None:
    """Register :data:`SEED_VIEW` on either engine (createDataFrame + createOrReplaceTempView)."""
    frame = session.createDataFrame(SEED_ROWS, SEED_COLUMNS)  # type: ignore[attr-defined]
    frame.createOrReplaceTempView(SEED_VIEW)


def register_null_seed_view(session: object) -> None:
    """Register :data:`NULL_SEED_VIEW` (id + nullable v) on either engine."""
    frame = session.createDataFrame(  # type: ignore[attr-defined]
        [(1, 10), (2, None), (3, 20), (4, None), (5, 30)],
        ["id", "v"],
    )
    frame.createOrReplaceTempView(NULL_SEED_VIEW)


def register_temporal_seed_views(session: object) -> None:
    """Register the three G5b temporal seeds on either engine (timestamp, date, nullable ts)."""
    session.createDataFrame(  # type: ignore[attr-defined]
        TEMPORAL_TS_ROWS, ["id", "ts", "v"]
    ).createOrReplaceTempView(TEMPORAL_TS_VIEW)
    session.createDataFrame(  # type: ignore[attr-defined]
        TEMPORAL_DATE_ROWS, ["id", "d", "v"]
    ).createOrReplaceTempView(TEMPORAL_DATE_VIEW)
    session.createDataFrame(  # type: ignore[attr-defined]
        TEMPORAL_NULL_TS_ROWS, ["id", "ts", "v"]
    ).createOrReplaceTempView(TEMPORAL_NULL_TS_VIEW)


def _to_arrow(frame: object) -> pa.Table:
    """Arrow export common to both engines (``to_arrow`` / ``toArrow``)."""
    to_arrow = getattr(frame, "to_arrow", None) or frame.toArrow  # type: ignore[attr-defined]
    return to_arrow()  # type: ignore[no-any-return]


def dataframe_api_partition_row_number(session: object) -> pa.Table:
    """``F.row_number().over(Window.partitionBy("grp").orderBy("id"))`` — DF-API entry point.

    Total order on ``id`` within each partition so the golden is deterministic (CP-7).
    """
    functions = _functions_module(session)
    window_class = _window_class(session)
    frame = session.createDataFrame(SEED_ROWS, SEED_COLUMNS)  # type: ignore[attr-defined]
    window = window_class.partitionBy("grp").orderBy("id")
    projected = frame.select(
        frame.id,
        frame.grp,
        functions.row_number().over(window).alias("rn"),
    ).orderBy("grp", "id")
    return _to_arrow(projected)


def dataframe_api_rows_between_sum(session: object) -> pa.Table:
    """``F.sum("v").over(Window.partitionBy("grp").orderBy("id").rowsBetween(-1, 0))``.

    Explicit ROWS sliding frame through the DataFrame API (CP-11 sibling of the SQL frame rows).
    """
    functions = _functions_module(session)
    window_class = _window_class(session)
    frame = session.createDataFrame(SEED_ROWS, SEED_COLUMNS)  # type: ignore[attr-defined]
    window = window_class.partitionBy("grp").orderBy("id").rowsBetween(-1, 0)
    projected = frame.select(
        frame.id,
        frame.grp,
        frame.v,
        functions.sum("v").over(window).alias("s"),
    ).orderBy("grp", "id")
    return _to_arrow(projected)


DF_RECIPES: dict[str, object] = {
    "partition_row_number": dataframe_api_partition_row_number,
    "rows_between_sum": dataframe_api_rows_between_sum,
}


def run_row(row: WindowRow, session: object) -> pa.Table:
    """Run one row's recipe on a session (either engine) and return its Arrow output.

    Shared with the record driver so the recipe the oracle ran and the recipe asserted here are
    the same code, not two copies. Callers that expect a raise must catch around this helper.
    """
    if row.entry_point == "dataframe_api":
        assert row.df_recipe is not None, f"{row.name}: dataframe_api row needs df_recipe"
        recipe = DF_RECIPES[row.df_recipe]
        return recipe(session)  # type: ignore[operator, no-any-return]
    # SQL rows: register both seed views so recipes may read either without per-row flags.
    register_seed_view(session)
    register_null_seed_view(session)
    # The three temporal seeds cost two extra createDataFrame round trips per row on the JVM
    # side, so they are registered only for the family that reads them.
    if row.family == TEMPORAL_FAMILY:
        register_temporal_seed_views(session)
    frame = session.sql(row.sql)  # type: ignore[attr-defined]
    return _to_arrow(frame)


# ==================================================================================================
# Gap G5 — window frames, ranking, offsets (spark halves filled after record mode)
# ==================================================================================================
#
# Placeholders: spark=None until `_record_window_goldens.py --emit` pastes live goldens. The suite
# fails closed on missing goldens (equality/disclosure runners require a spark table). Record mode
# is the only path that fills them.

ROWS: list[WindowRow] = [
    # ----- 1. Default-frame trap (RANGE peers with ties) — name-gated family --------------------
    WindowRow(
        "default_frame_sum_with_ties",
        "default_frame",
        # ORDER BY k only → peers on k=1 both see sum of peer group under default RANGE.
        f"SELECT id, k, v, sum(v) OVER (ORDER BY k) AS s {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [
                ("id", pa.int64(), True),
                ("k", pa.int64(), True),
                ("v", pa.int64(), True),
                ("s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "k": [1, 1, 2, 1, 3],
                "v": [10, 20, 30, 40, 50],
                "s": [70, 70, 100, 70, 150],
            },
        ),
        None,
        "default-frame trap: with ORDER BY and no frame clause Spark uses "
        "RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (peers included). Two rows with "
        "k=1 must share the same cumulative sum of the peer group. Flipped to equality by "
        f"{FIX_G5} if currently disclosed.",
    ),
    WindowRow(
        "default_frame_avg_with_ties",
        "default_frame",
        f"SELECT id, k, v, avg(v) OVER (ORDER BY k) AS a {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [
                ("id", pa.int64(), True),
                ("k", pa.int64(), True),
                ("v", pa.int64(), True),
                ("a", pa.float64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "k": [1, 1, 2, 1, 3],
                "v": [10, 20, 30, 40, 50],
                "a": [23.333333333333332, 23.333333333333332, 25.0, 23.333333333333332, 30.0],
            },
        ),
        None,
        "default-frame trap on avg: peers on k share the same running average under RANGE. "
        f"Name-gated with default_frame_* so a control cannot satisfy the pin. {FIX_G5}.",
    ),
    WindowRow(
        "default_frame_count_with_ties",
        "default_frame",
        f"SELECT id, k, count(*) OVER (ORDER BY k) AS c {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("c", pa.int64(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "c": [3, 3, 4, 3, 5]},
        ),
        None,
        "default-frame trap on count(*): peer group size under RANGE (k=1 → count 2 for both "
        f"peer rows when only those two share the lowest k among the prefix). {FIX_G5}.",
    ),
    WindowRow(
        "default_frame_partitioned_sum_with_ties",
        "default_frame",
        f"SELECT id, grp, k, v, sum(v) OVER (PARTITION BY grp ORDER BY k) AS s "
        f"{SEED_VALUES_SQL} ORDER BY grp, id",
        _table(
            [
                ("id", pa.int64(), True),
                ("grp", pa.string(), True),
                ("k", pa.int64(), True),
                ("v", pa.int64(), True),
                ("s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "grp": ["A", "A", "A", "B", "B"],
                "k": [1, 1, 2, 1, 3],
                "v": [10, 20, 30, 40, 50],
                "s": [30, 30, 60, 40, 90],
            },
        ),
        None,
        "partitioned default-frame trap: peers only within grp. A-side k=1 peers (ids 1,2) "
        f"share the A-side peer sum; B has no k-ties. {FIX_G5}.",
    ),
    # ----- 2. Explicit frames — ROWS vs RANGE, bounded/unbounded/sliding ------------------------
    WindowRow(
        "rows_unbounded_preceding_current_total_order",
        "explicit_frame",
        # Total order on (k, id) so ROWS is deterministic.
        f"SELECT id, k, v, sum(v) OVER (ORDER BY k, id "
        f"ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s "
        f"{SEED_VALUES_SQL} ORDER BY id",
        _table(
            [
                ("id", pa.int64(), True),
                ("k", pa.int64(), True),
                ("v", pa.int64(), True),
                ("s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "k": [1, 1, 2, 1, 3],
                "v": [10, 20, 30, 40, 50],
                "s": [10, 30, 100, 70, 150],
            },
        ),
        None,
        "explicit ROWS unbounded-preceding→current with total order (k, id). Contrast with "
        "default RANGE peers: ROWS does not include later peers at the same k.",
    ),
    WindowRow(
        "range_unbounded_preceding_current_with_ties",
        "explicit_frame",
        f"SELECT id, k, v, sum(v) OVER (ORDER BY k "
        f"RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS s "
        f"{SEED_VALUES_SQL} ORDER BY id",
        _table(
            [
                ("id", pa.int64(), True),
                ("k", pa.int64(), True),
                ("v", pa.int64(), True),
                ("s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "k": [1, 1, 2, 1, 3],
                "v": [10, 20, 30, 40, 50],
                "s": [70, 70, 100, 70, 150],
            },
        ),
        None,
        "explicit RANGE unbounded-preceding→current — the written form of Spark's default. "
        "Must match default_frame_sum_with_ties on the measured columns.",
    ),
    WindowRow(
        "rows_vs_range_peers_differ_on_ties",
        "explicit_frame",
        # Project both frames side-by-side under the same ORDER BY k, id so the ROWS half is
        # deterministic and the RANGE half still peers on k alone.
        f"SELECT id, k, v, "
        f"sum(v) OVER (ORDER BY k, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_s, "
        f"sum(v) OVER (ORDER BY k RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS range_s "
        f"{SEED_VALUES_SQL} ORDER BY id",
        _table(
            [
                ("id", pa.int64(), True),
                ("k", pa.int64(), True),
                ("v", pa.int64(), True),
                ("rows_s", pa.int64(), True),
                ("range_s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "k": [1, 1, 2, 1, 3],
                "v": [10, 20, 30, 40, 50],
                "rows_s": [10, 30, 100, 70, 150],
                "range_s": [70, 70, 100, 70, 150],
            },
        ),
        None,
        "ROWS vs RANGE side-by-side with ties on k: the two sum columns MUST differ for at least "
        "one peer row (the classic silent-divergence detector, name-gated).",
    ),
    WindowRow(
        "rows_sliding_1_preceding_1_following",
        "explicit_frame",
        f"SELECT id, v, sum(v) OVER (ORDER BY id "
        f"ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS s "
        f"{SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "v": [10, 20, 30, 40, 50], "s": [30, 60, 90, 120, 90]},
        ),
        None,
        "sliding ROWS frame: 1 preceding + current + 1 following under total order on id.",
    ),
    WindowRow(
        "rows_current_to_unbounded_following",
        "explicit_frame",
        f"SELECT id, v, sum(v) OVER (ORDER BY id "
        f"ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS s "
        f"{SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "v": [10, 20, 30, 40, 50], "s": [150, 140, 120, 90, 50]},
        ),
        None,
        "ROWS current→unbounded following (suffix sum) under total order on id.",
    ),
    WindowRow(
        "range_value_offset_numeric_order",
        "explicit_frame",
        f"SELECT id, k, v, sum(v) OVER (ORDER BY k "
        f"RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s "
        f"{SEED_VALUES_SQL} ORDER BY id",
        _table(
            [
                ("id", pa.int64(), True),
                ("k", pa.int64(), True),
                ("v", pa.int64(), True),
                ("s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "k": [1, 1, 2, 1, 3],
                "v": [10, 20, 30, 40, 50],
                "s": [70, 70, 100, 70, 80],
            },
        ),
        None,
        "RANGE value-offset frame on numeric ORDER BY (k): includes peers within k-distance 1.",
    ),
    WindowRow(
        "rows_partitioned_sliding",
        "explicit_frame",
        f"SELECT id, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY id "
        f"ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s "
        f"{SEED_VALUES_SQL} ORDER BY grp, id",
        _table(
            [
                ("id", pa.int64(), True),
                ("grp", pa.string(), True),
                ("v", pa.int64(), True),
                ("s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "grp": ["A", "A", "A", "B", "B"],
                "v": [10, 20, 30, 40, 50],
                "s": [10, 30, 50, 40, 90],
            },
        ),
        None,
        "partitioned sliding ROWS (1 preceding + current) — frames do not cross partitions.",
    ),
    # ----- 3. Ranking family with ties ----------------------------------------------------------
    WindowRow(
        "rank_with_ties",
        "ranking",
        f"SELECT id, k, rank() OVER (ORDER BY k) AS r {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("r", pa.int32(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "r": [1, 1, 4, 1, 5]},
        ),
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("r", pa.uint64(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "r": [1, 1, 4, 1, 5]},
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "rank() with ties on k: peers share rank; next rank skips (1,1,3,…).",
    ),
    WindowRow(
        "dense_rank_with_ties",
        "ranking",
        f"SELECT id, k, dense_rank() OVER (ORDER BY k) AS r {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("r", pa.int32(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "r": [1, 1, 2, 1, 3]},
        ),
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("r", pa.uint64(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "r": [1, 1, 2, 1, 3]},
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "dense_rank() with ties on k: peers share rank; next rank does not skip (1,1,2,…).",
    ),
    WindowRow(
        "row_number_total_order",
        "ranking",
        f"SELECT id, k, row_number() OVER (ORDER BY k, id) AS rn {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("rn", pa.int32(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "rn": [1, 2, 4, 3, 5]},
        ),
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("rn", pa.uint64(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "rn": [1, 2, 4, 3, 5]},
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "row_number() under total order (k, id) — deterministic 1..n (CP-7).",
    ),
    WindowRow(
        "ntile_4_total_order",
        "ranking",
        f"SELECT id, ntile(4) OVER (ORDER BY id) AS bucket {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("bucket", pa.int32(), False)],
            {"id": [1, 2, 3, 4, 5], "bucket": [1, 1, 2, 3, 4]},
        ),
        _table(
            [("id", pa.int64(), True), ("bucket", pa.uint64(), False)],
            {"id": [1, 2, 3, 4, 5], "bucket": [1, 1, 2, 3, 4]},
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "ntile(4) under total order on id — bucket assignment 1..4 over 5 rows.",
    ),
    WindowRow(
        "percent_rank_with_ties",
        "ranking",
        f"SELECT id, k, percent_rank() OVER (ORDER BY k) AS pr {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("k", pa.int64(), True), ("pr", pa.float64(), False)],
            {"id": [1, 2, 3, 4, 5], "k": [1, 1, 2, 1, 3], "pr": [0.0, 0.0, 0.75, 0.0, 1.0]},
        ),
        None,
        "percent_rank() with ties on k: (rank-1)/(n-1); peers share the same percent_rank.",
    ),
    WindowRow(
        "rank_partitioned_with_ties",
        "ranking",
        f"SELECT id, grp, k, rank() OVER (PARTITION BY grp ORDER BY k) AS r "
        f"{SEED_VALUES_SQL} ORDER BY grp, id",
        _table(
            [
                ("id", pa.int64(), True),
                ("grp", pa.string(), True),
                ("k", pa.int64(), True),
                ("r", pa.int32(), False),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "grp": ["A", "A", "A", "B", "B"],
                "k": [1, 1, 2, 1, 3],
                "r": [1, 1, 3, 1, 2],
            },
        ),
        _table(
            [
                ("id", pa.int64(), True),
                ("grp", pa.string(), True),
                ("k", pa.int64(), True),
                ("r", pa.uint64(), False),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "grp": ["A", "A", "A", "B", "B"],
                "k": [1, 1, 2, 1, 3],
                "r": [1, 1, 3, 1, 2],
            },
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "partitioned rank() with ties inside grp A on k=1.",
    ),
    # ----- 4. Offset family — lag / lead --------------------------------------------------------
    WindowRow(
        "lag_default_offset_1",
        "offset",
        f"SELECT id, v, lag(v) OVER (ORDER BY id) AS prev {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("prev", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "v": [10, 20, 30, 40, 50], "prev": [None, 10, 20, 30, 40]},
        ),
        None,
        "lag(v) default offset 1 under total order on id; first row is NULL.",
    ),
    WindowRow(
        "lag_offset_2_with_default_value",
        "offset",
        f"SELECT id, v, lag(v, 2, -1) OVER (ORDER BY id) AS prev {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("prev", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "v": [10, 20, 30, 40, 50], "prev": [-1, -1, 10, 20, 30]},
        ),
        None,
        "lag(v, 2, -1): explicit default value for rows without a 2-back predecessor.",
    ),
    WindowRow(
        "lead_default_offset_1",
        "offset",
        f"SELECT id, v, lead(v) OVER (ORDER BY id) AS nxt {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("nxt", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "v": [10, 20, 30, 40, 50], "nxt": [20, 30, 40, 50, None]},
        ),
        None,
        "lead(v) default offset 1 under total order on id; last row is NULL.",
    ),
    WindowRow(
        "lead_offset_1_with_default_value",
        "offset",
        f"SELECT id, v, lead(v, 1, 0) OVER (ORDER BY id) AS nxt {SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("nxt", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "v": [10, 20, 30, 40, 50], "nxt": [20, 30, 40, 50, 0]},
        ),
        None,
        "lead(v, 1, 0): explicit default value at the trailing edge.",
    ),
    WindowRow(
        "lag_over_null_values",
        "offset",
        f"SELECT id, v, lag(v, 1, -999) OVER (ORDER BY id) AS prev "
        f"{NULL_SEED_VALUES_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("prev", pa.int64(), True)],
            {
                "id": [1, 2, 3, 4, 5],
                "v": [10, None, 20, None, 30],
                "prev": [-999, 10, None, 20, None],
            },
        ),
        None,
        "lag over a column that itself holds NULLs: NULL is a value lag returns; the default "
        "only fills missing predecessors, not NULL payloads.",
    ),
    # ----- 5. Partitioned vs unpartitioned; NULLS FIRST/LAST ------------------------------------
    WindowRow(
        "partitioned_vs_unpartitioned_row_number",
        "partition_nulls",
        f"SELECT id, grp, "
        f"row_number() OVER (PARTITION BY grp ORDER BY id) AS rn_part, "
        f"row_number() OVER (ORDER BY grp, id) AS rn_global "
        f"{SEED_VALUES_SQL} ORDER BY grp, id",
        _table(
            [
                ("id", pa.int64(), True),
                ("grp", pa.string(), True),
                ("rn_part", pa.int32(), False),
                ("rn_global", pa.int32(), False),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "grp": ["A", "A", "A", "B", "B"],
                "rn_part": [1, 2, 3, 1, 2],
                "rn_global": [1, 2, 3, 4, 5],
            },
        ),
        _table(
            [
                ("id", pa.int64(), True),
                ("grp", pa.string(), True),
                ("rn_part", pa.uint64(), False),
                ("rn_global", pa.uint64(), False),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "grp": ["A", "A", "A", "B", "B"],
                "rn_part": [1, 2, 3, 1, 2],
                "rn_global": [1, 2, 3, 4, 5],
            },
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "partitioned vs unpartitioned row_number side-by-side under total orders.",
    ),
    WindowRow(
        "order_by_nulls_first_row_number",
        "partition_nulls",
        f"SELECT id, v, row_number() OVER (ORDER BY v ASC NULLS FIRST, id) AS rn "
        f"{NULL_SEED_VALUES_SQL} ORDER BY rn",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("rn", pa.int32(), False)],
            {"id": [2, 4, 1, 3, 5], "v": [None, None, 10, 20, 30], "rn": [1, 2, 3, 4, 5]},
        ),
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("rn", pa.uint64(), False)],
            {"id": [2, 4, 1, 3, 5], "v": [None, None, 10, 20, 30], "rn": [1, 2, 3, 4, 5]},
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "ORDER BY v ASC NULLS FIRST inside a window: NULL rows take the leading row_numbers "
        "(total order via id tie-break).",
        order_sensitive=True,
    ),
    WindowRow(
        "order_by_nulls_last_row_number",
        "partition_nulls",
        f"SELECT id, v, row_number() OVER (ORDER BY v ASC NULLS LAST, id) AS rn "
        f"{NULL_SEED_VALUES_SQL} ORDER BY rn",
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("rn", pa.int32(), False)],
            {"id": [1, 3, 5, 2, 4], "v": [10, 20, 30, None, None], "rn": [1, 2, 3, 4, 5]},
        ),
        _table(
            [("id", pa.int64(), True), ("v", pa.int64(), True), ("rn", pa.uint64(), False)],
            {"id": [1, 3, 5, 2, 4], "v": [10, 20, 30, None, None], "rn": [1, 2, 3, 4, 5]},
        ),
        TYPE_DISC + f"{FIX_G5} when the SQL door matches the DF cast. "
        "ORDER BY v ASC NULLS LAST inside a window: NULL rows take the trailing row_numbers.",
        order_sensitive=True,
    ),
    # ----- 6. DataFrame-API entry points (CP-11) — ≥2 rows --------------------------------------
    WindowRow(
        "df_api_partition_by_row_number",
        "dataframe_api",
        # Documentation of the equivalent SQL; the DF recipe is what runs.
        "SELECT id, grp, row_number() OVER (PARTITION BY grp ORDER BY id) AS rn "
        "FROM win_seed ORDER BY grp, id",
        _table(
            [("id", pa.int64(), True), ("grp", pa.string(), True), ("rn", pa.int32(), False)],
            {"id": [1, 2, 3, 4, 5], "grp": ["A", "A", "A", "B", "B"], "rn": [1, 2, 3, 1, 2]},
        ),
        None,
        "DataFrame-API entry point (CP-11): F.row_number().over(Window.partitionBy('grp')"
        ".orderBy('id')). Distinct from facade sql() — crosses as a WindowSpec expression.",
        entry_point="dataframe_api",
        df_recipe="partition_row_number",
    ),
    WindowRow(
        "df_api_rows_between_sum",
        "dataframe_api",
        "SELECT id, grp, v, sum(v) OVER (PARTITION BY grp ORDER BY id "
        "ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM win_seed ORDER BY grp, id",
        _table(
            [
                ("id", pa.int64(), True),
                ("grp", pa.string(), True),
                ("v", pa.int64(), True),
                ("s", pa.int64(), True),
            ],
            {
                "id": [1, 2, 3, 4, 5],
                "grp": ["A", "A", "A", "B", "B"],
                "v": [10, 20, 30, 40, 50],
                "s": [10, 30, 50, 40, 90],
            },
        ),
        None,
        "DataFrame-API entry point (CP-11): F.sum('v').over(Window.partitionBy('grp')"
        ".orderBy('id').rowsBetween(-1, 0)). Explicit ROWS frame through the DF door.",
        entry_point="dataframe_api",
        df_recipe="rows_between_sum",
    ),
    # ----- 7. G5b temporal RANGE frames (interval bounds over datetime order keys) --------------
    #
    # The G5b section-0 recon (task/g5b-temporal-range-ledger.md) found this family ALREADY
    # matching Spark for interval-bounded frames — it was never "rejected outright" — and found
    # the real divergences at its edges. The equalities below are the standing detector for the
    # working path; the disclosures pin every edge that still differs, each naming what flips it.
    #
    # The refuse-arm twin (`RANGE BETWEEN <n> PRECEDING` over a TIMESTAMP key, where BOTH engines
    # raise once the G5b fix lands) cannot be expressed as a WindowRow — the shape forbids
    # pinning both engines raising — so it is pinned by
    # `test_temporal_range_bare_offset_over_timestamp_refuses` below, and on the Spark door by
    # `temporal_range_bare_offset_over_timestamp_key_refuses_like_spark`
    # (crates/repark-spark/src/tests/window_temporal_range.rs).
    WindowRow(
        "temporal_range_ts_asc_interval_day",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [10, 30, 60, 90, 90]},
        ),
        None,
        "Ascending timestamp order key, one-day trailing interval frame. The tied ids 4/5 both "
        "see the whole peer group (40+50), so this measures the frame, not a running total.",
    ),
    WindowRow(
        "temporal_range_ts_desc_interval_day",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts DESC RANGE BETWEEN INTERVAL '1' DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [60, 50, 30, 90, 90]},
        ),
        None,
        "DESC order key: PRECEDING must reach the LATER timestamps. The values differ from the "
        "ascending row on ids 1-3, so a direction-blind implementation cannot pass both.",
    ),
    WindowRow(
        "temporal_range_ts_peer_group_zero_interval",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '0' DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [10, 20, 30, 90, 90]},
        ),
        None,
        "TIES on the order key: a zero-width interval frame is exactly the peer group, so only "
        "the tied ids 4/5 see more than themselves. An engine that silently used ROWS here would "
        "answer 10/20/30/40/50.",
    ),
    WindowRow(
        "temporal_range_ts_null_order_keys",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_NULL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4], "s": [10, 60, 40, 60]},
        ),
        None,
        "NULL order keys under a temporal frame: the NULL rows form their own peer group under "
        "Spark's ASC NULLS FIRST default, so ids 2 and 4 sum each other (20+40).",
    ),
    WindowRow(
        "temporal_range_date_order_key_interval_day",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY d RANGE BETWEEN INTERVAL '1' DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_DATE_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3], "s": [10, 30, 30]},
        ),
        None,
        "DATE (not timestamp) order key: the day-granular key must still frame by interval. "
        "id 3 is three days after id 2, so it sees only itself.",
    ),
    WindowRow(
        "temporal_range_ts_partitioned",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (PARTITION BY id % 2 ORDER BY ts RANGE BETWEEN "
        f"INTERVAL '1' DAY PRECEDING AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [10, 20, 40, 40, 50]},
        ),
        None,
        "PARTITION BY with a temporal frame: the interval must not reach across partitions. The "
        "ids 4/5 tie is split by the partition key, so neither sees the other.",
    ),
    WindowRow(
        "temporal_range_ts_both_sides_interval",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY PRECEDING "
        f"AND INTERVAL '1' DAY FOLLOWING) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [60, 60, 60, 90, 90]},
        ),
        None,
        "Both bounds are intervals — a centred window, the shape a trailing-only implementation "
        "gets wrong. ids 1-3 all span the same +/-1 day neighbourhood.",
    ),
    WindowRow(
        "temporal_range_ts_hour_unit",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '12' HOUR PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [10, 30, 50, 90, 90]},
        ),
        None,
        "Sub-day unit: HOUR must not be read as DAY. id 3 sees id 2 (12h back) but not id 1 "
        "(24h back), so this row differs from the DAY row and pins that the unit is honoured.",
    ),
    WindowRow(
        "temporal_range_bare_offset_over_date_key_means_days",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY d RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s "
        f"{TEMPORAL_DATE_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3], "s": [10, 30, 30]},
        ),
        None,
        "THE G5b FIX, facade half: Spark reads a unit-less RANGE offset over a DATE key as one "
        "DAY. DataFusion coerces it to Interval(MonthDayNano), where Arrow reads a bare '1' as "
        "one MONTH — the same query, a silently wider window, no error (it answered 10/30/60). "
        "The Spark door now restates the bound as INTERVAL '1' DAY and re-plans. Reverting that "
        "turns this row red on value.",
    ),
    # --- Recorded residual divergences: each names the spelling or bound class it waits on ------
    WindowRow(
        "temporal_range_unquoted_interval_literal",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL 1 DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [10, 30, 60, 90, 90]},
        ),
        None,
        "DISCLOSURE (spelling): Spark accepts the unquoted `INTERVAL 1 DAY` frame bound and "
        "answers exactly as the quoted form. DataFusion's frame-bound parser accepts only a "
        "string-literal interval value there (`INTERVAL expression cannot be Value(Number...)`), "
        "even though the same unquoted literal works fine in an ordinary expression. Flipped by "
        + FIX_G5B,
        repark_raises="PySparkException",
    ),
    WindowRow(
        "temporal_range_day_to_second_literal",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1 12:00:00' DAY TO SECOND "
        f"PRECEDING AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [10, 30, 60, 90, 90]},
        ),
        None,
        "DISCLOSURE (spelling): Spark accepts a `DAY TO SECOND` qualified interval as a frame "
        "bound. DataFusion concatenates the literal and the LEADING field only, handing Arrow "
        "`1 12:00:00 DAY`, which its interval parser rejects. Flipped by " + FIX_G5B,
        repark_raises="PySparkException",
    ),
    WindowRow(
        "temporal_range_negative_offset_sum",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-1' DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [None, None, None, None, None]},
        ),
        None,
        "DISCLOSURE (defect): a NEGATIVE interval offset makes the frame end before it starts. "
        "Spark returns an empty frame (sum NULL). repark reaches DataFusion's sliding-sum "
        "accumulator with a malformed range and PANICS ('attempt to subtract with overflow'), "
        "surfacing at the boundary as an internal error. The panic is debug-build only: a "
        "release wheel wraps instead, so this is a wrong-answer class, not only a crash class. "
        "Flipped by " + FIX_G5B,
        repark_raises="PySparkException",
    ),
    WindowRow(
        "temporal_range_negative_offset_count",
        TEMPORAL_FAMILY,
        f"SELECT id, count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '-1' DAY PRECEDING "
        f"AND CURRENT ROW) AS c {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("c", pa.int64(), False)],
            {"id": [1, 2, 3, 4, 5], "c": [0, 0, 0, 0, 0]},
        ),
        _table(
            [("id", pa.int64(), True), ("c", pa.int64(), False)],
            {"id": [1, 2, 3, 4, 5], "c": [-1, -1, 0, 0, 0]},
        ),
        "DISCLOSURE (defect, silent): the same negative-offset frame as the row above, measured "
        "with count(*) instead of sum. Spark counts 0 rows in the empty frame; repark returns a "
        "NEGATIVE COUNT (-1) with no error at all. This is the row that shows the negative-offset "
        "class is a silent wrong answer and not merely a crash. Flipped by " + FIX_G5B,
    ),
    WindowRow(
        "temporal_range_following_to_following_window",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '1' DAY FOLLOWING "
        f"AND INTERVAL '2' DAY FOLLOWING) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [30, None, 90, None, None]},
        ),
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [30, None, 120, None, None]},
        ),
        "DISCLOSURE (value): a frame entirely in the FUTURE (both bounds FOLLOWING). For id 3 "
        "the frame is (ts+1d, ts+2d] = ids 4 and 5 -> Spark 90; repark answers 120, i.e. it also "
        "counts the current row, which is OUTSIDE its own frame. Silent: no error, a plausible "
        "number. Flipped by " + FIX_G5B,
    ),
    WindowRow(
        "temporal_range_interval_bound_over_int_key",
        TEMPORAL_FAMILY,
        f"SELECT id, sum(v) OVER (ORDER BY v RANGE BETWEEN INTERVAL '1' DAY PRECEDING "
        f"AND CURRENT ROW) AS s {TEMPORAL_TS_SQL} ORDER BY id",
        _table(
            [("id", pa.int64(), True), ("s", pa.int64(), True)],
            {"id": [1, 2, 3, 4, 5], "s": [10, 20, 30, 40, 50]},
        ),
        None,
        "DISCLOSURE (error class): an INTERVAL bound over an INT order key. Spark resolves it to "
        "a frame no row falls into, so every row sees only itself; repark surfaces a raw Arrow "
        "cast error (`Cannot cast string '1 DAY' to value of Int64 type`) instead of a Spark "
        "error class or a table. The G5b fix deliberately does not touch this arm (it only "
        "handles unit-less bounds over datetime keys). Flipped by " + FIX_G5B,
        repark_raises="PySparkException",
    ),
]


# ==================================================================================================
# Session + classification helpers
# ==================================================================================================


def _session() -> ReparkSession:
    """A plain repark session for window SQL / DataFrame API (no zone knob)."""
    import repark

    return repark.ReparkSession.builder.appName("window-parity").getOrCreate()


def _frames_differ(actual: pa.Table, expected: pa.Table, *, order_sensitive: bool = False) -> bool:
    """True when the parity comparator rejects the pair (schema, row count, or any value)."""
    try:
        assert_frames_equal(actual, expected, order_sensitive=order_sensitive)
    except FrameMismatchError:
        return True
    return False


def _exception_name(exc: BaseException) -> str:
    """Stable class-name string for raise-class matching."""
    return type(exc).__name__


def _matches_raise(exc: BaseException, expected_substring: str) -> bool:
    """True when the exception class name / MRO / message contains the recorded raise token."""
    names = [type(exc).__name__, *[base.__name__ for base in type(exc).mro()]]
    if any(expected_substring in name for name in names):
        return True
    return expected_substring in str(exc)


# ==================================================================================================
# The differential rows
# ==================================================================================================


@pytest.mark.parametrize("row", ROWS, ids=[row.name for row in ROWS])
def test_window_row_matches_spark_or_still_diverges(row: WindowRow) -> None:
    """Every recorded row, on the Arrow path (value AND exact Arrow type AND nullability).

    Equality rows assert ``repark == Spark``.

    Disclosure rows assert repark's pinned actual output — and when that assertion fails, the
    failure is CLASSIFIED before it is raised: CONVERGED (flip-don't-delete) vs regression
    (re-derive both halves). Raise-class rows assert the raising side still raises the recorded
    exception class and the non-raising side still matches its pinned table.
    """
    session = _session()

    # ----- raise-class rows ---------------------------------------------------------------------
    if row.repark_raises is not None:
        with pytest.raises(Exception) as excinfo:
            run_row(row, session)
        assert _matches_raise(excinfo.value, row.repark_raises), (
            f"{row.name}: repark was expected to raise matching {row.repark_raises!r}, got "
            f"{_exception_name(excinfo.value)}: {excinfo.value!s:.200}. {row.note}"
        )
        if row.spark is not None and row.repark is not None:
            assert _frames_differ(row.repark, row.spark, order_sensitive=row.order_sensitive), (
                f"{row.name}: both halves identical on a raise-class disclosure - re-record. "
                f"{row.note}"
            )
        return

    if row.spark_raises is not None:
        try:
            actual = run_row(row, session)
        except Exception as exc:
            if _matches_raise(exc, row.spark_raises):
                raise AssertionError(
                    f"{row.name}: repark and Spark have CONVERGED on the RAISE - repark now "
                    f"raises {row.spark_raises} too. Flip this disclosure to a shared-raise "
                    f"equality (or drop the repark pin) and record the convergence. {row.note}"
                ) from exc
            raise AssertionError(
                f"{row.name}: repark raised {_exception_name(exc)} instead of producing its "
                f"pinned table (and the raise is not the Spark {row.spark_raises} either) - "
                f"regression. Re-derive. {row.note}"
            ) from exc

        assert row.repark is not None, f"{row.name}: spark_raises row must pin a repark table"
        try:
            assert_frames_equal(actual, row.repark, order_sensitive=row.order_sensitive)
        except FrameMismatchError as mismatch:
            raise AssertionError(
                f"{row.name}: repark moved OFF its pinned disclosure for a spark_raises row - "
                f"regression. Re-derive both halves in record mode. {row.note}"
            ) from mismatch
        return

    # ----- table rows (equality or disclosure) ---------------------------------------------------
    assert row.spark is not None, (
        f"{row.name}: missing spark golden — run "
        f"python/repark/tests/_record_window_goldens.py --emit and paste"
    )
    actual = run_row(row, session)

    if row.repark is None:
        assert_frames_equal(actual, row.spark, order_sensitive=row.order_sensitive)
        return

    try:
        assert_frames_equal(actual, row.repark, order_sensitive=row.order_sensitive)
    except FrameMismatchError as mismatch:
        if not _frames_differ(actual, row.spark, order_sensitive=row.order_sensitive):
            raise AssertionError(
                f"{row.name}: repark and Spark have CONVERGED - repark now produces the RECORDED "
                f"SPARK output, so this disclosure is stale. Do not delete the row: flip it to an "
                f"equality row (repark=None) and record the convergence. {row.note}"
            ) from mismatch
        raise AssertionError(
            f"{row.name}: repark moved OFF its pinned disclosure and does NOT match the recorded "
            f"Spark golden either - this is a regression, not a convergence. Re-derive both "
            f"halves in record mode (see this module's docstring) before touching the pin. "
            f"{row.note}"
        ) from mismatch

    assert _frames_differ(row.repark, row.spark, order_sensitive=row.order_sensitive), (
        f"{row.name}: the row's two recorded halves are IDENTICAL, so it is not a disclosure at "
        f"all - either it converged and was half-edited, or the Spark half was pasted over the "
        f"repark half. Flip it to an equality row (repark=None) or re-record it. {row.note}"
    )


def test_window_row_set_covers_gap_budgets() -> None:
    """The pin budget is part of the unit, so the corpus size and shape are pinned, not incidental.

    G5's budget is 20-28 differential rows. At least :data:`MIN_EQUALITY_ROWS` plain equalities
    keep the corpus from degenerating into all-disclosures, and at most
    :data:`MAX_DISCLOSURE_ROWS` disclosures keep a future edit from turning every control red into
    a silent disclosure. Family coverage pins are name-gated or semantics-gated so a CONTROL row
    cannot satisfy them (CP-2).
    """
    assert G5_BUDGET_MIN <= len(ROWS) <= G5_BUDGET_MAX, (
        f"G5 budget {G5_BUDGET_MIN}-{G5_BUDGET_MAX}, got {len(ROWS)}"
    )
    assert len({row.name for row in ROWS}) == len(ROWS), "differential row names are unique"

    equalities = [row for row in ROWS if row.is_equality()]
    disclosures = [row for row in ROWS if row.is_disclosure()]
    assert len(equalities) >= MIN_EQUALITY_ROWS, (
        f"at least {MIN_EQUALITY_ROWS} control equality rows required so the corpus cannot "
        f"degenerate to all-disclosures; got {len(equalities)}"
    )
    assert len(disclosures) <= MAX_DISCLOSURE_ROWS, (
        f"at most {MAX_DISCLOSURE_ROWS} disclosures so the corpus cannot silently absorb every "
        f"regression as a new disclosure; got {len(disclosures)}"
    )
    assert all(row.spark is not None or row.spark_raises is not None for row in ROWS), (
        "every row must carry a recorded spark golden or spark_raises token"
    )

    names = {row.name for row in ROWS}

    # 1. Default-frame trap family — name-gated (≥3) so a control cannot satisfy (CP-2).
    default_frame = [row for row in ROWS if row.name.startswith("default_frame_")]
    assert len(default_frame) >= 3, (
        "G5 must keep the default-frame-trap family "
        f"(>=3 rows named default_frame_*); got {len(default_frame)}"
    )
    assert any("sum" in row.name and "ties" in row.name for row in default_frame), (
        "default-frame family must pin aggregate-over-window sum with ties"
    )

    # 2. Explicit frames — ROWS vs RANGE, sliding, unbounded.
    assert any("rows_vs_range" in name for name in names), "must pin ROWS vs RANGE peer difference"
    assert any("sliding" in name for name in names), "must pin a sliding ROWS frame"
    assert any("range_value_offset" in name for name in names), "must pin RANGE value-offset"
    assert any("unbounded_following" in name for name in names), (
        "must pin current→unbounded following"
    )

    # 3. Ranking family — name-gated so a lone row_number control cannot cover rank/dense/ntile.
    ranking_needles = [
        "rank_with_ties",
        "dense_rank_with_ties",
        "row_number",
        "ntile_",
        "percent_rank",
    ]
    for needle in ranking_needles:
        assert any(needle in name for name in names), f"missing ranking coverage for {needle!r}"

    # 4. Offset family — lag/lead default + explicit default value + NULL payload.
    assert any(name.startswith("lag_default") for name in names), "must pin lag default"
    assert any("lag_offset" in name and "default_value" in name for name in names), (
        "must pin lag with explicit default value"
    )
    assert any(name.startswith("lead_default") for name in names), "must pin lead default"
    assert any("lag_over_null" in name for name in names), "must pin lag over NULL payloads"

    # 5. Partitioned vs unpartitioned; NULLS FIRST/LAST.
    assert any("partitioned_vs_unpartitioned" in name for name in names), (
        "must pin partitioned vs unpartitioned"
    )
    assert any("nulls_first" in name for name in names), "must pin ORDER BY NULLS FIRST"
    assert any("nulls_last" in name for name in names), "must pin ORDER BY NULLS LAST"

    # 6. DataFrame-API entry points (CP-11) — ≥2, name-gated.
    df_api = [row for row in ROWS if row.entry_point == "dataframe_api"]
    assert len(df_api) >= 2, f"CP-11 requires ≥2 DataFrame-API rows; got {len(df_api)}"
    assert all(row.df_recipe in DF_RECIPES for row in df_api), "df_recipe must resolve"

    # Determinism discipline: every DF-API recipe uses total order (documented on the helper).
    # SQL rows that use non-total ORDER BY must be peer-determined (rank/default-RANGE) —
    # enforced by construction in the row notes; pin that default_frame rows do NOT order by id
    # alone as the window key (they order by k, which has ties, intentionally).
    for row in default_frame:
        assert "ORDER BY k" in row.sql or "ORDER BY k)" in row.sql or "ORDER BY k " in row.sql, (
            f"{row.name}: default-frame trap must ORDER BY the tied key k (not a total order)"
        )

    # 7. G5b temporal-RANGE family — name-gated so a numeric-RANGE control cannot satisfy it.
    temporal = [row for row in ROWS if row.family == TEMPORAL_FAMILY]
    assert len(temporal) >= 12, (
        "G5b must keep the temporal-RANGE family (>=12 rows in family "
        f"{TEMPORAL_FAMILY!r}); got {len(temporal)}"
    )
    temporal_names = {row.name for row in temporal}
    for needle in (
        "ts_asc_interval_day",
        "ts_desc_interval_day",
        "peer_group_zero_interval",
        "null_order_keys",
        "date_order_key",
        "hour_unit",
        "bare_offset_over_date_key_means_days",
    ):
        assert any(needle in name for name in temporal_names), (
            f"temporal-RANGE family is missing coverage for {needle!r}"
        )
    # The family must keep BOTH halves: working-path equalities and recorded residuals. An edit
    # that deletes either side would leave the claim unbacked or the divergences unrecorded.
    assert sum(row.is_equality() for row in temporal) >= 8, (
        "temporal-RANGE family must keep >=8 equality rows (the working interval-bounded path)"
    )
    assert sum(row.is_disclosure() for row in temporal) >= 5, (
        "temporal-RANGE family must keep >=5 recorded residual divergences"
    )

    # Row-shape well-formedness.
    for row in ROWS:
        assert not (row.spark_raises and row.repark_raises), (
            f"{row.name}: a row cannot pin both engines raising"
        )
        if row.spark_raises is not None:
            assert row.spark is None, f"{row.name}: spark_raises forbids a spark table half"
            assert row.repark is not None, f"{row.name}: spark_raises requires a repark table pin"
        if row.repark_raises is not None:
            assert row.repark is None, f"{row.name}: repark_raises forbids a repark table half"
            assert row.spark is not None, f"{row.name}: repark_raises requires a spark table pin"
        if row.entry_point == "dataframe_api":
            assert row.df_recipe is not None
        if row.is_equality():
            assert row.spark is not None and row.repark is None


# ==================================================================================================
# G5b — the refuse arm, which the WindowRow shape cannot express (both engines raise)
# ==================================================================================================


def test_temporal_range_bare_offset_over_timestamp_refuses() -> None:
    """A unit-less RANGE offset over a TIMESTAMP order key must refuse with Spark's error class.

    Spark 4.1.2 raises ``DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE`` here (recorded live in the
    G5b section-0 recon). Before the fix repark ANSWERED instead: DataFusion coerces the bare
    offset to ``Interval(MonthDayNano)`` and Arrow reads a unit-less ``"1"`` as one *month*, so a
    migrated query silently got a one-month window and no warning.

    This lives outside :data:`ROWS` because a differential row cannot pin both engines raising
    (``WindowRow`` forbids ``spark_raises`` and ``repark_raises`` together) — the shared refusal
    is the point, so it is asserted directly. The Spark-door twin is
    ``temporal_range_bare_offset_over_timestamp_key_refuses_like_spark`` in
    ``crates/repark-spark/src/tests/window_temporal_range.rs``.
    """
    session = _session()
    register_temporal_seed_views(session)

    for frame in (
        "RANGE BETWEEN 1 PRECEDING AND CURRENT ROW",
        "RANGE BETWEEN 30 PRECEDING AND CURRENT ROW",
        "RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING",
    ):
        sql = f"SELECT id, sum(v) OVER (ORDER BY ts {frame}) AS s {TEMPORAL_TS_SQL} ORDER BY id"
        with pytest.raises(Exception) as excinfo:
            _to_arrow(session.sql(sql))
        message = str(excinfo.value)
        assert "DATATYPE_MISMATCH.RANGE_FRAME_INVALID_TYPE" in message, (
            f"{frame!r} must refuse with Spark's error class, got: {message}"
        )
        assert 'does not support the data type "INT"' in message, (
            f"{frame!r} must carry Spark's INT-in-range-frame wording, got: {message}"
        )


def test_temporal_range_bare_offset_over_date_key_is_days_not_months() -> None:
    """The DATE arm of the same fix, asserted as a value claim on two different offsets.

    One offset alone cannot carry this: over the three-day seed a one-MONTH window and a
    thirty-DAY window produce the same numbers. ``1 PRECEDING`` (one day, ``[10, 30, 30]``) and
    ``30 PRECEDING`` (thirty days, ``[10, 30, 60]``) together pin that the unit is days.
    """
    session = _session()
    register_temporal_seed_views(session)

    def sums(offset: int) -> list[int | None]:
        table = _to_arrow(
            session.sql(
                f"SELECT id, sum(v) OVER (ORDER BY d RANGE BETWEEN {offset} PRECEDING "
                f"AND CURRENT ROW) AS s {TEMPORAL_DATE_SQL} ORDER BY id"
            )
        )
        return table.column("s").to_pylist()

    assert sums(1) == [10, 30, 30], "1 PRECEDING over a DATE key is one DAY (Spark), not a month"
    assert sums(30) == [10, 30, 60], "30 PRECEDING over a DATE key is thirty DAYS"
