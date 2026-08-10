"""Shared scenario registry for the **live PySpark oracle tier** (L1).

The parity discipline is *record-then-pin*: goldens are derived from live PySpark 4.1.2 at
authoring time and pinned inline in the facade tests; routine CI is JVM-free and never re-checks
them. Two failure classes are then invisible until a human re-runs the oracle by hand:

* **golden drift** — a stale or hand-edited pin no longer matches what Spark actually produces;
* **oracle drift** — a Spark bump silently changes semantics under a still-green pin.

This module is the drift detector's engine. It holds every mandated golden as an *engine-agnostic
recipe* (`Scenario.recipe`) plus its pinned `golden` table. Because repark is a **near-drop-in for
PySpark** — the same `createDataFrame` / DataFrame-API / `functions` surface, only the import line
differs — one recipe runs unchanged on BOTH engines. The live tier
(`test_parity_live.py`) then asserts the full triple **repark == pinned golden == live Spark**
(value AND Arrow-path type/nullability) for every scenario; routine CI runs only the JVM-free
`repark == golden` half of the same recipes (`test_scenario_recipe_matches_golden_on_repark`), so
the recipes themselves carry no-JVM coverage.

Nothing here imports pyspark at module load — the pyspark import is deferred into
`build_spark_engine`, so this module (and the tests that import it) collect cleanly on a runner with
neither pyspark nor a JVM installed (the routine-CI contract, L3).

Session config (VERIFIED against live PySpark 4.1.2, not guessed): the Group E / columns / date
goldens were recorded under Spark 4.1.2 defaults — **ANSI mode ON** (Spark 4 default; the
int-UNION-string disclosure literally depends on it) — so `build_spark_engine` pins
`spark.sql.ansi.enabled=true` explicitly. The registry's **default** session zone is `UTC` for
determinism across runners. `master("local[2]")` per the plan.

**Per-scenario session-conf override (H-1a).** A registry pinned to one session zone is
structurally incapable of catching a session-timezone divergence — the whole class is invisible to
it. `Scenario.session_conf` therefore carries conf pairs applied to BOTH engines for that scenario
only: the oracle takes them through `spark_session_conf` (set, run, restore), and repark takes them
by BUILDING a session with them, because repark resolves the session zone once at session
construction. Scenarios that declare no override behave exactly as before.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

# ==================================================================================================
# Live-mode gate
# ==================================================================================================

LIVE_ENV_VAR = "REPARK_PARITY_LIVE"
LIVE_SKIP_REASON = (
    f"{LIVE_ENV_VAR} unset — the live PySpark oracle tier is skipped (routine CI is JVM-free). "
    f"Set {LIVE_ENV_VAR}=1 with a JVM present (JAVA_HOME=/usr/lib/jvm/zulu-17-amd64) to run it."
)


def live_enabled(environ: Mapping[str, str] | None = None) -> bool:
    """Return whether the live oracle tier is armed.

    The gate is the exact string ``"1"`` in ``REPARK_PARITY_LIVE`` — an unset var, empty string,
    ``"0"``, or any other value leaves the tier OFF (tests SKIP, never silently pass). Takes an
    explicit ``environ`` so the L6(a) detector can pin the predicate without mutating the process
    environment.
    """
    env = os.environ if environ is None else environ
    return env.get(LIVE_ENV_VAR) == "1"


LIVE = live_enabled()


# ==================================================================================================
# Per-scenario session-conf override (H-1a)
# ==================================================================================================

# Conf pairs as a TUPLE of pairs, not a dict: `Scenario` is a frozen dataclass and the override is
# part of a scenario's identity, so it must be hashable and immutable like the rest of it.
SessionConf = tuple[tuple[str, str], ...]

# The ONE session-timezone conf key (mirrors `repark.session.session_time_zone`; PySpark's own
# spelling, so the identical pair configures both engines).
SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"

# The registry's default oracle zone, and the two non-UTC zones scenarios override it with.
DEFAULT_SESSION_TIME_ZONE = "UTC"
ZONE_NEW_YORK = "America/New_York"
ZONE_TOKYO = "Asia/Tokyo"


# ==================================================================================================
# Engine abstraction — one recipe, two engines
# ==================================================================================================


@dataclass(frozen=True)
class Engine:
    """A uniform handle over either engine's PySpark-shaped API surface.

    `session` exposes `createDataFrame` / `sql`; `functions` is the ``F`` module, `types` the ``T``
    module, `window` the ``Window`` class; `arrow_of` extracts a `pyarrow.Table` (repark's
    ``to_arrow`` vs PySpark 4's ``toArrow``). Recipes touch only this surface, so the identical
    recipe body runs on both engines.
    """

    name: str
    session: Any
    functions: Any
    types: Any
    window: Any
    arrow_of: Callable[[Any], pa.Table]


def build_repark_engine(session_conf: SessionConf = ()) -> Engine:
    """A fresh repark engine (cheap — no JVM).

    `session_conf` is applied at BUILD time, because repark resolves build-time knobs (the session
    timezone among them) once at `getOrCreate` — a runtime `conf.set` of one would move the facade
    and leave the live engine session where it was. When an override is requested, an already-active
    session is stopped first so `getOrCreate` cannot hand back a session carrying the previous
    scenario's conf. With no override the call is byte-for-byte the pre-H-1a behavior.
    """
    import repark
    from repark import Window
    from repark import functions as rfunctions
    from repark import types as rtypes

    builder = repark.ReparkSession.builder.appName("repark-parity-live")
    if session_conf:
        active = repark.ReparkSession.getActiveSession()
        if active is not None:
            active.stop()
        for key, value in session_conf:
            builder = builder.config(key, value)
    session = builder.getOrCreate()
    return Engine(
        name="repark",
        session=session,
        functions=rfunctions,
        types=rtypes,
        window=Window,
        arrow_of=lambda df: df.to_arrow(),
    )


def build_spark_engine() -> Engine:
    """The live PySpark oracle engine. Imports pyspark lazily (never at module load) and pins the
    session config the goldens were recorded under (ANSI on, UTC, ``local[2]``)."""
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as sfunctions
    from pyspark.sql import types as stypes

    session = (
        SparkSession.builder.master("local[2]")
        .appName("repark-parity-live-oracle")
        .config("spark.sql.ansi.enabled", "true")  # Spark 4 default; the goldens' recorded basis
        # The registry DEFAULT zone; a scenario overrides it per run via `spark_session_conf`.
        .config(SESSION_TIME_ZONE_KEY, DEFAULT_SESSION_TIME_ZONE)
        .config("spark.sql.shuffle.partitions", "2")  # tiny fixtures — keep shuffles cheap
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return Engine(
        name="spark",
        session=session,
        functions=sfunctions,
        types=stypes,
        window=Window,
        arrow_of=lambda df: df.toArrow(),
    )


@contextlib.contextmanager
def spark_session_conf(engine: Engine, session_conf: SessionConf) -> Iterator[None]:
    """Apply `session_conf` to the SHARED live oracle session, then restore it.

    The oracle session is session-scoped (one JVM per pytest run), so a scenario override must be
    reversible or it would leak into every later scenario. PySpark's `conf.set` is live for the
    session-timezone key, which is exactly why the two engines need different application
    mechanisms for the same override.
    """
    if not session_conf:
        yield
        return
    previous = {key: engine.session.conf.get(key) for key, _ in session_conf}
    try:
        for key, value in session_conf:
            engine.session.conf.set(key, value)
        yield
    finally:
        for key, value in previous.items():
            engine.session.conf.set(key, value)


def run_scenario(scenario: Scenario, engine: Engine) -> pa.Table:
    """Execute a scenario's recipe on an engine and return its Arrow output."""
    return engine.arrow_of(scenario.recipe(engine))


# ==================================================================================================
# Shared fixtures (engine-agnostic)
# ==================================================================================================


def _na_frame(engine: Engine) -> Any:
    """The shared na fixture: (i, s, d) with an all-null middle row and a trailing-null d — built
    via ``createDataFrame`` so both engines infer nullable ``string`` (not ``string_view``)."""
    return engine.session.createDataFrame(
        [(1, "a", 1.0), (None, None, None), (3, "c", None)], ["i", "s", "d"]
    )


def _date_spine(engine: Engine, dates: list[str]) -> Any:
    """A one-column nullable ``calendar_date`` DataFrame from ISO date strings, built via
    ``createDataFrame`` + ``cast(DateType())`` (both engines: ``date32``/nullable)."""
    f, t = engine.functions, engine.types
    rows = [(value,) for value in dates]
    return engine.session.createDataFrame(rows, ["calendar_date_str"]).select(
        f.col("calendar_date_str").cast(t.DateType()).alias("calendar_date")
    )


# ==================================================================================================
# Scenario registry — the 23-golden family (Group E + compound-agg display name + columns + dates)
# ==================================================================================================


@dataclass(frozen=True)
class Scenario:
    """A named golden: an engine-agnostic `recipe` and the pinned `golden` it must reproduce on
    BOTH engines. `order_sensitive` mirrors the source pin (True where an ``ORDER BY`` is under
    test). `session_conf` is the per-scenario override (H-1a): conf pairs applied to both engines
    for this scenario only — empty for every scenario recorded under the registry default."""

    name: str
    recipe: Callable[[Engine], Any]
    golden: pa.Table
    order_sensitive: bool = False
    session_conf: SessionConf = field(default=())


# ----- Group E: group-by / aggregate (7) ---------------------------------------------------------


def _sc_groupby_sum_no_args(engine: Engine) -> Any:
    src = engine.session.createDataFrame(
        [(1, 10, 100), (1, 20, 200), (2, 30, 300)], ["g", "x", "y"]
    )
    return src.groupBy("g").sum()


def _sc_groupby_min_no_args(engine: Engine) -> Any:
    src = engine.session.createDataFrame(
        [(1, 10, 100), (1, 20, 200), (2, 30, 300)], ["g", "x", "y"]
    )
    return src.groupBy("g").min()


def _sc_groupby_sum_skips_nulls(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.createDataFrame([(1, 10), (1, None), (2, 30), (2, 40)], ["g", "x"])
    return src.groupBy("g").agg(f.sum("x"))


def _sc_groupby_sum_compound_display_name(engine: Engine) -> Any:
    """Compound-arg aggregate naming: ``sum((x + 1))`` (live PySpark 4.1.2 recorded)."""
    f = engine.functions
    src = engine.session.createDataFrame([(1, 10), (1, 20), (2, 30)], ["g", "x"])
    return src.groupBy("g").agg(f.sum(f.col("x") + 1)).orderBy("g")


def _sc_count_star_vs_count_col(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.createDataFrame([(1, 10), (1, None), (2, 30)], ["g", "x"])
    return src.groupBy("g").agg(f.count("*"), f.count("x"))


def _sc_avg_is_double(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.createDataFrame([(1, 10), (1, 20), (2, 30)], ["g", "x"])
    return src.groupBy("g").agg(f.avg("x"))


def _sc_min_max_preserve_type(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.createDataFrame([(1, 10), (1, 40), (2, 30)], ["g", "x"])
    return src.groupBy("g").agg(f.min("x"), f.max("x"))


def _sc_count_distinct(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.createDataFrame([(1, 5), (1, 5), (1, 7), (2, 9)], ["g", "x"])
    return src.groupBy("g").agg(f.countDistinct("x"))


# ----- Group E: na family (3) --------------------------------------------------------------------


def _sc_fillna_dict(engine: Engine) -> Any:
    return _na_frame(engine).fillna({"i": -1, "s": "X", "d": -9.0})


def _sc_na_fill_string(engine: Engine) -> Any:
    return _na_frame(engine).na.fill("Z")


def _sc_dropna_any(engine: Engine) -> Any:
    return _na_frame(engine).dropna()


# ----- Group E: union / dedup (3) ----------------------------------------------------------------


def _sc_union_type_coercion(engine: Engine) -> Any:
    ints = engine.session.createDataFrame([(1,)], ["v"])
    doubles = engine.session.createDataFrame([(2.5,)], ["v"])
    return ints.union(doubles)


def _sc_union_by_name_allow_missing(engine: Engine) -> Any:
    a = engine.session.createDataFrame([(1, "a")], ["id", "name"])
    wide = engine.session.createDataFrame([(9, "z", 99)], ["id", "name", "extra"])
    return a.unionByName(wide, allowMissingColumns=True)


def _sc_drop_duplicates_subset(engine: Engine) -> Any:
    src = engine.session.createDataFrame([(1, "a"), (1, "a"), (2, "b")], ["k", "v"])
    return src.dropDuplicates(["k"])


# ----- Columns / expressions (5) -----------------------------------------------------------------


def _sc_coalesce_cast_chain(engine: Engine) -> Any:
    f, t = engine.functions, engine.types
    src = engine.session.createDataFrame([(1, "x"), (2, None), (3, "z")], ["id", "name"])
    return (
        src.withColumn("clean_name", f.coalesce(f.col("name"), f.lit("unknown")))
        .withColumn("id_str", f.col("id").cast(t.StringType()))
        .select("id", "clean_name", "id_str")
    )


def _sc_filter_orderby(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.createDataFrame([(1, 5.0), (2, 9.0), (3, 1.0)], ["id", "amt"])
    return src.filter(f.col("id") >= 2).orderBy(f.col("amt").desc()).select("id", "amt")


def _sc_integer_division(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.sql("SELECT * FROM (VALUES (7, 2), (9, 2)) AS t(a, b)")
    return src.withColumn("d", f.col("a") / f.col("b")).select("d")


def _sc_division_union(engine: Engine) -> Any:
    """A union of two integer divisions — the Group L-write write-schema regression's expression
    class, exercised on the SELECT path so the live tier re-derives its double {2.5, 3.5} oracle
    (both engines: integer `/` is always-double, and the set-op parent reconciles to double)."""
    return engine.session.sql("SELECT 5/2 AS q UNION ALL SELECT 7/2")


def _sc_division_bare(engine: Engine) -> Any:
    """A bare integer division — the simple-expression control for the union case above (double)."""
    return engine.session.sql("SELECT 7/2 AS q")


def _sc_concat_null(engine: Engine) -> Any:
    f = engine.functions
    src = engine.session.sql(
        "SELECT * FROM (VALUES ('a', 'b'), ('c', CAST(NULL AS STRING))) AS t(x, y)"
    )
    return src.withColumn("j", f.concat(f.col("x"), f.col("y"))).select("j")


def _sc_end_to_end_chain(engine: Engine) -> Any:
    # Unique per-call view names so the shared live SparkSession never leaks state between
    # scenarios (Critic session-leakage guard). Two createDataFrame frames can't be name-joined
    # directly on repark (shared subquery alias) — temp views give distinct qualifiers on both.
    f, t = engine.functions, engine.types
    token = uuid.uuid4().hex[:8]
    facts_view, dims_view = f"e2e_facts_{token}", f"e2e_dims_{token}"
    engine.session.createDataFrame(
        [(1, 10.0), (2, 20.0), (3, 30.0)], ["id", "amt"]
    ).createOrReplaceTempView(facts_view)
    engine.session.createDataFrame(
        [(1, "A"), (2, "B"), (3, "C")], ["id", "label"]
    ).createOrReplaceTempView(dims_view)
    facts = engine.session.sql(f"SELECT * FROM {facts_view}")
    dims = engine.session.sql(f"SELECT * FROM {dims_view}")
    return (
        facts.withColumn("amt_x2", (f.col("amt") * 2).cast(t.IntegerType()))
        .filter(f.col("id") > 1)
        .join(dims, on="id", how="inner")
        .orderBy(f.col("id").asc())
        .select("id", "label", "amt_x2")
    )


# ----- Date functions (4) ------------------------------------------------------------------------


def _sc_date_extractor(engine: Engine) -> Any:
    f = engine.functions
    src = _date_spine(engine, ["2016-02-29", "2025-04-01"])
    return src.select(
        f.year("calendar_date").alias("year"),
        f.quarter("calendar_date").alias("quarter"),
        f.month("calendar_date").alias("month"),
        f.dayofmonth("calendar_date").alias("day"),
        f.dayofyear("calendar_date").alias("day_of_year"),
        f.dayofweek("calendar_date").alias("day_of_week"),
    )


def _sc_date_math(engine: Engine) -> Any:
    f = engine.functions
    src = _date_spine(engine, ["2016-02-29", "2025-01-15"])
    return src.select(
        f.add_months("calendar_date", -12).alias("prior_year"),
        f.last_day("calendar_date").alias("month_end"),
        f.date_add("calendar_date", 1).alias("next_day"),
        f.trunc("calendar_date", "quarter").alias("quarter_start"),
    )


def _sc_date_format(engine: Engine) -> Any:
    f = engine.functions
    src = _date_spine(engine, ["2025-01-08", "2025-05-14"])
    return src.select(
        f.date_format("calendar_date", "yyyyMMdd").alias("date_key"),
        f.date_format("calendar_date", "yyyy'Q'q").alias("year_quarter"),
        f.date_format("calendar_date", "MMMM").alias("month_name"),
    )


def _sc_row_number_ordered(engine: Engine) -> Any:
    f, window = engine.functions, engine.window
    src = engine.session.createDataFrame([(30,), (10,), (20,)], ["v"])
    return (
        src.withColumn("rn", f.row_number().over(window.orderBy(f.col("v").asc())))
        .orderBy(f.col("rn").asc())
        .select("rn", "v")
    )


# ----- Filter-predicate rewriter (2, audit G2) ---------------------------------------------------


def _sc_filter_unambiguous_on_case_colliding_frame(engine: Engine) -> Any:
    """A frame whose `id`/`ID` collide only by case is legal on BOTH engines; a SQL-string predicate
    naming the UNAMBIGUOUS `other` column still filters. (Naming `id` raises AMBIGUOUS_REFERENCE on
    both — the raise is pinned JVM-free in test_filter_predicate_rewrite.py; this leg is the
    value-returning half, the one the over-refusal regression broke.)"""
    src = engine.session.createDataFrame([(1, 2, 3)], ["id", "ID", "other"])
    return src.filter("other > 0")


def _sc_filter_keyword_literal_false_column(engine: Engine) -> Any:
    """`false` keeps its grammar meaning against a column literally named `false`: the predicate is
    the boolean literal, so the result is EMPTY (not a bind to the int column)."""
    src = engine.session.createDataFrame([(1, 2), (3, 4)], ["false", "b"])
    return src.filter("false")


# ----- Non-UTC oracle session (2, H-1a) ----------------------------------------------------------
#
# The first scenarios in this registry that run the ORACLE under a non-UTC session zone — the
# reason `Scenario.session_conf` exists. Both assert a real invariant: a DATE carries no instant,
# so DATE extraction and DATE arithmetic must NOT move with the session zone. That is what makes
# them safe to assert as EQUALITY today (the session-timezone extraction gap is a TIMESTAMP gap)
# and load-bearing tomorrow: a fix that pushed the session zone into the DATE path reds here.
# The divergent TIMESTAMP rows for the same class live in test_session_timezone_parity.py as
# recorded disclosures until that fix lands.


def _sc_date_extractor_under_new_york_session(engine: Engine) -> Any:
    """DATE extraction under an `America/New_York` oracle session — zone-independent by contract."""
    return engine.session.sql(
        "SELECT year(to_date('2024-02-29')) AS year_part, "
        "month(to_date('2024-02-29')) AS month_part, "
        "dayofmonth(to_date('2024-02-29')) AS day_part"
    )


def _sc_date_math_under_tokyo_session(engine: Engine) -> Any:
    """Leap-day DATE arithmetic under an `Asia/Tokyo` oracle session — the other side of UTC."""
    return engine.session.sql(
        "SELECT last_day(to_date('2024-02-01')) AS month_end, "
        "trunc(to_date('2024-02-29'), 'YEAR') AS year_start, "
        "datediff(to_date('2024-03-01'), to_date('2024-02-01')) AS february_days"
    )


SCENARIOS: list[Scenario] = [
    # ----- group-by / aggregate -----
    Scenario(
        "groupby_sum_no_args",
        _sc_groupby_sum_no_args,
        pa.table(
            [
                pa.array([1, 2], pa.int64()),
                pa.array([2, 2], pa.int64()),
                pa.array([30, 30], pa.int64()),
                pa.array([300, 300], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("sum(g)", pa.int64(), nullable=True),
                    pa.field("sum(x)", pa.int64(), nullable=True),
                    pa.field("sum(y)", pa.int64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "groupby_min_no_args",
        _sc_groupby_min_no_args,
        pa.table(
            [
                pa.array([1, 2], pa.int64()),
                pa.array([1, 2], pa.int64()),
                pa.array([10, 30], pa.int64()),
                pa.array([100, 300], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("min(g)", pa.int64(), nullable=True),
                    pa.field("min(x)", pa.int64(), nullable=True),
                    pa.field("min(y)", pa.int64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "groupby_sum_compound_display_name",
        _sc_groupby_sum_compound_display_name,
        pa.table(
            [
                pa.array([1, 2], pa.int64()),
                pa.array([32, 31], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("sum((x + 1))", pa.int64(), nullable=True),
                ]
            ),
        ),
        order_sensitive=True,
    ),
    Scenario(
        "groupby_sum_skips_nulls",
        _sc_groupby_sum_skips_nulls,
        pa.table(
            [pa.array([1, 2], pa.int64()), pa.array([10, 70], pa.int64())],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("sum(x)", pa.int64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "count_star_vs_count_col",
        _sc_count_star_vs_count_col,
        pa.table(
            [
                pa.array([1, 2], pa.int64()),
                pa.array([2, 1], pa.int64()),
                pa.array([1, 1], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("count(1)", pa.int64(), nullable=False),
                    pa.field("count(x)", pa.int64(), nullable=False),
                ]
            ),
        ),
    ),
    Scenario(
        "avg_is_double",
        _sc_avg_is_double,
        pa.table(
            [pa.array([1, 2], pa.int64()), pa.array([15.0, 30.0], pa.float64())],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("avg(x)", pa.float64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "min_max_preserve_type",
        _sc_min_max_preserve_type,
        pa.table(
            [
                pa.array([1, 2], pa.int64()),
                pa.array([10, 30], pa.int64()),
                pa.array([40, 30], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("min(x)", pa.int64(), nullable=True),
                    pa.field("max(x)", pa.int64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "count_distinct",
        _sc_count_distinct,
        pa.table(
            [pa.array([1, 2], pa.int64()), pa.array([2, 1], pa.int64())],
            schema=pa.schema(
                [
                    pa.field("g", pa.int64(), nullable=True),
                    pa.field("count(DISTINCT x)", pa.int64(), nullable=False),
                ]
            ),
        ),
    ),
    # ----- na family -----
    Scenario(
        "fillna_dict",
        _sc_fillna_dict,
        pa.table(
            [
                pa.array([1, -1, 3], pa.int64()),
                pa.array(["a", "X", "c"], pa.string()),
                pa.array([1.0, -9.0, -9.0], pa.float64()),
            ],
            schema=pa.schema(
                [
                    pa.field("i", pa.int64(), nullable=False),
                    pa.field("s", pa.string(), nullable=False),
                    pa.field("d", pa.float64(), nullable=False),
                ]
            ),
        ),
    ),
    Scenario(
        "na_fill_string",
        _sc_na_fill_string,
        pa.table(
            [
                pa.array([1, None, 3], pa.int64()),
                pa.array(["a", "Z", "c"], pa.string()),
                pa.array([1.0, None, None], pa.float64()),
            ],
            schema=pa.schema(
                [
                    pa.field("i", pa.int64(), nullable=True),
                    pa.field("s", pa.string(), nullable=False),
                    pa.field("d", pa.float64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "dropna_any",
        _sc_dropna_any,
        pa.table(
            [
                pa.array([1], pa.int64()),
                pa.array(["a"], pa.string()),
                pa.array([1.0], pa.float64()),
            ],
            schema=pa.schema(
                [
                    pa.field("i", pa.int64(), nullable=True),
                    pa.field("s", pa.string(), nullable=True),
                    pa.field("d", pa.float64(), nullable=True),
                ]
            ),
        ),
    ),
    # ----- union / dedup -----
    Scenario(
        "union_type_coercion",
        _sc_union_type_coercion,
        pa.table(
            [pa.array([1.0, 2.5], pa.float64())],
            schema=pa.schema([pa.field("v", pa.float64(), nullable=True)]),
        ),
    ),
    Scenario(
        "union_by_name_allow_missing",
        _sc_union_by_name_allow_missing,
        pa.table(
            [
                pa.array([1, 9], pa.int64()),
                pa.array(["a", "z"], pa.string()),
                pa.array([None, 99], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=True),
                    pa.field("name", pa.string(), nullable=True),
                    pa.field("extra", pa.int64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "drop_duplicates_subset",
        _sc_drop_duplicates_subset,
        pa.table(
            [pa.array([1, 2], pa.int64()), pa.array(["a", "b"], pa.string())],
            schema=pa.schema(
                [
                    pa.field("k", pa.int64(), nullable=True),
                    pa.field("v", pa.string(), nullable=True),
                ]
            ),
        ),
    ),
    # ----- columns / expressions -----
    Scenario(
        "coalesce_cast_chain",
        _sc_coalesce_cast_chain,
        pa.table(
            [
                pa.array([1, 2, 3], pa.int64()),
                pa.array(["x", "unknown", "z"], pa.string()),
                pa.array(["1", "2", "3"], pa.string()),
            ],
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=True),
                    pa.field("clean_name", pa.string(), nullable=False),
                    pa.field("id_str", pa.string(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "filter_orderby",
        _sc_filter_orderby,
        pa.table(
            {
                "id": pa.array([2, 3], pa.int64()),
                "amt": pa.array([9.0, 1.0], pa.float64()),
            }
        ),
        order_sensitive=True,
    ),
    Scenario(
        "integer_division",
        _sc_integer_division,
        pa.table({"d": pa.array([3.5, 4.5], pa.float64())}),
    ),
    Scenario(
        "division_union",
        _sc_division_union,
        pa.table(
            [pa.array([2.5, 3.5], pa.float64())],
            schema=pa.schema([pa.field("q", pa.float64(), nullable=True)]),
        ),
    ),
    Scenario(
        "division_bare",
        _sc_division_bare,
        pa.table(
            [pa.array([3.5], pa.float64())],
            schema=pa.schema([pa.field("q", pa.float64(), nullable=True)]),
        ),
    ),
    Scenario(
        "concat_null",
        _sc_concat_null,
        pa.table({"j": pa.array(["ab", None], pa.string())}),
    ),
    Scenario(
        "end_to_end_chain",
        _sc_end_to_end_chain,
        pa.table(
            {
                "id": pa.array([2, 3], pa.int64()),
                "label": pa.array(["B", "C"], pa.string()),
                "amt_x2": pa.array([40, 60], pa.int32()),
            }
        ),
        order_sensitive=True,
    ),
    # ----- date functions -----
    Scenario(
        "date_extractor",
        _sc_date_extractor,
        pa.table(
            {
                "year": pa.array([2016, 2025], pa.int32()),
                "quarter": pa.array([1, 2], pa.int32()),
                "month": pa.array([2, 4], pa.int32()),
                "day": pa.array([29, 1], pa.int32()),
                "day_of_year": pa.array([60, 91], pa.int32()),
                "day_of_week": pa.array([2, 3], pa.int32()),
            }
        ),
    ),
    Scenario(
        "date_math",
        _sc_date_math,
        pa.table(
            {
                "prior_year": pa.array([dt.date(2015, 2, 28), dt.date(2024, 1, 15)], pa.date32()),
                "month_end": pa.array([dt.date(2016, 2, 29), dt.date(2025, 1, 31)], pa.date32()),
                "next_day": pa.array([dt.date(2016, 3, 1), dt.date(2025, 1, 16)], pa.date32()),
                "quarter_start": pa.array([dt.date(2016, 1, 1), dt.date(2025, 1, 1)], pa.date32()),
            }
        ),
    ),
    Scenario(
        "date_format",
        _sc_date_format,
        pa.table(
            {
                "date_key": pa.array(["20250108", "20250514"], pa.string()),
                "year_quarter": pa.array(["2025Q1", "2025Q2"], pa.string()),
                "month_name": pa.array(["January", "May"], pa.string()),
            }
        ),
    ),
    Scenario(
        "row_number_ordered",
        _sc_row_number_ordered,
        pa.table(
            [pa.array([1, 2, 3], pa.int32()), pa.array([10, 20, 30], pa.int64())],
            schema=pa.schema(
                [
                    pa.field("rn", pa.int32(), nullable=False),
                    pa.field("v", pa.int64(), nullable=True),
                ]
            ),
        ),
        order_sensitive=True,
    ),
    # ----- filter-predicate rewriter (audit G2) -----
    Scenario(
        "filter_unambiguous_on_case_colliding_frame",
        _sc_filter_unambiguous_on_case_colliding_frame,
        pa.table(
            [
                pa.array([1], pa.int64()),
                pa.array([2], pa.int64()),
                pa.array([3], pa.int64()),
            ],
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=True),
                    pa.field("ID", pa.int64(), nullable=True),
                    pa.field("other", pa.int64(), nullable=True),
                ]
            ),
        ),
    ),
    Scenario(
        "filter_keyword_literal_false_column",
        _sc_filter_keyword_literal_false_column,
        pa.table(
            [pa.array([], pa.int64()), pa.array([], pa.int64())],
            schema=pa.schema(
                [
                    pa.field("false", pa.int64(), nullable=True),
                    pa.field("b", pa.int64(), nullable=True),
                ]
            ),
        ),
    ),
    # ----- non-UTC oracle session (H-1a) -----
    Scenario(
        "date_extractor_under_new_york_session",
        _sc_date_extractor_under_new_york_session,
        pa.table(
            [
                pa.array([2024], pa.int32()),
                pa.array([2], pa.int32()),
                pa.array([29], pa.int32()),
            ],
            schema=pa.schema(
                [
                    pa.field("year_part", pa.int32(), nullable=True),
                    pa.field("month_part", pa.int32(), nullable=True),
                    pa.field("day_part", pa.int32(), nullable=True),
                ]
            ),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "date_math_under_tokyo_session",
        _sc_date_math_under_tokyo_session,
        pa.table(
            [
                pa.array([dt.date(2024, 2, 29)], pa.date32()),
                pa.array([dt.date(2024, 1, 1)], pa.date32()),
                pa.array([29], pa.int32()),
            ],
            schema=pa.schema(
                [
                    pa.field("month_end", pa.date32(), nullable=True),
                    pa.field("year_start", pa.date32(), nullable=True),
                    pa.field("february_days", pa.int32(), nullable=True),
                ]
            ),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_TOKYO),),
    ),
]


# ==================================================================================================
# Disclosure registry — recorded DIVERGENCES (repark != Spark). Live mode re-asserts that the
# recorded Spark behavior STILL differs from repark, so a silent convergence goes RED and forces
# the disclosure to be revisited (docs/testing.md divergence-class discipline).
# ==================================================================================================


def _expect_raises(fn: Callable[[], Any]) -> None:
    """Assert ``fn()`` raises — the recorded 'Spark errors here' half of a disclosure."""
    try:
        fn()
    except Exception:
        # Any engine-side raise satisfies the recorded "Spark errors here" half of the disclosure.
        return
    raise AssertionError("expected a raise but none occurred (the disclosure may have converged)")


@dataclass(frozen=True)
class Disclosure:
    """A recorded divergence pinned on BOTH engines: `repark_check` asserts repark's actual
    (divergent) behavior; `spark_check` asserts the recorded live-Spark behavior it differs from.
    If either engine converges toward the other, its check flips RED."""

    name: str
    repark_check: Callable[[Engine], None]
    spark_check: Callable[[Engine], None]
    note: str


def _disc_int_union_string_repark(engine: Engine) -> None:
    # repark coerces int/string union to STRING and returns rows (no raise).
    ints = engine.session.createDataFrame([(1,)], ["v"])
    strs = engine.session.createDataFrame([("x",)], ["v"])
    out = engine.arrow_of(ints.union(strs))
    assert pa.types.is_string(out.schema.field("v").type), "repark coerces int/string -> string"
    assert sorted(out.column("v").to_pylist()) == ["1", "x"]


def _disc_int_union_string_spark(engine: Engine) -> None:
    # ANSI Spark 4 coerces to BIGINT and RAISES CAST_INVALID_INPUT when the rows are materialized.
    ints = engine.session.createDataFrame([(1,)], ["v"])
    strs = engine.session.createDataFrame([("x",)], ["v"])
    unioned = ints.union(strs)
    assert unioned.schema["v"].dataType.simpleString() == "bigint", "Spark union type is bigint"
    _expect_raises(lambda: engine.arrow_of(unioned))


def _disc_fillna_nullability_repark(engine: Engine) -> None:
    # repark's scalar-numeric fillna makes the filled integer column non-nullable (coalesce).
    filled = engine.arrow_of(_na_frame(engine).fillna(0))
    assert filled.schema.field("i").nullable is False, "repark fillna(0): i is non-nullable"


def _disc_fillna_nullability_spark(engine: Engine) -> None:
    # Spark 4.1.2 is INCONSISTENT: a scalar-numeric fillna leaves the integer column i nullable
    # while making the filled double column d non-nullable. The divergence from repark is on `i`.
    filled = engine.arrow_of(_na_frame(engine).fillna(0))
    assert filled.schema.field("i").nullable is True, "Spark fillna(0): i stays nullable"
    assert filled.schema.field("d").nullable is False, "Spark fillna(0): d is non-nullable"


def _collision_frame(engine: Engine) -> Any:
    """The audit-G2 fixture: `id` and `ID` collide only by case, `other` does not."""
    return engine.session.createDataFrame([(1, 2, 3)], ["id", "ID", "other"])


def _disc_filter_case_collision_bypasses_repark(engine: Engine) -> None:
    # Two accepted spellings never reach the SQL-string rewriter, so neither refuses: the Column
    # form resolves exact-case-first, and an explicitly double-quoted ident is a protected span
    # DataFusion then resolves case-SENSITIVELY. `id` is 1 and `ID` is 2, so `> 1` discriminates.
    src = _collision_frame(engine)
    assert engine.arrow_of(src.filter(src["ID"] > 1)).num_rows == 1, "Column form binds `ID`"
    assert engine.arrow_of(src.filter(src["id"] > 1)).num_rows == 0, "Column form binds `id`"
    assert engine.arrow_of(src.filter('"ID" > 1')).num_rows == 1, 'quoted "ID" binds `ID`'
    assert engine.arrow_of(src.filter('"id" > 1')).num_rows == 0, 'quoted "id" binds `id`'


def _disc_filter_case_collision_bypasses_spark(engine: Engine) -> None:
    # Spark 4.1.2 refuses the Column form (AMBIGUOUS_REFERENCE) and reads `"ID"` as a string
    # LITERAL, not an identifier — under ANSI it raises CAST_INVALID_INPUT comparing it to 1.
    src = _collision_frame(engine)
    _expect_raises(lambda: engine.arrow_of(src.filter(src["ID"] > 1)))
    _expect_raises(lambda: engine.arrow_of(src.filter(src["id"] > 1)))
    _expect_raises(lambda: engine.arrow_of(src.filter('"ID" > 1')))


def _disc_filter_backtick_identifier_repark(engine: Engine) -> None:
    # PRE-EXISTING hole (main had no backtick handling either): backticks are not a protected span,
    # so the token inside them is rewritten and DataFusion re-quotes it -> No field named """x""".
    src = engine.session.createDataFrame([(1, 2)], ["x", "b"])
    _expect_raises(lambda: engine.arrow_of(src.filter("`x` > 0")))


def _disc_filter_backtick_identifier_spark(engine: Engine) -> None:
    # Spark's own quoting spelling: it just filters.
    src = engine.session.createDataFrame([(1, 2)], ["x", "b"])
    assert engine.arrow_of(src.filter("`x` > 0")).num_rows == 1, "Spark honours backtick idents"


DISCLOSURES: list[Disclosure] = [
    Disclosure(
        "int_union_string",
        _disc_int_union_string_repark,
        _disc_int_union_string_spark,
        "repark coerces int/string union to string (lossless, no error); ANSI Spark 4 coerces to "
        "bigint and raises CAST_INVALID_INPUT at collect.",
    ),
    Disclosure(
        "fillna_scalar_numeric_nullability",
        _disc_fillna_nullability_repark,
        _disc_fillna_nullability_spark,
        "repark's scalar-numeric fillna makes the filled integer column non-nullable; Spark 4.1.2 "
        "inconsistently leaves the integer column nullable while making a filled double non-null.",
    ),
    Disclosure(
        "filter_case_collision_bypasses",
        _disc_filter_case_collision_bypasses_repark,
        _disc_filter_case_collision_bypasses_spark,
        "repark's case-collision refusal covers the bare SQL-string form only: the Column form "
        "(df[ID]) resolves exact-case-first and an explicitly double-quoted ident resolves "
        "case-sensitively in DataFusion, both returning rows; Spark 4.1.2 raises "
        "AMBIGUOUS_REFERENCE for the Column form and reads a double-quoted span as a string "
        "literal (CAST_INVALID_INPUT under ANSI). Audit G2 — disclosed, not fixed.",
    ),
    Disclosure(
        "filter_backtick_identifier",
        _disc_filter_backtick_identifier_repark,
        _disc_filter_backtick_identifier_spark,
        "backtick-quoted identifiers are not a protected span in repark's filter-predicate "
        "rewriter, so a backticked ident is rewritten and then re-quoted by DataFusion into a "
        "triple-double-quoted field name that resolves to nothing; Spark filters normally. "
        "PRE-EXISTING (not an audit-G2 regression); the fix and its pin belong in a follow-up "
        "unit.",
    ),
]
