"""Shared scenario registry for the **live PySpark oracle tier** (L1).

The parity discipline is *record-then-pin*: goldens are derived from live PySpark 4.1.2 at
authoring time and pinned inline in the facade tests; routine CI is JVM-free and never re-checks
them. Two failure classes are then invisible until a human re-runs the oracle by hand:

* **golden drift** — a stale or hand-edited pin no longer matches what Spark actually produces;
* **oracle drift** — a Spark bump silently changes semantics under a still-green pin.

This module is the drift detector's engine. It holds two recipe kinds:

1. **Single-shot** (``Scenario`` / ``SCENARIOS``) — an engine-agnostic
   ``recipe: Engine → DataFrame`` plus its pinned ``golden``. Group E / columns / dates /
   filter-rewriter / non-UTC date controls / the G1 extraction-class timezone live rows. Because
   repark is a **near-drop-in for PySpark** — the same ``createDataFrame`` / DataFrame-API /
   ``functions`` surface, only the import line differs — one recipe runs unchanged on BOTH engines.
2. **Lifecycle** (``LifecycleScenario`` / ``LIFECYCLE_SCENARIOS``) — multi-statement table lifecycle
   ``create → seed → [register source view] → act → read``, with always-cleanup. Used by the live
   MERGE drift detector (Iceberg table + optional ``merge_src`` view). repark path uses a memory
   catalog + COW TBLPROPERTIES; Spark path uses ``build_spark_iceberg_engine`` (Hadoop catalog +
   the pinned Iceberg GAV from :mod:`_oracle_pins`).

The live tier (``test_parity_live.py``) asserts the full triple **repark == pinned golden == live
Spark** (value AND Arrow-path type/nullability) for every scenario of both kinds; routine CI runs
only the JVM-free ``repark == golden`` half of the same recipes, so the recipes themselves carry
no-JVM coverage.

Nothing here imports pyspark at module load — the pyspark import is deferred into
``build_spark_engine`` / ``build_spark_iceberg_engine``, so this module (and the tests that import
it) collect cleanly on a runner with neither pyspark nor a JVM installed (the routine-CI
contract, L3).

Session config (VERIFIED against live PySpark 4.1.2, not guessed): the Group E / columns / date
goldens were recorded under Spark 4.1.2 defaults — **ANSI mode ON** (Spark 4 default; the
int-UNION-string disclosure literally depends on it) — so ``build_spark_engine`` pins
``spark.sql.ansi.enabled=true`` explicitly. The registry's **default** session zone is ``UTC`` for
determinism across runners. ``master("local[2]")`` per the plan.

**Per-scenario session-conf override (H-1a).** A registry pinned to one session zone is
structurally incapable of catching a session-timezone divergence — the whole class is invisible to
it. ``Scenario.session_conf`` (and ``LifecycleScenario.session_conf``) therefore carry conf pairs
applied to BOTH engines for that scenario only: the oracle takes them through
``spark_session_conf`` (set, run, restore), and repark takes them by BUILDING a session with them,
because repark resolves the session zone once at session construction. Scenarios that declare no
override behave exactly as before.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass, field
from pathlib import Path
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


def _arm_iceberg_packages_on_first_spark() -> None:
    """Put the Iceberg runtime on the *first* SparkContext in this process.

    ``spark.jars.packages`` cannot be added after a SparkContext exists. The full
    facade suite starts Spark from other modules before the live lifecycle tests;
    ``PYSPARK_SUBMIT_ARGS`` is the hook that reaches that first context. Armed
    only when the live tier is on (so ``make preflight`` / JVM-free runs stay
    Iceberg-free). L-1: without this, full-suite lifecycle tests raise
    ``ClassNotFoundException: SparkCatalog``.
    """
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV

    token = ICEBERG_SPARK_RUNTIME_GAV
    existing = os.environ.get("PYSPARK_SUBMIT_ARGS", "").strip()
    if token in existing:
        return
    prefix = (
        f"--packages {token} "
        "--conf spark.sql.extensions="
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
    if existing:
        if existing.endswith("pyspark-shell"):
            os.environ["PYSPARK_SUBMIT_ARGS"] = f"{prefix} {existing}"
        else:
            os.environ["PYSPARK_SUBMIT_ARGS"] = f"{prefix} {existing} pyspark-shell"
    else:
        os.environ["PYSPARK_SUBMIT_ARGS"] = f"{prefix} pyspark-shell"


if LIVE:
    _arm_iceberg_packages_on_first_spark()


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


def build_spark_iceberg_engine(warehouse: Path, session_conf: SessionConf = ()) -> Engine:
    """Live PySpark + Iceberg engine for multi-statement table lifecycle scenarios.

    Sibling of :func:`build_spark_engine` (option A): the default live session has no Iceberg
    *catalog*; under ``REPARK_PARITY_LIVE=1`` the module arms ``PYSPARK_SUBMIT_ARGS`` with the
    GAV so the first SparkContext in the process can resolve ``SparkCatalog``. Only lifecycle
    tests request this provisioned engine. Pins the same
    GAV the MERGE differential record driver uses (from :mod:`_oracle_pins`), a local Hadoop
    catalog named ``local`` rooted at ``warehouse``, Iceberg session extensions, ANSI on, UTC by
    default, ``local[2]``. Optional ``session_conf`` is applied at BUILD time (the session is not
    shared with the plain spark engine, so build-time application is safe).
    """
    from _oracle_pins import ICEBERG_SPARK_RUNTIME_GAV
    from pyspark.sql import SparkSession, Window
    from pyspark.sql import functions as sfunctions
    from pyspark.sql import types as stypes

    catalog = LIFECYCLE_SPARK_CATALOG
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("repark-parity-live-iceberg")
        .config("spark.sql.ansi.enabled", "true")
        .config(SESSION_TIME_ZONE_KEY, DEFAULT_SESSION_TIME_ZONE)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.jars.packages", ICEBERG_SPARK_RUNTIME_GAV)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "hadoop")
        .config(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    )
    for key, value in session_conf:
        builder = builder.config(key, value)
    session = builder.getOrCreate()
    # Catalog keys are session-level; if getOrCreate reused an earlier context, set them live.
    session.conf.set(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
    session.conf.set(f"spark.sql.catalog.{catalog}.type", "hadoop")
    session.conf.set(f"spark.sql.catalog.{catalog}.warehouse", str(warehouse))
    session.sparkContext.setLogLevel("ERROR")
    return Engine(
        name="spark-iceberg",
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
# Lifecycle scenarios — multi-statement table lifecycle (create → seed → act → read)
# ==================================================================================================

# Catalog names used by lifecycle scenarios. repark registers a memory catalog under
# LIFECYCLE_REPARK_CATALOG; Spark Hadoop catalog is LIFECYCLE_SPARK_CATALOG (configured in
# build_spark_iceberg_engine). Each LifecycleScenario carries the catalog name that matches the
# engine under test — tests build engines and pick the matching scenario list, OR the scenario
# catalog is rewritten per engine via the per-engine lists below.
LIFECYCLE_REPARK_CATALOG = "mem"
LIFECYCLE_SPARK_CATALOG = "local"
LIFECYCLE_NAMESPACE = "ns"

# Shared COW table properties so repark's merge mode is explicit (matches test_merge_into.py /
# the MERGE differential corpus). Spark Iceberg 1.11 defaults accept MERGE without these; repark
# pins COW for determinism. Injected via {cow_props} in create_sql when with_cow_props=True.
COW_TBLPROPERTIES = (
    "'format-version' = '2', "
    "'write.delete.mode' = 'copy-on-write', "
    "'write.update.mode' = 'copy-on-write', "
    "'write.merge.mode' = 'copy-on-write'"
)

# Temp view name for optional MERGE source registration (matches the differential corpus).
LIFECYCLE_SOURCE_VIEW = "merge_src"


@dataclass(frozen=True)
class LifecycleScenario:
    """Multi-statement live scenario: setup → act → read, with always-cleanup.

    Engine-agnostic SQL steps over a resolved FQN (``{target}``). Optional ``source_sql``
    registers a temp view named ``merge_src`` before ``act_sql`` (MERGE needs a source relation).
    ``error_needle`` is intentionally absent on first landing — error-class twins ship later
    with ``run_lifecycle_expect_error`` when a consumer exists.
    """

    name: str
    catalog: str
    namespace: str
    table: str
    create_sql: str  # may use {target} and {cow_props}
    seed_sql: str  # may use {target}
    act_sql: str  # may use {target}
    read_sql: str  # may use {target}
    golden: pa.Table
    order_sensitive: bool = True
    session_conf: SessionConf = field(default=())
    source_sql: str | None = None  # optional SELECT registered as merge_src before act


def _lifecycle_target(scenario: LifecycleScenario) -> str:
    """Three-part Iceberg table name both engines accept."""
    return f"{scenario.catalog}.{scenario.namespace}.{scenario.table}"


def _drop_lifecycle_table(session: Any, fq_table: str) -> None:
    """Drop an Iceberg table if present. Best-effort: a missing table is fine."""
    with contextlib.suppress(Exception):
        session.sql(f"DROP TABLE IF EXISTS {fq_table}")
    with contextlib.suppress(Exception):
        session.sql(f"DROP TABLE {fq_table}")


def _drop_lifecycle_source_view(session: Any) -> None:
    """Drop the shared lifecycle source temp view if the session still holds it."""
    drop_temp = getattr(session, "catalog", None)
    if drop_temp is not None and hasattr(drop_temp, "dropTempView"):
        with contextlib.suppress(Exception):
            session.catalog.dropTempView(LIFECYCLE_SOURCE_VIEW)
    with contextlib.suppress(Exception):
        session.sql(f"DROP VIEW IF EXISTS {LIFECYCLE_SOURCE_VIEW}")


def run_lifecycle_scenario(
    scenario: LifecycleScenario, engine: Engine, *, with_cow_props: bool
) -> pa.Table:
    """create → seed → [register source] → act → read; drop target (+ source view) in finally.

    ``with_cow_props`` is a *caller* choice: repark always wants COW TBLPROPERTIES on CREATE;
    Spark Iceberg 1.11 does not need them. Encoded here rather than as a per-row dead knob.
    """
    session = engine.session
    fq_table = _lifecycle_target(scenario)
    cow_props = f" TBLPROPERTIES ({COW_TBLPROPERTIES})" if with_cow_props else ""
    session.sql(f"CREATE NAMESPACE IF NOT EXISTS {scenario.catalog}.{scenario.namespace}")
    _drop_lifecycle_table(session, fq_table)
    session.sql(scenario.create_sql.format(target=fq_table, cow_props=cow_props))
    session.sql(scenario.seed_sql.format(target=fq_table))
    if scenario.source_sql is not None:
        _drop_lifecycle_source_view(session)
        frame = session.sql(scenario.source_sql)
        frame.createOrReplaceTempView(LIFECYCLE_SOURCE_VIEW)
    try:
        session.sql(scenario.act_sql.format(target=fq_table))
        return engine.arrow_of(session.sql(scenario.read_sql.format(target=fq_table)))
    finally:
        _drop_lifecycle_table(session, fq_table)
        if scenario.source_sql is not None:
            _drop_lifecycle_source_view(session)


def _lifecycle_merge_table(
    fields: list[tuple[str, pa.DataType, bool]], values: dict[str, list[object]]
) -> pa.Table:
    """Build a short Arrow golden for a MERGE lifecycle row (name/type/nullability + values)."""
    schema = pa.schema([pa.field(name, kind, nullable=null) for name, kind, null in fields])
    return pa.table({name: pa.array(values[name], kind) for name, kind, _ in fields}, schema)


_I64 = pa.int64()
_STR = pa.string()


def _merge_lifecycle_rows(*, catalog: str) -> list[LifecycleScenario]:
    """The 2 live-tier MERGE scenarios, bound to ``catalog`` for the engine under test.

    Chosen pair (see ledger § chosen rows):
    * ``live_merge_basic_upsert`` — control equality (publish-job upsert shape).
    * ``live_merge_matched_arm_order`` — first-match-wins UPDATE-then-DELETE (not the builder
      upsert twin; detects arm-order drift that ``test_merge_into.py`` does not cover).

    Goldens are the recorded Spark halves from the MERGE differential corpus (short tables;
    duplicated here so ``_live_parity`` never imports the ``test_`` module).
    """
    return [
        LifecycleScenario(
            name="live_merge_basic_upsert",
            catalog=catalog,
            namespace=LIFECYCLE_NAMESPACE,
            table="live_merge_basic_upsert",
            create_sql="CREATE TABLE {target} (id BIGINT, name STRING) USING iceberg{cow_props}",
            seed_sql=(
                "INSERT INTO {target} "
                "SELECT CAST(1 AS BIGINT) AS id, 'a' AS name "
                "UNION ALL SELECT CAST(2 AS BIGINT), 'b'"
            ),
            source_sql=(
                "SELECT CAST(2 AS BIGINT) AS id, 'bee' AS name "
                "UNION ALL SELECT CAST(3 AS BIGINT), 'c'"
            ),
            act_sql=(
                "MERGE INTO {target} AS target USING merge_src AS source "
                "ON target.id = source.id "
                "WHEN MATCHED THEN UPDATE SET * "
                "WHEN NOT MATCHED THEN INSERT *"
            ),
            read_sql="SELECT id, name FROM {target} ORDER BY id",
            golden=_lifecycle_merge_table(
                [("id", _I64, True), ("name", _STR, True)],
                {"id": [1, 2, 3], "name": ["a", "bee", "c"]},
            ),
            order_sensitive=True,
        ),
        LifecycleScenario(
            name="live_merge_matched_arm_order",
            catalog=catalog,
            namespace=LIFECYCLE_NAMESPACE,
            table="live_merge_matched_arm_order",
            create_sql=("CREATE TABLE {target} (id BIGINT, score BIGINT) USING iceberg{cow_props}"),
            seed_sql=(
                "INSERT INTO {target} "
                "SELECT CAST(1 AS BIGINT) AS id, CAST(10 AS BIGINT) AS score "
                "UNION ALL SELECT CAST(2 AS BIGINT), CAST(20 AS BIGINT)"
            ),
            source_sql=(
                "SELECT CAST(1 AS BIGINT) AS id, CAST(100 AS BIGINT) AS score "
                "UNION ALL SELECT CAST(2 AS BIGINT), CAST(200 AS BIGINT)"
            ),
            act_sql=(
                "MERGE INTO {target} AS target USING merge_src AS source "
                "ON target.id = source.id "
                "WHEN MATCHED AND target.score = 10 THEN UPDATE SET target.score = source.score "
                "WHEN MATCHED THEN DELETE"
            ),
            read_sql="SELECT id, score FROM {target} ORDER BY id",
            golden=_lifecycle_merge_table(
                [("id", _I64, True), ("score", _I64, True)],
                {"id": [1], "score": [100]},
            ),
            order_sensitive=True,
        ),
    ]


# repark-facing list (memory catalog name). Spark-facing list is built with LIFECYCLE_SPARK_CATALOG
# so FQNs resolve against the Hadoop catalog configured in build_spark_iceberg_engine.
LIFECYCLE_SCENARIOS: list[LifecycleScenario] = _merge_lifecycle_rows(
    catalog=LIFECYCLE_REPARK_CATALOG
)
LIFECYCLE_SCENARIOS_SPARK: list[LifecycleScenario] = _merge_lifecycle_rows(
    catalog=LIFECYCLE_SPARK_CATALOG
)


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
# The TIMESTAMP rows of the same class live in test_session_timezone_parity.py; since H-1a split
# B (2026-08-10) most are EQUALITY rows — the extraction fix landed and they converged. The ones
# still recorded as disclosures are a different class each (TZ-4 export type, TZ-5 cast unit, TZ-6
# no NTZ, TZ-7 zoneless input), and each row names its own.


def _sc_date_extractor_under_new_york_session(engine: Engine) -> Any:
    """DATE extraction under an `America/New_York` oracle session — zone-independent by contract."""
    return engine.session.sql(
        "SELECT year(to_date('2024-02-29')) AS year_part, "
        "month(to_date('2024-02-29')) AS month_part, "
        "dayofmonth(to_date('2024-02-29')) AS day_part"
    )


# ==================================================================================================
# G1 / G16 extraction-class timezone live rows (N-2b item 3)
# ==================================================================================================
#
# The 13 equality rows that converged with the H-1a-b extraction fix (see
# test_session_timezone_parity.test_the_extraction_class_converged_and_the_residue_is_named).
# NOT the 2 composition date_trunc value-converged-but-type-disclosure rows, NOT the 2
# zone-independent DATE controls, NOT any disclosure (TZ-4 type, TZ-5 cast, TZ-6 NTZ, TZ-7
# zoneless). Goldens are the recorded Spark halves (equality rows: repark is None).


def _utc(*args: int) -> dt.datetime:
    """A tz-aware UTC instant (what PySpark's Arrow export produces for a TIMESTAMP)."""
    return dt.datetime(*args, tzinfo=dt.UTC)  # type: ignore[arg-type]


# Column-path fixture: same two instants as test_session_timezone_parity.COLUMN_INSTANTS.
_TZ_COLUMN_VIEW = "tz_aware_instants"
_TZ_COLUMN_INSTANTS: tuple[dt.datetime, ...] = (
    _utc(2024, 6, 15, 12, 0),
    _utc(2024, 1, 1, 4, 30),
)
_TZ_COLUMN_SQL = (
    "SELECT year(ts) AS year_part, month(ts) AS month_part, dayofmonth(ts) AS day_part, "
    f"hour(ts) AS hour_part FROM {_TZ_COLUMN_VIEW} ORDER BY ts"
)


def register_tz_column_view(engine: Engine) -> None:
    """Register the tz-aware TIMESTAMP column view used by column-path timezone scenarios.

    ``createDataFrame`` + ``createOrReplaceTempView`` are spelled identically on both engines.
    Schema is INFERRED deliberately so both engines carry an instant-typed TIMESTAMP.
    """
    frame = engine.session.createDataFrame([(instant,) for instant in _TZ_COLUMN_INSTANTS], ["ts"])
    frame.createOrReplaceTempView(_TZ_COLUMN_VIEW)


def _sc_sql(sql: str) -> Callable[[Engine], Any]:
    """Build a single-shot recipe that runs ``engine.session.sql(sql)``."""

    def recipe(engine: Engine) -> Any:
        return engine.session.sql(sql)

    return recipe


def _sc_column_sql(sql: str) -> Callable[[Engine], Any]:
    """Build a recipe that registers the tz column view, then runs ``sql``."""

    def recipe(engine: Engine) -> Any:
        register_tz_column_view(engine)
        return engine.session.sql(sql)

    return recipe


_INT32 = pa.int32()


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
    # ----- G1 / G16 extraction-class timezone live rows (N-2b item 3) -----
    # 13 equality rows that converged with the extraction fix. Size pin 29 -> 42
    # moved DELIBERATELY in the same diff as these 13 scenarios.
    Scenario(
        "tz_live_year_of_instant_under_new_york_session",
        _sc_sql("SELECT year(to_timestamp('2024-01-01T04:30:00Z')) AS year_part"),
        pa.table(
            [pa.array([2023], _INT32)],
            schema=pa.schema([pa.field("year_part", _INT32, nullable=True)]),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_month_of_instant_under_new_york_session",
        _sc_sql("SELECT month(to_timestamp('2024-03-01T02:15:00Z')) AS month_part"),
        pa.table(
            [pa.array([2], _INT32)],
            schema=pa.schema([pa.field("month_part", _INT32, nullable=True)]),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_day_of_instant_under_new_york_session",
        _sc_sql("SELECT dayofmonth(to_timestamp('2024-06-15T03:00:00Z')) AS day_part"),
        pa.table(
            [pa.array([14], _INT32)],
            schema=pa.schema([pa.field("day_part", _INT32, nullable=True)]),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_hour_of_instant_under_new_york_session",
        _sc_sql("SELECT hour(to_timestamp('2024-06-15T12:00:00Z')) AS hour_part"),
        pa.table(
            [pa.array([8], _INT32)],
            schema=pa.schema([pa.field("hour_part", _INT32, nullable=True)]),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_hour_of_instant_under_tokyo_session",
        _sc_sql("SELECT hour(to_timestamp('2024-06-15T12:00:00Z')) AS hour_part"),
        pa.table(
            [pa.array([21], _INT32)],
            schema=pa.schema([pa.field("hour_part", _INT32, nullable=True)]),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_TOKYO),),
    ),
    Scenario(
        "tz_live_year_month_day_of_instant_under_tokyo_session",
        _sc_sql(
            "SELECT year(to_timestamp('2023-12-31T16:30:00Z')) AS year_part, "
            "month(to_timestamp('2023-12-31T16:30:00Z')) AS month_part, "
            "dayofmonth(to_timestamp('2023-12-31T16:30:00Z')) AS day_part"
        ),
        pa.table(
            [
                pa.array([2024], _INT32),
                pa.array([1], _INT32),
                pa.array([1], _INT32),
            ],
            schema=pa.schema(
                [
                    pa.field("year_part", _INT32, nullable=True),
                    pa.field("month_part", _INT32, nullable=True),
                    pa.field("day_part", _INT32, nullable=True),
                ]
            ),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_TOKYO),),
    ),
    Scenario(
        "tz_live_dst_spring_forward_instant_hour",
        _sc_sql("SELECT hour(to_timestamp('2024-03-10T07:00:00Z')) AS hour_part"),
        pa.table(
            [pa.array([3], _INT32)],
            schema=pa.schema([pa.field("hour_part", _INT32, nullable=True)]),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_dst_fall_back_repeated_local_hour",
        _sc_sql(
            "SELECT hour(to_timestamp('2024-11-03T05:30:00Z')) AS before_part, "
            "hour(to_timestamp('2024-11-03T06:30:00Z')) AS after_part"
        ),
        pa.table(
            [pa.array([1], _INT32), pa.array([1], _INT32)],
            schema=pa.schema(
                [
                    pa.field("before_part", _INT32, nullable=True),
                    pa.field("after_part", _INT32, nullable=True),
                ]
            ),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_column_extract_under_new_york_session",
        _sc_column_sql(_TZ_COLUMN_SQL),
        pa.table(
            {
                "year_part": pa.array([2023, 2024], _INT32),
                "month_part": pa.array([12, 6], _INT32),
                "day_part": pa.array([31, 15], _INT32),
                "hour_part": pa.array([23, 8], _INT32),
            },
            schema=pa.schema(
                [
                    pa.field("year_part", _INT32, nullable=True),
                    pa.field("month_part", _INT32, nullable=True),
                    pa.field("day_part", _INT32, nullable=True),
                    pa.field("hour_part", _INT32, nullable=True),
                ]
            ),
        ),
        order_sensitive=True,
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_column_extract_under_tokyo_session",
        _sc_column_sql(_TZ_COLUMN_SQL),
        pa.table(
            {
                "year_part": pa.array([2024, 2024], _INT32),
                "month_part": pa.array([1, 6], _INT32),
                "day_part": pa.array([1, 15], _INT32),
                "hour_part": pa.array([13, 21], _INT32),
            },
            schema=pa.schema(
                [
                    pa.field("year_part", _INT32, nullable=True),
                    pa.field("month_part", _INT32, nullable=True),
                    pa.field("day_part", _INT32, nullable=True),
                    pa.field("hour_part", _INT32, nullable=True),
                ]
            ),
        ),
        order_sensitive=True,
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_TOKYO),),
    ),
    Scenario(
        "tz_live_pre_1970_extract_under_new_york_session",
        _sc_sql(
            "SELECT year(to_timestamp('1969-12-31T23:30:00Z')) AS year_part, "
            "month(to_timestamp('1969-12-31T23:30:00Z')) AS month_part, "
            "dayofmonth(to_timestamp('1969-12-31T23:30:00Z')) AS day_part, "
            "hour(to_timestamp('1969-12-31T23:30:00Z')) AS hour_part"
        ),
        pa.table(
            {
                "year_part": pa.array([1969], _INT32),
                "month_part": pa.array([12], _INT32),
                "day_part": pa.array([31], _INT32),
                "hour_part": pa.array([18], _INT32),
            },
            schema=pa.schema(
                [
                    pa.field("year_part", _INT32, nullable=True),
                    pa.field("month_part", _INT32, nullable=True),
                    pa.field("day_part", _INT32, nullable=True),
                    pa.field("hour_part", _INT32, nullable=True),
                ]
            ),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_year_boundary_extract_and_format_under_new_york_session",
        _sc_sql(
            "SELECT year(to_timestamp('2024-01-01T02:00:00Z')) AS year_part, "
            "date_format(to_timestamp('2024-01-01T02:00:00Z'), 'yyyy-MM-dd') AS local_date"
        ),
        pa.table(
            {
                "year_part": pa.array([2023], _INT32),
                "local_date": pa.array(["2023-12-31"], pa.string()),
            },
            schema=pa.schema(
                [
                    pa.field("year_part", _INT32, nullable=True),
                    pa.field("local_date", pa.string(), nullable=True),
                ]
            ),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
    Scenario(
        "tz_live_leap_day_extract_under_new_york_session",
        _sc_sql(
            "SELECT month(to_timestamp('2024-02-29T02:00:00Z')) AS month_part, "
            "dayofmonth(to_timestamp('2024-02-29T02:00:00Z')) AS day_part"
        ),
        pa.table(
            {
                "month_part": pa.array([2], _INT32),
                "day_part": pa.array([28], _INT32),
            },
            schema=pa.schema(
                [
                    pa.field("month_part", _INT32, nullable=True),
                    pa.field("day_part", _INT32, nullable=True),
                ]
            ),
        ),
        session_conf=((SESSION_TIME_ZONE_KEY, ZONE_NEW_YORK),),
    ),
]


# ==================================================================================================
# Disclosure registry — recorded DIVERGENCES (repark != Spark). Live mode re-asserts that the
# recorded Spark behavior STILL differs from repark, so a silent convergence goes RED and forces
# the disclosure to be revisited (docs/testing.md divergence-class discipline).
# ==================================================================================================


def _expect_raises(fn: Callable[[], Any], needle: str | None = None) -> None:
    """Assert ``fn()`` raises — the recorded 'Spark errors here' half of a disclosure.

    Optional ``needle`` (L-1) is a substring that must appear in the exception text. Existing
    callers omit it and still accept any raise.
    """
    try:
        fn()
    except Exception as exc:
        if needle is not None and needle not in str(exc):
            raise AssertionError(f"raised, but message did not contain {needle!r}: {exc}") from exc
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


# -------------------------------------------------------------------------------------------------
# L-1 landing-truth disclosures — recipes re-verified against merged main 2026-08-12 (baf6617).
# Names must match the registry `- `live-mirror: <name>`` bullets exactly.
# -------------------------------------------------------------------------------------------------


# `cast_date_to_int_spark_refuses` was a disclosure here until 2026-08-15. The G6-3 gate made
# both engines refuse `CAST(DATE '2020-01-01' AS INT)` with the same Spark class, so it is no
# longer a divergence and no longer has a registry §6 row to mirror. The convergence is pinned as
# a shared-raise equality on BOTH engines by
# `test_cast_failure_parity.py::test_cast_failure_row[date_to_int_spark_refuses_repark_days]`,
# which is a stronger detector than a disclosure was.


def _disc_cast_timestamp_to_int_repark(engine: Engine) -> None:
    """TZ-5 §10 form: value matches Spark; residual is nullability (literal non-null)."""
    out = engine.arrow_of(
        engine.session.sql("SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) AS n")
    )
    field = out.schema.field("n")
    assert field.type == pa.int32()
    assert field.nullable is False, "repark propagates the timestamp literal's non-null"
    assert out.column("n").to_pylist() == [1577836800]


def _disc_cast_timestamp_to_int_spark(engine: Engine) -> None:
    # Live spark engine is UTC (build_spark_engine pins session.timeZone=UTC).
    out = engine.arrow_of(
        engine.session.sql("SELECT CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) AS n")
    )
    field = out.schema.field("n")
    assert field.type == pa.int32()
    assert field.nullable is True, "Spark types CAST(ts AS INT) nullable"
    assert out.column("n").to_pylist() == [1577836800]


def _disc_null_safe_eq_sql_repark(engine: Engine) -> None:
    out = engine.arrow_of(
        engine.session.sql(
            "SELECT "
            "(CAST(NULL AS INT) = CAST(NULL AS INT)) AS eq, "
            "(CAST(NULL AS INT) <=> CAST(NULL AS INT)) AS nse"
        )
    )
    assert out.schema.field("nse").type == pa.bool_()
    assert out.schema.field("nse").nullable is True, "repark <=> result is nullable bool"
    assert out.column("nse").to_pylist() == [True]
    assert out.column("eq").to_pylist() == [None]


def _disc_null_safe_eq_sql_spark(engine: Engine) -> None:
    out = engine.arrow_of(
        engine.session.sql(
            "SELECT "
            "(CAST(NULL AS INT) = CAST(NULL AS INT)) AS eq, "
            "(CAST(NULL AS INT) <=> CAST(NULL AS INT)) AS nse"
        )
    )
    assert out.schema.field("nse").type == pa.bool_()
    assert out.schema.field("nse").nullable is False, "Spark <=> result is non-nullable bool"
    assert out.column("nse").to_pylist() == [True]
    assert out.column("eq").to_pylist() == [None]


def _disc_null_safe_eq_df_repark(engine: Engine) -> None:
    frame = engine.session.createDataFrame(
        [(1, 1), (None, None), (1, None), (None, 1)],
        ["a", "b"],
    )
    out = engine.arrow_of(frame.select(frame.a.eqNullSafe(frame.b).alias("nse")))
    assert out.schema.field("nse").type == pa.bool_()
    assert out.schema.field("nse").nullable is True
    assert out.column("nse").to_pylist() == [True, True, False, False]


def _disc_null_safe_eq_df_spark(engine: Engine) -> None:
    frame = engine.session.createDataFrame(
        [(1, 1), (None, None), (1, None), (None, 1)],
        ["a", "b"],
    )
    out = engine.arrow_of(frame.select(frame.a.eqNullSafe(frame.b).alias("nse")))
    assert out.schema.field("nse").type == pa.bool_()
    assert out.schema.field("nse").nullable is False
    assert out.column("nse").to_pylist() == [True, True, False, False]


# Same VALUES recipe as test_float_agg_parity / the Rust G7 fixture (order matters).
_G7_FIXTURE_VALUES_SQL = (
    "SELECT * FROM (VALUES "
    "(CAST(1.0e16 AS DOUBLE)), (CAST(1.0 AS DOUBLE)), (CAST(-1.0e16 AS DOUBLE)), "
    "(CAST(2.0 AS DOUBLE)), (CAST(1.0e16 AS DOUBLE)), (CAST(0.5 AS DOUBLE)), "
    "(CAST(-1.0e16 AS DOUBLE)), (CAST(0.25 AS DOUBLE))"
    ") AS t(v)"
)
_G7_SUM_SQL = f"SELECT sum(v) AS s FROM ({_G7_FIXTURE_VALUES_SQL}) src"
_G7_AVG_SQL = f"SELECT avg(v) AS a FROM ({_G7_FIXTURE_VALUES_SQL}) src"


def _disc_sum_catastrophic_cancellation_repark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_SUM_SQL))
    assert out.schema.field("s").type == pa.float64()
    assert out.schema.field("s").nullable is True
    assert out.column("s").to_pylist() == [3.75]


def _disc_sum_catastrophic_cancellation_spark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_SUM_SQL))
    assert out.schema.field("s").type == pa.float64()
    assert out.schema.field("s").nullable is True
    assert out.column("s").to_pylist() == [2.25]


def _disc_avg_catastrophic_cancellation_repark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_AVG_SQL))
    assert out.schema.field("a").type == pa.float64()
    assert out.schema.field("a").nullable is True
    assert out.column("a").to_pylist() == [0.46875]


def _disc_avg_catastrophic_cancellation_spark(engine: Engine) -> None:
    out = engine.arrow_of(engine.session.sql(_G7_AVG_SQL))
    assert out.schema.field("a").type == pa.float64()
    assert out.schema.field("a").nullable is True
    assert out.column("a").to_pylist() == [0.28125]


def _list_value_field(table: pa.Table, column: str) -> pa.Field:
    """The list value field of ``column`` (name + element type + element nullability)."""
    field = table.schema.field(column)
    typ = field.type
    assert pa.types.is_list(typ), f"{column} is not a list, got {typ}"
    return typ.field(0)


def _nested_array_frame(engine: Engine) -> Any:
    types = engine.types
    schema = types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField("items", types.ArrayType(types.LongType())),
        ]
    )
    return engine.session.createDataFrame(
        [(1, [10, 20]), (2, [30]), (3, [10, 20]), (4, None)],
        schema,
    ).select("id", "items")


def _disc_nested_array_list_field_repark(engine: Engine) -> None:
    out = engine.arrow_of(_nested_array_frame(engine))
    value = _list_value_field(out, "items")
    assert value.name == "item", f"repark list value field is 'item', got {value.name!r}"


def _disc_nested_array_list_field_spark(engine: Engine) -> None:
    out = engine.arrow_of(_nested_array_frame(engine))
    value = _list_value_field(out, "items")
    assert value.name == "element", f"Spark list value field is 'element', got {value.name!r}"


def _nested_collect_list_frame(engine: Engine) -> Any:
    functions = engine.functions
    frame = engine.session.createDataFrame(
        [(1, 10), (1, 20), (2, 30), (2, 40), (1, 15)],
        ["grp", "v"],
    )
    return frame.groupBy("grp").agg(functions.collect_list("v").alias("items"))


def _disc_nested_collect_list_repark(engine: Engine) -> None:
    out = engine.arrow_of(_nested_collect_list_frame(engine))
    items = out.schema.field("items")
    value = _list_value_field(out, "items")
    assert items.nullable is True
    assert value.name == "item"
    assert value.nullable is True


def _disc_nested_collect_list_spark(engine: Engine) -> None:
    out = engine.arrow_of(_nested_collect_list_frame(engine))
    items = out.schema.field("items")
    value = _list_value_field(out, "items")
    assert items.nullable is False
    assert value.name == "element"
    assert value.nullable is False


def _nested_aos_frame(engine: Engine) -> Any:
    types = engine.types
    element = types.StructType(
        [
            types.StructField("x", types.LongType()),
            types.StructField("y", types.StringType()),
        ]
    )
    schema = types.StructType(
        [
            types.StructField("id", types.LongType()),
            types.StructField("items", types.ArrayType(element)),
        ]
    )
    return engine.session.createDataFrame(
        [
            (1, [(10, "a"), (11, "b")]),
            (2, [(20, "c")]),
            (3, [(10, "a"), (11, "b")]),
        ],
        schema,
    ).select("id", "items")


def _disc_nested_aos_list_field_repark(engine: Engine) -> None:
    out = engine.arrow_of(_nested_aos_frame(engine))
    assert _list_value_field(out, "items").name == "item"


def _disc_nested_aos_list_field_spark(engine: Engine) -> None:
    out = engine.arrow_of(_nested_aos_frame(engine))
    assert _list_value_field(out, "items").name == "element"


def _semi_pair(engine: Engine) -> tuple[Any, Any]:
    left = engine.session.createDataFrame([(1, "a"), (2, "b"), (None, "n")], ["k", "a"])
    right = engine.session.createDataFrame([(1,), (9,)], ["k"])
    return left, right


def _disc_conditionless_semi_repark(engine: Engine) -> None:
    left, right = _semi_pair(engine)
    _expect_raises(
        lambda: left.join(right, None, "leftsemi"),
        needle="requires an `on` condition",
    )
    _expect_raises(
        lambda: left.join(right, [], "leftanti"),
        needle="requires an `on` condition",
    )


def _disc_conditionless_semi_spark(engine: Engine) -> None:
    # Live Spark 4.1.2 (g4b ledger §2 D1): on=None keeps every left row iff right is non-empty.
    left, right = _semi_pair(engine)
    out = engine.arrow_of(left.join(right, None, "leftsemi"))
    assert out.num_rows == 3, "Spark conditionless leftsemi keeps every left row (right non-empty)"
    # on=[] is a PySpark IndexError — a different Spark face; the live half pins on=None.


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
    Disclosure(
        "cast_timestamp_to_int_nullability",
        _disc_cast_timestamp_to_int_repark,
        _disc_cast_timestamp_to_int_spark,
        "CAST(TIMESTAMP '2020-01-01 00:00:00' AS INT) under UTC: both engines yield unix "
        "seconds 1577836800 as int32; Spark types the CAST nullable, repark propagates the "
        "literal's non-null. X-1's raise-vs-value split is STALE after #64; this is the TZ-5 "
        "§10 form. Corpus: "
        "test_cast_failure_parity.py::test_cast_failure_row"
        "[timestamp_to_int_nullability].",
    ),
    Disclosure(
        "null_safe_eq_sql_nullability",
        _disc_null_safe_eq_sql_repark,
        _disc_null_safe_eq_sql_spark,
        "SELECT (NULL <=> NULL): value TRUE on both engines; repark Arrow bool is nullable, "
        "Spark's is non-nullable. Corpus: "
        "test_three_valued_logic_parity.py::test_tvl_parity_row[null_eq_vs_null_safe_eq].",
    ),
    Disclosure(
        "null_safe_eq_df_nullability",
        _disc_null_safe_eq_df_repark,
        _disc_null_safe_eq_df_spark,
        "Column.eqNullSafe: values match Spark; result nullability diverges (repark nullable, "
        "Spark not). Corpus: "
        "test_three_valued_logic_parity.py::test_tvl_parity_row[df_eq_null_safe_select].",
    ),
    Disclosure(
        "sum_catastrophic_cancellation_fixture",
        _disc_sum_catastrophic_cancellation_repark,
        _disc_sum_catastrophic_cancellation_spark,
        "sum of the G7 catastrophic-cancellation fixture: repark lands 3.75; Spark 4.1.2 "
        "local[2]/shuffle=2 lands 2.25. Same Arrow float64 nullable; accumulation order "
        "diverges. Corpus: "
        "test_float_agg_parity.py::test_float_agg_parity_row"
        "[sum_catastrophic_cancellation_fixture].",
    ),
    Disclosure(
        "avg_catastrophic_cancellation_fixture",
        _disc_avg_catastrophic_cancellation_repark,
        _disc_avg_catastrophic_cancellation_spark,
        "avg of the same G7 fixture (sum/8): repark 0.46875 vs Spark 0.28125. Follows the "
        "sum divergence. Corpus: "
        "test_float_agg_parity.py::test_float_agg_parity_row"
        "[avg_catastrophic_cancellation_fixture].",
    ),
    Disclosure(
        "nested_array_list_field_name",
        _disc_nested_array_list_field_repark,
        _disc_nested_array_list_field_spark,
        "Array column createDataFrame: values match; list value-field name is 'item' "
        "(repark) vs 'element' (Spark). Corpus: "
        "test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges"
        "[array_column_roundtrip].",
    ),
    Disclosure(
        "nested_collect_list_nullability",
        _disc_nested_collect_list_repark,
        _disc_nested_collect_list_spark,
        "groupBy.agg(collect_list): values match under G18; repark list<item: int64> "
        "nullable vs Spark list<element: int64 not null> non-nullable. Corpus: "
        "test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges"
        "[collect_list_grouped].",
    ),
    Disclosure(
        "nested_array_of_struct_list_field_name",
        _disc_nested_aos_list_field_repark,
        _disc_nested_aos_list_field_spark,
        "Array-of-struct createDataFrame: values match; list value-field name is 'item' "
        "(repark) vs 'element' (Spark). Corpus: "
        "test_nested_container_parity.py::test_nested_row_matches_spark_or_still_diverges"
        "[array_of_struct_roundtrip].",
    ),
    Disclosure(
        "conditionless_semi_anti_refuses",
        _disc_conditionless_semi_repark,
        _disc_conditionless_semi_spark,
        "df.join(other, how='leftsemi'/'leftanti') with on=None or on=[]: repark refuses "
        "loud (Cartesian fallback would be a wrong answer); Spark on=None keeps every left "
        "row iff the right side is non-empty. Pin: "
        "test_g4b_semi_join.py::test_conditionless_semi_family_refuses_loud.",
    ),
]
