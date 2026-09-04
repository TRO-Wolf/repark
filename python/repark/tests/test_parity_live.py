"""The live PySpark oracle tier (L1) + its detector proofs (L6a).

Two layers over the shared scenario registry in :mod:`_live_parity`:

* **routine (JVM-free, every PR)** — ``test_scenario_recipe_matches_golden_on_repark`` runs each
  engine-agnostic recipe on repark and asserts ``repark == pinned golden``; lifecycle siblings
  (``test_lifecycle_scenario_matches_golden_on_repark``) cover the multi-statement MERGE rows the
  same way (memory Iceberg catalog, no JVM).
* **live (``REPARK_PARITY_LIVE=1``, nightly / dispatch / parity-live.yml)** — the ``*_live_*`` tests
  spin up ONE shared session-scoped local SparkSession (plus an Iceberg-provisioned engine for
  lifecycle rows) and assert the full triple **repark == pinned golden == live Spark** for every
  scenario, plus that every recorded divergence STILL diverges. Flag unset → every live test SKIPs
  with a visible reason — it never silently passes.

A third, always-on layer sits beside them: ``test_disclosures_mirror_the_registry`` checks the
``DISCLOSURES`` list against the divergence registry (``docs/spark-sql-iceberg-parity.md`` §6),
which is the SSOT for divergence *semantics*; the list is its machine-checked mirror.

Pyspark is imported lazily (inside the session fixture), so this file collects on a runner with no
pyspark and no JVM (the routine-CI contract, L3).
"""

from __future__ import annotations

import re
import tempfile
from collections.abc import Iterator
from pathlib import Path

import _live_parity as lp
import pytest

from repark_parity import assert_frames_equal

# Shared live SparkSession — built ONCE per session, only when the flag is armed


@pytest.fixture(scope="session")
def spark_engine() -> Iterator[lp.Engine]:
    """The single shared live PySpark oracle engine (session-scoped). Skips (never fails) when the
    live flag is unset, so requesting it outside live mode is a visible skip."""
    if not lp.LIVE:
        pytest.skip(lp.LIVE_SKIP_REASON)
    engine = lp.build_spark_engine()
    try:
        yield engine
    finally:
        engine.session.stop()


@pytest.fixture(scope="session")
def spark_iceberg_engine() -> Iterator[lp.Engine]:
    """Session-scoped Spark + Iceberg engine for lifecycle MERGE live tests (option A).

    Separate from :func:`spark_engine` so the default live session stays JVM-cheap and
    Iceberg-free. Warehouse is a temp directory removed after the session. Skips when the live
    flag is unset.
    """
    if not lp.LIVE:
        pytest.skip(lp.LIVE_SKIP_REASON)
    warehouse = Path(tempfile.mkdtemp(prefix="repark-parity-live-iceberg-"))
    engine = lp.build_spark_iceberg_engine(warehouse)
    try:
        yield engine
    finally:
        engine.session.stop()
        # Best-effort cleanup of the temp warehouse (Iceberg metadata residue is fine to drop).
        import shutil

        shutil.rmtree(warehouse, ignore_errors=True)


# Routine (JVM-free) — the shared recipes reproduce the goldens on repark


@pytest.mark.parametrize("scenario", lp.SCENARIOS, ids=[s.name for s in lp.SCENARIOS])
def test_scenario_recipe_matches_golden_on_repark(scenario: lp.Scenario) -> None:
    """Every registry recipe produces its pinned golden on repark — no JVM. This is the routine-CI
    home of the shared recipes (proves them before the live tier layers Spark on top).

    A scenario's `session_conf` is applied by BUILDING the repark session with it (repark resolves
    build-time knobs once at `getOrCreate`), so the JVM-free leg runs under the same session
    configuration the oracle leg will.
    """
    engine = lp.build_repark_engine(scenario.session_conf)
    actual = lp.run_scenario(scenario, engine)
    assert_frames_equal(actual, scenario.golden, order_sensitive=scenario.order_sensitive)


@pytest.mark.parametrize(
    "scenario", lp.LIFECYCLE_SCENARIOS, ids=[s.name for s in lp.LIFECYCLE_SCENARIOS]
)
def test_lifecycle_scenario_matches_golden_on_repark(
    scenario: lp.LifecycleScenario, tmp_path: Path
) -> None:
    """Every lifecycle recipe produces its pinned golden on repark — no JVM.

    repark path: build with session_conf, register a memory Iceberg catalog, run create→seed→act
    →read with COW TBLPROPERTIES (with_cow_props=True).
    """
    engine = lp.build_repark_engine(scenario.session_conf)
    engine.session.register_memory_catalog(scenario.catalog, tmp_path)
    actual = lp.run_lifecycle_scenario(scenario, engine, with_cow_props=True)
    assert_frames_equal(actual, scenario.golden, order_sensitive=scenario.order_sensitive)


# Live — repark == pinned golden == live Spark (value AND Arrow-path type/nullability)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("scenario", lp.SCENARIOS, ids=[s.name for s in lp.SCENARIOS])
def test_live_scenario_matches_repark_golden_and_spark(
    scenario: lp.Scenario, spark_engine: lp.Engine
) -> None:
    """The drift detector: the pinned golden is re-derived from LIVE Spark and must equal both the
    pin and repark. A stale/hand-edited pin (golden drift) OR a Spark semantics change (oracle
    drift) makes the Spark leg — or the repark leg — diverge from the pin, going RED here.
    """
    order = scenario.order_sensitive

    # repark == pinned golden (the scenario's session conf is applied at session BUILD)
    repark_out = lp.run_scenario(scenario, lp.build_repark_engine(scenario.session_conf))
    assert_frames_equal(repark_out, scenario.golden, order_sensitive=order)

    # pinned golden == LIVE Spark (the leg routine CI can never run) — re-derived, not re-asserted.
    # The oracle session is shared, so the scenario's conf is set around this leg and restored.
    with lp.spark_session_conf(spark_engine, scenario.session_conf):
        spark_out = lp.run_scenario(scenario, spark_engine)
    assert_frames_equal(spark_out, scenario.golden, order_sensitive=order)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize("disclosure", lp.DISCLOSURES, ids=[d.name for d in lp.DISCLOSURES])
def test_live_disclosure_still_diverges(disclosure: lp.Disclosure, spark_engine: lp.Engine) -> None:
    """Each recorded divergence still holds on BOTH live engines: repark keeps its (divergent)
    behavior and live Spark keeps the behavior it differs from. If either engine converged toward
    the other, its check flips RED — forcing the disclosure to be revisited rather than silently
    laundered into 'parity'.
    """
    disclosure.repark_check(lp.build_repark_engine())
    disclosure.spark_check(spark_engine)


_LOG1P_LIVE_SQL = (
    "SELECT log1p(CAST(1e-16 AS DOUBLE)) AS r0, "
    "log1p(CAST(1e-10 AS DOUBLE)) AS r1, "
    "log1p(CAST(-1.0 AS DOUBLE)) AS r2, "
    "log1p(CAST(-2.0 AS DOUBLE)) AS r3, "
    "expm1(CAST(1e-16 AS DOUBLE)) AS r4, "
    "expm1(CAST(1e-10 AS DOUBLE)) AS r5, "
    "expm1(CAST(710.0 AS DOUBLE)) AS r6, "
    "log1p(CAST('0.0000000000000001' AS DECIMAL(38,16))) AS r7, "
    "expm1(CAST('0.0000000000000001' AS DECIMAL(38,16))) AS r8"
)
_LOG1P_LIVE_WANT = (
    1e-16,
    9.999999999500001e-11,
    None,
    None,
    1e-16,
    1.00000000005e-10,
    float("inf"),
    1e-16,
    1e-16,
)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fn_fix_1_isnan_sha2_try_to_number_add_months(spark_engine: lp.Engine) -> None:
    """pins: fn-fix-1-registry-rows/C-003"""
    from pyspark.sql import functions as spark_fn

    hello = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
    isnan_table = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([(1.0,), (None,)], ["x"]).select(
            spark_fn.isnan("x").alias("v")
        )
    )
    assert isnan_table.column("v").to_pylist() == [False, False]
    assert isnan_table.schema.field("v").nullable is False
    sha_table = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([("hello",)], ["s"]).select(
            spark_fn.sha2("s", 256).alias("h"),
            spark_fn.sha2("s", 0).alias("z"),
        )
    )
    assert sha_table.column("h").to_pylist() == [hello]
    assert sha_table.column("z").to_pylist() == [hello]
    from pyspark.errors import AnalysisException as SparkAnalysisException

    try:
        spark_engine.session.createDataFrame([("123.45", "999.99")], ["s", "f"]).select(
            spark_fn.try_to_number(spark_fn.col("s"), spark_fn.col("f"))
        ).toArrow()
        raise AssertionError("try_to_number non-foldable format must raise")
    except SparkAnalysisException as exc:
        assert "NON_FOLDABLE_INPUT" in str(exc)
    months = spark_engine.arrow_of(
        spark_engine.session.sql("SELECT add_months(DATE '2015-02-28', 1) AS r")
    )
    assert str(months.column("r").to_pylist()[0]) == "2015-03-28"


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fn_fix_1_last_and_approx_percentile(spark_engine: lp.Engine) -> None:
    """pins: fn-fix-1-registry-rows/C-003"""
    from pyspark.sql import Window as SparkWindow
    from pyspark.sql import functions as spark_fn

    frame = spark_engine.session.createDataFrame(
        [("a", 1), ("a", 2), ("a", 3), ("a", None), ("b", 4), ("b", 6)],
        ["k", "v"],
    )
    window = (
        SparkWindow.partitionBy("k")
        .orderBy(spark_fn.col("v").asc_nulls_last())
        .rowsBetween(SparkWindow.unboundedPreceding, SparkWindow.unboundedFollowing)
    )
    last_table = spark_engine.arrow_of(
        frame.select("k", spark_fn.last("v", True).over(window).alias("l")).distinct()
    )
    answers = sorted((row["k"], row["l"]) for row in last_table.to_pylist())
    assert answers == [("a", 3), ("b", 6)]
    pct = spark_engine.arrow_of(frame.select(spark_fn.approx_percentile("v", 0.5).alias("p")))
    assert pct.column("p").to_pylist() == [3]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fn_fix_1_arrays(spark_engine: lp.Engine) -> None:
    """pins: fn-fix-1-registry-rows/C-003"""
    from pyspark.sql import functions as spark_fn

    pos = spark_engine.arrow_of(
        spark_engine.session.createDataFrame(
            [([1, 2, 3], 2), ([1, 2, 3], 9), (None, 1)], ["a", "x"]
        ).select(spark_fn.array_position("a", spark_fn.col("x")).alias("p"))
    )
    assert pos.column("p").to_pylist() == [2, 0, None]
    sort_table = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([([2, None, 1],)], ["a"]).select(
            spark_fn.array_sort("a").alias("s"),
            spark_fn.sort_array("a").alias("sa"),
        )
    )
    assert sort_table.column("s").to_pylist() == [[1, 2, None]]
    assert sort_table.column("sa").to_pylist() == [[None, 1, 2]]
    overlap = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([([1, None], [2, None])], ["a", "b"]).select(
            spark_fn.arrays_overlap("a", "b").alias("o")
        )
    )
    assert overlap.column("o").to_pylist() == [None]
    flat = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([([[1], None],)], ["a"]).select(
            spark_fn.flatten("a").alias("f")
        )
    )
    assert flat.column("f").to_pylist() == [None]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fn_fix_1_nan_ingest(spark_engine: lp.Engine) -> None:
    """pins: fn-fix-1-registry-rows/C-003"""
    from pyspark.sql import functions as spark_fn

    table = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([(float("nan"),)]).select(
            spark_fn.isnan("_1").alias("ina"),
            spark_fn.isnull("_1").alias("n"),
        )
    )
    assert table.column("ina").to_pylist() == [True]
    assert table.column("n").to_pylist() == [False]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fn_fix_2_strings(spark_engine: lp.Engine) -> None:
    """pins: fn-fix-2-string-rows/C-003, fn-fix-2-ctrl-1-controls/C-002"""
    from pyspark.sql import functions as spark_fn

    initcap_table = spark_engine.arrow_of(
        spark_engine.session.createDataFrame(
            [("a-b",), ("foo.bar",), ("o'neil",), ("x\ty",)], ["s"]
        ).select(spark_fn.initcap("s").alias("v"))
    )
    assert initcap_table.column("v").to_pylist() == ["A-b", "Foo.bar", "O'neil", "X\ty"]
    initcap_ctrl = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([("ünï_9 ab",), ("",), (None,)], ["s"]).select(
            spark_fn.initcap("s").alias("v")
        )
    )
    assert initcap_ctrl.column("v").to_pylist() == ["Ünï_9 Ab", "", None]
    chr_table = spark_engine.arrow_of(
        spark_engine.session.createDataFrame([(256,), (300,), (65601,), (-1,)], ["n"]).select(
            spark_fn.chr("n").alias("c"), spark_fn.char("n").alias("h")
        )
    )
    assert chr_table.column("c").to_pylist() == ["\x00", ",", "A", ""]
    assert chr_table.column("h").to_pylist() == ["\x00", ",", "A", ""]
    chr_ctrl = spark_engine.arrow_of(
        spark_engine.session.createDataFrame(
            [(0,), (65536,), (1114112,), (-1,), (None,)], ["n"]
        ).select(spark_fn.chr("n").alias("c"))
    )
    assert chr_ctrl.column("c").to_pylist() == ["\x00", "\x00", "\x00", "", None]
    trim_table = spark_engine.arrow_of(
        spark_engine.session.range(1).select(
            spark_fn.trim(spark_fn.lit("xxSparkxx"), spark_fn.lit("x")).alias("t"),
            spark_fn.ltrim(spark_fn.lit("xxSparkxx"), spark_fn.lit("x")).alias("l"),
            spark_fn.rtrim(spark_fn.lit("xxSparkxx"), spark_fn.lit("x")).alias("r"),
        )
    )
    assert trim_table.column("t").to_pylist() == ["Spark"]
    assert trim_table.column("l").to_pylist() == ["Sparkxx"]
    assert trim_table.column("r").to_pylist() == ["xxSpark"]
    trim_ctrl = spark_engine.arrow_of(
        spark_engine.session.range(1).select(
            spark_fn.trim(spark_fn.lit("abc"), spark_fn.lit("")).alias("t"),
            spark_fn.ltrim(spark_fn.lit("abc"), spark_fn.lit("")).alias("l"),
            spark_fn.rtrim(spark_fn.lit("abc"), spark_fn.lit("")).alias("r"),
        )
    )
    assert trim_ctrl.column("t").to_pylist() == ["abc"]
    assert trim_ctrl.column("l").to_pylist() == ["abc"]
    assert trim_ctrl.column("r").to_pylist() == ["abc"]
    trim_null = spark_engine.arrow_of(
        spark_engine.session.sql("SELECT TRIM(BOTH NULL FROM 'abc') AS t")
    )
    assert trim_null.column("t").to_pylist() == [None]
    trim_side_null = spark_engine.arrow_of(
        spark_engine.session.range(1).select(
            spark_fn.ltrim(spark_fn.lit("abc"), spark_fn.lit(None).cast("string")).alias("l"),
            spark_fn.rtrim(spark_fn.lit("abc"), spark_fn.lit(None).cast("string")).alias("r"),
        )
    )
    assert trim_side_null.column("l").to_pylist() == [None]
    assert trim_side_null.column("r").to_pylist() == [None]
    trim_side_sql = spark_engine.arrow_of(
        spark_engine.session.sql(
            "SELECT TRIM(LEADING NULL FROM 'abc') AS l, TRIM(TRAILING NULL FROM 'abc') AS r"
        )
    )
    assert trim_side_sql.column("l").to_pylist() == [None]
    assert trim_side_sql.column("r").to_pylist() == [None]
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", "false"),)):
        elt_off = spark_engine.arrow_of(
            spark_engine.session.range(1).select(
                spark_fn.elt(spark_fn.lit(3), spark_fn.lit("a"), spark_fn.lit("b")).alias("a"),
                spark_fn.elt(spark_fn.lit(0), spark_fn.lit("a"), spark_fn.lit("b")).alias("b"),
            )
        )
        assert elt_off.column("a").to_pylist() == [None]
        assert elt_off.column("b").to_pylist() == [None]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_fn_fix_2_regex_like(spark_engine: lp.Engine) -> None:
    """pins: fn-fix-2-string-rows/C-003, fn-fix-2-ctrl-1-controls/C-002"""
    from pyspark.errors import AnalysisException as SparkAnalysisException
    from pyspark.errors.exceptions.captured import ArrayIndexOutOfBoundsException
    from pyspark.sql import functions as spark_fn

    frame = spark_engine.session.createDataFrame([("a1b2 Ünï_9",), ("foo",), ("aabbaa",)], ["s"])
    count_table = spark_engine.arrow_of(
        frame.select(spark_fn.regexp_count("s", spark_fn.lit("[[:alpha:]]")).alias("c"))
    )
    assert count_table.column("c").to_pylist() == [1, 0, 4]
    rlike_table = spark_engine.arrow_of(
        frame.select(spark_fn.rlike("s", spark_fn.lit("[[:alpha:]]")).alias("m"))
    )
    assert rlike_table.column("m").to_pylist() == [True, False, True]
    replace_table = spark_engine.arrow_of(
        frame.select(
            spark_fn.regexp_replace("s", spark_fn.lit("[[:alpha:]]"), spark_fn.lit("#")).alias("r")
        )
    )
    assert replace_table.column("r").to_pylist() == ["#1b2 Ünï_9", "foo", "##bb##"]
    bracket = spark_engine.session.createDataFrame([("x",), ("fox",)], ["s"])
    bracket_rlike = spark_engine.arrow_of(
        bracket.select(spark_fn.rlike("s", spark_fn.lit("[[:alpha:]x]")).alias("m"))
    )
    assert bracket_rlike.column("m").to_pylist() == [True, True]
    bracket_like = spark_engine.arrow_of(
        bracket.select(spark_fn.regexp_like("s", spark_fn.lit("[[:alpha:]x]")).alias("m"))
    )
    assert bracket_like.column("m").to_pylist() == [True, True]
    extract_match = spark_engine.arrow_of(
        spark_engine.session.sql("SELECT regexp_extract('alpha', '([[:alpha:]]+)', 1) AS e")
    )
    assert extract_match.column("e").to_pylist() == ["alpha"]
    extract_nomatch = spark_engine.arrow_of(
        spark_engine.session.sql("SELECT regexp_extract('fox', '([[:alpha:]]+)', 1) AS e")
    )
    assert extract_nomatch.column("e").to_pylist() == [""]
    rlike_keyword = spark_engine.arrow_of(
        spark_engine.session.sql(
            "SELECT 'x' RLIKE '[[:alpha:]x]' AS a, 'fox' RLIKE '[[:alpha:]x]' AS b"
        )
    )
    assert rlike_keyword.column("a").to_pylist() == [True]
    assert rlike_keyword.column("b").to_pylist() == [True]
    try:
        spark_engine.session.range(1).select(
            spark_fn.elt(spark_fn.lit(3), spark_fn.lit("a"), spark_fn.lit("b"))
        ).toArrow()
        raise AssertionError("elt index 3 must raise")
    except ArrayIndexOutOfBoundsException as exc:
        assert "INVALID_ARRAY_INDEX" in str(exc)
    try:
        spark_engine.session.range(1).select(
            spark_fn.like(spark_fn.lit("ab"), spark_fn.lit("ab\\"))
        ).toArrow()
        raise AssertionError("like escape-at-end must raise")
    except SparkAnalysisException as exc:
        assert "INVALID_FORMAT.ESC_AT_THE_END" in str(exc)
    try:
        spark_engine.arrow_of(
            spark_engine.session.sql("SELECT 'a%' LIKE 'a\\\\' ESCAPE '\\\\' AS m")
        )
        raise AssertionError("like explicit escape-at-end must raise")
    except SparkAnalysisException as exc:
        assert "INVALID_FORMAT.ESC_AT_THE_END" in str(exc)
    with lp.spark_session_conf(spark_engine, (("spark.sql.ansi.enabled", "false"),)):
        try:
            spark_engine.session.range(1).select(
                spark_fn.like(spark_fn.lit("ab"), spark_fn.lit("ab\\"))
            ).toArrow()
            raise AssertionError("like escape-at-end must raise with ANSI off")
        except SparkAnalysisException as exc:
            assert "INVALID_FORMAT.ESC_AT_THE_END" in str(exc)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_log1p_expm1_tiny_args_and_domain(spark_engine: lp.Engine) -> None:
    """pins: log1p-1-precise-kernels/C-001, C-004"""
    table = spark_engine.arrow_of(spark_engine.session.sql(_LOG1P_LIVE_SQL))
    for index, want in enumerate(_LOG1P_LIVE_WANT):
        got = table.column(f"r{index}").to_pylist()[0]
        if want is None:
            assert got is None, index
        else:
            assert got == want, index


_DATE_FN_1_LIVE_SQL = (
    "SELECT DATE(TIMESTAMP '2024-06-15 03:00:00') AS d_ts, "
    "DATE('2024-06-15') AS d_s, "
    "DATE(DATE '2024-06-15') AS d_d, "
    "unix_timestamp(TIMESTAMP '2024-06-15 12:00:00') AS u_ts, "
    "unix_timestamp('2024-06-15 12:00:00') AS u_s, "
    "CAST((unix_timestamp(TIMESTAMP '2026-01-01 10:15:00') "
    "- unix_timestamp(TIMESTAMP '2026-01-01 10:00:00')) / 60 AS INT) AS w"
)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_date_fn_1_date_and_unix_timestamp(spark_engine: lp.Engine) -> None:
    """pins: date-fn-1-spark-date-spelling/C-001, C-003"""
    table = spark_engine.arrow_of(spark_engine.session.sql(_DATE_FN_1_LIVE_SQL))
    assert str(table.column("d_ts").to_pylist()[0]) == "2024-06-15"
    assert str(table.column("d_s").to_pylist()[0]) == "2024-06-15"
    assert str(table.column("d_d").to_pylist()[0]) == "2024-06-15"
    assert table.column("u_ts").to_pylist() == [1_718_452_800]
    assert table.column("u_s").to_pylist() == [1_718_452_800]
    assert table.column("w").to_pylist() == [15]


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
@pytest.mark.parametrize(
    "repark_scenario,spark_scenario",
    list(zip(lp.LIFECYCLE_SCENARIOS, lp.LIFECYCLE_SCENARIOS_SPARK, strict=True)),
    ids=[s.name for s in lp.LIFECYCLE_SCENARIOS],
)
def test_live_lifecycle_scenario_matches_repark_golden_and_spark(
    repark_scenario: lp.LifecycleScenario,
    spark_scenario: lp.LifecycleScenario,
    spark_iceberg_engine: lp.Engine,
    tmp_path: Path,
) -> None:
    """Lifecycle drift detector: repark == pinned golden == live Spark+Iceberg.

    Uses the dedicated Iceberg-provisioned engine (not the plain spark_engine). repark gets a
    fresh memory catalog + COW props; Spark runs without COW props (Iceberg 1.11 default).
    Catalog names differ (mem vs local) but SQL shapes and goldens are identical.

    Collected *after* the scenario + disclosure live tests so the default SparkContext (no
    Iceberg GAV) is finished before ``build_spark_iceberg_engine`` stops it and starts a
    packages-capable context. ``spark.jars.packages`` is SparkContext-level; getOrCreate on
    the default session would leave ``SparkCatalog`` unloaded (L-1 amendment).
    """
    order = repark_scenario.order_sensitive
    assert repark_scenario.name == spark_scenario.name
    assert repark_scenario.golden.equals(spark_scenario.golden)

    repark_engine = lp.build_repark_engine(repark_scenario.session_conf)
    repark_engine.session.register_memory_catalog(repark_scenario.catalog, tmp_path)
    repark_out = lp.run_lifecycle_scenario(repark_scenario, repark_engine, with_cow_props=True)
    assert_frames_equal(repark_out, repark_scenario.golden, order_sensitive=order)

    with lp.spark_session_conf(spark_iceberg_engine, spark_scenario.session_conf):
        spark_out = lp.run_lifecycle_scenario(
            spark_scenario, spark_iceberg_engine, with_cow_props=False
        )
    assert_frames_equal(spark_out, spark_scenario.golden, order_sensitive=order)


# L6(a) detector — the flag gate. Deterministic, always runs (no JVM): proves the tier is OFF
# for any value other than exactly "1", so an unarmed run SKIPs rather than silently passing.


def test_live_flag_predicate_gates_on_exact_env_value() -> None:
    assert lp.live_enabled({}) is False, "unset -> off"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": ""}) is False, "empty -> off"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": "0"}) is False, "'0' -> off"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": "true"}) is False, "only exact '1' arms it"
    assert lp.live_enabled({"REPARK_PARITY_LIVE": "1"}) is True, "'1' -> on"


def test_registry_covers_the_mandated_golden_family() -> None:
    """Guard against accidental registry shrinkage: the coverage floor is the 23-golden family
    (group-agg/na/union + columns + dates + compound-agg display name) plus the two write-division
    goldens (union + bare), the two audit-G2 filter-rewriter goldens, the two H-1a non-UTC-oracle
    goldens, and the 13 G1/G16 extraction-class timezone live rows. DISCLOSURES are a separate
    exact-set pin.
    """
    assert len(lp.SCENARIOS) == 42, (
        "the 23-golden family + 2 Group L-write division goldens + 2 audit-G2 filter goldens "
        "+ 2 H-1a non-UTC-oracle goldens + 13 G1/G16 extraction-class timezone live rows"
    )
    assert len({s.name for s in lp.SCENARIOS}) == 42, "scenario names are unique"
    assert {d.name for d in lp.DISCLOSURES} == {
        "int_union_string",
        "fillna_scalar_numeric_nullability",
        "filter_case_collision_bypasses",
        "filter_backtick_identifier",
        "cast_timestamp_to_int_nullability",
        "null_safe_eq_sql_nullability",
        "null_safe_eq_df_nullability",
        "sum_catastrophic_cancellation_fixture",
        "avg_catastrophic_cancellation_fixture",
        "nested_array_list_field_name",
        "nested_collect_list_nullability",
        "nested_array_of_struct_list_field_name",
        "conditionless_semi_anti_refuses",
    }, "every load-bearing disclosure is present"
    # A disclosure is a DIVERGENCE detector; a converged pair belongs in the corpus as a
    # shared-raise equality — that keeps `test_disclosures_mirror_the_registry` green both ways.
    assert len(lp.DISCLOSURES) == 13, "disclosure roster is an exact-set pin, not a floor"


def test_lifecycle_registry_budget() -> None:
    """Budget pin: exactly 2 live-tier MERGE lifecycle scenarios (N-2b item 2 / G3 live half)."""
    assert len(lp.LIFECYCLE_SCENARIOS) == 2, (
        f"G3 live budget is 2 MERGE lifecycle scenarios (got {len(lp.LIFECYCLE_SCENARIOS)})"
    )
    assert len({s.name for s in lp.LIFECYCLE_SCENARIOS}) == 2, "lifecycle names are unique"
    assert {s.name for s in lp.LIFECYCLE_SCENARIOS} == {
        "live_merge_basic_upsert",
        "live_merge_matched_arm_order",
    }
    # Spark-facing twin list must mirror names/goldens (catalog differs only).
    assert len(lp.LIFECYCLE_SCENARIOS_SPARK) == 2
    assert [s.name for s in lp.LIFECYCLE_SCENARIOS] == [
        s.name for s in lp.LIFECYCLE_SCENARIOS_SPARK
    ]


def test_registry_runs_at_least_two_scenarios_under_a_non_utc_oracle() -> None:
    """H-1a acceptance: the registry can put the ORACLE in a non-UTC session, and does.

    A registry pinned to one zone is structurally incapable of catching a session-timezone
    divergence — so the count is a pin, not a comment. Every override names the ONE session
    timezone key spelling; a second spelling would fail here rather than quietly configure nothing.
    """
    overridden = [scenario for scenario in lp.SCENARIOS if scenario.session_conf]
    assert len(overridden) >= 2, "at least two scenarios must run under a non-UTC oracle session"
    zones = {
        value
        for scenario in overridden
        for key, value in scenario.session_conf
        if key == lp.SESSION_TIME_ZONE_KEY
    }
    assert zones == {lp.ZONE_NEW_YORK, lp.ZONE_TOKYO}, (
        "the overrides must span both sides of UTC, so a sign error cannot pass both"
    )
    assert lp.DEFAULT_SESSION_TIME_ZONE not in zones, "an override to UTC is not an override"
    for scenario in overridden:
        assert all(key == lp.SESSION_TIME_ZONE_KEY for key, _ in scenario.session_conf), (
            "session-timezone overrides use the one authoritative key spelling"
        )


def test_build_repark_engine_override_stops_the_active_session_and_rebuilds() -> None:
    """The `active.stop()` branch inside `build_repark_engine`, exercised directly (JVM-free).

    Under the suite, conftest's `_isolate_active_session` clears the process-wide registry before
    each test, so the branch never fires there. It IS load-bearing: repark resolves the session
    zone once at construction, so with a session already active `getOrCreate` would hand that one
    back and the scenario would silently run under the PREVIOUS zone. Reds if the stop is removed
    or the override stops reaching the builder.
    """
    import repark

    first = lp.build_repark_engine()
    assert first.session.conf.get(lp.SESSION_TIME_ZONE_KEY) == lp.DEFAULT_SESSION_TIME_ZONE
    assert repark.ReparkSession.getActiveSession() is first.session

    second = lp.build_repark_engine(((lp.SESSION_TIME_ZONE_KEY, lp.ZONE_TOKYO),))
    assert second.session is not first.session, "an override must BUILD, never reuse"
    assert second.session.conf.get(lp.SESSION_TIME_ZONE_KEY) == lp.ZONE_TOKYO, (
        "the override zone must reach the freshly built session"
    )
    assert repark.ReparkSession.getActiveSession() is second.session

    # The documented cost: the previous handle is STOPPED, not left alive.
    with pytest.raises(Exception, match=r"stopped|SparkSession|alive"):
        first.session.sql("SELECT 1").to_arrow()
    second.session.stop()


# The divergence registry mirror — `DISCLOSURES` is the machine-checked mirror of the registry rows
# that declare a live mirror (docs/spark-sql-iceberg-parity.md §6). JVM-free: always runs.

#: The divergence registry — the SSOT for divergence *semantics* (this list mirrors it, never
#: the other way round). Resolved from this file so the check is cwd-independent.
_REGISTRY = Path(__file__).resolve().parents[3] / "docs" / "spark-sql-iceberg-parity.md"

#: A registry row opts into the live tier with a `live-mirror: <name>` bullet on its own line.
#: Prose that merely *mentions* the field must not register as a row; the exact spelling is
#: documented in the registry's §6 — the two must move together.
_LIVE_MIRROR_RE = re.compile(r"(?m)^- `live-mirror: ([a-z0-9_]+)`$")

#: Fail-closed half of the strict pattern: a strict-only match is satisfied by ZERO matches, so a
#: nearly-right bullet (bolded, indented, hyphenated name, trailing comment) would claim a mirror
#: that is never checked. Every ``-`` bullet mentioning the field is probed first and must satisfy
#: the strict form. Restricted to ``-`` bullets on purpose (§1's heading starts with ``*``).
_LIVE_MIRROR_PROBE = re.compile(r"(?m)^[ \t]*-.*live-mirror.*$")


def test_disclosures_mirror_the_registry() -> None:
    """Every registry row that claims a live mirror has a ``Disclosure`` of that name, and every
    ``Disclosure`` is claimed by a row. Both directions, because both failures are real: a row
    whose mirror was deleted quietly loses its drift detector, and a disclosure with no row is a
    divergence the registry does not describe.

    The registry is the SSOT for the *semantics* (``docs/spark-sql-iceberg-parity.md`` §6); this
    list is the checked mirror. Fix a RED by editing whichever side is wrong — never by relaxing
    this assertion. Fail-closed on a near-miss: a bullet mentioning ``live-mirror`` that does not
    match the exact spelling is a loud failure naming the line.
    """
    assert _REGISTRY.is_file(), (
        f"the divergence registry is missing at {_REGISTRY} — every citation in this repository "
        "resolves to it, and this mirror cannot be checked without it"
    )
    registry_text = _REGISTRY.read_text(encoding="utf-8")
    malformed = [
        line
        for line in _LIVE_MIRROR_PROBE.findall(registry_text)
        if _LIVE_MIRROR_RE.fullmatch(line) is None
    ]
    assert not malformed, (
        "a registry bullet claims a live mirror in a spelling this gate does not parse, so the "
        "row would advertise a drift detector that is never checked. The exact required form is "
        'a top-level bullet: "- " then `live-mirror: <name>` in backticks, <name> matching '
        f"[a-z0-9_]+, nothing before or after it on the line. Offending line(s): {malformed}"
    )
    declared = _LIVE_MIRROR_RE.findall(registry_text)
    assert len(declared) == len(set(declared)), (
        f"a live-mirror name is claimed by two registry rows: {sorted(declared)}"
    )

    disclosed = {d.name for d in lp.DISCLOSURES}
    assert set(declared) == disclosed, (
        "the registry's live-mirrored rows and the DISCLOSURES list disagree — "
        f"registry-only: {sorted(set(declared) - disclosed)}; "
        f"disclosure-only: {sorted(disclosed - set(declared))}"
    )


_BMOR3_CATALOG = "bmor3live"
_BMOR3_FLOOR_CATALOG = "bmor3floor"
_BMOR3_ALLOW = "repark.sql.allowCreateFormatVersion3"


def _bmor3_delete_pairs(arrow) -> list[tuple[str, int]]:
    rows = [
        (str(fmt).upper(), int(count))
        for fmt, count in zip(
            arrow.column("file_format").to_pylist(),
            arrow.column("record_count").to_pylist(),
            strict=True,
        )
    ]
    rows.sort()
    return rows


def _bmor3_ids(arrow) -> list[int]:
    values = [int(value) for value in arrow.column("id").to_pylist() if value is not None]
    values.sort()
    return values


def _bmor3_upgraded_parquet_repark(warehouse: Path, table: str, files: int) -> dict:
    from repark import ReparkSession

    spark = (
        ReparkSession.builder.appName("b-mor-3-live-repark")
        .config(_BMOR3_ALLOW, "true")
        .getOrCreate()
    )
    target = f"ice.sales.{table}"
    try:
        spark.register_memory_catalog("ice", warehouse)
        spark.sql("CREATE NAMESPACE ice.sales")
        spark.sql(
            f"CREATE TABLE {target} (id INT, name STRING) USING iceberg "
            "TBLPROPERTIES ('format-version' = '2', "
            "'write.delete.mode' = 'merge-on-read', "
            "'write.merge.mode' = 'merge-on-read', "
            "'write.update.mode' = 'merge-on-read', "
            "'write.delete.granularity' = 'file')"
        )
        for ident in range(1, files + 1):
            spark.sql(f"INSERT INTO {target} VALUES ({ident}, 'a'), ({ident + 100}, 'b')").collect()
        for ident in range(1, files + 1):
            spark.sql(f"DELETE FROM {target} WHERE id = {ident}").collect()
        spark.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('format-version' = '3')").collect()
        before = _bmor3_delete_pairs(
            spark.sql(f"SELECT file_format, record_count FROM {target}.delete_files").to_arrow()
        )
        first = spark.sql(
            f"CALL ice.system.rewrite_position_delete_files(table => 'sales.{table}')"
        ).to_arrow()
        after = _bmor3_delete_pairs(
            spark.sql(f"SELECT file_format, record_count FROM {target}.delete_files").to_arrow()
        )
        ids = _bmor3_ids(spark.sql(f"SELECT id FROM {target} ORDER BY id").to_arrow())
        second = spark.sql(
            f"CALL ice.system.rewrite_position_delete_files(table => 'sales.{table}')"
        ).to_arrow()
        return {
            "before": before,
            "rewritten": first.column("rewritten_delete_files_count")[0].as_py(),
            "added": first.column("added_delete_files_count")[0].as_py(),
            "after": after,
            "ids": ids,
            "second_rewritten": second.column("rewritten_delete_files_count")[0].as_py(),
            "second_added": second.column("added_delete_files_count")[0].as_py(),
        }
    finally:
        spark.stop()


def _bmor3_upgraded_parquet_spark(engine: lp.Engine, catalog: str, table: str, files: int) -> dict:
    import shutil

    warehouse = Path(tempfile.mkdtemp(prefix="bmor3-live-spark-"))
    session = engine.session
    keys = (
        f"spark.sql.catalog.{catalog}",
        f"spark.sql.catalog.{catalog}.type",
        f"spark.sql.catalog.{catalog}.warehouse",
    )
    for key, value in zip(
        keys, ("org.apache.iceberg.spark.SparkCatalog", "hadoop", str(warehouse)), strict=True
    ):
        session.conf.set(key, value)
    target = f"{catalog}.sales.{table}"
    try:
        session.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.sales")
        session.sql(
            f"CREATE TABLE {target} (id INT, name STRING) USING iceberg "
            "TBLPROPERTIES ('format-version'='2', "
            "'write.delete.mode'='merge-on-read', "
            "'write.merge.mode'='merge-on-read', "
            "'write.update.mode'='merge-on-read', "
            "'write.delete.granularity'='file')"
        )
        for ident in range(1, files + 1):
            session.createDataFrame(
                [(ident, "a"), (ident + 100, "b")],
                "id INT, name STRING",
            ).coalesce(1).writeTo(target).append()
        for ident in range(1, files + 1):
            session.sql(f"DELETE FROM {target} WHERE id = {ident}")
        session.sql(f"ALTER TABLE {target} SET TBLPROPERTIES ('format-version'='3')")
        before = _bmor3_delete_pairs(
            session.sql(f"SELECT file_format, record_count FROM {target}.delete_files").toArrow()
        )
        first = session.sql(
            f"CALL {catalog}.system.rewrite_position_delete_files(table => 'sales.{table}')"
        ).toArrow()
        after = _bmor3_delete_pairs(
            session.sql(f"SELECT file_format, record_count FROM {target}.delete_files").toArrow()
        )
        ids = _bmor3_ids(session.sql(f"SELECT id FROM {target} ORDER BY id").toArrow())
        second = session.sql(
            f"CALL {catalog}.system.rewrite_position_delete_files(table => 'sales.{table}')"
        ).toArrow()
        return {
            "before": before,
            "rewritten": first.column("rewritten_delete_files_count")[0].as_py(),
            "added": first.column("added_delete_files_count")[0].as_py(),
            "after": after,
            "ids": ids,
            "second_rewritten": second.column("rewritten_delete_files_count")[0].as_py(),
            "second_added": second.column("added_delete_files_count")[0].as_py(),
        }
    finally:
        session.sql(f"DROP TABLE IF EXISTS {target}")
        for key in keys:
            session.conf.unset(key)
        shutil.rmtree(warehouse, ignore_errors=True)


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_rewrite_position_delete_files_upgraded_parquet_matches_spark(
    tmp_path: Path, spark_iceberg_engine: lp.Engine
) -> None:
    """pins: b-mor-3-rewrite-position-deletes-v3/C-001, C-003"""
    assert _bmor3_upgraded_parquet_repark(tmp_path, "cellb", 5) == _bmor3_upgraded_parquet_spark(
        spark_iceberg_engine, _BMOR3_CATALOG, "cellb", 5
    )


@pytest.mark.skipif(not lp.LIVE, reason=lp.LIVE_SKIP_REASON)
def test_live_rewrite_position_delete_files_below_floor_matches_spark(
    tmp_path: Path, spark_iceberg_engine: lp.Engine
) -> None:
    """pins: rp-11-repin-f24/C-002"""
    assert _bmor3_upgraded_parquet_repark(tmp_path, "cellb2", 2) == _bmor3_upgraded_parquet_spark(
        spark_iceberg_engine, _BMOR3_FLOOR_CATALOG, "cellb2", 2
    )
