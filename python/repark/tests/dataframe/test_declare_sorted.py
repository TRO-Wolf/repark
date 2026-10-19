"""Facade pins for the SE-1 ``declareSorted`` door (repark extension, not PySpark).

``df.declareSorted(*cols)`` tells the engine that a ``createDataFrame`` source frame is
already sorted by those keys, so DataFusion can drop the ``SortExec`` a window over the
same keys would otherwise plan. The engine **always verifies** the claim first (O(n)
adjacent-pair scan) and refuses loudly when the data disagrees — there is no unverified
fast path, so a wrong declaration can never corrupt a result.

**Ordering spelling (disclosed):** the engine declares ``ASC NULLS LAST`` per key, matching
DataFusion's ``ORDER BY`` default. Spark's ``ORDER BY x ASC`` is ``NULLS FIRST``, and the
``Window`` facade follows Spark — so a window built from ``Window.orderBy("ts")`` over a
*nullable* key still plans its own sort (the null placement genuinely differs). The plan
pin below therefore spells the window ordering ``ASC NULLS LAST`` explicitly, which is the
shape that matches the declaration. Results parity is pinned on the Spark-default window.
"""

from __future__ import annotations

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom: `import ...functions as F`
from repark.errors import AnalysisException, PySparkValueError
from repark.spark.types import DoubleType, LongType, StringType, StructField, StructType
from repark.spark.window import Window

SCHEMA = StructType(
    [
        StructField("sym", StringType()),
        StructField("ts", LongType()),
        StructField("val", DoubleType()),
    ]
)

# 3 symbols x 300 ticks — enough rows for a real window plan, small enough for the suite.
SORTED_ROWS = [(sym, tick, float(tick)) for sym in ("AAA", "BBB", "CCC") for tick in range(300)]

# The window whose ordering matches the declared ASC NULLS LAST spelling.
WINDOW_SQL = (
    "SELECT sym, ts, "
    "row_number() OVER (PARTITION BY sym ORDER BY ts ASC NULLS LAST) AS rn "
    "FROM {view}"
)


@pytest.fixture
def spark() -> ReparkSession:
    """A single-partition session so the window plan is the tp=1 shape SE-1 pins."""
    return (
        ReparkSession.builder.appName("pytest-declare-sorted")
        .config("repark.target.partitions", 1)
        .getOrCreate()
    )


def _physical_plan(session: ReparkSession, query: str) -> str:
    """The physical-plan text of ``query`` (EXPLAIN rows, physical section only)."""
    rows = session.sql(f"EXPLAIN {query}").collect()
    physical = [row["plan"] for row in rows if row["plan_type"] == "physical_plan"]
    assert physical, f"EXPLAIN produced no physical plan: {rows}"
    return "\n".join(physical)


def test_declared_window_results_are_bit_identical_to_undeclared(spark: ReparkSession) -> None:
    """The door is a planner hint: same rows, same types, same bits."""
    declared = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts")
    plain = spark.createDataFrame(SORTED_ROWS, SCHEMA)

    window = Window.partitionBy("sym").orderBy("ts")
    left = declared.withColumn("rn", F.row_number().over(window)).to_arrow()
    right = plain.withColumn("rn", F.row_number().over(window)).to_arrow()

    assert left.num_rows == len(SORTED_ROWS)
    assert left.equals(right)


def test_declaration_elides_the_window_sortexec(spark: ReparkSession) -> None:
    """The door's whole point: SortExec 1 -> 0 for the declared frame (tp=1 shape).

    MUTATION: drop the re-plan in ``declare_sorted`` (the frame's logical plan keeps the
    table source captured before the declaration) -> the declared count goes back to 1.
    """
    declared = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts")
    plain = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    declared.createOrReplaceTempView("declared_src")
    plain.createOrReplaceTempView("plain_src")

    declared_plan = _physical_plan(spark, WINDOW_SQL.format(view="declared_src"))
    plain_plan = _physical_plan(spark, WINDOW_SQL.format(view="plain_src"))

    assert "SortExec" not in declared_plan, declared_plan
    assert plain_plan.count("SortExec") == 1, plain_plan
    # The declared ordering is what the scan now advertises.
    assert "output_ordering=sym@0 ASC NULLS LAST, ts@1 ASC NULLS LAST" in declared_plan
    assert "output_ordering" not in plain_plan
    assert (
        spark.sql(WINDOW_SQL.format(view="declared_src"))
        .to_arrow()
        .equals(spark.sql(WINDOW_SQL.format(view="plain_src")).to_arrow())
    )


def test_unsorted_data_refuses_loud_and_the_view_still_answers(spark: ReparkSession) -> None:
    """Verification is mandatory: a wrong claim raises and leaves the view untouched."""
    rows = [("BBB", 2, 1.0), ("AAA", 1, 2.0), ("CCC", 3, 3.0)]
    frame = spark.createDataFrame(rows, SCHEMA)

    with pytest.raises(AnalysisException) as excinfo:
        frame.declareSorted("sym", "ts")
    message = str(excinfo.value)
    assert "not sorted as declared" in message
    assert "rows 0 and 1" in message, message
    assert "sym, ts" in message, message

    # Declaration refused => nothing changed; the frame is a normal frame.
    assert frame.count() == 3
    assert [row["sym"] for row in frame.collect()] == ["BBB", "AAA", "CCC"]
    frame.createOrReplaceTempView("unsorted_src")
    assert "SortExec" in _physical_plan(spark, WINDOW_SQL.format(view="unsorted_src"))


@pytest.mark.parametrize("transform", ["filter", "select", "withColumn"])
def test_transformed_frames_refuse_with_the_source_frames_message(
    spark: ReparkSession, transform: str
) -> None:
    """Only the frame createDataFrame handed back carries a declarable view."""
    source = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    frames = {
        "filter": lambda: source.filter(F.col("ts") >= 0),
        "select": lambda: source.select("sym", "ts"),
        "withColumn": lambda: source.withColumn("two", F.lit(2)),
    }
    derived = frames[transform]()

    with pytest.raises(PySparkValueError) as excinfo:
        derived.declareSorted("sym")
    assert "declareSorted applies to source frames" in str(excinfo.value)


def test_no_keys_refuses(spark: ReparkSession) -> None:
    """``declareSorted()`` with no keys is a caller bug, not an empty declaration."""
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    with pytest.raises(PySparkValueError) as excinfo:
        frame.declareSorted()
    assert "at least one column" in str(excinfo.value)


def test_unknown_column_refuses_and_lists_the_available_names(spark: ReparkSession) -> None:
    """Names resolve through the select bind machinery, so misses name the alternatives."""
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    with pytest.raises(AnalysisException) as excinfo:
        frame.declareSorted("sym", "timestamp")
    message = str(excinfo.value)
    assert "`timestamp`" in message
    assert "'sym', 'ts', 'val'" in message, message


def test_display_resolution_is_case_insensitive(spark: ReparkSession) -> None:
    """A capitalized column declares under any spelling (same rules as ``select``)."""
    schema = StructType(
        [
            StructField("Sym", StringType()),
            StructField("TS", LongType()),
            StructField("val", DoubleType()),
        ]
    )
    frame = spark.createDataFrame(SORTED_ROWS, schema)

    # lowercase spelling of capitalized fields resolves to the engine field names
    assert frame.declareSorted("sym", "ts") is frame
    assert frame.count() == len(SORTED_ROWS)
    frame.createOrReplaceTempView("mixed_case_src")
    plan = _physical_plan(spark, "SELECT * FROM mixed_case_src")
    assert "output_ordering=Sym@0 ASC NULLS LAST, TS@1 ASC NULLS LAST" in plan, plan


def test_snake_and_camel_spellings_are_the_same_function() -> None:
    """``declare_sorted`` / ``declareSorted`` are one door, not two implementations."""
    from repark.spark.dataframe import DataFrame

    assert DataFrame.declareSorted is DataFrame.declare_sorted


def test_declaring_twice_is_idempotent(spark: ReparkSession) -> None:
    """Re-declaring the same keys re-verifies and lands on the same plan (chainable)."""
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    assert frame.declare_sorted("sym", "ts") is frame
    first = frame.to_arrow()
    assert frame.declareSorted("sym", "ts") is frame
    assert frame.to_arrow().equals(first)

    frame.createOrReplaceTempView("twice_src")
    assert "SortExec" not in _physical_plan(spark, WINDOW_SQL.format(view="twice_src"))


def test_cached_source_frame_refuses_and_keeps_its_cache(spark: ReparkSession) -> None:
    """cache()/persist() redirect the frame to a cache view; declaring afterwards would
    detach it while ``is_cached`` kept reporting true. The door refuses loud and the cache
    stays intact."""
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    frame.persist()
    assert frame.count() == len(SORTED_ROWS)
    assert frame.is_cached
    with pytest.raises(Exception, match="before cache"):
        frame.declare_sorted("sym", "ts")
    # The cache pin survived the refusal — same handle, same cache view, right answers.
    assert frame.is_cached
    assert frame.count() == len(SORTED_ROWS)
    frame.unpersist()


def test_declare_then_cache_is_the_sanctioned_order(spark: ReparkSession) -> None:
    """Declare first, cache afterwards — both effects hold on the same handle."""
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    assert frame.declare_sorted("sym", "ts") is frame
    frame.persist()
    assert frame.count() == len(SORTED_ROWS)
    assert frame.is_cached
    frame.unpersist()
