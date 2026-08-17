"""SE-1 PR-D1 facade pins for ``declareSorted(..., tightenNulls=True)``.

The existing 13 nodes in ``test_declare_sorted.py`` stay byte-identical (hint mode).
This file pins the c+ flag: value AND type, refuse-on-nulls, and hint-after-tighten
restore. Serving-shape SortExec elision is the Rust Spark-door execution-layer pin
(``crates/repark-spark/tests/declared_sorted_tighten.rs``) — facade EXPLAIN is still
the unwritten plan until PR-D3.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import AnalysisException
from repark.spark.dataframe import DataFrame
from repark.spark.types import DoubleType, LongType, StringType, StructField, StructType
from repark.spark.window import Window

SCHEMA = StructType(
    [
        StructField("sym", StringType(), True),
        StructField("ts", LongType(), True),
        StructField("val", DoubleType(), True),
    ]
)

SORTED_ROWS = [(sym, tick, float(tick)) for sym in ("AAA", "BBB", "CCC") for tick in range(50)]


@pytest.fixture
def spark() -> ReparkSession:
    return (
        ReparkSession.builder.appName("pytest-declare-sorted-tighten")
        .config("repark.target.partitions", 1)
        .getOrCreate()
    )


def test_tighten_results_match_hint_and_keys_report_non_nullable(spark: ReparkSession) -> None:
    """Value-identical to hint; tightened keys are non-nullable on the Arrow path.

    Also pins exact ``df.schema`` types (R-3 type-exactness) and that the internal
    ``repark.tighten_nulls`` tag is stripped from user-visible ``to_arrow()``.
    """
    hint = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts")
    tight = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts", tightenNulls=True)
    window = Window.partitionBy("sym").orderBy("ts")
    hint_arrow = hint.withColumn("rn", F.row_number().over(window)).to_arrow()
    tight_arrow = tight.withColumn("rn", F.row_number().over(window)).to_arrow()
    # Compare values only: tighten changes key nullability (the lever).
    assert hint_arrow.column("rn").equals(tight_arrow.column("rn"))
    assert hint_arrow.column("sym").equals(tight_arrow.column("sym"))
    assert hint_arrow.column("ts").equals(tight_arrow.column("ts"))
    assert tight_arrow.schema.field("sym").nullable is False
    assert tight_arrow.schema.field("ts").nullable is False
    assert hint_arrow.schema.field("sym").nullable is True
    assert hint_arrow.schema.field("ts").nullable is True
    # F4: df.schema is the analyzed logical path, distinct from to_arrow().
    assert tight.schema["sym"].nullable is False
    assert tight.schema["ts"].nullable is False
    assert hint.schema["sym"].nullable is True
    assert hint.schema["ts"].nullable is True
    assert isinstance(tight.schema["sym"].dataType, StringType)
    assert isinstance(tight.schema["ts"].dataType, LongType)
    assert isinstance(tight.schema["val"].dataType, DoubleType)
    assert tight.schema["val"].nullable is True
    assert tight_arrow.schema.field("val").nullable is True
    ts_meta = tight_arrow.schema.field("ts").metadata or {}
    assert b"repark.tighten_nulls" not in ts_meta
    assert "repark.tighten_nulls" not in ts_meta
    table_meta = tight_arrow.schema.metadata or {}
    assert b"repark.tighten_nulls" not in table_meta
    assert "repark.tighten_nulls" not in table_meta


def test_tighten_refuses_nulls_in_a_declared_key(spark: ReparkSession) -> None:
    rows = [("AAA", 1, 1.0), ("AAA", None, 2.0)]
    frame = spark.createDataFrame(rows, SCHEMA)
    with pytest.raises(AnalysisException, match="tightenNulls") as excinfo:
        frame.declareSorted("sym", "ts", tightenNulls=True)
    assert "'ts'" in str(excinfo.value) or "ts" in str(excinfo.value)
    # View still answers and is still nullable.
    assert frame.count() == 2
    assert frame.to_arrow().schema.field("ts").nullable is True


def test_hint_after_tighten_restores_nullability(spark: ReparkSession) -> None:
    frame = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    frame.declareSorted("sym", "ts", tightenNulls=True)
    assert frame.to_arrow().schema.field("ts").nullable is False
    assert frame.schema["ts"].nullable is False
    frame.declareSorted("sym", "ts")
    assert frame.to_arrow().schema.field("ts").nullable is True
    assert frame.schema["ts"].nullable is True
    frame.declareSorted("sym", "ts", tightenNulls=True)
    assert frame.to_arrow().schema.field("ts").nullable is False
    assert frame.schema["ts"].nullable is False


def test_tighten_nulls_keyword_is_shared_by_both_spellings() -> None:
    assert DataFrame.declareSorted is DataFrame.declare_sorted


@pytest.fixture
def spark_catalog(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-declare-sorted-tighten-write").getOrCreate()
    session.register_memory_catalog("glue_catalog", tmp_path)
    session.sql("CREATE NAMESPACE glue_catalog.writer_ns")
    return session


def test_save_as_table_create_refuses_tightened_and_derived(
    spark_catalog: ReparkSession,
) -> None:
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    with pytest.raises(AnalysisException, match="tightenNulls"):
        tight.write.saveAsTable("glue_catalog.writer_ns.tight_src")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        derived.write.saveAsTable("glue_catalog.writer_ns.tight_derived")


def test_write_to_create_refuses_tightened_and_derived(spark_catalog: ReparkSession) -> None:
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    with pytest.raises(AnalysisException, match="tightenNulls"):
        tight.writeTo("glue_catalog.writer_ns.wt_src").create()
    with pytest.raises(AnalysisException, match="tightenNulls"):
        derived.writeTo("glue_catalog.writer_ns.wt_derived").create()


def test_facade_layer_refuses_when_engine_source_walk_is_silent(
    spark_catalog: ReparkSession,
) -> None:
    """Kills: deleting ``_refuse_tightened_iceberg_create`` from saveAsTable / writeTo.

    Engine plan-source walk is silent here (no tagged scan). Only the facade marker
    refuses. A delete-the-facade-layer mutant lets CREATE succeed.
    """
    loose = spark_catalog.createDataFrame([(1,)], "x INT")
    marked = loose.select(F.lit(1).alias("one"))
    marked._tighten_derived = True
    assert marked.schema["one"].nullable is False
    with pytest.raises(AnalysisException, match="tightenNulls"):
        marked.write.saveAsTable("glue_catalog.writer_ns.facade_only")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        marked.writeTo("glue_catalog.writer_ns.facade_only_wt").create()
    with pytest.raises(AnalysisException, match="tightenNulls"):
        marked.writeTo("glue_catalog.writer_ns.facade_only_cor").createOrReplace()


def test_right_side_combinators_propagate_tighten_marker(spark: ReparkSession) -> None:
    """Kills: ``_spawn`` copying ``_tighten_derived`` from self only (R-C)."""
    loose = spark.createDataFrame(SORTED_ROWS, SCHEMA)
    tight = spark.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted("sym", "ts", tightenNulls=True)
    assert loose.union(tight)._tighten_derived is True
    assert loose.unionByName(tight)._tighten_derived is True
    assert loose.intersect(tight)._tighten_derived is True
    assert loose.subtract(tight)._tighten_derived is True
    assert loose.crossJoin(tight.limit(1))._tighten_derived is True
    assert loose.join(tight, "sym")._tighten_derived is True

    def _identity(batches: object) -> object:
        yield from batches  # type: ignore[misc]

    mapped = tight.mapInArrow(_identity, SCHEMA)
    assert mapped._tighten_derived is True
    joined = loose.pl.join(tight.pl, on="sym").spark
    assert joined._tighten_derived is True


def test_cache_of_derived_still_refuses_iceberg_create(spark_catalog: ReparkSession) -> None:
    """Kills: cache/persist remint dropping tighten provenance (R-A)."""
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    cached = derived.cache()
    cached.count()
    with pytest.raises(AnalysisException, match="tightenNulls"):
        cached.write.saveAsTable("glue_catalog.writer_ns.cached_derived")
    cached.createOrReplaceTempView("cached_derived")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        spark_catalog.sql(
            "CREATE TABLE glue_catalog.writer_ns.cached_sql AS SELECT * FROM cached_derived"
        )


def test_all_nullable_projection_create_and_insert_are_allowed(
    spark_catalog: ReparkSession,
) -> None:
    """Kills: hoisting refuse onto all-nullable CREATE or onto INSERT/append (R-D)."""
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    only_val = tight.select("val")
    only_val.write.saveAsTable("glue_catalog.writer_ns.nullable_only")
    only_val.write.mode("append").saveAsTable("glue_catalog.writer_ns.nullable_only")


def test_literal_over_tightened_source_is_refused(spark_catalog: ReparkSession) -> None:
    """Kills: dropping the facade R-D half (marker ∧ non-null output).

    Engine source-walk is covered by the Rust/SQL-door conservative pins, not this node.
    """
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    with pytest.raises(AnalysisException, match="tightenNulls"):
        tight.select(F.lit(1).alias("one")).write.saveAsTable(
            "glue_catalog.writer_ns.lit_over_tight"
        )


def test_sql_derived_write_and_lazy_view_create_refuse(
    spark_catalog: ReparkSession,
) -> None:
    """Kills: engine walk that does not enter into_view / lazy temp-view plans (Q-001)."""
    tight = spark_catalog.createDataFrame(SORTED_ROWS, SCHEMA).declareSorted(
        "sym", "ts", tightenNulls=True
    )
    tight.createOrReplaceTempView("tight_src")
    derived_sql = spark_catalog.sql("SELECT ts + 1 AS ts2 FROM tight_src")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        derived_sql.write.saveAsTable("glue_catalog.writer_ns.sql_derived_write")
    derived = tight.select((F.col("ts") + 1).alias("ts2"))
    derived.createOrReplaceTempView("d")
    with pytest.raises(AnalysisException, match="tightenNulls"):
        spark_catalog.sql("CREATE TABLE glue_catalog.writer_ns.view_hop AS SELECT * FROM d")


def test_declare_sorted_docstring_examples_execute() -> None:
    """Kills: fabricated ``>>>`` examples that raise when run as doctest."""
    import doctest

    from repark.spark.dataframe.core import DataFrame as FacadeDataFrame

    finder = doctest.DocTestFinder()
    runner = doctest.DocTestRunner(optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE)
    failed = 0
    for test in finder.find(FacadeDataFrame.declare_sorted, name="declare_sorted"):
        failed += runner.run(test).failed
    assert failed == 0
