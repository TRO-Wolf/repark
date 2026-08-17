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
    """Value-identical to hint; tightened keys are non-nullable on the Arrow path."""
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
