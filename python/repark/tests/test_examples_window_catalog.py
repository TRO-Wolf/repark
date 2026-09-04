"""Divergence pins for the EX-20 window/catalog and EX-22 types/WriterV2 batches (§7 EX-WIN-1, EX-CAT-1..3, EX-W2-1..3)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import Window
from repark.spark import functions as F  # noqa: N812
from repark.spark.catalog import Database

TIED_ROWS = [
    ("a", 1, 10.0),
    ("a", 2, 20.0),
    ("a", 3, 30.0),
    ("b", 1, 50.0),
    ("b", 2, 60.0),
    ("b", 3, 70.0),
]


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex20-window-catalog").getOrCreate()
    yield session
    session.stop()


def test_window_default_frame_tie_peers(spark: ReparkSession) -> None:
    """sum over Window.orderBy with tied keys runs per-row; Spark shares peer sums (EX-WIN-1)."""
    frame = spark.createDataFrame(TIED_ROWS, ["g", "k", "v"])
    spec = Window.orderBy("k")
    sums = {
        (row[0], row[1]): row[3] for row in frame.withColumn("cs", F.sum("v").over(spec)).collect()
    }
    assert sums == {
        ("a", 1): 10.0,
        ("a", 2): 80.0,
        ("a", 3): 170.0,
        ("b", 1): 60.0,
        ("b", 2): 140.0,
        ("b", 3): 240.0,
    }


def test_get_database_default_fields(spark: ReparkSession) -> None:
    """getDatabase('default') answers None description/locationUri; Spark fills both (EX-CAT-1)."""
    database = spark.catalog.getDatabase("default")
    assert database == Database(
        name="default", catalog="spark_catalog", description=None, locationUri=None
    )


def test_list_databases_fields_none(spark: ReparkSession) -> None:
    """listDatabases rows carry None fields where Spark fills both (EX-CAT-2, FA-2)."""
    assert [tuple(row) for row in spark.catalog.listDatabases()] == [
        ("default", "spark_catalog", None, None)
    ]


def test_function_exists_db_name_arm(spark: ReparkSession) -> None:
    """functionExists with dbName answers True where Spark scopes it False (EX-CAT-3)."""
    spark.udf.register("ex20_fn", lambda value: value)
    assert spark.catalog.functionExists("ex20_fn", "default") is True
    assert spark.catalog.functionExists("ex20_fn") is True


@pytest.fixture
def spark_v2(tmp_path: Path) -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex22-writerv2").getOrCreate()
    session.register_memory_catalog("local", tmp_path / "wh")
    session.sql("CREATE NAMESPACE local.ns")
    yield session
    session.stop()


def test_writerv2_overwrite_condition_refuses(spark_v2: ReparkSession) -> None:
    """overwrite(condition) raises; Spark overwrites the matching rows (EX-W2-1)."""
    session = spark_v2
    session.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id, name)").writeTo(
        "local.ns.t_pin_ow"
    ).create()
    with pytest.raises(UnsupportedOperationException, match=r"overwrite\(condition\)"):
        session.sql("SELECT * FROM (VALUES (1,'aa')) AS t(id, name)").writeTo(
            "local.ns.t_pin_ow"
        ).overwrite(F.col("id") == 1)


def test_writerv2_overwrite_partitions_empty_refuses(spark_v2: ReparkSession) -> None:
    """Empty-source overwritePartitions raises AnalysisException; Spark no-ops (EX-W2-2)."""
    session = spark_v2
    session.sql("SELECT * FROM (VALUES (1,'a'),(2,'b')) AS t(id, cat)").writeTo(
        "local.ns.t_pin_owp"
    ).partitionedBy("cat").create()
    session.sql("SELECT * FROM (VALUES (9,'a')) AS t(id, cat)").writeTo(
        "local.ns.t_pin_owp"
    ).overwritePartitions()
    with pytest.raises(AnalysisException, match="Cannot dynamically overwrite partitions"):
        session.sql("SELECT id, cat FROM local.ns.t_pin_owp WHERE false").writeTo(
            "local.ns.t_pin_owp"
        ).overwritePartitions()
    still = session.sql("SELECT id, cat FROM local.ns.t_pin_owp ORDER BY id").to_arrow().to_pylist()
    assert still == [{"id": 2, "cat": "b"}, {"id": 9, "cat": "a"}]


def test_writerv2_option_branch_refuses(spark_v2: ReparkSession) -> None:
    """option('branch', …) raises; Spark silently writes the default branch (EX-W2-3)."""
    session = spark_v2
    session.sql("SELECT * FROM (VALUES (1,'a')) AS t(id, name)").writeTo(
        "local.ns.t_pin_br"
    ).create()
    with pytest.raises(UnsupportedOperationException, match="branch"):
        session.sql("SELECT * FROM (VALUES (3,'c')) AS t(id, name)").writeTo(
            "local.ns.t_pin_br"
        ).option("branch", "b1").append()
