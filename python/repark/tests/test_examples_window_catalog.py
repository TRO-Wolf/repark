"""Divergence pins for the EX-20 window/catalog, EX-21 session and EX-22 types/WriterV2 batches.

Registry §7 rows EX-WIN-1, EX-CAT-1..3 (EX-20), EX-SES-1..5 (EX-21) and EX-W2-1..3 (EX-22).
"""

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
    """functionExists with dbName answers True where Spark scopes the check (EX-CAT-3)."""
    spark.udf.register("ex20_fn", lambda value: value)
    assert spark.catalog.functionExists("ex20_fn", "default") is True
    assert spark.catalog.functionExists("ex20_fn") is True


def test_register_function_returns_udf_object(spark: ReparkSession) -> None:
    """registerFunction answers the UDF object where Spark's alias returns f (EX-SES-1)."""
    registered = spark.catalog.registerFunction("ex21_pin_fn", lambda value: f"u{value}")
    assert isinstance(registered, F.UserDefinedFunction)


def test_new_session_action_promotes_active(spark: ReparkSession) -> None:
    """A newSession() action promotes it active where Spark keeps the caller (EX-SES-2)."""
    spare = spark.newSession()
    spare.sql("SELECT 1 AS one").collect()
    assert ReparkSession.getActiveSession() is spare
    spare.stop()
    assert ReparkSession.getActiveSession() is None


def test_create_dataframe_empty_name_list_answers_empty(spark: ReparkSession) -> None:
    """create_dataframe([], ['a']) answers [] vs Spark CANNOT_INFER_EMPTY_SCHEMA (EX-SES-3)."""
    frame = spark.create_dataframe([], ["a"])
    assert frame.collect() == []
    assert frame.dtypes == [("a", "string")]


def test_conf_get_unset_key_raises_bare_exception(spark: ReparkSession) -> None:
    """conf.get on an unset key raises bare Exception vs Spark SQL_CONF_NOT_FOUND (EX-SES-4)."""
    with pytest.raises(Exception, match=r"Configuration property .* is not set\.") as info:
        spark.conf.get("ex21.unset.key")
    assert type(info.value) is Exception


def test_missing_file_readers_analysis_exception(spark: ReparkSession, tmp_path: Path) -> None:
    """A missing file raises AnalysisException vs Spark PATH_NOT_FOUND (EX-SES-5)."""
    for reader, name in (
        ("read_csv", "ex21_nope.csv"),
        ("read_json", "ex21_nope.json"),
        ("read_parquet", "ex21_nope.parquet"),
    ):
        with pytest.raises(AnalysisException, match="No files found"):
            getattr(spark, reader)(str(tmp_path / name)).collect()


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
