"""Divergence pins for the EX-20 window/catalog batch (registry §7 EX-WIN-1, EX-CAT-1..3)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
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
