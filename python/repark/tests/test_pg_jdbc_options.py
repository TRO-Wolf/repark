"""Offline pure-logic pins for spark.read.jdbc / format('postgres') option surface (PG2).

No network, no REPARK_PG_DSN, no password guessing. These exercise the shipped facade
entry points for mutual exclusion, caps, and format alias teaching errors.
"""

from __future__ import annotations

import pytest

from repark import SparkSession
from repark.errors import IllegalArgumentException


@pytest.fixture
def spark() -> SparkSession:
    """Fresh session per test -- avoids stopped-session bleed after engine errors."""
    return SparkSession.builder.master("local[1]").appName("pg2-options").getOrCreate()


def test_format_postgres_requires_url(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="url"):
        spark.read.format("postgres").option("dbtable", "public.t").load()


def test_format_jdbc_is_postgres_alias_requires_dbtable_or_query(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="dbtable"):
        spark.read.format("jdbc").option("url", "postgresql://localhost/db").load()


def test_dbtable_query_mutually_exclusive(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="mutually exclusive"):
        (
            spark.read.format("postgres")
            .option("url", "postgresql://localhost/db")
            .option("dbtable", "t")
            .option("query", "SELECT 1")
            .load()
        )


def test_partial_range_bag_fails_loud(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="together"):
        (
            spark.read.format("postgres")
            .option("url", "postgresql://localhost/db")
            .option("dbtable", "t")
            .option("partitionColumn", "id")
            .option("numPartitions", "4")
            .load()
        )


def test_jdbc_predicates_xor_range(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="cannot be combined"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            column="id",
            lower_bound=0,
            upper_bound=100,
            num_partitions=2,
            predicates=["id > 0"],
            properties={},
        )


def test_jdbc_empty_predicates_fails(spark: SparkSession) -> None:
    with pytest.raises(IllegalArgumentException, match="non-empty"):
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            predicates=[],
            properties={},
        )


def test_format_postgresql_alias_recognized(spark: SparkSession) -> None:
    # Alias accepted; fails on missing dbtable, not on unknown format.
    with pytest.raises(IllegalArgumentException, match="dbtable"):
        spark.read.format("postgresql").option("url", "postgresql://localhost/db").load()


def test_format_postgres_bad_partition_int_is_illegal_argument(spark: SparkSession) -> None:
    """Bare int() must not leak ValueError: typed IllegalArgumentException."""
    with pytest.raises(IllegalArgumentException, match="lowerBound"):
        (
            spark.read.format("postgres")
            .option("url", "postgresql://localhost/db")
            .option("dbtable", "t")
            .option("partitionColumn", "id")
            .option("lowerBound", "not-an-int")
            .option("upperBound", "100")
            .option("numPartitions", "2")
            .load()
        )


def test_jdbc_dbtable_from_properties_is_forwarded(spark: SparkSession) -> None:
    """Shipped path: table=None + properties['dbtable'] must reach read_postgres (not None)."""
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.jdbc(
            "postgresql://localhost/db",
            table=None,
            properties={"dbtable": "public.orders", "user": "u"},
        )
    assert captured.get("dbtable") == "public.orders"
    assert captured.get("url") == "postgresql://localhost/db"
