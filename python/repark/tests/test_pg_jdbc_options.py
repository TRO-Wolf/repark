"""Offline pure-logic pins for spark.read.jdbc / format('postgres') option surface (PG2).

No network, no REPARK_PG_DSN, no password guessing. These exercise the shipped facade
entry points for mutual exclusion, caps, and format alias teaching errors.

IO-JDBC-1 (R-2, 2026-09-14): the ``jdbc`` reader/writer names are the declared
NOT_IMPLEMENTED refusal until the 1.6 native connectors; the reads alternative is the
session's ``read_postgres`` / ``format('postgres')`` connector, which these pins keep.
"""

from __future__ import annotations

import pytest

from repark import SparkSession
from repark.errors import IllegalArgumentException, PySparkNotImplementedError


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


def test_jdbc_read_refuses_not_implemented(spark: SparkSession) -> None:
    """spark.read.jdbc refuses NOT_IMPLEMENTED jdbc at the call (IO-JDBC-1).

    The old mutual-exclusion and range-bag teaching errors left with the postgres
    connector path; the declared refusal fires first, exactly like Spark's own driver
    failure fires before partition planning.
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.read.jdbc(
            "postgresql://localhost/db",
            "t",
            column="id",
            lowerBound=0,
            upperBound=100,
            numPartitions=2,
            predicates=["id > 0"],
            properties={},
        )
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "jdbc"}


def test_jdbc_read_from_properties_refuses_not_implemented(spark: SparkSession) -> None:
    """table=None + properties['dbtable'] reaches the declared refusal, not the connector.

    pins: io-declared-1/C-003
    """
    with pytest.raises(PySparkNotImplementedError) as raised:
        spark.read.jdbc(
            "postgresql://localhost/db",
            table=None,
            properties={"dbtable": "public.orders", "user": "u"},
        )
    assert raised.value.getCondition() == "NOT_IMPLEMENTED"
    assert raised.value.getMessageParameters() == {"feature": "jdbc"}


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


def test_jdbc_dbtable_from_properties_is_forwarded_by_the_alternative(spark: SparkSession) -> None:
    """Shipped path: the session alternative forwards dbtable (IO-JDBC-1 naming).

    The jdbc reader name refuses (IO-JDBC-1); ``format('postgres')`` stays the wired
    connector entry, so the forwarding contract moves onto that spelling.
    """
    captured: dict[str, object] = {}

    class _FakeSession:
        def read_postgres(self, **kwargs: object) -> object:
            captured.update(kwargs)
            raise RuntimeError("stop-after-capture")

    reader = spark.read
    reader._session = _FakeSession()  # type: ignore[assignment]
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        reader.format("postgres").option("url", "postgresql://localhost/db").option(
            "dbtable", "public.orders"
        ).option("user", "u").load()
    assert captured.get("dbtable") == "public.orders"
    assert captured.get("url") == "postgresql://localhost/db"
