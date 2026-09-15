"""Pins for IO-JDBC-FORMAT-1: ``format("jdbc")`` skips the ``read.jdbc`` URL dispatch."""

from __future__ import annotations

import pytest

from repark import SparkSession
from repark.errors import IllegalArgumentException


@pytest.fixture
def spark() -> SparkSession:
    """Fresh session per test."""
    return SparkSession.builder.master("local[1]").appName("io-jdbc-format-1").getOrCreate()


@pytest.mark.parametrize(
    "url",
    [
        "jdbc:mysql://localhost/db",
        "jdbc:sqlserver://localhost;databaseName=db",
        "jdbc:postgres://h/db",
    ],
)
def test_format_jdbc_non_postgres_url_reaches_the_postgres_option_checks(
    spark: SparkSession, url: str
) -> None:
    """Today's answer: the PostgreSQL option validation runs for any URL, no NOT_IMPLEMENTED."""
    with pytest.raises(IllegalArgumentException, match="dbtable"):
        spark.read.format("jdbc").option("url", url).load()
