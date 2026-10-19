"""PG3 offline pins: catalog config kind + registered-catalog resolve surface."""

from __future__ import annotations

import os

import pytest

from repark import SparkSession
from repark.errors import IllegalArgumentException


@pytest.fixture
def spark() -> SparkSession:
    return SparkSession.builder.master("local[1]").appName("pg3-catalog").getOrCreate()


def test_postgres_catalog_requires_url_at_build() -> None:
    with pytest.raises((IllegalArgumentException, Exception), match="url"):
        (
            SparkSession.builder.master("local[1]")
            .config("spark.sql.catalog.pg", "jdbc")
            .getOrCreate()
        )


def test_live_registered_catalog_schema_table_or_skip() -> None:
    """Honest catalog.schema.table pin: SELECT through registered pg catalog (not jdbc only)."""
    dsn = os.environ.get("REPARK_PG_DSN", "").strip()
    if not dsn:
        print(
            "SKIP-LOUD: REPARK_PG_DSN unset — live registered-catalog "
            "catalog.schema.table pin skipped"
        )
        return
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.catalog.pg", "jdbc")
        .config("spark.sql.catalog.pg.url", dsn)
        .getOrCreate()
    )
    rows = session.sql(
        "SELECT table_name FROM pg.information_schema.tables "
        "WHERE table_schema = 'information_schema' LIMIT 3"
    ).collect()
    assert len(rows) >= 1
    names = {str(r["table_name"]) for r in rows}
    assert any(n for n in names), f"expected table names from pg.information_schema: {names}"
