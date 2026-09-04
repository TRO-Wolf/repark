"""Divergence pins for the EX-21 catalog/session batch (registry §7 EX-SES-1, EX-SES-2)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-ex21-catalog-session").getOrCreate()
    yield session
    session.stop()


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
