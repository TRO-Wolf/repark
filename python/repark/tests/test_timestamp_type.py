"""Q10 — ``spark.sql.timestampType`` LTZ default + NTZ opt-in.

Default mode is pinned by the existing tz / cast suites (zero edits there).
This file pins: conf get/set round-trip, invalid-value refusal naming both
legal tokens, and NTZ opt-in per entry point (Spark SQL, DataFrame
``selectExpr``, createDataFrame inference) on the Arrow path — value AND type.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.spark.session import _reset_active_session_for_tests
from repark.spark.session.timestamp_type import (
    TIMESTAMP_LTZ_VALUE,
    TIMESTAMP_NTZ_VALUE,
    TIMESTAMP_TYPE_KEY,
)


@pytest.fixture(autouse=True)
def _isolated_session() -> Iterator[None]:
    _reset_active_session_for_tests()
    yield
    _reset_active_session_for_tests()


def _session(*pairs: tuple[str, str]) -> ReparkSession:
    builder = ReparkSession.builder.appName("q10-timestamp-type")
    for key, value in pairs:
        builder = builder.config(key, value)
    return builder.getOrCreate()


def test_conf_default_is_timestamp_ltz() -> None:
    spark = _session()
    assert spark.conf.get(TIMESTAMP_TYPE_KEY) == TIMESTAMP_LTZ_VALUE


def test_conf_round_trip_get_set() -> None:
    spark = _session()
    spark.conf.set(TIMESTAMP_TYPE_KEY, TIMESTAMP_NTZ_VALUE)
    assert spark.conf.get(TIMESTAMP_TYPE_KEY) == TIMESTAMP_NTZ_VALUE
    spark.conf.set(TIMESTAMP_TYPE_KEY, TIMESTAMP_LTZ_VALUE)
    assert spark.conf.get(TIMESTAMP_TYPE_KEY) == TIMESTAMP_LTZ_VALUE


def test_invalid_conf_set_names_both_legal_values() -> None:
    spark = _session()
    with pytest.raises(IllegalArgumentException, match="TIMESTAMP_LTZ") as raised:
        spark.conf.set(TIMESTAMP_TYPE_KEY, "TIMESTAMP")
    assert "TIMESTAMP_NTZ" in str(raised.value)
    assert TIMESTAMP_TYPE_KEY in str(raised.value)


def test_invalid_builder_value_fails_loud() -> None:
    with pytest.raises(Exception, match="TIMESTAMP_LTZ") as raised:
        _session((TIMESTAMP_TYPE_KEY, "not-a-type"))
    message = str(raised.value)
    assert "TIMESTAMP_NTZ" in message
    assert TIMESTAMP_TYPE_KEY in message


def test_default_sql_timestamp_literal_is_ltz() -> None:
    spark = _session()
    table = spark.sql("SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts").to_arrow()
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert table.column("ts")[0].as_py() == datetime.fromisoformat("2024-06-15T12:00:00+00:00")


def test_ntz_opt_in_sql_literal_and_cast() -> None:
    spark = _session((TIMESTAMP_TYPE_KEY, TIMESTAMP_NTZ_VALUE))
    assert spark.conf.get(TIMESTAMP_TYPE_KEY) == TIMESTAMP_NTZ_VALUE
    wall = datetime(2024, 6, 15, 12, 0, 0)
    for sql in (
        "SELECT TIMESTAMP '2024-06-15 12:00:00' AS ts",
        "SELECT CAST('2024-06-15 12:00:00' AS TIMESTAMP) AS ts",
    ):
        table = spark.sql(sql).to_arrow()
        assert table.schema.field("ts").type == pa.timestamp("us"), sql
        assert table.column("ts")[0].as_py() == wall, sql


def test_ntz_opt_in_dataframe_select_expr() -> None:
    # ``F.expr`` analyzes on a bare SessionContext (no carrier) — that is the
    # binding-owned path, out of this fence. ``selectExpr`` is the DataFrame
    # entry that routes through ``session.sql`` and therefore the session knob.
    spark = _session((TIMESTAMP_TYPE_KEY, TIMESTAMP_NTZ_VALUE))
    table = spark.range(1).selectExpr("TIMESTAMP '2024-06-15 12:00:00' AS ts").to_arrow()
    assert table.schema.field("ts").type == pa.timestamp("us")
    assert table.column("ts")[0].as_py() == datetime(2024, 6, 15, 12, 0, 0)


def test_ntz_opt_in_createdataframe_inference() -> None:
    spark = _session((TIMESTAMP_TYPE_KEY, TIMESTAMP_NTZ_VALUE))
    wall = datetime(2024, 6, 15, 12, 0, 0)
    table = spark.createDataFrame([(wall,)], ["ts"]).to_arrow()
    assert table.schema.field("ts").type == pa.timestamp("us")
    assert table.column("ts")[0].as_py() == wall


def test_ntz_opt_in_leaves_to_timestamp_as_ltz() -> None:
    spark = _session((TIMESTAMP_TYPE_KEY, TIMESTAMP_NTZ_VALUE))
    table = spark.sql("SELECT to_timestamp('2024-06-15 12:00:00') AS ts").to_arrow()
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
