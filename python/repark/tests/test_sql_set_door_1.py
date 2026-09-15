"""SQL ``SET`` / ``RESET`` door pins — SQL-SET-DOOR-1 (oracle ``fixtures-batch1.json`` BTZ5-0..19).

Every recognised shape runs ``spark.conf.set``/``get``/``unset`` underneath, so the SQL door
inherits the facade's recorded contract: a runtime set of ``spark.sql.session.timeZone`` or
``spark.sql.ansi.enabled`` is accepted and stored (timezone: accepted but not stored) yet not
applied to the live engine — the pinned divergences below point at registry rows TZ-3 and
SET-ANSI-RUNTIME-1 rather than at Spark's applied behaviour. Result frames are asserted on the
``to_arrow`` path (value AND Arrow type AND field nullability). Error classes and message
needles mirror live PySpark 4.1.2 minus the recorded no-SQLSTATE delta.
"""

from __future__ import annotations

import warnings

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    PySparkException,
    UnsupportedOperationException,
)

SESSION_TIME_ZONE_KEY = "spark.sql.session.timeZone"


def _session() -> ReparkSession:
    """Build a plain session for one SET/RESET scenario."""
    return ReparkSession.builder.appName("sql-set-door-1").getOrCreate()


def _kv_frame_assertion(
    frame_rows: list[dict[str, object]],
    frame_schema: pa.Schema,
    expected_rows: list[tuple[str, str]],
) -> None:
    """Assert a ``(key, value)`` result frame: two non-null string columns, exact rows."""
    assert frame_schema.names == ["key", "value"]
    assert frame_schema.field("key").type == pa.string()
    assert frame_schema.field("value").type == pa.string()
    assert frame_schema.field("key").nullable is False
    assert frame_schema.field("value").nullable is False
    assert frame_rows == [{"key": key, "value": value} for key, value in expected_rows]


def _arrow(frame: object) -> tuple[pa.Schema, list[dict[str, object]]]:
    """Materialize a result frame on the Arrow path (schema + rows)."""
    table = frame.to_arrow()  # type: ignore[attr-defined]
    return table.schema, table.to_pylist()


def _set_and_get(spark: ReparkSession, statement: str) -> tuple[pa.Schema, list[dict[str, object]]]:
    """Run one SET/RESET statement, tolerating the once-per-process zone warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return _arrow(spark.sql(statement))


def test_set_key_value_returns_the_pair_frame() -> None:
    """``SET k = v`` answers one ``(key, value)`` row — BTZ5-0."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions = 2")
    _kv_frame_assertion(rows, schema, [("spark.sql.shuffle.partitions", "2")])
    spark.stop()


def test_set_key_read_reports_the_stored_value() -> None:
    """``SET k`` answers the stored value — BTZ5-1 / BTZ5-12."""
    spark = _session()
    _set_and_get(spark, "SET spark.sql.shuffle.partitions = 2")
    _set_and_get(spark, "SET spark.some.unknown.key = 7")
    schema, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    _kv_frame_assertion(rows, schema, [("spark.sql.shuffle.partitions", "2")])
    schema, rows = _set_and_get(spark, "SET spark.some.unknown.key")
    _kv_frame_assertion(rows, schema, [("spark.some.unknown.key", "7")])
    spark.stop()


def test_set_key_read_of_a_never_set_key_answers_undefined() -> None:
    """``SET k`` on a never-set key answers ``<undefined>`` (D-2 fallback)."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.never.touched")
    _kv_frame_assertion(rows, schema, [("spark.never.touched", "<undefined>")])
    spark.stop()


def test_set_session_time_zone_is_accepted_but_not_applied() -> None:
    """``SET spark.sql.session.timeZone = <zone>`` stores nothing — residue TZ-3.

    Spark applies the zone and echoes ``America/New_York`` (BTZ5-2/3); repark's runtime
    ``conf.set`` of the build-time zone knob is accepted without effect, so the honest
    answer is the zone the live engine session actually has — ``UTC``.
    """
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.sql.session.timeZone = America/New_York")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    assert spark.conf.get(SESSION_TIME_ZONE_KEY) == "UTC"
    spark.stop()


def test_current_timezone_answers_the_session_zone() -> None:
    """``current_timezone()`` answers the zone the session was built with — D-5."""
    spark = _session()
    schema, rows = _arrow(spark.sql("SELECT current_timezone() tz"))
    assert schema.names == ["tz"]
    assert schema.field("tz").type == pa.string()
    assert schema.field("tz").nullable is False
    assert rows == [{"tz": "UTC"}]
    spark.stop()
    zoned = (
        ReparkSession.builder.appName("sql-set-door-1-zoned")
        .config(SESSION_TIME_ZONE_KEY, "America/New_York")
        .getOrCreate()
    )
    schema, rows = _arrow(zoned.sql("SELECT current_timezone() tz"))
    assert rows == [{"tz": "America/New_York"}]
    assert schema.field("tz").nullable is False
    zoned.stop()


def test_set_quoted_time_zone_value_refuses_with_spark_error() -> None:
    """``SET k = '<zone>'`` keeps the quotes and refuses — BTZ5-4 (quotes are not stripped)."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.session.timeZone = 'Asia/Tokyo'")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TIME_ZONE]" in message
    assert "''Asia/Tokyo''" in message
    assert 'config "spark.sql.session.timeZone"' in message
    assert "Cannot resolve the given timezone" in message
    spark.stop()


def test_set_unresolvable_time_zone_refuses_with_spark_error() -> None:
    """``SET spark.sql.session.timeZone = Invalid/Zone`` refuses — BTZ5-6."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.session.timeZone = Invalid/Zone")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TIME_ZONE]" in message
    assert "'Invalid/Zone'" in message
    assert "Cannot resolve the given timezone" in message
    spark.stop()


def test_set_ansi_enabled_stores_but_does_not_apply() -> None:
    """``SET spark.sql.ansi.enabled = false`` stores the flag — residue SET-ANSI-RUNTIME-1.

    Spark then answers ``1/0`` as NULL (BTZ5-8); repark's ANSI flag is installed at session
    build, so the stored value never reaches the engine and ``1/0`` still raises
    ``DIVIDE_BY_ZERO`` — pinned as the measured divergence.
    """
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.sql.ansi.enabled = false")
    _kv_frame_assertion(rows, schema, [("spark.sql.ansi.enabled", "false")])
    assert spark.conf.get("spark.sql.ansi.enabled") == "false"
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0 d").to_arrow()
    spark.stop()


def test_reset_key_returns_an_empty_frame() -> None:
    """``RESET k`` answers zero rows with no columns — BTZ5-9."""
    spark = _session()
    _set_and_get(spark, "SET spark.sql.ansi.enabled = false")
    schema, rows = _set_and_get(spark, "RESET spark.sql.ansi.enabled")
    assert len(schema) == 0
    assert rows == []
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0 d").to_arrow()
    spark.stop()


def test_reset_all_clears_the_runtime_keys() -> None:
    """``RESET`` clears every runtime-set key — BTZ5-18/19 shape."""
    spark = _session()
    _set_and_get(spark, "SET spark.sql.shuffle.partitions = 2")
    _set_and_get(spark, "SET repark.anything = 1")
    schema, rows = _set_and_get(spark, "RESET")
    assert len(schema) == 0
    assert rows == []
    _, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    assert rows == [{"key": "spark.sql.shuffle.partitions", "value": "<undefined>"}]
    spark.stop()


def test_set_static_sql_conf_refuses_with_spark_error() -> None:
    """``SET spark.sql.warehouse.dir = …`` refuses — BTZ5-13."""
    spark = _session()
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SET spark.sql.warehouse.dir = /tmp/x")
    message = str(caught.value)
    assert "[CANNOT_MODIFY_STATIC_CONFIG]" in message
    assert '"spark.sql.warehouse.dir"' in message
    spark.stop()


def test_set_repark_namespaced_key_round_trips() -> None:
    """``SET repark.anything = 1`` stores like any runtime key — BTZ5-14."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET repark.anything = 1")
    _kv_frame_assertion(rows, schema, [("repark.anything", "1")])
    spark.stop()


def test_set_time_zone_literal_returns_the_pair() -> None:
    """``SET TIME ZONE '<zone>'`` sets ``spark.sql.session.timeZone`` — BTZ5-15/16."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET TIME ZONE 'UTC'")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    schema, rows = _arrow(spark.sql("SELECT current_timezone() tz"))
    assert rows == [{"tz": "UTC"}]
    spark.stop()


def test_set_time_zone_literal_with_an_unresolvable_zone_refuses() -> None:
    """``SET TIME ZONE '<bad>'`` refuses with Spark's timezone error class."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET TIME ZONE 'Mars/Olympus_Mons'")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TIME_ZONE]" in message
    assert "'Mars/Olympus_Mons'" in message
    spark.stop()


def test_set_time_zone_local_is_a_declared_refusal() -> None:
    """``SET TIME ZONE LOCAL`` refuses — repark never reads the host zone (registry)."""
    spark = _session()
    with pytest.raises(UnsupportedOperationException, match="SET TIME ZONE LOCAL"):
        spark.sql("SET TIME ZONE LOCAL")
    spark.stop()


def test_set_shuffle_partitions_non_integer_refuses_with_spark_error() -> None:
    """``SET spark.sql.shuffle.partitions = abc`` refuses — BTZ5-17."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.shuffle.partitions = abc")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TYPE_MISMATCH]" in message
    assert "'abc'" in message
    assert '"spark.sql.shuffle.partitions"' in message
    assert "'int'" in message
    spark.stop()


def test_set_bare_lists_the_explicitly_set_keys_sorted() -> None:
    """Bare ``SET`` lists one row per runtime-set key, sorted — D-2."""
    spark = _session()
    _set_and_get(spark, "SET repark.anything = 1")
    _set_and_get(spark, "SET spark.sql.shuffle.partitions = 2")
    schema, rows = _set_and_get(spark, "SET")
    _kv_frame_assertion(
        rows,
        schema,
        [("repark.anything", "1"), ("spark.sql.shuffle.partitions", "2")],
    )
    spark.stop()


def test_set_verbose_lists_the_same_keys_with_empty_metadata() -> None:
    """``SET -v`` answers ``key, value, meaning, Since version`` — BTZ5-19 shape."""
    spark = _session()
    _set_and_get(spark, "SET repark.anything = 1")
    schema, rows = _set_and_get(spark, "SET -v")
    assert schema.names == ["key", "value", "meaning", "Since version"]
    for name in schema.names:
        assert schema.field(name).type == pa.string()
        assert schema.field(name).nullable is False
    assert rows == [{"key": "repark.anything", "value": "1", "meaning": "", "Since version": ""}]
    _set_and_get(spark, "RESET")
    _, rows = _set_and_get(spark, "SET -v")
    assert rows == []
    spark.stop()


def test_datafusion_key_set_passes_through_to_the_engine() -> None:
    """``SET datafusion.*`` stays on the engine path — the ``conf.set`` forwarder cannot loop."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET datafusion.execution.batch_size = '64'")
    assert len(schema) == 0
    assert rows == []
    spark.conf.set("datafusion.execution.batch_size", "128")
    assert spark.conf.get("datafusion.execution.batch_size") == "128"
    spark.stop()


def test_unrecognised_statement_goes_to_the_engine_unchanged() -> None:
    """A statement outside the D-1 shapes reaches the engine — same refusal as before."""
    spark = _session()
    with pytest.raises(PySparkException):
        spark.sql("SET spark.sql.shuffle.partitions TO 2").to_arrow()
    spark.stop()
