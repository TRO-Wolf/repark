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
    ParseException,
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
    """``SET spark.sql.session.timeZone`` echoes live UTC; Spark would apply NY — TZ-3."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.sql.session.timeZone = America/New_York")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    assert spark.conf.get(SESSION_TIME_ZONE_KEY) == "UTC"
    tz_schema, tz_rows = _arrow(spark.sql("SELECT current_timezone() tz"))
    assert tz_schema.field("tz").nullable is False
    assert tz_rows == [{"tz": "UTC"}]
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
    assert "SQLSTATE: 22022" in message
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
    assert "SQLSTATE: 22022" in message
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
    _, rows = _set_and_get(spark, "SET spark.sql.ansi.enabled")
    assert rows == [{"key": "spark.sql.ansi.enabled", "value": "<undefined>"}]
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


def test_reset_all_spelling_also_clears_the_runtime_keys() -> None:
    """``RESET ALL`` is Spark's other spelling of bare ``RESET``, not a key named ALL."""
    spark = _session()
    _set_and_get(spark, "SET spark.sql.shuffle.partitions = 2")
    schema, rows = _set_and_get(spark, "RESET ALL")
    assert len(schema) == 0
    assert rows == []
    _, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    assert rows == [{"key": "spark.sql.shuffle.partitions", "value": "<undefined>"}]
    spark.stop()


def test_multi_statement_defers_to_the_engine() -> None:
    """``SET x = 1; SET y = 2`` is not one D-1 shape — the engine keeps today's answer."""
    spark = _session()
    with pytest.raises(PySparkException):
        spark.sql("SET spark.a = 1; SET spark.b = 2").to_arrow()
    _, rows = _set_and_get(spark, "SET spark.a")
    assert rows == [{"key": "spark.a", "value": "<undefined>"}]
    spark.stop()


def test_comments_strip_like_the_spark_parser() -> None:
    """``--``/``/* … */`` outside string literals drop before matching, like Spark."""
    spark = _session()
    _, rows = _set_and_get(spark, "SET spark.a = 1 -- kept the value")
    assert rows == [{"key": "spark.a", "value": "1"}]
    _, rows = _set_and_get(spark, "SET spark.a = 2 /* block */")
    assert rows == [{"key": "spark.a", "value": "2"}]
    _, rows = _set_and_get(spark, "-- leading\nSET spark.a = 3")
    assert rows == [{"key": "spark.a", "value": "3"}]
    _, rows = _set_and_get(spark, "SET spark.b = 'semi;colon'")
    assert rows == [{"key": "spark.b", "value": "'semi;colon'"}]
    with pytest.raises(PySparkException):
        spark.sql("SET spark.c = 'unterminated").to_arrow()
    _, rows = _set_and_get(spark, "SET spark.c")
    assert rows == [{"key": "spark.c", "value": "<undefined>"}]
    spark.stop()


def test_set_listing_masks_secret_shaped_values() -> None:
    """SET k=v is raw; SET k / SET / SET -v redact key-or-value — S5-secret-* / S5-listing."""
    spark = _session()
    redacted = "*********(redacted)"
    schema, rows = _set_and_get(spark, "SET spark.my.api.password = hunter2")
    _kv_frame_assertion(rows, schema, [("spark.my.api.password", "hunter2")])
    schema, rows = _set_and_get(spark, "SET spark.plain2 = mypassword")
    _kv_frame_assertion(rows, schema, [("spark.plain2", "mypassword")])
    schema, rows = _set_and_get(spark, "SET spark.x.token = abc")
    _kv_frame_assertion(rows, schema, [("spark.x.token", "abc")])
    schema, rows = _set_and_get(spark, "SET spark.hadoop.fs.s3a.access.key = AKIAEXAMPLE")
    _kv_frame_assertion(rows, schema, [("spark.hadoop.fs.s3a.access.key", "AKIAEXAMPLE")])
    _set_and_get(spark, "SET spark.foo.my_key = visible-nonsecret")
    _set_and_get(spark, "SET repark.plain = visible")
    schema, rows = _set_and_get(spark, "SET spark.my.api.password")
    _kv_frame_assertion(rows, schema, [("spark.my.api.password", redacted)])
    schema, rows = _set_and_get(spark, "SET spark.plain2")
    _kv_frame_assertion(rows, schema, [("spark.plain2", redacted)])
    listed = {row["key"]: row["value"] for row in _set_and_get(spark, "SET")[1]}
    assert listed["spark.my.api.password"] == redacted
    assert listed["spark.plain2"] == redacted
    assert listed["spark.foo.my_key"] == redacted
    assert listed["spark.hadoop.fs.s3a.access.key"] == redacted
    assert listed["spark.x.token"] == redacted
    assert listed["repark.plain"] == "visible"
    verbose = {row["key"]: row["value"] for row in _set_and_get(spark, "SET -v")[1]}
    assert verbose["spark.hadoop.fs.s3a.access.key"] == redacted
    assert verbose["spark.foo.my_key"] == redacted
    spark.stop()


def test_set_static_sql_conf_refuses_with_spark_error() -> None:
    """``SET spark.sql.warehouse.dir = …`` refuses — BTZ5-13."""
    spark = _session()
    with pytest.raises(AnalysisException) as caught:
        spark.sql("SET spark.sql.warehouse.dir = /tmp/x")
    message = str(caught.value)
    assert "[CANNOT_MODIFY_STATIC_CONFIG]" in message
    assert '"spark.sql.warehouse.dir"' in message
    assert "SQLSTATE: 46110" in message
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
    assert "SQLSTATE: 22022" in message
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
    assert "SQLSTATE: 22022" in message
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
    """``SET k TO v`` raises ParseException and stores nothing — S5-set-to."""
    spark = _session()
    with pytest.raises(ParseException) as caught:
        spark.sql("SET spark.sql.shuffle.partitions TO 2")
    assert "[INVALID_SET_SYNTAX]" in str(caught.value)
    _, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    assert rows == [{"key": "spark.sql.shuffle.partitions", "value": "<undefined>"}]
    spark.stop()


def test_offset_zone_plus05_is_accepted() -> None:
    """``SET TIME ZONE '+05'`` accepts; echo is TZ-3 UTC not Spark's +05 — S5-tz-plus05."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET TIME ZONE '+05'")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    _, tz_rows = _arrow(spark.sql("SELECT current_timezone() tz"))
    assert tz_rows == [{"tz": "UTC"}]
    spark.stop()


def test_offset_zone_key_plus05_is_accepted() -> None:
    """``SET spark.sql.session.timeZone = +05`` accepts — S5-tz-key-plus05."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.sql.session.timeZone = +05")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    spark.stop()


def test_offset_zone_plus5_is_accepted() -> None:
    """``SET TIME ZONE '+5'`` accepts like ZoneId.of — S5-tz-plus5."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET TIME ZONE '+5'")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    spark.stop()


def test_offset_zone_plus1800_is_accepted() -> None:
    """``SET TIME ZONE '+18:00'`` is the ZoneId offset ceiling — S5-tz-plus1800."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET TIME ZONE '+18:00'")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    spark.stop()


def test_offset_zone_plus1801_refuses_with_time_zone_class() -> None:
    """``SET TIME ZONE '+18:01'`` refuses TIME_ZONE — S5-tz-plus1801."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET TIME ZONE '+18:01'")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TIME_ZONE]" in message
    assert "'+18:01'" in message
    assert "SQLSTATE: 22022" in message
    spark.stop()


def test_offset_zone_gmt_plus8_is_accepted() -> None:
    """``SET spark.sql.session.timeZone = GMT+8`` accepts — S5-tz-gmt8."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.sql.session.timeZone = GMT+8")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    spark.stop()


def test_uppercase_conf_keys_are_case_sensitive() -> None:
    """Uppercase SET keys store as written and do not shadow lowercase — S5-upper-*."""
    spark = (
        ReparkSession.builder.appName("sql-set-door-1-upper")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    schema, rows = _set_and_get(spark, "SET SPARK.SQL.WAREHOUSE.DIR = /tmp/x")
    _kv_frame_assertion(rows, schema, [("SPARK.SQL.WAREHOUSE.DIR", "/tmp/x")])
    schema, rows = _set_and_get(spark, "SET SPARK.SQL.SHUFFLE.PARTITIONS = abc")
    _kv_frame_assertion(rows, schema, [("SPARK.SQL.SHUFFLE.PARTITIONS", "abc")])
    schema, rows = _set_and_get(spark, "SET SPARK.SQL.SESSION.TIMEZONE = Invalid/Zone")
    _kv_frame_assertion(rows, schema, [("SPARK.SQL.SESSION.TIMEZONE", "Invalid/Zone")])
    schema, rows = _set_and_get(spark, "SET SPARK.SQL.SHUFFLE.PARTITIONS = 3")
    _kv_frame_assertion(rows, schema, [("SPARK.SQL.SHUFFLE.PARTITIONS", "3")])
    schema, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    _kv_frame_assertion(rows, schema, [("spark.sql.shuffle.partitions", "8")])
    schema, rows = _set_and_get(spark, "SET SPARK.SQL.SHUFFLE.PARTITIONS")
    _kv_frame_assertion(rows, schema, [("SPARK.SQL.SHUFFLE.PARTITIONS", "3")])
    spark.stop()


def test_set_catalog_and_namespace_are_conf_reads() -> None:
    """Bare SET CATALOG / SET NAMESPACE answer ``<undefined>`` — S5-set-catalog/namespace."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET CATALOG")
    _kv_frame_assertion(rows, schema, [("CATALOG", "<undefined>")])
    schema, rows = _set_and_get(spark, "SET NAMESPACE")
    _kv_frame_assertion(rows, schema, [("NAMESPACE", "<undefined>")])
    spark.stop()


def test_set_role_is_not_intercepted() -> None:
    """Bare SET ROLE reaches the engine and stores nothing — S5-set-role."""
    spark = _session()
    with pytest.raises(ParseException) as caught:
        spark.sql("SET ROLE")
    message = str(caught.value)
    assert "[INVALID_STATEMENT_OR_CLAUSE]" not in message
    assert "equals sign or TO" in message
    assert spark.conf.get("ROLE", "<undefined>") == "<undefined>"
    spark.stop()


def test_set_time_zone_literal_america_new_york_is_tz3() -> None:
    """``SET TIME ZONE 'America/New_York'`` echoes UTC; Spark would echo NY — TZ-3 / L-005."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET TIME ZONE 'America/New_York'")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    _, tz_rows = _arrow(spark.sql("SELECT current_timezone() tz"))
    assert tz_rows == [{"tz": "UTC"}]
    spark.stop()


def test_reset_collation_key_is_g15_refusal() -> None:
    """RESET of a collation key stays the G15 valve — S5-collation-reset."""
    spark = _session()
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql("RESET spark.sql.session.collation.default")
    assert "collation" in str(caught.value).lower()
    spark.stop()


def test_shuffle_partitions_must_be_positive() -> None:
    """``spark.sql.shuffle.partitions`` of -1 or 0 raises REQUIREMENT — S5-neg/zero-partitions."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.shuffle.partitions = -1")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.REQUIREMENT]" in message
    assert "'-1'" in message
    assert "must be positive" in message
    assert "SQLSTATE: 22022" in message
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.shuffle.partitions = 0")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.REQUIREMENT]" in message
    assert "'0'" in message
    assert "SQLSTATE: 22022" in message
    _, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    assert rows == [{"key": "spark.sql.shuffle.partitions", "value": "<undefined>"}]
    spark.stop()


def test_boolean_typed_key_refuses_1_and_yes_and_keeps_true() -> None:
    """Boolean conf: 1/yes TYPE_MISMATCH; TRUE stores as written — S5-bool-*."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.ansi.enabled = 1")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TYPE_MISMATCH]" in message
    assert "'1'" in message
    assert "'boolean'" in message
    assert "SQLSTATE: 22022" in message
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.ansi.enabled = yes")
    message = str(caught.value)
    assert "'yes'" in message
    assert "'boolean'" in message
    assert "SQLSTATE: 22022" in message
    schema, rows = _set_and_get(spark, "SET spark.sql.ansi.enabled = TRUE")
    _kv_frame_assertion(rows, schema, [("spark.sql.ansi.enabled", "TRUE")])
    spark.stop()


def test_reset_restores_a_builder_seeded_value() -> None:
    """RESET of a builder-seeded key restores the builder value — S5-builder-after-reset."""
    spark = (
        ReparkSession.builder.appName("sql-set-door-1-builder")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    schema, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    _kv_frame_assertion(rows, schema, [("spark.sql.shuffle.partitions", "8")])
    _set_and_get(spark, "SET spark.sql.shuffle.partitions = 2")
    schema, rows = _set_and_get(spark, "RESET spark.sql.shuffle.partitions")
    assert len(schema) == 0
    assert rows == []
    schema, rows = _set_and_get(spark, "SET spark.sql.shuffle.partitions")
    _kv_frame_assertion(rows, schema, [("spark.sql.shuffle.partitions", "8")])
    spark.stop()


def test_backtick_quoted_key_is_stored_unquoted() -> None:
    """Backtick-quoted SET key stores unquoted — S5-quoted-key."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET `spark.sql.shuffle.partitions` = 4")
    _kv_frame_assertion(rows, schema, [("spark.sql.shuffle.partitions", "4")])
    spark.stop()


def test_set_time_zone_double_quoted_literal() -> None:
    """``SET TIME ZONE \"UTC\"`` is recognised — S5-tz-dq."""
    spark = _session()
    schema, rows = _set_and_get(spark, 'SET TIME ZONE "UTC"')
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    spark.stop()


def test_set_time_zone_interval_hour_to_minute() -> None:
    """INTERVAL '+08:00' HOUR TO MINUTE is recognised; echo is TZ-3 UTC — S5-tz-interval."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET TIME ZONE INTERVAL '+08:00' HOUR TO MINUTE")
    _kv_frame_assertion(rows, schema, [(SESSION_TIME_ZONE_KEY, "UTC")])
    spark.stop()


def test_empty_value_and_value_containing_equals() -> None:
    """Empty SET value is ''; a value containing '=' stays whole — S5-empty-value."""
    spark = _session()
    schema, rows = _set_and_get(spark, "SET spark.empty.value = ")
    _kv_frame_assertion(rows, schema, [("spark.empty.value", "")])
    schema, rows = _set_and_get(spark, "SET spark.eq.value = a=b")
    _kv_frame_assertion(rows, schema, [("spark.eq.value", "a=b")])
    spark.stop()


def test_long_select_is_not_intercepted() -> None:
    """A long SELECT is not treated as SET/RESET — P1-SET-FULLSCAN."""
    spark = _session()
    padding = " " * 20000
    table = spark.sql(f"SELECT 1{padding}AS x").to_arrow()
    assert table.schema.names == ["x"]
    assert table.to_pylist() == [{"x": 1}]
    spark.stop()
