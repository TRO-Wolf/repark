"""Runtime `SET` / `spark.conf.set` of the ANSI and time-zone knobs apply — SET-ANSI-RUNTIME-1.

Oracle: `/tmp/oc-worker/qc-oracle/fixtures-batch16-dc2rest-setansi.json`
(`sequence_one_session_in_order`, PySpark 4.1.2, builder
`spark.sql.session.timeZone=UTC`, `spark.sql.ansi.enabled=true`),
`fixtures-batch5.json` (S5-* SET-door cells) and `fixtures-batch1.json`
(BTZ5-*, BL11-* cells).

Snapshot timing (addendum, batch 16): ANSI binds when a frame is analysed, so a
frame built under ANSI=true keeps `DIVIDE_BY_ZERO` after the SET (S16-0); zone
value expressions answer the frame-build zone while `current_timezone()` folds
at collect. Every assertion runs on the Arrow path (value AND Arrow type AND
field nullability) on both doors wherever a Python API equivalent exists.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

import repark
from repark import ReparkSession
from repark import functions as F  # noqa: N812 — PySpark idiom
from repark.errors import IllegalArgumentException, PySparkException

ANSI_KEY = "spark.sql.ansi.enabled"
ZONE_KEY = "spark.sql.session.timeZone"
ZONE_TOKYO = "Asia/Tokyo"
ZONE_NEW_YORK = "America/New_York"


def _session() -> ReparkSession:
    """One session on the S16 oracle basis: zone UTC, ANSI on, both explicit."""
    return (
        ReparkSession.builder.appName("set-ansi-runtime-1")
        .config(ZONE_KEY, "UTC")
        .config(ANSI_KEY, "true")
        .getOrCreate()
    )


def _arrow(frame: Any) -> pa.Table:
    """Materialize a frame on the Arrow path."""
    return frame.to_arrow()


def test_s16_0_stale_frame_keeps_divide_by_zero() -> None:
    """A frame analysed under ANSI=true still raises after the SET flips to false."""
    spark = _session()
    stale = spark.sql("SELECT 1/0")
    spark.sql("SET spark.sql.ansi.enabled=false")
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        stale.to_arrow()
    spark.stop()


def test_s16_1_fresh_division_answers_null_after_set() -> None:
    """A fresh `SELECT 1/0` after the SET answers NULL double, nullable — S16-1."""
    spark = _session()
    spark.sql("SET spark.sql.ansi.enabled=false")
    table = _arrow(spark.sql("SELECT 1/0"))
    assert table.schema.field(0).type == pa.float64()
    assert table.schema.field(0).nullable is True
    assert table.to_pylist() == [{table.schema.names[0]: None}]
    spark.stop()


def test_s16_2_cast_x_still_raises_string_cast_residue() -> None:
    """A fresh `CAST('x' AS INT)` after the SET still raises — S16-2 residue.

    Spark answers NULL (unparsable string cast with ANSI off). repark raises the
    same `simplify_expressions` cast error with ANSI off at BUILD, so the snapshot
    cannot deliver the oracle cell: the string-cast path never reads the ANSI flag
    (narrow residue, registry SET-ANSI-RUNTIME-3). Division (S16-1) is the live
    proof fresh queries read the runtime flag.
    """
    spark = _session()
    spark.sql("SET spark.sql.ansi.enabled=false")
    with pytest.raises(PySparkException, match="Cannot cast string 'x'"):
        spark.sql("SELECT CAST('x' AS INT)").to_arrow()
    spark.stop()


def test_s16_3_conf_set_true_restores_raise() -> None:
    """`spark.conf.set` true restores `DIVIDE_BY_ZERO` for fresh queries — S16-3."""
    spark = _session()
    spark.sql("SET spark.sql.ansi.enabled=false")
    spark.conf.set(ANSI_KEY, "true")
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0").to_arrow()
    spark.stop()


def test_s16_4_unset_with_builder_true_raises() -> None:
    """`conf.unset` with builder true answers ANSI-true queries with the raise — S16-4."""
    spark = _session()
    spark.sql("SET spark.sql.ansi.enabled=false")
    spark.conf.unset(ANSI_KEY)
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0").to_arrow()
    spark.stop()


def test_s16_5_get_after_unset_reports_builder_true() -> None:
    """`conf.get` after `unset` with builder true answers `'true'` — S16-5."""
    spark = _session()
    spark.sql("SET spark.sql.ansi.enabled=false")
    spark.conf.unset(ANSI_KEY)
    assert spark.conf.get(ANSI_KEY) == "true"
    spark.stop()


def test_s16_6_stale_frame_split_binding() -> None:
    """A frame built under UTC and collected after the zone SET splits — S16-6.

    The value-bearing expressions keep the frame-build snapshot (UTC). Spark
    answers `current_timezone()` with the new zone (late binding at collect);
    repark answers the build zone (narrow residue, registry SET-ANSI-RUNTIME-2).
    """
    spark = _session()
    stale = spark.sql(
        "SELECT current_timezone() AS tz, "
        "CAST(from_unixtime(0) AS STRING) AS epoch, "
        "CAST(TIMESTAMP '2024-01-01 00:00:00' AS STRING) AS wall"
    )
    spark.sql("SET TIME ZONE 'Asia/Tokyo'")
    table = _arrow(stale)
    assert table.to_pylist() == [
        {"tz": "UTC", "epoch": "1970-01-01 00:00:00", "wall": "2024-01-01 00:00:00"}
    ]
    spark.stop()


def test_s16_7_fresh_query_follows_new_zone() -> None:
    """Fresh queries after the zone SET follow it — S16-7."""
    spark = _session()
    spark.sql("SET TIME ZONE 'Asia/Tokyo'")
    table = _arrow(
        spark.sql(
            "SELECT current_timezone() AS tz, "
            "CAST(from_unixtime(0) AS STRING) AS epoch, "
            "CAST(TIMESTAMP '2024-01-01 00:00:00' AS STRING) AS wall"
        )
    )
    assert table.to_pylist() == [
        {"tz": ZONE_TOKYO, "epoch": "1970-01-01 09:00:00", "wall": "2024-01-01 00:00:00"}
    ]
    spark.stop()


def test_s16_8_conf_set_zone_applies() -> None:
    """`spark.conf.set` of the zone applies to fresh queries — S16-8."""
    spark = _session()
    spark.sql("SET TIME ZONE 'Asia/Tokyo'")
    spark.conf.set(ZONE_KEY, ZONE_NEW_YORK)
    assert spark.conf.get(ZONE_KEY) == ZONE_NEW_YORK
    table = _arrow(
        spark.sql("SELECT current_timezone() AS tz, CAST(from_unixtime(0) AS STRING) AS epoch")
    )
    assert table.to_pylist() == [{"tz": ZONE_NEW_YORK, "epoch": "1969-12-31 19:00:00"}]
    spark.stop()


def test_s16_9_conf_set_invalid_zone_refuses() -> None:
    """`spark.conf.set` of a bad zone refuses before anything is stored — S16-9."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.conf.set(ZONE_KEY, "Not/AZone")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TIME_ZONE]" in message
    assert "'Not/AZone'" in message
    assert 'config "spark.sql.session.timeZone"' in message
    assert "Cannot resolve the given timezone" in message
    assert "SQLSTATE: 22022" in message
    assert spark.conf.get(ZONE_KEY) == "UTC"
    table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
    assert table.to_pylist() == [{"tz": "UTC"}]
    spark.stop()


def test_s16_10_set_ansi_maybe_refuses() -> None:
    """`SET spark.sql.ansi.enabled=maybe` refuses before anything is stored — S16-10."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.ansi.enabled=maybe")
    message = str(caught.value)
    assert "[INVALID_CONF_VALUE.TYPE_MISMATCH]" in message
    assert "'maybe'" in message
    assert 'config "spark.sql.ansi.enabled"' in message
    assert "'boolean'" in message
    assert "SQLSTATE: 22022" in message
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0").to_arrow()
    spark.stop()


def test_s16_11_reset_zone_restores_builder() -> None:
    """`RESET` of the zone restores the builder zone for fresh queries — S16-11."""
    spark = _session()
    spark.sql("SET TIME ZONE 'Asia/Tokyo'")
    spark.sql("RESET spark.sql.session.timeZone")
    assert spark.conf.get(ZONE_KEY) == "UTC"
    table = _arrow(
        spark.sql("SELECT current_timezone() AS tz, CAST(from_unixtime(0) AS STRING) AS epoch")
    )
    assert table.to_pylist() == [{"tz": "UTC", "epoch": "1970-01-01 00:00:00"}]
    spark.stop()


def test_c001_f_api_division_follows_runtime_ansi() -> None:
    """`F.lit(1) / F.lit(0)` on a post-SET frame answers NULL, and raises again on true."""
    spark = _session()
    spark.sql("SET spark.sql.ansi.enabled=false")
    table = _arrow(spark.sql("SELECT 1 AS anchor").select((F.lit(1) / F.lit(0)).alias("d")))
    assert table.schema.field("d").type == pa.float64()
    assert table.schema.field("d").nullable is True
    assert table.to_pylist() == [{"d": None}]
    spark.conf.set(ANSI_KEY, "true")
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1 AS anchor").select((F.lit(1) / F.lit(0)).alias("d")).to_arrow()
    spark.stop()


def test_c002_f_api_zone_follows() -> None:
    """`F.current_timezone()` and `F.from_unixtime` follow a runtime zone SET."""
    spark = _session()
    spark.sql("SET TIME ZONE 'Asia/Tokyo'")
    table = _arrow(
        spark.sql("SELECT 1 AS anchor").select(
            F.current_timezone().alias("tz"),
            F.from_unixtime(F.lit(0)).alias("epoch"),
        )
    )
    assert table.to_pylist() == [{"tz": ZONE_TOKYO, "epoch": "1970-01-01 09:00:00"}]
    spark.stop()


def test_btz5_2_3_set_zone_then_current_timezone() -> None:
    """`SET spark.sql.session.timeZone = America/New_York` applies — BTZ5-2/3."""
    spark = _session()
    spark.sql("SET spark.sql.session.timeZone = America/New_York")
    assert spark.conf.get(ZONE_KEY) == ZONE_NEW_YORK
    table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
    assert table.to_pylist() == [{"tz": ZONE_NEW_YORK}]
    spark.stop()


def test_btz5_5_refused_set_moves_nothing() -> None:
    """A refused zone SET stores nothing, so the zone query still answers NY — BTZ5-5."""
    spark = _session()
    spark.sql("SET spark.sql.session.timeZone = America/New_York")
    with pytest.raises(IllegalArgumentException, match=r"INVALID_CONF_VALUE\.TIME_ZONE"):
        spark.sql("SET spark.sql.session.timeZone = 'Asia/Tokyo'")
    table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
    assert table.to_pylist() == [{"tz": ZONE_NEW_YORK}]
    spark.stop()


def test_btz5_7_8_set_false_then_null() -> None:
    """`SET spark.sql.ansi.enabled = false` then `SELECT 1/0` answers NULL — BTZ5-7/8."""
    spark = ReparkSession.builder.appName("set-ansi-runtime-1-btz5").getOrCreate()
    spark.sql("SET spark.sql.ansi.enabled = false")
    table = _arrow(spark.sql("SELECT 1/0 AS d"))
    assert table.schema.field("d").type == pa.float64()
    assert table.schema.field("d").nullable is True
    assert table.to_pylist() == [{"d": None}]
    spark.stop()


def test_btz5_9_10_reset_then_raise() -> None:
    """`RESET spark.sql.ansi.enabled` with no builder value restores the raise — BTZ5-9/10."""
    spark = ReparkSession.builder.appName("set-ansi-runtime-1-btz5").getOrCreate()
    spark.sql("SET spark.sql.ansi.enabled = false")
    spark.sql("RESET spark.sql.ansi.enabled")
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0 AS d").to_arrow()
    spark.stop()


def test_s5_zone_cells_apply() -> None:
    """Offset-zone SET spellings apply: the zone query answers the SET string — S5-tz-*."""
    spark = _session()
    for zone in ("+05", "+5", "+18:00"):
        spark.sql(f"SET TIME ZONE '{zone}'")
        assert spark.conf.get(ZONE_KEY) == zone
        table = _arrow(spark.sql("SELECT current_timezone() AS tz"))
        assert table.to_pylist() == [{"tz": zone}]
    spark.sql("SET spark.sql.session.timeZone = GMT+8")
    assert spark.conf.get(ZONE_KEY) == "GMT+8"
    spark.sql("SET TIME ZONE INTERVAL '+08:00' HOUR TO MINUTE")
    assert spark.conf.get(ZONE_KEY) == "+08:00"
    with pytest.raises(IllegalArgumentException, match=r"INVALID_CONF_VALUE\.TIME_ZONE"):
        spark.sql("SET TIME ZONE '+18:01'")
    spark.stop()


def test_s5_bool_cells_still_refuse_and_keep_true() -> None:
    """`1`/`yes` still refuse TYPE_MISMATCH; `TRUE` stores as written — S5-bool-*."""
    spark = _session()
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.ansi.enabled = 1")
    assert "[INVALID_CONF_VALUE.TYPE_MISMATCH]" in str(caught.value)
    with pytest.raises(IllegalArgumentException) as caught:
        spark.sql("SET spark.sql.ansi.enabled = yes")
    assert "[INVALID_CONF_VALUE.TYPE_MISMATCH]" in str(caught.value)
    spark.sql("SET spark.sql.ansi.enabled = TRUE")
    assert spark.conf.get(ANSI_KEY) == "TRUE"
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0").to_arrow()
    spark.stop()


def test_reset_restores_builder_ansi_applied() -> None:
    """RESET with a builder-true session restores the raise and reports `'true'` — C-003."""
    spark = _session()
    spark.sql("SET spark.sql.ansi.enabled = false")
    table = _arrow(spark.sql("SELECT 1/0 AS d"))
    assert table.to_pylist() == [{"d": None}]
    spark.sql("RESET spark.sql.ansi.enabled")
    assert spark.conf.get(ANSI_KEY) == "true"
    with pytest.raises(PySparkException, match="DIVIDE_BY_ZERO"):
        spark.sql("SELECT 1/0 AS d").to_arrow()
    spark.stop()


def test_c004_native_door_ignores_spark_runtime_sets() -> None:
    """The native ANSI door answers `1/0` the same before and after Spark SETs — C-004."""
    spark = _session()
    assert _native_one_over_zero() == "PySparkException: Arrow error: Divide by zero error"
    spark.sql("SET spark.sql.ansi.enabled = false")
    spark.sql("SET TIME ZONE 'Asia/Tokyo'")
    assert _native_one_over_zero() == "PySparkException: Arrow error: Divide by zero error"
    spark.stop()


def _native_one_over_zero() -> str:
    """Today's native-door `SELECT 1/0` answer: DataFusion's own divide error, pinned."""
    try:
        table = repark.sql("SELECT 1/0").to_arrow()
    except Exception as error:
        return f"{type(error).__name__}: {error}"
    return str(table.to_pylist())
