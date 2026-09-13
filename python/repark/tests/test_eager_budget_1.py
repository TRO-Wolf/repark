"""EAGER-BUDGET-1 pins: retained bytes, the read-only conf, and the total budget refusal.

pins: eager-budget-1/C-002, eager-budget-1/C-003, eager-budget-1/C-004, eager-budget-1/C-005,
eager-budget-1/C-006, eager-budget-1/C-007, eager-budget-1/C-008, eager-budget-1/C-009
"""

from __future__ import annotations

import gc
import json
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException, PySparkException
from repark.spark import functions as F  # noqa: N812 — PySpark idiom

_RETAINED_KEY = "repark.cache.retained_bytes"
_TOTAL_KEY = "repark.cache.max_total_bytes"
_MAX_BYTES_KEY = "repark.cache.max_bytes"

_TWO_ROW_SQL = "SELECT 1 AS id, 'x' AS label UNION ALL SELECT 2, 'y'"

_REFUSAL_FIX = "release cached/eager frames with unpersist() or spark.catalog.clearCache()"


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-eager-budget-1").getOrCreate()
    yield session
    session.stop()


def _retained(spark: ReparkSession) -> int:
    return int(spark.conf.get(_RETAINED_KEY))


def _distinct_buffer_stats(table: Any) -> tuple[int, int]:
    """Distinct ``buffer.address`` byte sum and count over a ``toArrow()`` table."""
    distinct: dict[int, int] = {}
    for column in table.columns:
        for chunk in column.chunks:
            for buffer in chunk.buffers():
                if buffer is not None:
                    distinct.setdefault(buffer.address, buffer.size)
    return sum(distinct.values()), len(distinct)


def test_retained_bytes_track_cache_lifecycle(spark: ReparkSession) -> None:
    """C-002: cache()+action raises retained by the frame's distinct-buffer bytes."""
    assert _retained(spark) == 0
    frame = spark.sql(_TWO_ROW_SQL).cache()
    assert _retained(spark) == 0
    frame.count()
    retained = _retained(spark)
    assert retained > 0
    measured, buffer_count = _distinct_buffer_stats(frame.toArrow())
    assert measured > 0
    assert retained >= measured
    assert retained <= 2 * measured + 64 * buffer_count
    frame.unpersist()
    assert _retained(spark) == 0


def test_eager_on_eager_reuse_leaves_retained_unchanged(spark: ReparkSession) -> None:
    """C-002: ``b = a.eager()`` shares the view, so retained bytes do not move."""
    a = spark.sql(_TWO_ROW_SQL).eager()
    retained_before = _retained(spark)
    assert retained_before > 0
    b = a.eager()
    assert _retained(spark) == retained_before
    del b
    gc.collect()
    assert _retained(spark) == retained_before
    del a
    gc.collect()
    assert _retained(spark) == 0


def test_retained_returns_to_prior_value_when_last_holder_dies(
    spark: ReparkSession,
) -> None:
    """C-002: deleting the last holder plus gc drops retained to the prior level."""
    first = spark.sql(_TWO_ROW_SQL).eager()
    retained_one = _retained(spark)
    second = spark.sql(_TWO_ROW_SQL).eager()
    assert _retained(spark) > retained_one
    del second
    gc.collect()
    assert _retained(spark) == retained_one
    del first
    gc.collect()
    assert _retained(spark) == 0


def test_clear_cache_returns_retained_to_zero(spark: ReparkSession) -> None:
    """C-002: ``clearCache()`` drops every cache view and the retained sum."""
    first = spark.sql(_TWO_ROW_SQL).eager()
    second = spark.sql(_TWO_ROW_SQL).eager()
    assert _retained(spark) > 0
    spark.catalog.clearCache()
    assert _retained(spark) == 0
    assert first.count() == 2
    assert second.count() == 2


def test_retained_bytes_conf_is_read_only(spark: ReparkSession) -> None:
    """C-003: the readback parses as int; set/unset refuse; isModifiable is False."""
    value = spark.conf.get(_RETAINED_KEY)
    assert isinstance(int(value), int)
    assert spark.conf.getAll[_RETAINED_KEY] == value
    assert spark.conf.isModifiable(_RETAINED_KEY) is False
    with pytest.raises(IllegalArgumentException, match=r"INVALID_CONF_VALUE\.REQUIREMENT"):
        spark.conf.set(_RETAINED_KEY, "0")
    with pytest.raises(IllegalArgumentException, match=r"INVALID_CONF_VALUE\.REQUIREMENT"):
        spark.conf.unset(_RETAINED_KEY)
    spark.stop()
    with pytest.raises(RuntimeError):
        spark.conf.get(_RETAINED_KEY)


def _cache_view_table_names(spark: ReparkSession) -> list[str]:
    return sorted(
        table.name
        for table in spark.catalog.listTables()
        if table.name.startswith("__repark_cache_")
    )


def _handle_count(spark: ReparkSession) -> int:
    registry = spark._alive_token.get("cache_view_handles")
    return len(registry) if registry is not None else 0


def _assert_budget_refusal_message(message: str, budget: int, retained: int) -> None:
    assert "REPARK_CACHE_BUDGET_EXCEEDED" in message
    assert f"repark.cache.max_total_bytes={budget}" in message
    assert f"retained {retained} bytes" in message
    assert "admitted" in message and "bytes before refusal" in message
    assert _REFUSAL_FIX in message
    assert "raise repark.cache.max_total_bytes" in message


def test_max_total_bytes_conf_parse(spark: ReparkSession) -> None:
    """C-004: unset and 0 disable; invalid, negative and >u64 refuse exactly like max_bytes."""
    spark.conf.set(_TOTAL_KEY, "0")
    frame = spark.sql(_TWO_ROW_SQL).cache()
    assert frame.count() == 2
    frame.unpersist()
    for raw in ("not-a-budget", "-1", str(2**64)):
        spark.conf.set(_TOTAL_KEY, raw)
        refused = spark.sql(_TWO_ROW_SQL).cache()
        with pytest.raises(IllegalArgumentException) as excinfo:
            refused.count()
        message = str(excinfo.value)
        assert "INVALID_CONF_VALUE.REQUIREMENT" in message
        assert _TOTAL_KEY in message
        assert raw in message
    spark.conf.unset(_TOTAL_KEY)
    frame = spark.sql(_TWO_ROW_SQL).cache()
    assert frame.count() == 2


def test_max_total_bytes_builder_config_and_unset(spark: ReparkSession) -> None:
    """C-004: builder ``.config`` is honored; ``conf.unset`` at runtime overrides it."""
    spark.stop()
    built = (
        ReparkSession.builder.appName("pytest-budget-builder").config(_TOTAL_KEY, "1").getOrCreate()
    )
    try:
        frame = built.sql(_TWO_ROW_SQL).cache()
        with pytest.raises(IllegalArgumentException, match=r"REPARK_CACHE_BUDGET_EXCEEDED"):
            frame.count()
        built.conf.unset(_TOTAL_KEY)
        frame = built.sql(_TWO_ROW_SQL).cache()
        assert frame.count() == 2
    finally:
        built.stop()


def test_total_budget_refuses_cache_and_eager(spark: ReparkSession) -> None:
    """C-005: budget below one result refuses on cache()+action and on eager()."""
    spark.conf.set(_TOTAL_KEY, "1")
    cached = spark.sql(_TWO_ROW_SQL).cache()
    with pytest.raises(IllegalArgumentException) as excinfo:
        cached.count()
    _assert_budget_refusal_message(str(excinfo.value), budget=1, retained=0)
    with pytest.raises(IllegalArgumentException) as excinfo:
        spark.sql(_TWO_ROW_SQL).eager()
    _assert_budget_refusal_message(str(excinfo.value), budget=1, retained=0)
    assert _retained(spark) == 0


def test_total_budget_refusal_counts_live_retained_and_recovers(
    spark: ReparkSession,
) -> None:
    """C-005: retained live bytes count against the budget; unpersist frees headroom."""
    first = spark.sql(_TWO_ROW_SQL).cache()
    first.count()
    retained = _retained(spark)
    assert retained > 0
    spark.conf.set(_TOTAL_KEY, str(retained + 1))
    second = spark.sql("SELECT 3 AS id, 'z' AS label UNION ALL SELECT 4, 'w'").cache()
    with pytest.raises(IllegalArgumentException) as excinfo:
        second.count()
    message = str(excinfo.value)
    _assert_budget_refusal_message(message, budget=retained + 1, retained=retained)
    first.unpersist()
    assert _retained(spark) == 0
    assert second.count() == 2


def test_no_registration_survives_refusal(spark: ReparkSession) -> None:
    """C-007: a budget refusal leaves no view, no temp name, and no new handle."""
    tables_before = _cache_view_table_names(spark)
    temp_before = sorted(spark.list_temp_view_names())
    handles_before = _handle_count(spark)
    spark.conf.set(_TOTAL_KEY, "1")
    cached = spark.sql(_TWO_ROW_SQL).cache()
    with pytest.raises(IllegalArgumentException):
        cached.count()
    with pytest.raises(IllegalArgumentException):
        spark.sql(_TWO_ROW_SQL).eager()
    assert _cache_view_table_names(spark) == tables_before
    assert sorted(spark.list_temp_view_names()) == temp_before
    assert _handle_count(spark) == handles_before
    assert cached._cache_view is None
    assert cached._lineage_inner is None
    assert cached.is_cached is True


def test_no_registration_survives_collection_failure(spark: ReparkSession) -> None:
    """C-007: a mid-collection plan failure leaves no view, no temp name, no handle."""
    spark.stop()
    ansi = (
        ReparkSession.builder.appName("pytest-budget-ansi")
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )
    try:
        tables_before = _cache_view_table_names(ansi)
        temp_before = sorted(ansi.list_temp_view_names())
        handles_before = _handle_count(ansi)
        overflow = ansi.range(4).select((F.col("id") + F.lit(2147483647)).cast("int").alias("x"))
        with pytest.raises(PySparkException):
            overflow.cache().count()
        with pytest.raises(PySparkException):
            ansi.range(4).select((F.col("id") + F.lit(2147483647)).cast("int").alias("x")).eager()
        assert _cache_view_table_names(ansi) == tables_before
        assert sorted(ansi.list_temp_view_names()) == temp_before
        assert _handle_count(ansi) == handles_before
    finally:
        ansi.stop()


def test_max_bytes_contract_unchanged(spark: ReparkSession) -> None:
    """C-008: per-result max_bytes keeps the main metric, message and boundary.

    The 312-byte figure is the ``get_array_memory_size`` sum measured on the
    main tree for the three-row UNION plan (not the distinct-buffer metric).
    """
    three_row = "SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3"
    spark.conf.set(_MAX_BYTES_KEY, "100")
    refused = spark.sql(three_row).cache()
    with pytest.raises(IllegalArgumentException) as excinfo:
        refused.count()
    message = str(excinfo.value)
    assert "cache materialize size 312 bytes exceeds repark.cache.max_bytes=100" in message
    assert "raise the conf or avoid cache()/persist() on this plan" in message
    assert "no disk spill" in message
    assert "REPARK_CACHE_BUDGET_EXCEEDED" not in message
    assert refused.is_cached is True
    spark.conf.set(_MAX_BYTES_KEY, "312")
    boundary = spark.sql(three_row).cache()
    assert boundary.count() == 3
    boundary.unpersist()
    spark.conf.set(_MAX_BYTES_KEY, "311")
    refused = spark.sql(three_row).cache()
    with pytest.raises(IllegalArgumentException) as excinfo:
        refused.count()
    assert "cache materialize size 312 bytes exceeds repark.cache.max_bytes=311" in str(
        excinfo.value
    )


def test_max_bytes_stays_per_result_with_total_budget(spark: ReparkSession) -> None:
    """C-008/R12b-D-4: ``max_bytes`` measures this result even when it shares live buffers."""
    live = spark.sql(_TWO_ROW_SQL).cache()
    live.count()
    retained = _retained(spark)
    assert retained > 0
    spark.conf.set(_TOTAL_KEY, str(retained + 10_000_000))
    spark.conf.set(_MAX_BYTES_KEY, "1")
    scan = spark.sql(f"SELECT * FROM {live._cache_view}").cache()
    with pytest.raises(IllegalArgumentException) as excinfo:
        scan.count()
    message = str(excinfo.value)
    assert "cache materialize size" in message
    assert "exceeds repark.cache.max_bytes=1" in message
    assert "REPARK_CACHE_BUDGET_EXCEEDED" not in message
    assert scan._cache_view is None
    assert len(_cache_view_table_names(spark)) == 1
    spark.conf.unset(_MAX_BYTES_KEY)
    assert scan.count() == 2


def test_retained_delta_matches_each_admitted_cache(spark: ReparkSession) -> None:
    """C-002/D-1: each admitted cache raises retained by its own distinct-buffer bytes."""
    first = spark.sql(_TWO_ROW_SQL).cache()
    first.count()
    retained_one = _retained(spark)
    assert retained_one > 0
    second = spark.sql("SELECT 9 AS id, 'q' AS label UNION ALL SELECT 8, 'r'").cache()
    second.count()
    retained_two = _retained(spark)
    measured, buffer_count = _distinct_buffer_stats(second.toArrow())
    delta = retained_two - retained_one
    assert delta >= measured
    assert delta <= 2 * measured + 64 * buffer_count


def test_cache_paths_write_no_spill_files(spark: ReparkSession, tmp_path: Path) -> None:
    """C-009: refused and admitted caches write nothing under the temp/spill dir."""
    spark.stop()
    built = (
        ReparkSession.builder.appName("pytest-budget-tmpdir")
        .config("datafusion.runtime.temp_directory", str(tmp_path))
        .getOrCreate()
    )
    try:
        files_before = sorted(path for path in tmp_path.rglob("*") if path.is_file())
        built.conf.set(_TOTAL_KEY, "1")
        refused = built.sql(_TWO_ROW_SQL).cache()
        with pytest.raises(IllegalArgumentException):
            refused.count()
        assert sorted(path for path in tmp_path.rglob("*") if path.is_file()) == files_before
        built.conf.unset(_TOTAL_KEY)
        admitted = built.sql(_TWO_ROW_SQL).cache()
        assert admitted.count() == 2
        assert sorted(path for path in tmp_path.rglob("*") if path.is_file()) == files_before
    finally:
        built.stop()


_C006_WORKER = """
import json

from repark import ReparkSession
from repark.spark import functions as F


def peak_hwm():
    for line in open("/proc/self/status"):
        if line.startswith("VmHWM:"):
            return int(line.split()[1]) * 1024
    return 0


spark = (
    ReparkSession.builder.appName("eager-budget-1-c006")
    .config("repark.batch.size", "100000")
    .config("repark.target.partitions", "1")
    .getOrCreate()
)
frame = spark.range(1000000).select(
    F.col("id").cast("double").alias("a"),
    (F.col("id") + 1).cast("double").alias("b"),
    (F.col("id") + 2).cast("double").alias("c"),
    (F.col("id") * 2).cast("double").alias("d"),
    (F.col("id") - 3).cast("double").alias("e"),
    (F.col("id") / 3).cast("double").alias("g"),
)
frame.count()
baseline = peak_hwm()
budget = 12000000
spark.conf.set("repark.cache.max_total_bytes", str(budget))
cached = frame.cache()
error = ""
try:
    cached.count()
except Exception as exc:
    error = str(exc)
hwm_refused = peak_hwm()
spark.conf.unset("repark.cache.max_total_bytes")
cached.count()
hwm_full = peak_hwm()
print(
    "JSON"
    + json.dumps(
        {
            "baseline": baseline,
            "hwm_refused": hwm_refused,
            "hwm_full": hwm_full,
            "growth_refused": hwm_refused - baseline,
            "growth_full": hwm_full - hwm_refused,
            "error": error,
            "budget": budget,
            "retained": int(spark.conf.get("repark.cache.retained_bytes")),
        }
    ),
    flush=True,
)
"""


def test_refusal_happens_before_result_peak() -> None:
    """C-006: a refused cache peaks well below the unbudgeted materialize in one worker."""
    completed = subprocess.run(
        [sys.executable, "-c", _C006_WORKER],
        capture_output=True,
        text=True,
        timeout=600,
        check=False,
    )
    tail = completed.stdout.strip().splitlines()
    assert tail, (
        f"C-006 worker produced no output: rc={completed.returncode}\n{completed.stderr[-1200:]}"
    )
    marker = tail[-1]
    assert marker.startswith("JSON"), (
        f"C-006 worker bad tail: {marker[:200]}\n{completed.stderr[-600:]}"
    )
    result = json.loads(marker[len("JSON") :])
    assert "REPARK_CACHE_BUDGET_EXCEEDED" in result["error"]
    assert result["hwm_refused"] < result["hwm_full"]
    assert result["growth_refused"] > 0
    assert result["growth_full"] > 0
    assert result["growth_refused"] < 0.6 * result["growth_full"], (
        f"refused growth {result['growth_refused']} not below 60% of "
        f"unbudgeted growth {result['growth_full']} "
        f"(baseline {result['baseline']}, hwm_refused {result['hwm_refused']}, "
        f"hwm_full {result['hwm_full']}, retained {result['retained']})"
    )


def test_budget_keys_resolve_case_insensitively(spark: ReparkSession) -> None:
    """R12b-D-5: mixed-case runtime spellings bind at materialize; tomb is case-insensitive."""
    spark.conf.set("REPARK.CACHE.MAX_TOTAL_BYTES", "1")
    cached = spark.sql(_TWO_ROW_SQL).cache()
    with pytest.raises(IllegalArgumentException, match=r"REPARK_CACHE_BUDGET_EXCEEDED"):
        cached.count()
    spark.conf.unset("repark.cache.max_total_bytes")
    spark.conf.set("REPARK.CACHE.MAX_BYTES", "1")
    cached = spark.sql(_TWO_ROW_SQL).cache()
    with pytest.raises(IllegalArgumentException, match=r"cache materialize size"):
        cached.count()
    spark.conf.unset("Repark.Cache.Max_Bytes")
    assert spark.sql(_TWO_ROW_SQL).cache().count() == 2


def test_budget_key_case_last_set_wins(spark: ReparkSession) -> None:
    """R12b-D-5: a later differently-cased set overrides; runtime beats the builder."""
    spark.conf.set("REPARK.CACHE.MAX_TOTAL_BYTES", "1")
    spark.conf.set("repark.cache.max_total_bytes", "0")
    assert spark.sql(_TWO_ROW_SQL).cache().count() == 2
    spark.conf.set("REPARK.CACHE.MAX_TOTAL_BYTES", "1")
    cached = spark.sql(_TWO_ROW_SQL).cache()
    with pytest.raises(IllegalArgumentException, match=r"REPARK_CACHE_BUDGET_EXCEEDED"):
        cached.count()
    spark.stop()
    built = (
        ReparkSession.builder.appName("pytest-budget-case")
        .config("RePark.Cache.Max_Total_Bytes", "1")
        .getOrCreate()
    )
    try:
        cached = built.sql(_TWO_ROW_SQL).cache()
        with pytest.raises(IllegalArgumentException, match=r"REPARK_CACHE_BUDGET_EXCEEDED"):
            cached.count()
        built.conf.set("REPARK.CACHE.MAX_TOTAL_BYTES", "0")
        assert built.sql(_TWO_ROW_SQL).cache().count() == 2
    finally:
        built.stop()


def test_sql_set_repark_cache_keys_refused(spark: ReparkSession) -> None:
    """L-004: SQL ``SET`` on repark.cache keys raises the DataFusion namespace error."""
    for key in (_RETAINED_KEY, _TOTAL_KEY, _MAX_BYTES_KEY):
        with pytest.raises(PySparkException) as excinfo:
            spark.sql(f"SET {key}=1")
        assert 'config namespace "repark"' in str(excinfo.value)
