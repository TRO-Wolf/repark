"""S-1 spill-reach battery — recon §3 operator classes under a small FairSpillPool.

Needles and classes come from planning/hardening/SPILL-RECON-2026-08.md §3. Recipes stay
small (64 MiB pool, 2 partitions, generate_series-scale ``range``). ``spill_count > 0``
is the assertion that has teeth; hash-join and ``array_agg`` are pinned AS failures.
"""

from __future__ import annotations

import re

import pytest

import repark.spark.session as session_module
from repark import ReparkSession
from repark import functions as F  # noqa: N812
from repark.errors import PySparkException


def _clear_active() -> None:
    session_module._active_session = None


@pytest.fixture(autouse=True)
def _isolate_session() -> None:
    _clear_active()
    yield
    _clear_active()


def _small_fair_session(memory: str = "64M") -> ReparkSession:
    """FairSpillPool via runtime SET; 2 partitions; 1 MiB sort reservation."""
    spark = (
        ReparkSession.builder.config("datafusion.execution.target_partitions", "2")
        .config("datafusion.execution.sort_spill_reservation_bytes", "1048576")
        .getOrCreate()
    )
    spark.conf.set("datafusion.runtime.memory_limit", memory)
    return spark


def _register_series(spark: ReparkSession, name: str, n_rows: int) -> None:
    spark.range(n_rows).createOrReplaceTempView(name)


def _explain_analyze(spark: ReparkSession, sql: str) -> str:
    rows = spark.sql(f"EXPLAIN ANALYZE {sql}").collect()
    return "\n".join(str(row) for row in rows)


_SPILL_COUNT = re.compile(r"spill_count=(\d+)")


def _max_spill_count(plan_text: str) -> int:
    counts = [int(match.group(1)) for match in _SPILL_COUNT.finditer(plan_text)]
    return max(counts, default=0)


def test_runtime_set_memory_limit_error_is_fair_not_greedy() -> None:
    """Pool-type pin: SET datafusion.runtime.memory_limit must stay fair (recon §1.2)."""
    spark = (
        ReparkSession.builder.config("repark.memory.limit.gb", "1")
        .config("datafusion.execution.target_partitions", "128")
        .getOrCreate()
    )
    spark.conf.set("datafusion.runtime.memory_limit", "256M")
    base = spark.range(2_000_000)
    frame = base.select(
        F.col("id").alias("ts"),
        (F.col("id") % 1000).cast("double").alias("close"),
    )
    for index in range(17):
        frame = frame.withColumn(
            f"f{index}",
            (F.col("close") * (index + 1) + F.col("ts") % (index + 3)).cast("double"),
        )
    with pytest.raises(PySparkException) as raised:
        frame.sort(F.col("close").desc()).to_arrow()
    message = str(raised.value).lower()
    assert "fair(" in message, str(raised.value)
    assert "greedy(" not in message, str(raised.value)


def test_sort_spills_under_small_fair_pool() -> None:
    spark = _small_fair_session()
    _register_series(spark, "series", 400_000)
    text = _explain_analyze(
        spark,
        "SELECT id, md5(cast(id AS string)) AS h FROM series ORDER BY h DESC",
    )
    assert _max_spill_count(text) > 0, text


def test_hash_agg_spills_under_small_fair_pool() -> None:
    spark = _small_fair_session()
    _register_series(spark, "series", 400_000)
    text = _explain_analyze(
        spark,
        "SELECT md5(cast(id AS string)) AS h, count(*) AS n FROM series GROUP BY 1",
    )
    assert _max_spill_count(text) > 0, text


def test_hash_agg_distinct_spills_under_small_fair_pool() -> None:
    spark = _small_fair_session()
    _register_series(spark, "series", 400_000)
    text = _explain_analyze(
        spark,
        "SELECT count(DISTINCT md5(cast(id AS string))) FROM series",
    )
    assert _max_spill_count(text) > 0, text


def test_grouping_sets_spills_under_small_fair_pool() -> None:
    spark = _small_fair_session()
    _register_series(spark, "series", 400_000)
    text = _explain_analyze(
        spark,
        "SELECT h, count(*) AS n FROM (SELECT md5(cast(id AS string)) AS h FROM series) "
        "GROUP BY GROUPING SETS ((h), ())",
    )
    assert _max_spill_count(text) > 0, text


def test_distinct_spills_under_small_fair_pool() -> None:
    spark = _small_fair_session()
    _register_series(spark, "series", 400_000)
    text = _explain_analyze(
        spark,
        "SELECT DISTINCT md5(cast(id AS string)) FROM series",
    )
    assert _max_spill_count(text) > 0, text


def test_sort_merge_join_spills_under_small_fair_pool() -> None:
    spark = _small_fair_session()
    spark.conf.set("datafusion.optimizer.prefer_hash_join", "false")
    _register_series(spark, "left_s", 400_000)
    _register_series(spark, "right_s", 400_000)
    # Join on md5 so inputs are not pre-sorted (range is already ordered by id).
    text = _explain_analyze(
        spark,
        "SELECT l.h FROM "
        "(SELECT md5(cast(id AS string)) AS h FROM left_s) l "
        "JOIN (SELECT md5(cast(id AS string)) AS h FROM right_s) r "
        "ON l.h = r.h",
    )
    assert _max_spill_count(text) > 0, text


def test_hash_join_is_pinned_as_resources_exhausted() -> None:
    """Recon §3.1 hash_join: build side cannot spill. Needle class, not a number."""
    spark = _small_fair_session("16M")
    spark.range(1_000_000).selectExpr(
        "id", "md5(cast(id as string)) AS h", "repeat('x', 64) AS payload"
    ).createOrReplaceTempView("left_s")
    spark.range(1_000_000).selectExpr(
        "id", "md5(cast(id as string)) AS h", "repeat('x', 64) AS payload"
    ).createOrReplaceTempView("right_s")
    with pytest.raises(PySparkException) as raised:
        spark.sql(
            "SELECT l.id FROM left_s l JOIN right_s r ON l.h = r.h"
        ).to_arrow()
    message = str(raised.value)
    assert "Resources exhausted" in message, message
    assert "HashJoin" in message, message
    assert "fair(" in message.lower(), message
    assert "greedy(" not in message.lower(), message


def test_array_agg_spill_path_is_pinned_as_resources_exhausted() -> None:
    """Recon §3.1 hash_agg_collect: the aggregate decides to spill then cannot afford to."""
    spark = _small_fair_session("16M")
    _register_series(spark, "series", 1_000_000)
    with pytest.raises(PySparkException) as raised:
        spark.sql(
            "SELECT id % 2 AS g, array_agg(md5(cast(id AS string))) AS a "
            "FROM series GROUP BY 1"
        ).to_arrow()
    message = str(raised.value)
    assert "Resources exhausted" in message, message
    assert "array_agg" in message, message
    assert "fair(" in message.lower(), message
    assert "greedy(" not in message.lower(), message
