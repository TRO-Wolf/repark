"""EAGER-BUDGET-1 step-1 pins: distinct-buffer retained bytes and the read-only conf."""

from __future__ import annotations

import gc
from collections.abc import Iterator
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException

_RETAINED_KEY = "repark.cache.retained_bytes"

_TWO_ROW_SQL = "SELECT 1 AS id, 'x' AS label UNION ALL SELECT 2, 'y'"


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
