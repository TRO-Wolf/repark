"""EAGER-OWN-1 step-1 pins: a refcounted handle owns every ``__repark_cache_*`` view.

pins: eager-own-1/C-002, C-003, C-004, C-005, C-006, C-007, C-008, C-009, C-010
"""

from __future__ import annotations

import gc
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import IllegalArgumentException
from repark.spark._temp_views import local_view_name

_TWO_ROW_SQL = "SELECT 1 AS id, 'x' AS label UNION ALL SELECT 2, 'y'"


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-eager-own-1").getOrCreate()
    yield session
    session.stop()


def _cache_view_names(spark: ReparkSession) -> list[str]:
    """One-part names of every registered ``__repark_cache_*`` view, sorted."""
    return sorted(
        name for name in spark.list_temp_view_names() if name.startswith("__repark_cache_")
    )


def _cache_view_count(spark: ReparkSession) -> int:
    return len(_cache_view_names(spark))


def _live_handle_count(spark: ReparkSession) -> int:
    """Unreleased cache-view handles tracked on the session alive token."""
    registry = spark._alive_token.get("cache_view_handles")
    if registry is None:
        return 0
    return sum(1 for handle in list(registry) if not handle.released)


class _DropViewSpy:
    """Session proxy counting ``drop_temp_view`` calls reaching the native handle."""

    def __init__(self, native_session: Any) -> None:
        self._native_session = native_session
        self.drop_calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        return getattr(self._native_session, name)

    def drop_temp_view(self, view_name: str) -> bool:
        self.drop_calls.append(view_name)
        return bool(self._native_session.drop_temp_view(view_name))


class _FailScanOnCacheView:
    """Session proxy whose ``sql`` raises only for a ``__repark_cache_*`` scan."""

    def __init__(self, native_session: Any) -> None:
        self._native_session = native_session

    def __getattr__(self, name: str) -> Any:
        return getattr(self._native_session, name)

    def sql(self, query: str) -> Any:
        if "__repark_cache_" in query:
            raise RuntimeError("post-registration scan failure")
        return self._native_session.sql(query)


def test_repeated_bare_eager_releases_every_registration(spark: ReparkSession) -> None:
    """C-002: ten bare eager() calls leave zero registrations after gc, no clearCache."""
    lazy = spark.sql(_TWO_ROW_SQL)
    for _ in range(10):
        lazy.eager()
    gc.collect()
    assert _cache_view_names(spark) == []
    assert _live_handle_count(spark) == 0


def test_one_eager_frame_across_actions_registers_once(spark: ReparkSession) -> None:
    """C-003: reuse across actions adds no registration; values and Arrow types hold."""
    eager = spark.sql(_TWO_ROW_SQL).eager()
    assert _cache_view_count(spark) == 1
    assert eager.count() == 2
    expected_arrow = eager.to_arrow()
    expected_rows = sorted(row.id for row in eager.collect())
    assert eager.filter("id > 1").count() == 1
    assert eager.select("id").count() == 2
    assert _cache_view_count(spark) == 1
    second_arrow = eager.to_arrow()
    assert second_arrow.schema == expected_arrow.schema
    assert second_arrow.to_pylist() == expected_arrow.to_pylist()
    assert sorted(row.id for row in eager.collect()) == expected_rows


def test_eager_on_eager_reuses_the_backing(spark: ReparkSession) -> None:
    """C-004 (D-4): ``c = b.eager()`` shares the view, the handle, and the shape."""
    lazy = spark.sql(_TWO_ROW_SQL)
    b = lazy.eager()
    view = local_view_name(b._cache_view)
    c = b.eager()
    assert local_view_name(c._cache_view) == view
    assert c._eager_shape == b._eager_shape
    assert _cache_view_count(spark) == 1
    assert b._cache_view_owned_handle is not None
    assert b._cache_view_owned_handle in c._handles
    del b
    gc.collect()
    assert view in spark.list_temp_view_names()
    assert c.count() == 2
    assert sorted(row.id for row in c.collect()) == [1, 2]
    del c
    gc.collect()
    assert view not in spark.list_temp_view_names()

    b2 = lazy.eager()
    c2 = b2.eager()
    view2 = local_view_name(b2._cache_view)
    del c2
    gc.collect()
    assert view2 in spark.list_temp_view_names()
    assert b2.count() == 2
    b2.unpersist()
    assert view2 not in spark.list_temp_view_names()
    fresh = b2.eager()
    assert local_view_name(fresh._cache_view) != view2
    assert _cache_view_count(spark) == 1


def test_eager_after_explicit_drop_materializes_afresh(spark: ReparkSession) -> None:
    """C-004 (D-4 tail): a wrapper over a dropped view is not 'already eager'."""
    lazy = spark.sql(_TWO_ROW_SQL)
    b = lazy.eager()
    c = b.eager()
    dropped = local_view_name(b._cache_view)
    b.unpersist()
    assert dropped not in spark.list_temp_view_names()
    renewed = c.eager()
    assert local_view_name(renewed._cache_view) != dropped
    assert renewed.count() == 2


def test_survivors_keep_the_registration_until_the_last_dies(
    spark: ReparkSession,
) -> None:
    """C-005: lazy copy, derived frame, join, and union each outlive the eager owner."""
    source = spark.sql(_TWO_ROW_SQL)

    eager = source.eager()
    view = local_view_name(eager._cache_view)
    lazy_copy = eager.lazy()
    del eager
    gc.collect()
    assert view in spark.list_temp_view_names()
    assert lazy_copy.count() == 2
    del lazy_copy
    gc.collect()
    assert view not in spark.list_temp_view_names()

    eager = source.eager()
    view = local_view_name(eager._cache_view)
    expected_schema = eager.to_arrow().schema
    derived = eager.filter("id > 0").select("id", "label")
    del eager
    gc.collect()
    assert view in spark.list_temp_view_names()
    assert derived.count() == 2
    assert derived.to_arrow().schema == expected_schema
    assert sorted(row.id for row in derived.collect()) == [1, 2]
    del derived
    gc.collect()
    assert view not in spark.list_temp_view_names()

    left = source.eager()
    right = spark.sql("SELECT 1 AS id").eager()
    left_view = local_view_name(left._cache_view)
    right_view = local_view_name(right._cache_view)
    joined = left.join(right, "id")
    del left, right
    gc.collect()
    assert left_view in spark.list_temp_view_names()
    assert right_view in spark.list_temp_view_names()
    assert sorted(row.id for row in joined.collect()) == [1]
    del joined
    gc.collect()
    assert left_view not in spark.list_temp_view_names()
    assert right_view not in spark.list_temp_view_names()

    left = source.eager()
    right = spark.sql("SELECT 3 AS id, 'z' AS label").eager()
    left_view = local_view_name(left._cache_view)
    right_view = local_view_name(right._cache_view)
    unioned = left.union(right)
    del left, right
    gc.collect()
    assert left_view in spark.list_temp_view_names()
    assert right_view in spark.list_temp_view_names()
    assert unioned.count() == 3
    del unioned
    gc.collect()
    assert _cache_view_names(spark) == []


def test_unpersist_and_clear_cache_are_idempotent(spark: ReparkSession) -> None:
    """C-006: double unpersist and double clearCache both no-op cleanly."""
    eager = spark.sql(_TWO_ROW_SQL).eager()
    view = local_view_name(eager._cache_view)
    eager.unpersist()
    eager.unpersist()
    assert view not in spark.list_temp_view_names()
    assert eager.count() == 2

    second = spark.sql(_TWO_ROW_SQL).eager()
    spark.catalog.clearCache()
    spark.catalog.clearCache()
    assert _cache_view_names(spark) == []
    assert second.count() == 2


def test_wrapper_unpersist_releases_only_its_own_hold(spark: ReparkSession) -> None:
    """C-006 (R11-D-1): unpersist on an eager-on-eager wrapper never drops a live view."""
    b = spark.sql(_TWO_ROW_SQL).eager()
    c = b.eager()
    view = local_view_name(b._cache_view)
    c.unpersist()
    assert view in spark.list_temp_view_names()
    assert c._cache_view is None
    assert c._eager_shape is None
    assert b.count() == 2
    assert c.count() == 2
    b.unpersist()
    assert view not in spark.list_temp_view_names()


def test_finalizer_never_calls_a_stopped_session(spark: ReparkSession) -> None:
    """C-006: after stop(), dying eager frames call no drop_temp_view; alive they do."""
    lazy = spark.sql(_TWO_ROW_SQL)
    spy = _DropViewSpy(lazy._session)
    lazy._session = spy
    alive_probe = lazy.eager()
    probe_view = alive_probe._cache_view
    del alive_probe
    gc.collect()
    assert spy.drop_calls == [probe_view]

    held = lazy.eager()
    spark.stop()
    del lazy, held
    gc.collect()
    assert spy.drop_calls == [probe_view]


def test_max_bytes_refusal_leaves_no_registration_or_handle(
    spark: ReparkSession,
) -> None:
    """C-007: a repark.cache.max_bytes refusal registers nothing and owns nothing."""
    spark.conf.set("repark.cache.max_bytes", "1")
    lazy = spark.sql(_TWO_ROW_SQL)
    with pytest.raises(IllegalArgumentException, match="max_bytes"):
        lazy.eager()
    assert _cache_view_names(spark) == []
    assert _live_handle_count(spark) == 0


def test_post_registration_failure_leaves_no_registration(spark: ReparkSession) -> None:
    """C-007: a failure binding the view scan drops the just-registered view."""
    lazy = spark.sql(_TWO_ROW_SQL)
    lazy._session = _FailScanOnCacheView(lazy._session)
    with pytest.raises(RuntimeError, match="post-registration scan failure"):
        lazy.eager()
    assert _cache_view_names(spark) == []
    assert _live_handle_count(spark) == 0


def test_simultaneous_eager_results_are_independent(spark: ReparkSession) -> None:
    """C-008: two live eager frames own distinct views; releasing one keeps the other."""
    lazy = spark.sql(_TWO_ROW_SQL)
    first = lazy.eager()
    second = lazy.eager()
    first_view = local_view_name(first._cache_view)
    second_view = local_view_name(second._cache_view)
    assert first_view != second_view
    assert _cache_view_count(spark) == 2
    del first
    gc.collect()
    assert _cache_view_names(spark) == [second_view]
    assert second.count() == 2
    assert sorted(row.id for row in second.collect()) == [1, 2]
    del second
    gc.collect()
    assert _cache_view_names(spark) == []


def test_source_change_between_eager_calls_is_observed(
    spark: ReparkSession,
    tmp_path: Path,
) -> None:
    """C-009 (D-5): two eager() calls evaluate twice; the first snapshot keeps old data."""
    csv_path = tmp_path / "eager_own_source.csv"
    csv_path.write_text("id\n1\n2\n", encoding="utf-8")
    lazy = spark.read.csv(str(csv_path), header=True, inferSchema=True)
    first = lazy.eager()
    csv_path.write_text("id\n9\n", encoding="utf-8")
    second = lazy.eager()
    assert sorted(row.id for row in first.collect()) == [1, 2]
    assert sorted(row.id for row in second.collect()) == [9]
    assert _cache_view_count(spark) == 2
    del first, second
    gc.collect()
    assert _cache_view_names(spark) == []


def test_exports_outlive_the_registration(spark: ReparkSession) -> None:
    """C-010: Arrow, pandas, polars, and collect exports read unchanged post-drop."""
    eager = spark.sql(_TWO_ROW_SQL).eager()
    arrow_table = eager.to_arrow()
    pandas_frame = eager.to_pandas()
    polars_frame = eager.to_polars()
    rows = eager.collect()
    view = local_view_name(eager._cache_view)
    del eager
    gc.collect()
    assert view not in spark.list_temp_view_names()
    assert sorted(arrow_table.column("id").to_pylist()) == [1, 2]
    assert sorted(pandas_frame["id"].tolist()) == [1, 2]
    assert sorted(polars_frame["id"].to_list()) == [1, 2]
    assert sorted(row.id for row in rows) == [1, 2]


def test_cached_frame_registration_dies_with_last_holder(spark: ReparkSession) -> None:
    """C-010 (D-3): a cache()d frame's view drops when the last holder dies."""
    frame = spark.sql(_TWO_ROW_SQL).cache()
    assert frame.count() == 2
    view = local_view_name(frame._cache_view)
    assert view in spark.list_temp_view_names()
    derived = frame.filter("id > 0")
    del frame
    gc.collect()
    assert view in spark.list_temp_view_names()
    assert derived.count() == 2
    del derived
    gc.collect()
    assert view not in spark.list_temp_view_names()
