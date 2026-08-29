"""R-PERF-CACHE + CACHE1 cache-honesty — cache / persist / unpersist / clearCache.

Oracle shapes: live PySpark 4.1.2 (return self, is_cached, storageLevel repr for
MEMORY-backed frames). Object-identity only — disclosed.
"""

from __future__ import annotations

import time
import warnings

import pytest

from repark import ReparkSession, StorageLevel
from repark.spark._temp_views import local_view_name


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-cache").getOrCreate()
    yield session
    session.stop()


def _expensive_frame(spark: ReparkSession, rows: int = 8_000):
    """A plan that is not pre-MemTable'd (sql UNION chain) so cache timing is meaningful."""
    parts = [f"SELECT {index} AS id, 'r{index}' AS label" for index in range(min(rows, 200))]
    spark.sql("SELECT 1 AS k UNION ALL SELECT 2 UNION ALL SELECT 3").createOrReplaceTempView("side")
    base = " UNION ALL ".join(parts)
    spark.sql(base).createOrReplaceTempView("base_rows")
    return spark.sql("SELECT b.id, b.label, s.k FROM base_rows b CROSS JOIN side s")


def test_cache_returns_self_and_is_cached(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    assert frame.is_cached is False
    assert frame.storageLevel == StorageLevel.NONE
    same = frame.cache()
    assert same is frame
    assert frame.is_cached is True
    # Oracle cache() → MEMORY_AND_DISK_DESER: "Disk Memory Deserialized 1x Replicated"
    assert "Memory" in repr(frame.storageLevel)
    assert "Deserialized" in repr(frame.storageLevel)


def test_persist_records_level(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id").persist(StorageLevel.MEMORY_ONLY)
    assert frame.is_cached is True
    assert frame.storageLevel == StorageLevel.MEMORY_ONLY
    assert "Memory" in repr(frame.storageLevel)
    assert "Serialized" in repr(frame.storageLevel)


def test_take_materializes_cache(spark: ReparkSession) -> None:
    """take/isEmpty are actions — they must fill the MemTable (octo C1-Q-004)."""
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
    assert frame.is_cached is True
    assert frame._cache_view is None
    taken = {row[0] for row in frame.take(2)}
    assert taken == {1, 2}
    assert frame._cache_view is not None
    assert frame.isEmpty() is False


def test_second_action_after_cache_is_cheap(spark: ReparkSession) -> None:
    frame = _expensive_frame(spark, rows=200)
    frame.cache()
    assert frame.count() == 600  # 200 * 3
    t0 = time.perf_counter()
    assert frame.count() == 600
    second = time.perf_counter() - t0
    assert second < 2.0, f"second count after cache took {second:.3f}s — not MemTable?"


def test_derived_after_materialize_reads_cached(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3").cache()
    frame.count()  # materialize
    derived = frame.filter("id > 1")
    assert derived.count() == 2
    assert sorted(r[0] for r in derived.collect()) == [2, 3]


def test_unpersist_clears_mark_and_restores(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
    assert frame.count() == 2
    assert frame.is_cached is True
    same = frame.unpersist()
    assert same is frame
    assert frame.is_cached is False
    assert frame.storageLevel == StorageLevel.NONE
    # Still correct after restoring lineage.
    assert frame.count() == 2
    # Idempotent.
    frame.unpersist()
    assert frame.is_cached is False


def test_local_checkpoint_eager_not_is_cached(spark: ReparkSession) -> None:
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    out = frame.localCheckpoint(eager=True)
    assert out is frame
    assert frame.is_cached is False
    assert frame.count() == 2
    # Second action still works (lineage truncated to MemTable scan).
    assert {row[0] for row in frame.collect()} == {1, 2}


def test_local_checkpoint_after_cache_truncates_lineage(spark: ReparkSession) -> None:
    """localCheckpoint after a materialized cache must truncate lineage (C4-L-001).

    Mutation: early-return on any `_cache_view` → sticky `_checkpoint_lazy`, lineage kept,
    view stays under ``__repark_cache_*`` (clearCache would drop a "checkpoint").
    """
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
    assert frame.count() == 2
    old_cache_view = frame._cache_view
    assert old_cache_view is not None
    # R7-1: the handle carries the HOME-qualified spelling (`"datafusion"."public"."…"`) so the
    # scan that follows the materialize cannot be re-resolved against a `SET` default catalog;
    # `list_temp_view_names` still answers one-part names, so compare on the local name.
    old_cache_local = local_view_name(old_cache_view)
    assert old_cache_local.startswith("__repark_cache_")
    frame.localCheckpoint(eager=True)
    assert frame.is_cached is False
    assert frame._checkpoint_lazy is False
    assert frame._lineage_inner is None
    assert frame._cache_view is None
    assert old_cache_local not in spark.list_temp_view_names()
    ckpt_views = [n for n in spark.list_temp_view_names() if n.startswith("__repark_ckpt_")]
    assert ckpt_views, "checkpoint must register a __repark_ckpt_* MemTable"
    assert frame.count() == 2


def test_cache_type_error_on_bad_level(spark: ReparkSession) -> None:
    from repark.errors import PySparkTypeError

    frame = spark.sql("SELECT 1 AS id")
    with pytest.raises(PySparkTypeError, match="StorageLevel"):
        frame.persist("MEMORY_ONLY")  # type: ignore[arg-type]


def test_object_identity_only_no_shared_cache(spark: ReparkSession) -> None:
    """Two separately-built identical plans do not share a cache (disclosed)."""
    a = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    b = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    a.cache()
    a.count()
    assert a.is_cached is True
    assert b.is_cached is False


def test_cache_transform_child_does_not_inherit_mark(spark: ReparkSession) -> None:
    """Object-identity: filter child is not cached (octo C2-L-002)."""
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
    child = frame.filter("id > 0")
    assert frame.is_cached is True
    assert child.is_cached is False
    assert child.count() == 2
    # Parent still lazy-marked until an action on *parent* materializes.
    assert frame._cache_view is None or frame.is_cached is True


# === CACHE1: cache-honesty ===


def test_clear_cache_drops_session_cache_views(spark: ReparkSession) -> None:
    """clearCache really drops __repark_cache_* MemTables and resets live handles (Q11)."""
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
    assert frame.count() == 2
    assert frame._cache_view is not None
    # R7-1: the handle keeps the HOME-qualified spelling; the session's name list is one-part.
    view_name = local_view_name(frame._cache_view)
    assert view_name in spark.list_temp_view_names()
    assert spark.catalog.clearCache() is None
    assert frame.is_cached is False
    assert frame._cache_view is None
    assert view_name not in spark.list_temp_view_names()
    # After clearCache the plan re-executes (lineage restored).
    assert frame.count() == 2


def test_clear_cache_clears_lazy_mark_before_materialize(spark: ReparkSession) -> None:
    """clearCache also clears persist marks that have not yet materialize'd."""
    frame = spark.sql("SELECT 1 AS id").cache()
    assert frame.is_cached is True
    assert frame._cache_view is None
    spark.catalog.clearCache()
    assert frame.is_cached is False


def test_clear_cache_drops_orphan_cache_views(spark: ReparkSession) -> None:
    """clearCache drops __repark_cache_* even when the handle was GC'd (Q11 orphan path).

    Mutation: remove the orphan prefix loop in Catalog.clear_cache → this REDS while
    live-handle clearCache pins stay green.
    """
    import gc
    import weakref

    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
    assert frame.count() == 2
    assert frame._cache_view is not None
    view_name = local_view_name(frame._cache_view)  # R7-1 home-qualified handle → local name
    assert view_name in spark.list_temp_view_names()
    proxy = weakref.ref(frame)
    del frame
    gc.collect()
    assert proxy() is None, "handle must be GC'd so only the orphan MemTable remains"
    assert view_name in spark.list_temp_view_names()
    spark.catalog.clearCache()
    assert view_name not in spark.list_temp_view_names()


def test_clear_cache_leaves_checkpoint_views(spark: ReparkSession) -> None:
    """clearCache must not drop __repark_ckpt_* (checkpoint is not a session cache pin)."""
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    frame.localCheckpoint(eager=True)
    temps_before = set(spark.list_temp_view_names())
    ckpt_views = [name for name in temps_before if name.startswith("__repark_ckpt_")]
    assert ckpt_views, "eager localCheckpoint must register a __repark_ckpt_* MemTable"
    # Also pin a real cache so clearCache does real work.
    cached = spark.sql("SELECT 3 AS id").cache()
    cached.count()
    cache_view = cached._cache_view
    assert cache_view is not None
    spark.catalog.clearCache()
    temps_after = set(spark.list_temp_view_names())
    for name in ckpt_views:
        assert name in temps_after, f"checkpoint view {name} must survive clearCache"
    assert cache_view not in temps_after
    assert frame.count() == 2


def test_storage_level_disk_warns_once_per_session(spark: ReparkSession) -> None:
    """Cosmetic disk/off-heap/replication flags warn once per session (OTH-005)."""
    with pytest.warns(UserWarning, match="MemTable"):
        spark.sql("SELECT 1 AS id").persist(StorageLevel.MEMORY_AND_DISK)
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        spark.sql("SELECT 2 AS id").persist(StorageLevel.DISK_ONLY)


def test_storage_level_memory_only_does_not_warn(spark: ReparkSession) -> None:
    """MEMORY_ONLY replication=1 is honest — no cosmetic warning."""
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        spark.sql("SELECT 1 AS id").persist(StorageLevel.MEMORY_ONLY)


def test_cache_max_bytes_refuses_oversized_materialize(spark: ReparkSession) -> None:
    """repark.cache.max_bytes fails loud when collected size exceeds the budget (OTH-014)."""
    from repark.errors import IllegalArgumentException

    spark.conf.set("repark.cache.max_bytes", "1")
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3").cache()
    with pytest.raises(IllegalArgumentException, match=r"repark\.cache\.max_bytes"):
        frame.count()
    # Failed materialize must not leave a cache pin; lineage must not be half-committed.
    assert frame._cache_view is None
    assert frame._lineage_inner is None
    assert frame.is_cached is True  # mark remains until unpersist (Spark-like)


def test_cache_max_bytes_zero_disables_guard(spark: ReparkSession) -> None:
    """repark.cache.max_bytes=0 means no size guard (same as unset)."""
    spark.conf.set("repark.cache.max_bytes", "0")
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
    assert frame.count() == 2
    assert frame._cache_view is not None


def test_cache_max_bytes_invalid_conf_refuses(spark: ReparkSession) -> None:
    """Non-integer / negative / >u64 repark.cache.max_bytes fails loud with the named key."""
    from repark.errors import IllegalArgumentException

    spark.conf.set("repark.cache.max_bytes", "not-a-budget")
    frame = spark.sql("SELECT 1 AS id").cache()
    with pytest.raises(IllegalArgumentException, match=r"repark\.cache\.max_bytes"):
        frame.count()
    spark.conf.set("repark.cache.max_bytes", "-1")
    frame2 = spark.sql("SELECT 2 AS id").cache()
    with pytest.raises(IllegalArgumentException, match=r"repark\.cache\.max_bytes"):
        frame2.count()
    # Must not leak as raw PyO3 OverflowError (C2-Q-001).
    spark.conf.set("repark.cache.max_bytes", str(2**64))
    frame3 = spark.sql("SELECT 3 AS id").cache()
    with pytest.raises(IllegalArgumentException, match=r"repark\.cache\.max_bytes"):
        frame3.count()


def test_cache_max_bytes_builder_config(spark: ReparkSession) -> None:
    """Builder ``.config(repark.cache.max_bytes, …)`` is honored (not only spark.conf.set)."""
    from repark.errors import IllegalArgumentException

    # Fresh session: fixture already built one; builder path needs its own getOrCreate after stop.
    spark.stop()
    built = (
        ReparkSession.builder.appName("pytest-cache-builder-max")
        .config("repark.cache.max_bytes", "1")
        .getOrCreate()
    )
    try:
        frame = built.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
        with pytest.raises(IllegalArgumentException, match=r"repark\.cache\.max_bytes"):
            frame.count()
    finally:
        built.stop()


def test_cache_max_bytes_unset_does_not_resurrect_builder(spark: ReparkSession) -> None:
    """spark.conf.unset must disable the guard even when builder still carries the key."""
    spark.stop()
    built = (
        ReparkSession.builder.appName("pytest-cache-unset-max")
        .config("repark.cache.max_bytes", "1")
        .getOrCreate()
    )
    try:
        built.conf.unset("repark.cache.max_bytes")
        frame = built.sql("SELECT 1 AS id UNION ALL SELECT 2").cache()
        # Without tomb honor, builder "1" would refuse materialize.
        assert frame.count() == 2
        assert frame._cache_view is not None
    finally:
        built.stop()


def test_cache_materialize_uses_cache_entry_point_not_temp_view(spark: ReparkSession) -> None:
    """Caller-level branch: cache path must call materialize_as_cache_view only (R-PERF-VALUES).

    PyO3 methods are read-only on the native object — wrap via a session proxy on the handle.
    """
    frame = spark.sql("SELECT 1 AS id UNION ALL SELECT 2")
    real_session = frame._session
    calls = {"cache": 0, "temp": 0}

    class _SessionProxy:
        def materialize_as_cache_view(self, *args: object, **kwargs: object) -> object:
            calls["cache"] += 1
            return real_session.materialize_as_cache_view(*args, **kwargs)

        def materialize_as_temp_view(self, *args: object, **kwargs: object) -> object:
            calls["temp"] += 1
            return real_session.materialize_as_temp_view(*args, **kwargs)

        def __getattr__(self, name: str) -> object:
            return getattr(real_session, name)

    frame._session = _SessionProxy()  # type: ignore[assignment]
    try:
        frame.cache()
        assert frame.count() == 2
        assert calls["cache"] == 1, "cache materialize must use materialize_as_cache_view"
        assert calls["temp"] == 0, "cache path must not call materialize_as_temp_view"
    finally:
        frame._session = real_session


def test_cache_child_plan_sharing_out_divergence(spark: ReparkSession) -> None:
    """Architectural: child of cached parent does not share the cache mark (CACHE1 §4)."""
    parent = spark.sql("SELECT 1 AS id UNION ALL SELECT 2 UNION ALL SELECT 3").cache()
    parent.count()  # materialize parent
    child = parent.filter("id > 1")
    assert parent.is_cached is True
    assert parent._cache_view is not None
    assert child.is_cached is False
    assert child._cache_view is None
    assert child.count() == 2
