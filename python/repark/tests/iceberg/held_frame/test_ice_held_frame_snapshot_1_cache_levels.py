from __future__ import annotations

import itertools
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import catalog_surface
from repark.spark._temp_views import scratch_view_name
from repark.spark.dataframe import cache_handle

pytestmark = pytest.mark.xdist_group("ice_held_frame_snapshot_1_cache_levels")
MODES = ["true", "false"]
SPARK = {
    "bare_then_table": [2, 3, True, 3, True],
    "bare_then_is_cached": [2, True, True, 3],
    "bare_props_then_table": [2, 2, True, 2],
    "alias_then_table": [2, 3, True, 3],
}
_NAMES = itertools.count()


def _view_of(s: Any, t: str) -> Any:
    name = "v" + t.rsplit(".", 1)[1]
    s.table(t).createOrReplaceTempView(name)
    return s.table(name)


SHAPES: dict[str, tuple[Callable[[Any, str], Any], bool]] = {
    "bare": (lambda s, t: s.table(t), True),
    "read_table": (lambda s, t: s.read.table(t), True),
    "alias": (lambda s, t: s.table(t).alias("a"), True),
    "alias_twice": (lambda s, t: s.table(t).alias("a").alias("b"), True),
    "sql_star": (lambda s, t: s.sql(f"SELECT * FROM {t}"), False),
    "select": (lambda s, t: s.table(t).select("n", "c"), False),
    "filter": (lambda s, t: s.table(t).filter("n > 0"), False),
    "agg": (lambda s, t: s.table(t).groupBy("c").count(), False),
    "filter_alias": (lambda s, t: s.table(t).filter("n > 0").alias("a"), False),
    "df_view": (_view_of, False),
}


def _session(root: Path, ansi: str) -> ReparkSession:
    return (
        ReparkSession.builder.appName("ice-held-frame-snapshot-1-cache-levels")
        .config("spark.sql.catalog.lc", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lc.type", "hadoop")
        .config("spark.sql.catalog.lc.warehouse", str(root / "lc"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", ansi)
        .getOrCreate()
    )


def _make(s: Any) -> str:
    t = f"lc.db.l{next(_NAMES)}"
    s.sql(f"CREATE TABLE {t} (n INT, c STRING) USING iceberg").collect()
    s.sql(f"INSERT INTO {t} VALUES (1, 'a'), (2, 'b')").collect()
    return t


def _insert(s: Any, t: str) -> None:
    s.sql(f"INSERT INTO {t} VALUES (9, 'z')").collect()


def _bare_then_table(s: Any) -> list[Any]:
    t = _make(s)
    held = s.table(t).cache()
    first = held.count()
    _insert(s, t)
    fresh = s.table(t).count()
    return [first, fresh, held.is_cached, held.count(), s.catalog.isCached(t)]


def _bare_then_is_cached(s: Any) -> list[Any]:
    t = _make(s)
    held = s.table(t).cache()
    first = held.count()
    _insert(s, t)
    return [first, s.catalog.isCached(t), held.is_cached, held.count()]


def _bare_props_then_table(s: Any) -> list[Any]:
    t = _make(s)
    held = s.table(t).cache()
    first = held.count()
    s.sql(f"ALTER TABLE {t} SET TBLPROPERTIES ('k' = 'v')").collect()
    fresh = s.table(t).count()
    return [first, fresh, held.is_cached, held.count()]


def _alias_then_table(s: Any) -> list[Any]:
    t = _make(s)
    held = s.table(t).alias("a").cache()
    first = held.count()
    _insert(s, t)
    fresh = s.table(t).count()
    return [first, fresh, held.is_cached, held.count()]


CELLS = {
    "bare_then_table": _bare_then_table,
    "bare_then_is_cached": _bare_then_is_cached,
    "bare_props_then_table": _bare_props_then_table,
    "alias_then_table": _alias_then_table,
}


@pytest.fixture(params=MODES)
def spark(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Any]:
    session = _session(tmp_path, request.param)
    session.sql("CREATE NAMESPACE IF NOT EXISTS lc.db").collect()
    yield session
    session.stop()


@pytest.mark.parametrize("cell", sorted(CELLS))
def test_a_cached_table_frame_answers_as_spark_after_a_same_session_commit(
    spark: Any, cell: str
) -> None:
    found = CELLS[cell](spark)
    spark.catalog.clearCache()
    assert found == SPARK[cell], (cell, found)


@pytest.mark.parametrize("shape", sorted(SHAPES))
def test_each_shape_caches_at_the_level_spark_caches_it(spark: Any, shape: str) -> None:
    build, table_level = SHAPES[shape]
    frame = build(spark, _make(spark))
    assert cache_handle._table_level(frame) is table_level, shape
    native = frame._session
    levels: list[Any] = []

    class _Recording:
        def register_lazy_cache_view(self, *args: Any) -> Any:
            levels.append(args[3])
            return native.register_lazy_cache_view(*args)

        def __getattr__(self, name: str) -> Any:
            return getattr(native, name)

    frame._session = _Recording()
    try:
        frame.cache()
    finally:
        frame._session = native
    assert levels == [table_level], shape
    assert frame.count() >= 1
    assert cache_handle._held_by_table_fill(frame) is table_level, shape
    spark.catalog.clearCache()


@pytest.mark.parametrize("statement", [None, "CACHE TABLE {t}", "CACHE LAZY TABLE {t}"])
def test_a_catalog_cache_of_the_table_caches_at_the_table_level(
    spark: Any, statement: str | None
) -> None:
    t = _make(spark)
    if statement is None:
        spark.catalog.cacheTable(t)
    else:
        spark.sql(statement.format(t=t)).collect()
    resolved = spark.resolve_table_name(t, prefer_temp_view=True)
    frame = catalog_surface._cached_tables(spark._alive_token)[resolved][0]
    assert cache_handle._table_level(frame) is True
    assert cache_handle._held_by_table_fill(frame) is True
    spark.catalog.clearCache()


def test_an_eager_cache_is_not_held_by_a_table_level_fill(spark: Any) -> None:
    t = _make(spark)
    frame = spark.table(t)
    frame._persist_requested = True
    cache_handle._materialize_now(frame, scratch_view_name(frame._session, "__repark_cache_"))
    assert cache_handle._table_level(frame) is True
    assert frame.count() == 2
    assert cache_handle._held_by_table_fill(frame) is False
    _insert(spark, t)
    assert spark.table(t).count() == 3
    assert frame.is_cached is False
    spark.catalog.clearCache()
