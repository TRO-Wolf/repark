from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from repark import ReparkSession, _native
from repark.errors import AnalysisException


@pytest.fixture
def spark() -> ReparkSession:
    return ReparkSession.builder.appName("pytest-attr-id-1-v456").getOrCreate()


def _twins(spark: ReparkSession) -> Any:
    return spark.createDataFrame([(1, 2, 3), (4, 5, 6)], ["id", "ID", "z"])


_F_REFUSES: list[tuple[str, Callable[[Any], Any]]] = [
    ("getitem_only", lambda tw: tw.select(tw["id"])),
    ("attr_only", lambda tw: tw.select(tw.id)),
    ("union_attr", lambda tw: tw.union(tw).select(tw.ID)),
    ("union_getitem", lambda tw: tw.union(tw).select(tw["ID"])),
    ("distinct_attr", lambda tw: tw.distinct().select(tw.id)),
    ("limit_attr", lambda tw: tw.limit(5).select(tw.id)),
    ("sort_attr", lambda tw: tw.orderBy("z").select(tw.id)),
    ("filter_attr_filter", lambda tw: tw.filter("z > 0").filter(tw.ID > 2)),
    ("distinct_drop", lambda tw: tw.distinct().drop(tw.ID)),
    ("distinct_order", lambda tw: tw.distinct().orderBy(tw.ID.desc()).select("z")),
    ("cache_attr", lambda tw: tw.cache().select(tw.ID)),
    ("repart_attr", lambda tw: tw.repartition(1).select(tw.ID)),
    ("dd_attr", lambda tw: tw.dropDuplicates().select(tw.ID)),
]

_T_BINDS: list[tuple[str, Callable[[Any], Any], list[list[int]]]] = [
    ("getitem_only", lambda tw: tw.select(tw["id"]), [[1], [4]]),
    ("attr_only", lambda tw: tw.select(tw.id), [[1], [4]]),
    ("union_attr", lambda tw: tw.union(tw).select(tw.ID), [[2], [2], [5], [5]]),
    ("union_getitem", lambda tw: tw.union(tw).select(tw["ID"]), [[2], [2], [5], [5]]),
    ("distinct_attr", lambda tw: tw.distinct().select(tw.id), [[1], [4]]),
    ("limit_attr", lambda tw: tw.limit(5).select(tw.id), [[1], [4]]),
    ("sort_attr", lambda tw: tw.orderBy("z").select(tw.id), [[1], [4]]),
    ("filter_attr_filter", lambda tw: tw.filter("z > 0").filter(tw.ID > 2), [[4, 5, 6]]),
    ("distinct_drop", lambda tw: tw.distinct().drop(tw.ID), [[1, 3], [4, 6]]),
    ("distinct_order", lambda tw: tw.distinct().orderBy(tw.ID.desc()).select("z"), [[6], [3]]),
    ("cache_attr", lambda tw: tw.cache().select(tw.ID), [[2], [5]]),
    ("repart_attr", lambda tw: tw.repartition(1).select(tw.ID), [[2], [5]]),
    ("dd_attr", lambda tw: tw.dropDuplicates().select(tw.ID), [[2], [5]]),
    ("pin_filter_getitem", lambda tw: tw.filter("1 > 0").select(tw["id"]), [[1], [4]]),
    ("pin_alias_getitem", lambda tw: tw.alias("t").select(tw["id"]), [[1], [4]]),
    ("pin_star_getitem", lambda tw: tw.select("*").select(tw["id"]), [[1], [4]]),
]


def _frames(spark: ReparkSession) -> tuple[Any, Any]:
    left = spark.createDataFrame([(1, 10), (2, 20), (3, 30)], ["id", "v"])
    right = spark.createDataFrame([(1, 100), (2, 200)], ["id", "w"])
    return left, right


def test_v4_condition_built_before_cache(spark: ReparkSession) -> None:
    """A join condition built before cache materialization still binds. C-045."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    left, right = _frames(spark)
    cond = left.id == right.id
    left.cache()
    left.count()
    got = sorted(tuple(row) for row in left.join(right, cond).select(left.v, right.w).collect())
    assert got == [(10, 100), (20, 200)]


def test_v4_drop_col_taken_before_cache(spark: ReparkSession) -> None:
    """`drop` of a Column taken before cache materialization still drops it. C-045."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    left, right = _frames(spark)
    column = left.v
    left.cache()
    left.count()
    assert left.join(right, left.id == right.id).drop(column).columns == ["id", "id", "w"]


def test_v4_col_taken_during_cache_binds_after_unpersist(spark: ReparkSession) -> None:
    """A Column taken during cache binds after unpersist (Spark `p7_cache`). C-045."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    left, right = _frames(spark)
    left.cache()
    left.count()
    column = left.id
    left.unpersist()
    got = sorted(tuple(row) for row in right.join(left, right.id == column).collect())
    assert got == [(1, 100, 1, 10), (2, 200, 2, 20)]


def test_v4_col_taken_during_cache_selects_after_unpersist(spark: ReparkSession) -> None:
    """A Column taken during cache selects on a child after unpersist (Spark `p7`). C-045."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    left, _right = _frames(spark)
    left.cache()
    left.count()
    column = left.v
    left.unpersist()
    got = sorted(tuple(row) for row in left.filter("id > 1").select(column).collect())
    assert got == [(20,), (30,)]


def test_v4_col_taken_before_checkpoint_binds(spark: ReparkSession) -> None:
    """A Column taken before `localCheckpoint` binds on the checkpoint (Spark `p7`). C-045."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    left, right = _frames(spark)
    column = left.id
    checked = left.localCheckpoint()
    got = sorted(tuple(row) for row in checked.join(right, column == right.id).collect())
    assert got == [(1, 10, 1, 100), (2, 20, 2, 200)]


def test_v4_cache_materialization_keeps_ids_continuous(spark: ReparkSession) -> None:
    """Cache, unpersist and checkpoint keep the frame's ids byte for byte. C-045."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    left, _right = _frames(spark)
    before = list(_native.attribute_ids(left._plan()))
    assert all(id is not None for id in before)
    left.cache()
    left.count()
    assert list(_native.attribute_ids(left._plan())) == before
    left.unpersist()
    assert list(_native.attribute_ids(left._plan())) == before
    left.localCheckpoint()
    assert list(_native.attribute_ids(left._plan())) == before


def test_v5_twin_parent_on_child_refuses_insensitive(spark: ReparkSession) -> None:
    """A case-twin parent Column on a child refuses as Spark does. pins: attr-id-1/C-044."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    tw = spark.createDataFrame([(1, 2, 3)], ["id", "ID", "z"])
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        tw.distinct().select(tw.id).collect()


@pytest.mark.parametrize("name,build", [pytest.param(n, b, id=n) for n, b in _F_REFUSES])
def test_v5_twin_cells_refuse_insensitive(
    spark: ReparkSession, name: str, build: Callable[[Any], Any]
) -> None:
    """Every `p5_twins` F_ cell refuses `AMBIGUOUS_REFERENCE` (live Spark 4.1.2). C-044."""
    spark.conf.set("spark.sql.caseSensitive", "false")
    with pytest.raises(AnalysisException, match="AMBIGUOUS_REFERENCE"):
        build(_twins(spark)).collect()


@pytest.mark.parametrize(
    "name,build,expected", [pytest.param(n, b, e, id=n) for n, b, e in _T_BINDS]
)
def test_v5_twin_cells_bind_sensitive(
    spark: ReparkSession, name: str, build: Callable[[Any], Any], expected: list[list[int]]
) -> None:
    """Every `p5_twins` T_ cell binds with Spark's rows (caseSensitive=true). C-044."""
    spark.conf.set("spark.sql.caseSensitive", "true")
    got = [list(row) for row in build(_twins(spark)).collect()]
    if name == "distinct_order":
        assert got == expected
    else:
        assert sorted(got) == sorted(expected)
