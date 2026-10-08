from __future__ import annotations

import re
from collections.abc import Callable, Iterator
from functools import partial
from typing import Any

import pytest

from repark import ReparkSession, _native
from repark.errors import PySparkException
from repark.spark import functions as spark_functions
from repark.spark.dataframe import join_attr_tokens

_SCRATCH = re.compile(r"_repark_(?:jl|jr|explain)_[0-9a-f]{12,32}|__repark_[lr]_[0-9a-f]{12}_")


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-stamp-2-r5p6-2").getOrCreate()
    yield session
    session.stop()


def _frames(spark: ReparkSession) -> dict[str, Any]:
    base = spark.createDataFrame(
        [(1, 10, "a"), (2, 20, "b"), (3, None, "v"), (None, 40, None)],
        "id INT, v INT, Data STRING",
    )
    other = spark.createDataFrame(
        [(1, 5, 6), (2, None, 7), (None, 1, 2)], "id INT, x INT, y BIGINT"
    )
    renamed = spark.createDataFrame([(1, 2)], "id INT, ab INT").withColumnRenamed("ab", "a b")
    return {
        "base": base,
        "other": other,
        "pair": base.select("id", "v"),
        "big": base.select(spark_functions.col("v")).filter(spark_functions.col("v") > 15),
        "twins": base.select("id", "v", "v"),
        "a": base.select("id", "v").alias("a"),
        "b": base.select("id", "v").alias("b"),
        "mapped": spark.createDataFrame([(1, {"a": 1})], "id INT, m MAP<STRING,INT>"),
        "spaced": renamed,
        "lone": spark.createDataFrame([(1,)], "z INT"),
    }


def _answer(frame: Any, sides: tuple[Any, Any]) -> tuple[Any, ...]:
    held = list(_native.attribute_ids(frame._plan()))
    sources = [attr for side in sides for attr in _native.attribute_ids(side._plan())]
    origins = [sources.index(attr) if attr in sources else "minted" for attr in held]
    return (
        frame.columns,
        frame.dtypes,
        frame.schema.jsonValue(),
        sorted(map(tuple, frame.collect()), key=repr),
        frame._display_names,
        _masked(frame._engine_names),
        _masked(list(_native.logical_column_names(frame._plan()))),
        origins,
        [held.index(attr) for attr in held],
        _native.frame_case_sensitive(frame._plan()),
        _SCRATCH.sub("_", frame._explain_text(True)),
    )


def _masked(names: list[str] | None) -> list[str] | None:
    if names is None:
        return None
    return [_SCRATCH.sub("_", name) for name in names]


def _outcome(build: Callable[[], Any], sides: tuple[Any, Any]) -> tuple[Any, ...]:
    try:
        return _answer(build(), sides)
    except PySparkException as error:
        return ("refused", type(error).__name__, str(error))


def _spy_exact(routes: list[bool], exact: Any, *args: Any) -> Any:
    planned = exact(*args)
    routes.append(planned is not None)
    return planned


def _no_exact(*_: Any) -> None:
    return None


def _routes_and_answers(
    monkeypatch: pytest.MonkeyPatch, build: Callable[[], Any], sides: tuple[Any, Any]
) -> tuple[list[bool], tuple[Any, ...], tuple[Any, ...]]:
    routes: list[bool] = []
    with monkeypatch.context() as patch:
        exact = partial(_spy_exact, routes, join_attr_tokens._join_exact_plan)
        patch.setattr(join_attr_tokens, "_join_exact_plan", exact)
        native = _outcome(build, sides)
    with monkeypatch.context() as patch:
        patch.setattr(join_attr_tokens, "_join_exact_plan", _no_exact)
        sql = _outcome(build, sides)
    return routes, native, sql


def _cross(left: Any, right: Any) -> Any:
    return left.crossJoin(right)


def _cross_projected(left: Any, right: Any) -> Any:
    return left.crossJoin(right).select("a.id", "b.v")


def _join_how(left: Any, right: Any, how: str | None) -> Any:
    return left.join(right, how=how)


def _join_no_keys(left: Any, right: Any) -> Any:
    return left.join(right, [])


def _cross_on_condition(left: Any, right: Any) -> Any:
    return left.join(right, left["id"] == right["id"], "cross")


def _cross_shapes(frames: dict[str, Any]) -> dict[str, tuple[Callable[[], Any], tuple[Any, Any]]]:
    shapes: dict[str, tuple[Callable[..., Any], tuple[Any, Any]]] = {
        "filtered": (_cross, (frames["pair"], frames["big"])),
        "self": (_cross, (frames["base"], frames["base"])),
        "twins": (_cross, (frames["twins"], frames["other"])),
        "aliased": (_cross, (frames["a"], frames["b"])),
        "aliased_projected": (_cross_projected, (frames["a"], frames["b"])),
        "map_side": (_cross, (frames["mapped"], frames["base"])),
        "no_condition": (partial(_join_how, how=None), (frames["base"], frames["other"])),
        "how_cross": (partial(_join_how, how="cross"), (frames["base"], frames["other"])),
        "no_keys": (_join_no_keys, (frames["base"], frames["other"])),
        "condition_how_cross": (_cross_on_condition, (frames["base"], frames["other"])),
    }
    return {name: (partial(build, *sides), sides) for name, (build, sides) in shapes.items()}


def test_cross_join_plans_natively_with_the_sql_route_answers(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    spark.conf.set("spark.sql.crossJoin.enabled", "true")
    frames = _frames(spark)
    answers = {}
    for name, (build, sides) in _cross_shapes(frames).items():
        routes, native, sql = _routes_and_answers(monkeypatch, build, sides)
        assert routes == [True], name
        assert native == sql, name
        answers[name] = native
    assert answers["filtered"][3] == sorted(
        [
            (key, value, big)
            for key, value in ((1, 10), (2, 20), (3, None), (None, 40))
            for big in (20, 40)
        ],
        key=repr,
    )
    assert answers["filtered"][7] == [0, 1, "minted"]
    assert answers["self"][7] == [0, 1, 2, "minted", "minted", "minted"]
    assert answers["twins"][7:9] == ([0, 1, 1, 3, 4, 5], [0, 1, 1, 3, 4, 5])


def test_cross_join_over_an_inexact_side_keeps_the_sql_route(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames = _frames(spark)
    spark.conf.set("spark.sql.caseSensitive", "true")
    cased = frames["pair"].withColumn("V", spark_functions.lit(7))
    assert cased.columns == ["id", "v", "V"]
    for name, left in (("spaced", frames["spaced"]), ("case_twins", cased)):
        sides = (left, frames["other"])
        routes, native, sql = _routes_and_answers(monkeypatch, partial(_cross, *sides), sides)
        assert routes == [False], name
        assert native == sql, name
        assert native[0] != "refused", name


def test_native_cross_join_carries_the_session_state_of_the_join_call(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames = _frames(spark)
    sides = (frames["base"], frames["other"])
    assert not _native.frame_case_sensitive(frames["base"]._plan())
    spark.conf.set("spark.sql.caseSensitive", "true")
    routes, native, sql = _routes_and_answers(monkeypatch, partial(_cross, *sides), sides)
    assert routes == [True]
    assert native == sql
    assert native[9] is True


def test_raising_native_cross_join_keeps_the_sql_route_answer(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames = _frames(spark)
    sides = (frames["pair"], frames["big"])
    monkeypatch.setattr(join_attr_tokens, "_join_exact_native", _raise_native)
    routes, native, sql = _routes_and_answers(monkeypatch, partial(_cross, *sides), sides)
    assert routes == [False]
    assert native == sql
    assert native[0] == ["id", "v", "v"]


def _raise_native(*_: Any) -> None:
    raise PySparkException("the native door raised")
