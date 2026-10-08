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


_HOWS = ("inner", "left", "right", "full", "semi", "anti")
_LEFT = "`datafusion`.`public`.`_repark_jl_0`"
_RIGHT = "`datafusion`.`public`.`_repark_jr_0`"
_EXACT_KEYS = {
    f"({_LEFT}.`id` = {_RIGHT}.`x`)": ("id", "x", True),
    f"({_RIGHT}.`x` = {_LEFT}.`id`)": ("id", "x", False),
    f"({_LEFT}.`__repark_l_1_v` = {_RIGHT}.`V`)": ("__repark_l_1_v", "V", True),
}
_INEXACT_KEYS = (
    f"{_LEFT}.`id` = {_RIGHT}.`x`",
    f"({_LEFT}.`id` < {_RIGHT}.`x`)",
    f"({_LEFT}.`id` <=> {_RIGHT}.`x`)",
    f"({_LEFT}.`id` = {_LEFT}.`v`)",
    f"({_RIGHT}.`x` = {_RIGHT}.`y`)",
    f"({_LEFT}.`id` = 5)",
    f"(({_LEFT}.`id` = {_RIGHT}.`x`) AND ({_LEFT}.`v` > 5))",
    f"(({_LEFT}.`id` = {_RIGHT}.`x`) AND ({_LEFT}.`v` = {_RIGHT}.`y`))",
    f"(({_LEFT}.`id` + 1) = {_RIGHT}.`x`)",
    f"(CAST({_LEFT}.`id` AS BIGINT) = {_RIGHT}.`y`)",
    f"({_LEFT}.`a b` = {_RIGHT}.`x`)",
    f"({_LEFT}.`s`.`a` = {_RIGHT}.`x`)",
    "(`v` = `x`)",
    "v = x",
)


def _on_ids(left: Any, right: Any, how: str, reverse: bool) -> Any:
    condition = right["id"] == left["id"] if reverse else left["id"] == right["id"]
    return left.join(right, condition, how)


def _on_columns(left: Any, right: Any, left_name: str, right_name: str) -> Any:
    return left.join(right, left[left_name] == right[right_name])


def _on_aliases(left: Any, right: Any) -> Any:
    return left.join(right, spark_functions.col("a.id") == spark_functions.col("b.id"))


def _on_aliases_projected(left: Any, right: Any) -> Any:
    return _on_aliases(left, right).select("a.id", "a.v", "b.v")


def _on_both(left: Any, right: Any) -> Any:
    return left.join(right, (left["id"] == right["id"]) & (left["v"] == right["x"]))


def _on_key_and_literal(left: Any, right: Any) -> Any:
    return left.join(right, (left["id"] == right["id"]) & (left["v"] > 5))


def _on_less(left: Any, right: Any) -> Any:
    return left.join(right, left["id"] < right["id"])


def _on_null_safe(left: Any, right: Any) -> Any:
    return left.join(right, left["id"].eqNullSafe(right["id"]))


def _on_text(left: Any, right: Any) -> Any:
    return left.join(right, spark_functions.expr("v = x"))


def _on_bare_names(left: Any, right: Any) -> Any:
    return left.join(right, spark_functions.col("v") == spark_functions.col("x"))


def _on_arithmetic(left: Any, right: Any) -> Any:
    return left.join(right, left["id"] + 1 == right["id"])


def _on_cast(left: Any, right: Any) -> Any:
    return left.join(right, left["id"].cast("bigint") == right["y"])


def _on_one_side(left: Any, right: Any) -> Any:
    return left.join(right, left["id"] == left["v"])


def _on_greater_self(left: Any, right: Any) -> Any:
    return left.join(right, left["id"] > right["id"])


def _on_ambiguous_name(left: Any, right: Any) -> Any:
    return left.join(right, spark_functions.col("id") == spark_functions.col("id"))


def _on_foreign(left: Any, right: Any, foreign: Any) -> Any:
    return left.join(right, left["id"] == foreign["z"])


def _on_maps(left: Any, right: Any) -> Any:
    renamed = right.select(spark_functions.col("m").alias("m2"))
    return left.join(renamed, spark_functions.col("m") == spark_functions.col("m2"))


def _exact_shapes(frames: dict[str, Any]) -> dict[str, tuple[Callable[[], Any], tuple[Any, Any]]]:
    pair = (frames["base"], frames["other"])
    shapes: dict[str, tuple[Callable[..., Any], tuple[Any, Any]]] = {
        f"{how}_{'reversed' if reverse else 'written'}": (
            partial(_on_ids, how=how, reverse=reverse),
            pair,
        )
        for how in _HOWS
        for reverse in (False, True)
    }
    shapes["aliased"] = (_on_aliases, (frames["a"], frames["b"]))
    shapes["aliased_projected"] = (_on_aliases_projected, (frames["a"], frames["b"]))
    shapes["self"] = (partial(_on_ids, how="inner", reverse=False), (frames["base"],) * 2)
    shapes["twins"] = (partial(_on_ids, how="inner", reverse=False), (frames["twins"], pair[1]))
    shapes["int_bigint"] = (partial(_on_columns, left_name="id", right_name="y"), pair)
    shapes["string_int"] = (partial(_on_columns, left_name="Data", right_name="id"), pair)
    return {name: (partial(build, *sides), sides) for name, (build, sides) in shapes.items()}


def _inexact_shapes(
    frames: dict[str, Any],
) -> dict[str, tuple[Callable[[], Any], tuple[Any, Any]]]:
    pair = (frames["base"], frames["other"])
    shapes: dict[str, tuple[Callable[..., Any], tuple[Any, Any]]] = {
        "both": (_on_both, pair),
        "key_and_literal": (_on_key_and_literal, pair),
        "less": (_on_less, pair),
        "null_safe": (_on_null_safe, pair),
        "text": (_on_text, pair),
        "bare_names": (_on_bare_names, pair),
        "arithmetic": (_on_arithmetic, pair),
        "cast": (_on_cast, pair),
        "one_side": (_on_one_side, pair),
        "spaced_side": (partial(_on_ids, how="inner", reverse=False), (frames["spaced"], pair[0])),
    }
    return {name: (partial(build, *sides), sides) for name, (build, sides) in shapes.items()}


def test_exact_join_keys_are_one_equality_between_the_two_sides() -> None:
    for on_sql, keys in _EXACT_KEYS.items():
        assert join_attr_tokens._join_exact_keys(on_sql, _LEFT, _RIGHT) == keys, on_sql
    for on_sql in _INEXACT_KEYS:
        assert join_attr_tokens._join_exact_keys(on_sql, _LEFT, _RIGHT) is None, on_sql


def test_exact_key_join_plans_natively_with_the_sql_route_answers(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames = _frames(spark)
    answers = {}
    for name, (build, sides) in _exact_shapes(frames).items():
        routes, native, sql = _routes_and_answers(monkeypatch, build, sides)
        assert routes == [True], name
        assert native == sql, name
        answers[name] = native
    assert answers["inner_written"][3] == [(1, 10, "a", 1, 5, 6), (2, 20, "b", 2, None, 7)]
    assert answers["semi_reversed"][3] == [(1, 10, "a"), (2, 20, "b")]
    assert answers["anti_written"][3] == [(3, None, "v"), (None, 40, None)]
    assert answers["aliased_projected"][3] == [(1, 10, 10), (2, 20, 20), (3, None, None)]
    assert answers["aliased_projected"][7] == [0, 1, "minted"]
    assert answers["self"][7] == [0, 1, 2, "minted", "minted", "minted"]
    assert answers["int_bigint"][3] == [(2, 20, "b", None, 1, 2)]
    assert answers["string_int"][:2] == ("refused", "PySparkException")


def test_inexact_join_conditions_keep_the_sql_route(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames = _frames(spark)
    answers = {}
    for name, (build, sides) in _inexact_shapes(frames).items():
        routes, native, sql = _routes_and_answers(monkeypatch, build, sides)
        assert routes == [False], name
        assert native == sql, name
        assert native[0] != "refused", name
        answers[name] = native
    assert answers["key_and_literal"][3] == [(1, 10, "a", 1, 5, 6), (2, 20, "b", 2, None, 7)]
    assert answers["both"][3] == []
    assert answers["null_safe"][3] == [
        (1, 10, "a", 1, 5, 6),
        (2, 20, "b", 2, None, 7),
        (None, 40, None, None, 1, 2),
    ]


def test_join_refusals_are_the_sql_route_refusals(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frames = _frames(spark)
    base, other = frames["base"], frames["other"]
    refusals: dict[str, tuple[Callable[[], Any], tuple[Any, Any], list[bool], str]] = {
        "self_join": (partial(_on_greater_self, base, base), (base, base), [], "are ambiguous"),
        "ambiguous_name": (
            partial(_on_ambiguous_name, base, other),
            (base, other),
            [],
            "[AMBIGUOUS_REFERENCE]",
        ),
        "missing": (
            partial(_on_foreign, base, other, frames["lone"]),
            (base, other),
            [],
            "[MISSING_ATTRIBUTES.RESOLVED_ATTRIBUTE_MISSING_FROM_INPUT]",
        ),
        "map_key": (
            partial(_on_maps, frames["mapped"], frames["mapped"]),
            (frames["mapped"],) * 2,
            [False],
            "[DATATYPE_MISMATCH.INVALID_ORDERING_TYPE]",
        ),
    }
    for name, (build, sides, attempts, text) in refusals.items():
        routes, native, sql = _routes_and_answers(monkeypatch, build, sides)
        assert routes == attempts, name
        assert native == sql, name
        assert native[:2] == ("refused", "AnalysisException"), name
        assert text in native[2], name
