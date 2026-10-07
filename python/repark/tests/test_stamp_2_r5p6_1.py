from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime
from decimal import Decimal
from functools import partial
from typing import Any

import pytest

from repark import ReparkSession, _native
from repark.errors import PySparkException
from repark.spark import functions as spark_functions
from repark.spark.dataframe import join_attr_tokens

_NATIVE_CASTS = ("tinyint", "smallint", "int", "bigint", "float", "double", "decimal(10,2)")
_SQL_CAST_TWINS = ("TIMESTAMP", "DATE", "TIMESTAMP_NTZ", "void")
_CAST_LITERALS = (0, 1, 999999999)
_SORT_TRACES = (
    "sort_hits_meet_at_join",
    "sort_input_carries_twice",
    "sort_sourced_twin_engine",
    "sort_project_input_spelling",
)


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-stamp-2-r5p6-1").getOrCreate()
    yield session
    session.stop()


def _frames(spark: ReparkSession) -> dict[str, Any]:
    base = spark.createDataFrame([(1, 10, "a"), (2, None, "b")], "id INT, v INT, Data STRING")
    other = spark.createDataFrame([(1, 5, 6), (2, None, 7)], "id INT, x INT, y INT")
    twins = base.select("id", "v", "v")
    return {
        "twins": twins,
        "star": base.select("*", "v"),
        "appended": twins.withColumn("w", spark_functions.lit(1)),
        "union_other": twins.union(other.select("id", "x", "y")),
        "self_join": base.select("id", "v").join(base.select("id", "v"), "id"),
    }


def _answer(frame: Any) -> tuple[Any, ...]:
    held = list(_native.attribute_ids(frame._plan()))
    shared = [held.index(attr) if attr is not None else None for attr in held]
    return (
        frame.columns,
        frame.dtypes,
        sorted(map(tuple, frame.collect()), key=repr),
        frame._display_names,
        frame._engine_names,
        list(_native.logical_column_names(frame._plan())),
        shared,
    )


def _outcome(build: Callable[[], Any]) -> tuple[Any, ...]:
    try:
        return _answer(build())
    except PySparkException as error:
        return ("refused", type(error).__name__, str(error))


def _spy_exact(
    routes: list[bool], exact: Any, frame: Any, projected: list[Any], engine_names: list[str]
) -> Any:
    child = exact(frame, projected, engine_names)
    routes.append(child is not None)
    return child


def _no_exact(*_: Any) -> None:
    return None


def _spy_trace(traces: list[str], name: str, native: Any, *args: Any) -> Any:
    traces.append(name)
    return native(*args)


def _routes_and_answers(
    monkeypatch: pytest.MonkeyPatch, build: Callable[[], Any]
) -> tuple[list[bool], tuple[Any, ...], tuple[Any, ...]]:
    routes: list[bool] = []
    with monkeypatch.context() as patch:
        exact = partial(_spy_exact, routes, join_attr_tokens._attr_exact_child)
        patch.setattr(join_attr_tokens, "_attr_exact_child", exact)
        native = _outcome(build)
    with monkeypatch.context() as patch:
        patch.setattr(join_attr_tokens, "_attr_exact_child", _no_exact)
        sql = _outcome(build)
    return routes, native, sql


def _over_frames(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch, build: Callable[[Any], Any]
) -> dict[str, tuple[list[bool], tuple[Any, ...], tuple[Any, ...]]]:
    return {
        name: _routes_and_answers(monkeypatch, partial(build, frame))
        for name, frame in _frames(spark).items()
    }


def _fill_zero(frame: Any) -> Any:
    return frame.fillna(0, subset=["v"])


def _fill_mapping(frame: Any) -> Any:
    return frame.fillna({"v": 7})


def _fill_decimal(frame: Any) -> Any:
    return frame.fillna(1.5, subset=["v"])


def _coalesce_id(frame: Any, value: Any) -> Any:
    filled = spark_functions.coalesce(frame["id"], spark_functions.lit(value)).alias("c")
    return frame.select(filled, "v")


def _coalesce_spaced(frame: Any) -> Any:
    filled = spark_functions.coalesce(frame["id"], spark_functions.lit(3)).alias("a b")
    return frame.select(filled)


def _coalesce_second_twin(frame: Any) -> Any:
    bounds = frame._iter_bound_columns()
    filled = spark_functions.coalesce(bounds[2], spark_functions.lit(3)).alias("c")
    return frame.select(bounds[0], filled)


def _cast_twins(spark: ReparkSession, kind: str) -> Any:
    if kind == "void":
        base = spark.createDataFrame([(1,)], "i INT").withColumn("x", spark_functions.lit(None))
    else:
        base = spark.createDataFrame([(1, None)], f"i INT, x {kind}")
    return base.select("i", "x", "x")


def _coalesce_cast(frame: Any, value: Any, target: str) -> Any:
    filled = spark_functions.coalesce(frame["x"], spark_functions.lit(value).cast(target))
    return frame.select(filled.alias("c"), "i")


def _coalesce_bool(frame: Any) -> Any:
    return frame.select(spark_functions.coalesce(frame["b"], spark_functions.lit(0)))


def test_twin_fill_skips_the_sql_replan_with_equal_answers(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    results = _over_frames(spark, monkeypatch, _fill_zero)
    for name, (routes, native, sql) in results.items():
        assert native == sql, name
        assert routes == ([] if native[0] == "refused" else [True]), name
    assert [name for name, result in results.items() if result[1][0] == "refused"] == ["self_join"]
    assert results["union_other"][1][2] == [(1, 10, 10), (1, 5, 5), (2, 0, 0), (2, 0, 0)]


def test_twin_fill_mapping_skips_the_sql_replan_with_equal_answers(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    for name, (routes, native, sql) in _over_frames(spark, monkeypatch, _fill_mapping).items():
        assert native == sql, name
        assert routes == ([] if native[0] == "refused" else [True]), name


def test_inexact_fill_literal_keeps_the_sql_replan(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    results = _over_frames(spark, monkeypatch, _fill_decimal)
    for name, (routes, native, sql) in results.items():
        assert native == sql, name
        assert routes == ([] if native[0] == "refused" else [False]), name
    assert results["twins"][1][2] == [
        (1, Decimal("10.0"), Decimal("10.0")),
        (2, Decimal("1.5"), Decimal("1.5")),
    ]


def test_select_coalesce_route_follows_the_literal_shape(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    twins = _frames(spark)["twins"]
    for value, exact in ((1.5, False), (3, True)):
        routes, native, sql = _routes_and_answers(monkeypatch, partial(_coalesce_id, twins, value))
        assert routes == [exact], value
        assert native == sql, value
    assert _coalesce_id(twins, 1.5).dtypes[0] == ("c", "decimal(11,1)")
    routes, native, sql = _routes_and_answers(monkeypatch, partial(_coalesce_spaced, twins))
    assert routes == [False]
    assert native == sql
    assert native[4] == ["__repark_sel_0"]


def test_second_twin_reference_keeps_the_sql_replan(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    frame = _frames(spark)["union_other"]
    routes, native, sql = _routes_and_answers(monkeypatch, partial(_coalesce_second_twin, frame))
    assert routes == [False]
    assert native == sql
    assert native[2] == [(1, 10), (1, 5), (2, 3), (2, 3)]


def test_unique_sort_key_binds_without_the_lineage_trace(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    traces: list[str] = []
    for name in _SORT_TRACES:
        monkeypatch.setattr(
            _native, name, partial(_spy_trace, traces, name, getattr(_native, name))
        )
    base = spark.createDataFrame([(1, 30), (2, 10), (3, 20)], "id INT, v INT")
    unique = base.select("id", "v").orderBy("v")
    assert [tuple(row) for row in unique.collect()] == [(2, 10), (3, 20), (1, 30)]
    assert traces == []
    ambiguous = base.select("id", "v", (spark_functions.col("v") * -1).alias("v"))
    rows = [tuple(row) for row in ambiguous.orderBy("v").collect()]
    assert rows == [(2, 10, -10), (3, 20, -20), (1, 30, -30)]
    assert traces


def test_coalesce_cast_to_a_native_type_skips_the_sql_replan(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    for target in _NATIVE_CASTS:
        twins = _cast_twins(spark, target)
        for value in _CAST_LITERALS:
            build = partial(_coalesce_cast, twins, value, target)
            routes, native, sql = _routes_and_answers(monkeypatch, build)
            assert routes == [True], (target, value)
            assert native == sql, (target, value)


def test_coalesce_cast_to_a_datetime_keeps_the_sql_replan(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    seconds = {1: datetime(1970, 1, 1, 0, 0, 1), 999999999: datetime(2001, 9, 9, 1, 46, 39)}
    for kind in _SQL_CAST_TWINS:
        twins = _cast_twins(spark, kind)
        for value, expected in seconds.items():
            build = partial(_coalesce_cast, twins, value, "timestamp")
            routes, native, sql = _routes_and_answers(monkeypatch, build)
            assert routes == [False], (kind, value)
            assert native == sql, (kind, value)
            assert native[2] == [(expected, 1)], (kind, value)


def test_raising_native_probe_keeps_the_sql_route_refusal(
    spark: ReparkSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    base = spark.createDataFrame([(1, True), (2, None)], "i INT, b BOOLEAN")
    twins = base.select("i", "b", "b")
    filled = partial(_coalesce_bool, twins)
    routes, native, sql = _routes_and_answers(monkeypatch, filled)
    assert routes == [False]
    assert native == sql
    assert native[:2] == ("refused", "AnalysisException")
    assert "coalesce(Boolean, Int64)" in native[2]
