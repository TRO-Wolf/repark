from __future__ import annotations

from collections.abc import Callable, Iterator
from functools import partial
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import PySparkTypeError
from repark.spark import functions as spark_functions
from repark.spark.dataframe import join_attr_tokens


@pytest.fixture
def spark() -> Iterator[ReparkSession]:
    session = ReparkSession.builder.appName("pytest-cross-join-condition-1").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def route_hits() -> dict[str, int]:
    return {"native": 0, "sql": 0}


@pytest.fixture(params=["native", "sql"])
def route(
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
    route_hits: dict[str, int],
) -> str:
    original = join_attr_tokens._join_exact_plan
    counting = partial(_counting_exact_plan, route_hits, original, request.param == "sql")
    monkeypatch.setattr(join_attr_tokens, "_join_exact_plan", counting)
    return str(request.param)


def _frames(spark: ReparkSession) -> dict[str, Any]:
    left = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "s"])
    right = spark.createDataFrame([(2, "x"), (3, "y"), (4, "z"), (9, "q")], ["k", "t"])
    shared = spark.createDataFrame([(2, "x"), (3, "y"), (4, "z"), (9, "q")], ["id", "t"])
    return {"left": left, "right": right, "shared": shared}


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(tuple(row) for row in frame.collect())


def _counting_exact_plan(
    hits: dict[str, int], original: Callable[..., Any], force_sql: bool, *args: Any
) -> Any:
    if force_sql:
        hits["sql"] += 1
        return None
    planned = original(*args)
    hits["sql" if planned is None else "native"] += 1
    return planned


def _assert_route_taken(route: str, hits: dict[str, int], joins: int) -> None:
    if route == "native":
        assert hits == {"native": joins, "sql": 0}
    else:
        assert hits == {"native": 0, "sql": joins}


def _outcome(build: Callable[[], Any]) -> tuple[Any, ...]:
    try:
        frame = build()
        return ("rows", frame.columns, _rows(frame))
    except Exception as error:
        return ("refused", type(error).__name__, str(error))


def test_cross_join_with_an_equality_condition_answers_the_inner_rows(
    spark: ReparkSession, route: str, route_hits: dict[str, int]
) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    joined = left.join(right, left["id"] == right["k"], "cross")
    assert joined.columns == ["id", "s", "k", "t"]
    assert _rows(joined) == [(2, "b", 2, "x"), (3, "c", 3, "y")]
    inner = left.join(right, left["id"] == right["k"], "inner")
    assert _rows(joined) == _rows(inner)
    assert joined.columns == inner.columns
    _assert_route_taken(route, route_hits, 2)


@pytest.mark.parametrize("route", ["sql"], indirect=True)
def test_cross_join_with_an_inequality_condition_answers_nine_rows(
    spark: ReparkSession, route: str
) -> None:
    assert route == "sql"
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    joined = left.join(right, left["id"] < right["k"], "cross")
    assert _rows(joined) == [
        (1, "a", 2, "x"),
        (1, "a", 3, "y"),
        (1, "a", 4, "z"),
        (1, "a", 9, "q"),
        (2, "b", 3, "y"),
        (2, "b", 4, "z"),
        (2, "b", 9, "q"),
        (3, "c", 4, "z"),
        (3, "c", 9, "q"),
    ]


@pytest.mark.parametrize("route", ["sql"], indirect=True)
def test_cross_join_with_a_false_condition_answers_no_rows(
    spark: ReparkSession, route: str
) -> None:
    assert route == "sql"
    frames = _frames(spark)
    joined = frames["left"].join(frames["right"], spark_functions.lit(False), "cross")
    assert joined.columns == ["id", "s", "k", "t"]
    assert _rows(joined) == []


def test_cross_join_with_an_aliased_condition_answers_the_inner_rows(
    spark: ReparkSession, route: str, route_hits: dict[str, int]
) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    left = frames["left"].alias("l")
    right = frames["right"].alias("r")
    joined = left.join(
        right, spark_functions.col("l.id") == spark_functions.col("r.k"), "cross"
    ).select("l.id", "r.t")
    assert _rows(joined) == [(2, "x"), (3, "y")]
    _assert_route_taken(route, route_hits, 1)


def test_cross_join_with_a_condition_then_a_filter_answers_one_row(
    spark: ReparkSession, route: str, route_hits: dict[str, int]
) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    joined = left.join(right, left["id"] == right["k"], "cross").where(left["id"] > 2)
    assert _rows(joined) == [(3, "c", 3, "y")]
    _assert_route_taken(route, route_hits, 1)


def test_join_none_cross_and_cross_join_stay_cartesian(
    spark: ReparkSession, route: str, route_hits: dict[str, int]
) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    by_none = left.join(right, None, "cross")
    by_call = left.crossJoin(right)
    assert len(_rows(by_none)) == 12
    assert len(_rows(by_call)) == 12
    assert by_none.columns == ["id", "s", "k", "t"]
    assert by_call.columns == ["id", "s", "k", "t"]
    assert _rows(by_none) == _rows(by_call)
    _assert_route_taken(route, route_hits, 2)


def test_cross_join_with_shared_names_stays_refused(spark: ReparkSession, route: str) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    left = frames["left"]
    shared = frames["shared"]
    with pytest.raises(ValueError, match='unsupported join type "cross"'):
        left.join(shared, "id", "cross")
    with pytest.raises(ValueError, match='unsupported join type "cross"'):
        left.join(shared, ["id"], "cross")


def test_cross_join_with_a_column_list_stays_refused(spark: ReparkSession, route: str) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    with pytest.raises(PySparkTypeError, match="expects a column name"):
        left.join(right, [left["id"] == right["k"], left["s"] < right["t"]], "cross")
    with pytest.raises(PySparkTypeError, match="expects a column name"):
        left.join(right, [left["id"] == right["k"], left["s"] < right["t"]], "inner")


def test_cross_join_of_a_frame_with_itself_answers_as_inner(
    spark: ReparkSession, route: str, route_hits: dict[str, int]
) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    left = frames["left"]
    cross = _outcome(lambda: left.join(left, left["id"] == left["id"], "cross"))
    inner = _outcome(lambda: left.join(left, left["id"] == left["id"], "inner"))
    assert cross == inner
    assert cross[0] == "rows"
    assert cross[2] == [(1, "a", 1, "a"), (2, "b", 2, "b"), (3, "c", 3, "c")]
    _assert_route_taken(route, route_hits, 2)


def test_cross_join_of_two_derivations_answers_as_inner(
    spark: ReparkSession, route: str, route_hits: dict[str, int]
) -> None:
    assert route in ("native", "sql")
    frames = _frames(spark)
    base = frames["left"]
    grown = base.filter(base["id"] > 1)
    shrunk = base.filter(base["id"] < 3)
    cross = _outcome(lambda: grown.join(shrunk, grown["id"] == shrunk["id"], "cross"))
    inner = _outcome(lambda: grown.join(shrunk, grown["id"] == shrunk["id"], "inner"))
    assert cross == inner
    assert cross[0] == "rows"
    assert cross[2] == [(2, "b", 2, "b")]
    _assert_route_taken(route, route_hits, 2)


def test_cross_join_with_a_nondeterministic_condition_refuses_as_inner(
    spark: ReparkSession,
) -> None:
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    cross = _outcome(
        lambda: left.join(
            right, (left["id"] == right["k"]) & (spark_functions.rand(1) >= 0), "cross"
        )
    )
    inner = _outcome(
        lambda: left.join(
            right, (left["id"] == right["k"]) & (spark_functions.rand(1) >= 0), "inner"
        )
    )
    assert cross == inner
    assert cross[0] == "refused"
    assert cross[1] == "AnalysisException"
    assert "[INVALID_NON_DETERMINISTIC_EXPRESSIONS]" in cross[2]


def test_cross_join_with_an_untyped_null_condition_refuses_as_inner(
    spark: ReparkSession,
) -> None:
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    cross = _outcome(lambda: left.join(right, spark_functions.lit(None), "cross"))
    inner = _outcome(lambda: left.join(right, spark_functions.lit(None), "inner"))
    assert cross == inner
    assert cross[0] == "refused"
    assert cross[1] == "AnalysisException"
    assert "[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE]" in cross[2]
