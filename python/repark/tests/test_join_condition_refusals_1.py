"""Join-condition refusals: nondeterministic and non-boolean conditions refuse like Spark.

The oracle is ``join_condition_refusals_1_spark_oracle.json`` (live Spark 4.1.2,
196 cells over 7 hows, 14 conditions, the DataFrame and SQL doors). Every
refusal pin compares the class, the condition, SQLSTATE and the full head line;
the head line carries Spark's sentence with RePark's condition rendering. The
engine prefixes the analyzer-rule header (``join_condition_refusals`` /
``caused by``), which defeats the condition parser the way the
``spark_expr_semantics`` header does elsewhere, so the pins strip the header
and read the condition off the head line. Controls replay the oracle's rows
and columns.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException
from repark.spark import functions as spark_functions

_NONDET_HEAD = (
    "[INVALID_NON_DETERMINISTIC_EXPRESSIONS] The operator expects a deterministic "
    "expression, but the actual expression is {condition}. SQLSTATE: 42K0E"
)

_NOT_BOOLEAN_HEAD = (
    '[JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE] The join condition "{condition}" has the '
    'invalid type "{spark_type}", expected "BOOLEAN". SQLSTATE: 42K0E'
)

_RULE_HEADER = "join_condition_refusals\ncaused by\n"

_ENGINE_PREFIX = "Error during planning: "

_HOWS = ["inner", "cross", "left", "right", "full", "left_semi", "left_anti"]

_SQL_HOWS = {
    "inner": "INNER JOIN",
    "left": "LEFT JOIN",
    "right": "RIGHT JOIN",
    "full": "FULL OUTER JOIN",
    "left_semi": "LEFT SEMI JOIN",
    "left_anti": "LEFT ANTI JOIN",
}

_ORACLE = json.loads(
    Path(__file__).with_name("join_condition_refusals_1_spark_oracle.json").read_text()
)["cells"]


@pytest.fixture
def spark() -> Any:
    session = ReparkSession.builder.appName("pytest-join-condition-refusals-1").getOrCreate()
    yield session
    session.stop()


def _frames(spark: ReparkSession) -> dict[str, Any]:
    left = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["id", "s"])
    right = spark.createDataFrame([(2, "x"), (3, "y"), (4, "z"), (9, "q")], ["k", "t"])
    left.createOrReplaceTempView("l")
    right.createOrReplaceTempView("r")
    return {"left": left, "right": right}


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted((tuple(row) for row in frame.collect()), key=repr)


def _head_of(error: BaseException) -> str:
    text = str(error)
    assert text.startswith(_RULE_HEADER + _ENGINE_PREFIX)
    return text[len(_RULE_HEADER + _ENGINE_PREFIX) :]


def _assert_refusal(build: Any, condition: str, head: str) -> None:
    with pytest.raises(AnalysisException) as caught:
        build()
    assert type(caught.value) is AnalysisException
    assert caught.value.getSqlState() == "42K0E"
    assert _head_of(caught.value) == head
    assert head.startswith(f"[{condition}]")


def _df_conditions(frames: dict[str, Any]) -> dict[str, Any]:
    left = frames["left"]
    right = frames["right"]
    return {
        "rand_lt_half": spark_functions.rand(7) < 0.5,
        "eq_and_rand": (left["id"] == right["k"]) & (spark_functions.rand(1) >= 0),
        "null_untyped": spark_functions.lit(None),
        "int_lit": spark_functions.lit(1),
        "str_lit": spark_functions.lit("true"),
        "bare_rand": spark_functions.rand(1),
    }


_SQL_CONDITIONS = {
    "rand_lt_half": "rand(7) < 0.5",
    "eq_and_rand": "l.id = r.k AND rand(1) >= 0",
    "null_untyped": "NULL",
    "int_lit": "1",
    "str_lit": "'true'",
    "bare_rand": "rand(1)",
}

_HEADS = {
    "rand_lt_half": (
        "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
        _NONDET_HEAD.format(condition='"(rand(7) < 0.5)"'),
    ),
    "eq_and_rand": (
        "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
        _NONDET_HEAD.format(condition='"((id = k) AND (rand(1) >= 0))"'),
    ),
    "null_untyped": (
        "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE",
        _NOT_BOOLEAN_HEAD.format(condition="NULL", spark_type="VOID"),
    ),
    "int_lit": (
        "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE",
        _NOT_BOOLEAN_HEAD.format(condition="1", spark_type="INT"),
    ),
    "str_lit": (
        "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE",
        _NOT_BOOLEAN_HEAD.format(condition="true", spark_type="STRING"),
    ),
    "bare_rand": (
        "JOIN_CONDITION_IS_NOT_BOOLEAN_TYPE",
        _NOT_BOOLEAN_HEAD.format(condition="rand(1)", spark_type="DOUBLE"),
    ),
}


@pytest.mark.parametrize("how", _HOWS)
@pytest.mark.parametrize("name", list(_HEADS))
def test_dataframe_door_refuses_per_how(spark: ReparkSession, how: str, name: str) -> None:
    frames = _frames(spark)
    condition_text, head = _HEADS[name]
    _assert_refusal(
        lambda: frames["left"].join(frames["right"], _df_conditions(frames)[name], how),
        condition_text,
        head,
    )


@pytest.mark.parametrize("how", list(_SQL_HOWS))
@pytest.mark.parametrize("name", list(_HEADS))
def test_sql_door_refuses_per_how(spark: ReparkSession, how: str, name: str) -> None:
    _frames(spark)
    condition_text, head = _HEADS[name]
    query = f"SELECT * FROM l {_SQL_HOWS[how]} r ON {_SQL_CONDITIONS[name]}"
    _assert_refusal(lambda: spark.sql(query), condition_text, head)


@pytest.mark.parametrize(
    ("build_name", "condition", "head_condition", "head_rendering"),
    [
        (
            "uuid",
            "uuid() is not null",
            "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
            "(uuid() IS NOT NULL)",
        ),
        (
            "shuffle",
            "size(shuffle(array(l.id, r.k))) = 2",
            "INVALID_NON_DETERMINISTIC_EXPRESSIONS",
            "(size(shuffle(array(id, k))) = 2)",
        ),
        ("randn", "randn(3) < 0", "INVALID_NON_DETERMINISTIC_EXPRESSIONS", "(randn(3) < 0)"),
    ],
)
def test_other_nondeterministic_functions_refuse_on_both_doors(
    spark: ReparkSession,
    build_name: str,
    condition: str,
    head_condition: str,
    head_rendering: str,
) -> None:
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    head = _NONDET_HEAD.format(condition=f'"{head_rendering}"')
    if build_name == "uuid":
        df_condition: Any = spark_functions.expr("uuid() is not null")
    elif build_name == "shuffle":
        shuffled = spark_functions.shuffle(spark_functions.array(left["id"], right["k"]))
        df_condition = spark_functions.size(shuffled) == 2
    else:
        df_condition = spark_functions.randn(3) < 0
    _assert_refusal(lambda: left.join(right, df_condition, "inner"), head_condition, head)
    _assert_refusal(
        lambda: spark.sql(f"SELECT * FROM l INNER JOIN r ON {condition}"),
        head_condition,
        head,
    )


@pytest.mark.parametrize("how", _HOWS)
@pytest.mark.parametrize("name", ["eq", "true_lit", "null_typed", "current_ts"])
def test_dataframe_controls_answer_the_recorded_rows(
    spark: ReparkSession, how: str, name: str
) -> None:
    frames = _frames(spark)
    left = frames["left"]
    right = frames["right"]
    if name == "eq":
        condition: Any = left["id"] == right["k"]
    elif name == "true_lit":
        condition = spark_functions.lit(True)
    elif name == "null_typed":
        condition = spark_functions.lit(None).cast("boolean")
    else:
        condition = (left["id"] == right["k"]) & (
            spark_functions.current_timestamp()
            > spark_functions.lit("2000-01-01").cast("timestamp")
        )
    joined = left.join(right, condition, how)
    recorded = _ORACLE[f"df/{how}/{name}"]
    assert recorded["outcome"] == "rows"
    assert joined.columns == recorded["columns"]
    assert [list(row) for row in _rows(joined)] == recorded["rows"]


@pytest.mark.parametrize("how", list(_SQL_HOWS))
@pytest.mark.parametrize("name", ["eq", "true_lit", "null_typed", "current_ts"])
def test_sql_controls_answer_the_recorded_rows(spark: ReparkSession, how: str, name: str) -> None:
    _frames(spark)
    conditions = {
        "eq": "l.id = r.k",
        "true_lit": "TRUE",
        "null_typed": "CAST(NULL AS BOOLEAN)",
        "current_ts": "l.id = r.k AND current_timestamp() > TIMESTAMP '2000-01-01 00:00:00'",
    }
    joined = spark.sql(f"SELECT * FROM l {_SQL_HOWS[how]} r ON {conditions[name]}")
    recorded = _ORACLE[f"sql/{how}/{name}"]
    assert recorded["outcome"] == "rows"
    assert joined.columns == recorded["columns"]
    assert [list(row) for row in _rows(joined)] == recorded["rows"]


def test_sql_cross_join_on_keeps_the_parse_refusal(spark: ReparkSession) -> None:
    _frames(spark)
    with pytest.raises(ParseException) as caught:
        spark.sql("SELECT * FROM l CROSS JOIN r ON l.id = r.k")
    assert type(caught.value) is ParseException
    assert (
        str(caught.value)
        == 'SQL error: ParserError("Expected: end of statement, found: ON at Line: 1, Column: 78")'
    )
