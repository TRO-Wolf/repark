"""R-CS2P-1: DataFrame-alias qualifiers keep their case and bind under both case rules.

``DataFrame.alias`` builds its ``SubqueryAlias`` with the alias's exact spelling.
Under ``spark.sql.caseSensitive=true`` a qualifier matches only that spelling, so a
wrong-case qualifier in a filter string or a ``Column`` refuses
``UNRESOLVED_COLUMN.WITH_SUGGESTION`` naming the written reference, and a
right-case qualifier over a struct path answers. Under ``false`` every spelling of
the qualifier, unquoted or backticked, binds as Spark folds it.

Every expectation below is live Spark 4.1.2 (local[1], measured 2026-10-03 by
``cs2p1/probe.py``); the six R-CS2P-1 cells are ``rc2_1``, ``rc3_1``,
``j1_col_l``, ``j1_fstrl``, ``pred_join_j8`` and ``selfjoin_filter_S1``, each
pinned under both rules beside right-case and backticked controls.

pins: casesens-2/C-013
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions

Build = Callable[[ReparkSession], Any]


def _base(session: ReparkSession) -> Any:
    return session.createDataFrame([(1, "a", 10), (2, "b", 20)], ["id", "Data", "Val"])


def _nested(session: ReparkSession) -> Any:
    return session.createDataFrame(
        [
            (1, "a\\b", 10, True, (1, "g1"), [1, 5], "T.id"),
            (2, "b", 20, False, (2, "g2"), [2], "x%"),
            (3, "c", 30, True, (3, "g3"), [7, 8], "it's"),
        ],
        "id INT, Data STRING, Val INT, flag BOOLEAN, s STRUCT<f: INT, G: STRING>, "
        "arr ARRAY<INT>, note STRING",
    )


def _j1(session: ReparkSession) -> Any:
    left = session.createDataFrame([(1, "a", 10)], ["id", "Data", "Val"]).union(
        session.createDataFrame([(2, "b", 20)], ["id", "Data", "Val"])
    )
    right = session.createDataFrame([(1, "x", 7)], ["ID", "Data", "W"]).union(
        session.createDataFrame([(3, "y", 8)], ["ID", "Data", "W"])
    )
    return left.alias("L").join(right.alias("R"), functions.col("L.id") == functions.col("R.ID"))


def _pred_join(session: ReparkSession) -> Any:
    right = session.createDataFrame([(1, "x"), (2, "y"), (3, "z")], ["id", "W"])
    return (
        _nested(session)
        .alias("L")
        .join(right.alias("R"), functions.col("L.id") == functions.col("R.id"))
    )


def _selfjoin(session: ReparkSession) -> Any:
    frame = session.createDataFrame(
        [(1, 10, "a"), (2, 20, "b"), (3, 30, "c"), (4, None, "d")], ["id", "v", "Data"]
    )
    return frame.alias("s1").join(
        frame.alias("s2"), functions.col("s1.id") == functions.col("s2.id")
    )


_BASE_ROW = (["id", "Data", "Val"], [[2, "b", 20]])
_J1_ROW = (["id", "Data", "Val", "ID", "Data", "W"], [[1, "a", 10, 1, "x", 7]])

CELLS: dict[str, Build] = {
    "rc2_1": lambda s: _base(s).alias("Tb").where("tb.id > 1"),
    "rc2_1_right_case": lambda s: _base(s).alias("Tb").where("Tb.id > 1"),
    "rc2_1_upper": lambda s: _base(s).alias("Tb").where("TB.id > 1"),
    "rc2_1_backtick_right": lambda s: _base(s).alias("Tb").where("`Tb`.id > 1"),
    "rc2_1_backtick_wrong": lambda s: _base(s).alias("Tb").where("`tb`.`id` > 1"),
    "rc3_1": lambda s: _nested(s).alias("T").filter("T.s.f > 1").select("id"),
    "rc3_1_wrong_case": lambda s: _nested(s).alias("T").filter("t.s.f > 1").select("id"),
    "j1_col_l": lambda s: _j1(s).select(functions.col("l.id")),
    "j1_col_L": lambda s: _j1(s).select(functions.col("L.id")),
    "j1_fstrl": lambda s: _j1(s).filter("l.id > 0"),
    "j1_fstr": lambda s: _j1(s).filter("L.id > 0"),
    "pred_join_j8": lambda s: _pred_join(s).filter("r.W = 'y' -- R.id").select("W"),
    "pred_join_j3": lambda s: _pred_join(s).filter("r.w = 'y'").select("W"),
    "pred_join_j7": lambda s: _pred_join(s).filter("L.s.f = 2").select("W"),
    "selfjoin_filter_S1": lambda s: _selfjoin(s).filter("S1.v > 15").select("s2.id"),
    "selfjoin_filter_s1": lambda s: _selfjoin(s).filter("s1.v > 15").select("s2.id"),
}

SPARK_TRUE: dict[str, Any] = {
    "rc2_1": "`tb`.`id`",
    "rc2_1_right_case": _BASE_ROW,
    "rc2_1_upper": "`TB`.`id`",
    "rc2_1_backtick_right": _BASE_ROW,
    "rc2_1_backtick_wrong": "`tb`.`id`",
    "rc3_1": (["id"], [[2], [3]]),
    "rc3_1_wrong_case": "`t`.`s`.`f`",
    "j1_col_l": "`l`.`id`",
    "j1_col_L": (["id"], [[1]]),
    "j1_fstrl": "`l`.`id`",
    "j1_fstr": _J1_ROW,
    "pred_join_j8": "`r`.`W`",
    "pred_join_j3": "`r`.`w`",
    "pred_join_j7": (["W"], [["y"]]),
    "selfjoin_filter_S1": "`S1`.`v`",
    "selfjoin_filter_s1": (["id"], [[2], [3]]),
}

SPARK_FALSE: dict[str, Any] = {
    "rc2_1": _BASE_ROW,
    "rc2_1_right_case": _BASE_ROW,
    "rc2_1_upper": _BASE_ROW,
    "rc2_1_backtick_right": _BASE_ROW,
    "rc2_1_backtick_wrong": _BASE_ROW,
    "rc3_1": (["id"], [[2], [3]]),
    "rc3_1_wrong_case": (["id"], [[2], [3]]),
    "j1_col_l": (["id"], [[1]]),
    "j1_col_L": (["id"], [[1]]),
    "j1_fstrl": _J1_ROW,
    "j1_fstr": _J1_ROW,
    "pred_join_j8": (["W"], [["y"]]),
    "pred_join_j3": (["W"], [["y"]]),
    "pred_join_j7": (["W"], [["y"]]),
    "selfjoin_filter_S1": (["id"], [[2], [3]]),
    "selfjoin_filter_s1": (["id"], [[2], [3]]),
}


def _rows(frame: Any) -> list[list[Any]]:
    return sorted((list(row) for row in frame.collect()), key=repr)


def _check(flag: str, key: str, expected: Any) -> None:
    session = ReparkSession.builder.appName("cs2p1").getOrCreate()
    session.conf.set("spark.sql.caseSensitive", flag)
    try:
        if isinstance(expected, str):
            with pytest.raises(AnalysisException) as caught:
                CELLS[key](session).collect()
            message = str(caught.value)
            assert "[UNRESOLVED_COLUMN.WITH_SUGGESTION]" in message, message
            assert f"with name {expected} cannot be resolved" in message, message
            return
        columns, rows = expected
        frame = CELLS[key](session)
        assert list(frame.columns) == columns
        assert _rows(frame) == rows
    finally:
        session.conf.set("spark.sql.caseSensitive", "false")


@pytest.mark.parametrize("key", sorted(SPARK_TRUE))
def test_true_qualifiers_match_the_alias_spelling(key: str) -> None:
    """Under caseSensitive=true each cell answers or refuses as live Spark did."""
    _check("true", key, SPARK_TRUE[key])


@pytest.mark.parametrize("key", sorted(SPARK_FALSE))
def test_false_qualifiers_fold_as_spark(key: str) -> None:
    """Under caseSensitive=false every qualifier spelling binds as live Spark did."""
    _check("false", key, SPARK_FALSE[key])
