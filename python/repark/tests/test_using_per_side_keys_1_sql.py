from __future__ import annotations

from pathlib import Path
from typing import Any

import _sm2_shared as sm2
import pytest

from repark.errors import AnalysisException

_HOWS = {
    "inner": "INNER",
    "left": "LEFT",
    "right": "RIGHT",
    "full": "FULL",
    "left_semi": "LEFT SEMI",
    "left_anti": "LEFT ANTI",
}
_STAR: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(2, "b", "x"), (3, "c", "y")],
    "left": [(1, "a", None), (2, "b", "x"), (3, "c", "y")],
    "right": [(2, "b", "x"), (3, "c", "y"), (4, None, "z")],
    "full": [(1, "a", None), (2, "b", "x"), (3, "c", "y"), (4, None, "z")],
    "left_semi": [(2, "b"), (3, "c")],
    "left_anti": [(1, "a")],
}
_OVER_TWO: dict[str, list[tuple[Any, ...]]] = {
    "inner": [(3,)],
    "left": [(3,)],
    "right": [(3,), (4,)],
    "full": [(3,), (4,)],
    "left_semi": [(3,)],
    "left_anti": [],
}
_STAR_KEYS: dict[str, list[tuple[Any, ...]]] = {
    how: [(row[0],) for row in rows] for how, rows in _STAR.items()
}
_BY_LEFT = {"right": [4, 2, 3], "full": [4, 1, 2, 3]}
_BY_RIGHT = {"right": [2, 3, 4], "full": [1, 2, 3, 4]}
_SOURCES = ["tl l {how} JOIN tr r USING (id)", "tl {how} JOIN tr USING (id)"]


def _views(session: Any) -> None:
    left = session.createDataFrame([(1, "a"), (2, "b"), (3, "c")], "id INT, s STRING")
    right = session.createDataFrame([(2, "x"), (3, "y"), (4, "z")], "id INT, t STRING")
    other = session.createDataFrame([(2, "p"), (3, "q"), (4, "r")], "id INT, u STRING")
    left.createOrReplaceTempView("tl")
    right.createOrReplaceTempView("tr")
    other.createOrReplaceTempView("tq")


def _rows(frame: Any) -> list[tuple[Any, ...]]:
    return sorted(
        (tuple(row) for row in frame.collect()),
        key=lambda row: tuple(str(value) for value in row),
    )


@pytest.mark.parametrize("how", list(_HOWS))
@pytest.mark.parametrize("source", _SOURCES)
def test_sql_using_star_and_unqualified_key(tmp_path: Path, how: str, source: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-{how}")
    _views(session)
    joined = source.format(how=_HOWS[how])
    keys = [(row[0],) for row in _STAR[how]]
    assert _rows(session.sql(f"SELECT * FROM {joined}")) == _STAR[how]
    assert _rows(session.sql(f"SELECT id FROM {joined}")) == keys
    assert session.sql(f"SELECT id FROM {joined}").columns == ["id"]
    assert _rows(session.sql(f"SELECT id FROM {joined} WHERE id > 2")) == _OVER_TWO[how]
    assert [row[0] for row in session.sql(f"SELECT * FROM {joined} ORDER BY id").collect()] == [
        key for (key,) in keys
    ]
    assert _rows(session.sql(f"SELECT id, count(*) FROM {joined} GROUP BY id")) == [
        (key, 1) for (key,) in keys
    ]
    assert _rows(session.sql(f"SELECT id + 1 FROM {joined}")) == [(key + 1,) for (key,) in keys]
    session.stop()


@pytest.mark.parametrize("how", ["right", "full"])
def test_sql_using_per_side_keys_keep_their_values(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-sides-{how}")
    _views(session)
    joined = f"tl l {_HOWS[how]} JOIN tr r USING (id)"
    by_left = session.sql(f"SELECT * FROM {joined} ORDER BY l.id")
    assert by_left.columns == ["id", "s", "t"]
    assert [row[0] for row in by_left.collect()] == _BY_LEFT[how]
    by_right = session.sql(f"SELECT * FROM {joined} ORDER BY r.id")
    assert [row[0] for row in by_right.collect()] == _BY_RIGHT[how]
    assert _rows(session.sql(f"SELECT * FROM {joined} WHERE r.id > 2")) == [
        (3, "c", "y"),
        (4, None, "z"),
    ]
    assert _rows(session.sql(f"SELECT * FROM {joined} WHERE l.id > 2")) == [(3, "c", "y")]
    pairs = {"right": [(2, 2), (3, 3), (None, 4)], "full": [(1, None), (2, 2), (3, 3), (None, 4)]}
    assert _rows(session.sql(f"SELECT l.id, r.id FROM {joined}")) == pairs[how]
    session.stop()


@pytest.mark.parametrize("how", ["right", "full"])
def test_sql_chained_using_matches_on_the_merged_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-chain-{how}")
    _views(session)
    first = f"tl l {_HOWS[how]} JOIN tr r USING (id)"
    expected = [(2, "b", "x", "p"), (3, "c", "y", "q"), (4, None, "z", "r")]
    inner = _rows(session.sql(f"SELECT * FROM {first} JOIN tq q USING (id)"))
    assert inner == expected
    if how == "full":
        expected = [(1, "a", None, None), *expected]
    chained = session.sql(f"SELECT * FROM {first} FULL JOIN tq q USING (id)")
    assert chained.columns == ["id", "s", "t", "u"]
    assert _rows(chained) == expected
    assert _rows(session.sql(f"SELECT id FROM {first} FULL JOIN tq q USING (id)")) == [
        (row[0],) for row in expected
    ]
    session.stop()


def test_sql_using_scopes_and_shadows(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "upsk-sql-scopes")
    _views(session)
    full = "tl l FULL JOIN tr r USING (id)"
    shadow = session.sql(f"SELECT s AS id FROM {full} ORDER BY id").collect()
    assert [row[0] for row in shadow] == [None, "a", "b", "c"]
    assert _rows(session.sql(f"SELECT x.id FROM (SELECT * FROM {full}) x")) == [
        (1,),
        (2,),
        (3,),
        (4,),
    ]
    assert _rows(session.sql(f"SELECT *, r.id FROM {full}")) == [
        (1, "a", None, None),
        (2, "b", "x", 2),
        (3, "c", "y", 3),
        (4, None, "z", 4),
    ]
    session.stop()


def test_sql_shapes_the_plan_pass_reaches(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "upsk-sql-shapes")
    _views(session)
    full = "tl l FULL JOIN tr r USING (id)"
    mixed = _rows(session.sql(f"SELECT * FROM {full} JOIN tq q ON r.id = q.id"))
    assert mixed == [(2, "b", "x", 2, "p"), (3, "c", "y", 3, "q"), (4, None, "z", 4, "r")]
    natural = _rows(session.sql("SELECT * FROM tl l NATURAL FULL JOIN tr r"))
    assert natural == [(1, "a", None), (2, "b", "x"), (3, "c", "y"), (4, None, "z")]
    derived = _rows(session.sql("SELECT * FROM tl RIGHT JOIN (SELECT * FROM tr) USING (id)"))
    assert derived == [(2, "b", "x"), (3, "c", "y"), (4, None, "z")]
    session.stop()


def test_sql_using_declared_divergences(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "upsk-sql-declared")
    _views(session)
    session.createDataFrame(
        [(3, "m"), (4, "n"), (5, "o")], "id INT, w STRING"
    ).createOrReplaceTempView("tp")
    full = "tl l FULL JOIN tr r USING (id)"
    refused = sm2._refusal_of(lambda: session.sql(f"SELECT id, l.id, r.id FROM {full}").collect())
    assert isinstance(refused, AnalysisException)
    four = "SELECT * FROM tl JOIN tr USING (id) JOIN tq USING (id) JOIN tp USING (id)"
    assert sm2._condition_of(sm2._refusal_of(lambda: session.sql(four).collect())) == (
        "AMBIGUOUS_REFERENCE"
    )
    natural = session.sql("SELECT id FROM tl NATURAL FULL JOIN tr")
    assert _rows(natural) == [(1,), (2,), (3,), (4,)]
    session.stop()


@pytest.mark.parametrize("how", ["inner", "left", "right", "full"])
def test_sql_lambda_parameters_shadow_the_key(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-lambda-{how}")
    _views(session)
    joined = f"tl {_HOWS[how]} JOIN tr USING (id)"
    rows = session.sql(
        f"SELECT id, transform(array(10, 20), id -> id + 1) AS x FROM {joined}"
    ).collect()
    assert sorted(row[0] for row in rows) == [key for (key,) in sorted(_STAR_KEYS[how])]
    assert all(list(row[1]) == [11, 21] for row in rows)
    kept = session.sql(f"SELECT id FROM {joined} WHERE exists(array(1, 2), id -> id = 2)")
    assert _rows(kept) == sorted(_STAR_KEYS[how])
    filtered = session.sql(
        f"SELECT id, filter(array(1, 2, 3), id -> id > 1) AS f, "
        f"aggregate(array(1, 2), 0, (acc, id) -> acc + id) AS a, "
        f"transform(array(1), x -> transform(array(5), id -> id + x)) AS n FROM {joined} "
        f"WHERE id > 2"
    ).collect()
    assert sorted(row[0] for row in filtered) == [key for (key,) in _OVER_TWO[how]]
    assert all(list(row[1]) == [2, 3] and row[2] == 3 for row in filtered)
    assert all([list(inner) for inner in row[3]] == [[6]] for row in filtered)
    session.stop()


@pytest.mark.parametrize("how", ["right", "full"])
def test_sql_distinct_and_order_by_outside_the_select_list(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-distinct-{how}")
    _views(session)
    keys = [key for (key,) in sorted(_STAR_KEYS[how])]
    for joined in (
        f"tl {_HOWS[how]} JOIN tr USING (id)",
        f"tl l {_HOWS[how]} JOIN tr r USING (id)",
    ):
        distinct = session.sql(f"SELECT DISTINCT id FROM {joined} ORDER BY id").collect()
        assert [row[0] for row in distinct] == keys
        pairs = session.sql(f"SELECT DISTINCT id, s FROM {joined} ORDER BY id DESC").collect()
        assert [row[0] for row in pairs] == keys[::-1]
    aliased = f"tl l {_HOWS[how]} JOIN tr r USING (id)"
    by_left = session.sql(f"SELECT id FROM {aliased} ORDER BY l.id, t").collect()
    assert [row[0] for row in by_left] == _BY_LEFT[how]
    by_expr = session.sql(f"SELECT id FROM {aliased} ORDER BY l.id, upper(s)").collect()
    assert [row[0] for row in by_expr] == _BY_LEFT[how]
    session.stop()


@pytest.mark.parametrize("how", ["right", "full"])
def test_sql_shared_non_key_names_keep_spark_names(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-shared-{how}")
    session.createDataFrame([(1, 10), (2, 20)], "id INT, v INT").createOrReplaceTempView("mv1")
    session.createDataFrame([(1, 100), (2, 200)], "id INT, v INT").createOrReplaceTempView("mv2")
    joined = f"mv1 l {_HOWS[how]} JOIN mv2 r USING (id)"
    for text in (
        f"SELECT * FROM {joined} ORDER BY r.id",
        f"SELECT * FROM {joined} ORDER BY l.id",
        f"SELECT id, l.v, r.v FROM {joined} ORDER BY l.id",
    ):
        frame = session.sql(text)
        assert frame.columns == ["id", "v", "v"], text
        assert frame.schema.simpleString() == "struct<id:int,v:int,v:int>", text
        assert [tuple(row) for row in frame.collect()] == [(1, 10, 100), (2, 20, 200)], text
    session.stop()


@pytest.mark.parametrize("sensitive", ["false", "true"])
def test_sql_key_case_follows_the_session(tmp_path: Path, sensitive: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-case-{sensitive}")
    session.conf.set("spark.sql.caseSensitive", sensitive)
    _views(session)
    full = "tl l FULL JOIN tr r USING (ID)"
    texts = [
        f"SELECT * FROM {full}",
        f"SELECT * FROM {full} WHERE Id > 3",
        f"SELECT Id FROM {full} ORDER BY iD DESC",
    ]
    if sensitive == "true":
        for text in texts:
            refused = sm2._refusal_of(lambda text=text: session.sql(text).collect())
            assert isinstance(refused, AnalysisException), text
        exact = "tl l FULL JOIN tr r USING (id)"
        assert _rows(session.sql(f"SELECT * FROM {exact} WHERE id > 3")) == [(4, None, "z")]
    else:
        assert _rows(session.sql(texts[0])) == _STAR["full"]
        assert _rows(session.sql(texts[1])) == [(4, None, "z")]
        assert [row[0] for row in session.sql(texts[2]).collect()] == [4, 3, 2, 1]
    session.conf.set("spark.sql.caseSensitive", "false")
    session.stop()


def test_sql_explain_shows_the_plan_that_runs(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "upsk-sql-explain")
    _views(session)
    full = "tl l FULL JOIN tr r USING (id)"
    star = "\n".join(str(row[1]) for row in session.sql(f"EXPLAIN SELECT * FROM {full}").collect())
    assert "ELSE r.id" in star
    where = session.sql(f"EXPLAIN SELECT id FROM {full} WHERE id > 2").collect()
    assert "ELSE r.id" in "\n".join(str(row[1]) for row in where)
    inner = session.sql("EXPLAIN SELECT * FROM tl l JOIN tr r ON l.id = r.id").collect()
    assert "ELSE r.id" not in "\n".join(str(row[1]) for row in inner)
    session.stop()


def test_sql_helper_names_do_not_collide(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "upsk-sql-helper")
    _views(session)
    for helper in ("__repark_using_k0", "__repark_using_o0"):
        frame = session.sql(
            f"SELECT * FROM (SELECT id, s AS {helper} FROM tl) l FULL JOIN tr r USING (id) "
            "ORDER BY l.id"
        )
        assert frame.columns == ["id", helper, "t"]
        assert [row[0] for row in frame.collect()] == [4, 1, 2, 3]
    aliased = session.sql(
        "SELECT * FROM tl __repark_using_w FULL JOIN tr r USING (id) ORDER BY __repark_using_w.id"
    )
    assert [row[0] for row in aliased.collect()] == [4, 1, 2, 3]
    session.stop()


@pytest.mark.parametrize("how", ["left", "right", "full"])
def test_sql_on_join_is_unchanged(tmp_path: Path, how: str) -> None:
    session = sm2._open(tmp_path, f"upsk-sql-on-{how}")
    _views(session)
    frame = session.sql(f"SELECT * FROM tl l {_HOWS[how]} JOIN tr r ON l.id = r.id")
    expected = {
        "left": [(1, "a", None, None), (2, "b", 2, "x"), (3, "c", 3, "y")],
        "right": [(2, "b", 2, "x"), (3, "c", 3, "y"), (None, None, 4, "z")],
        "full": [
            (1, "a", None, None),
            (2, "b", 2, "x"),
            (3, "c", 3, "y"),
            (None, None, 4, "z"),
        ],
    }
    assert frame.columns == ["id", "s", "id", "t"]
    assert _rows(frame) == expected[how]
    session.stop()
