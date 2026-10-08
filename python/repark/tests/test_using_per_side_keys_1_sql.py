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
        (None, None, "z", 4),
    ]
    session.stop()


def test_sql_using_declared_divergences(tmp_path: Path) -> None:
    session = sm2._open(tmp_path, "upsk-sql-declared")
    _views(session)
    full = "tl l FULL JOIN tr r USING (id)"
    mixed = _rows(session.sql(f"SELECT * FROM {full} JOIN tq q ON r.id = q.id"))
    assert mixed == [(2, "b", "x", 2, "p"), (3, "c", "y", 3, "q"), (None, None, "z", 4, "r")]
    natural = _rows(session.sql("SELECT * FROM tl l NATURAL FULL JOIN tr r"))
    assert natural == [(1, "a", None), (2, "b", "x"), (3, "c", "y"), (None, None, "z")]
    refused = sm2._refusal_of(lambda: session.sql(f"SELECT id, l.id, r.id FROM {full}").collect())
    assert isinstance(refused, AnalysisException)
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
