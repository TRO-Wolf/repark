"""SEM-3 — ``regexp_extract_all`` accepts a string ``idx``, as every other door already does.

Spark resolves a bare string ``regexp`` as a COLUMN NAME, but a string ``idx`` is a literal group
index. Every expected value is Spark's own answer from live PySpark 4.1.2, not read back out of
repark.
"""

from __future__ import annotations

import pytest

from repark.spark import functions as F  # noqa: N812 — PySpark idiom


def _session():
    from repark.spark import SparkSession

    return SparkSession.builder.appName("sem3-string-idx").getOrCreate()


def _frame():
    return _session().createDataFrame([("a1b2", "([a-z])([0-9])")], "s string, p string")


def _extract(regexp: object, idx: object | None = None) -> list[str]:
    arguments = ("s", regexp) if idx is None else ("s", regexp, idx)
    return (
        _frame()
        .select(F.regexp_extract_all(*arguments).alias("r"))
        .toArrow()
        .column("r")
        .to_pylist()[0]
    )


PAIRS = "([a-z])([0-9])"


@pytest.mark.parametrize(
    ("idx", "expected"),
    [("0", ["a1", "b2"]), ("1", ["a", "b"]), ("2", ["1", "2"])],
)
def test_a_string_idx_is_a_literal_group_index(idx: str, expected: list[str]) -> None:
    """Spark's answers; the string form must not raise ``No field named "<idx>"``."""
    assert _extract(F.lit(PAIRS), idx) == expected


@pytest.mark.parametrize(
    ("idx", "expected"),
    [(0, ["a1", "b2"]), (1, ["a", "b"]), (2, ["1", "2"])],
)
def test_an_int_idx_still_means_the_same_thing(idx: int, expected: list[str]) -> None:
    """The integer form keeps its meaning."""
    assert _extract(F.lit(PAIRS), idx) == expected


def test_a_column_idx_is_still_a_column_not_a_literal() -> None:
    """``lit_indices`` must stay empty for a Column ``idx``, or a genuine column reference is
    stringified into a literal.
    """
    assert _extract(F.lit(PAIRS), F.lit(2)) == ["1", "2"]


def test_a_bare_string_regexp_is_still_a_column_name() -> None:
    """``p`` is a column holding the pattern: Spark resolves the bare string as a column
    reference, not as a pattern literal (live oracle: ``['a', 'b']``).
    """
    assert _extract("p") == ["a", "b"]
